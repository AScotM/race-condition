package main

import (
	"context"
	"encoding/csv"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"math"
	"math/rand"
	"os"
	"os/signal"
	"runtime"
	"sort"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"syscall"
	"time"
)

const version = "2.0.0"

type Config struct {
	Modes      string
	Workers    int
	Iterations int
	Trials     int
	Warmups    int
	Shards     int
	Buffer     int
	GOMAXPROCS int
	YieldEvery int
	Delay      time.Duration
	Timeout    time.Duration
	Seed       int64
	Format     string
	Output     string
	Unsafe     bool
	Verify     bool
	Quiet      bool
	Version    bool
}

type Result struct {
	Mode             string  `json:"mode"`
	Trial            int     `json:"trial"`
	Workers          int     `json:"workers"`
	Iterations       int     `json:"iterations"`
	Expected         int64   `json:"expected"`
	Observed         int64   `json:"observed"`
	Lost             int64   `json:"lost"`
	Correct          bool    `json:"correct"`
	DurationNS       int64   `json:"duration_ns"`
	Throughput       float64 `json:"operations_per_second"`
	NanosecondsPerOp float64 `json:"nanoseconds_per_operation"`
	TimedOut         bool    `json:"timed_out"`
	Error            string  `json:"error,omitempty"`
}

type Summary struct {
	Mode           string  `json:"mode"`
	Trials         int     `json:"trials"`
	Correct        int     `json:"correct"`
	Incorrect      int     `json:"incorrect"`
	Errors         int     `json:"errors"`
	MeanNS         float64 `json:"mean_duration_ns"`
	MedianNS       float64 `json:"median_duration_ns"`
	MinNS          int64   `json:"min_duration_ns"`
	MaxNS          int64   `json:"max_duration_ns"`
	P95NS          float64 `json:"p95_duration_ns"`
	StddevNS       float64 `json:"stddev_duration_ns"`
	MeanThroughput float64 `json:"mean_operations_per_second"`
	TotalLost      int64   `json:"total_lost_updates"`
}

type Report struct {
	Version    string     `json:"version"`
	Timestamp  string     `json:"timestamp"`
	GoVersion  string     `json:"go_version"`
	OS         string     `json:"os"`
	Arch       string     `json:"arch"`
	CPUs       int        `json:"cpus"`
	GOMAXPROCS int        `json:"gomaxprocs"`
	Config     ConfigView `json:"config"`
	Results    []Result   `json:"results"`
	Summaries  []Summary  `json:"summaries"`
	Warnings   []string   `json:"warnings,omitempty"`
}

type ConfigView struct {
	Modes      []string `json:"modes"`
	Workers    int      `json:"workers"`
	Iterations int      `json:"iterations"`
	Trials     int      `json:"trials"`
	Warmups    int      `json:"warmups"`
	Shards     int      `json:"shards"`
	Buffer     int      `json:"channel_buffer"`
	YieldEvery int      `json:"yield_every"`
	Delay      string   `json:"delay"`
	Timeout    string   `json:"timeout"`
	Seed       int64    `json:"seed"`
	Unsafe     bool     `json:"unsafe_enabled"`
}

var knownModes = []string{"unsafe", "mutex", "rwmutex", "atomic", "channel", "sharded", "local", "cas"}

func parseConfig() (Config, error) {
	var c Config
	flag.StringVar(&c.Modes, "modes", "safe", "Comma-separated modes: unsafe,mutex,rwmutex,atomic,channel,sharded,local,cas, safe, all")
	flag.IntVar(&c.Workers, "workers", 10, "Number of goroutines")
	flag.IntVar(&c.Iterations, "iterations", 1000, "Increments per goroutine")
	flag.IntVar(&c.Trials, "trials", 5, "Measured trials per mode")
	flag.IntVar(&c.Warmups, "warmups", 1, "Warmup trials per mode")
	flag.IntVar(&c.Shards, "shards", 16, "Number of sharded counters")
	flag.IntVar(&c.Buffer, "buffer", 1024, "Channel buffer capacity")
	flag.IntVar(&c.GOMAXPROCS, "gomaxprocs", 0, "Runtime parallelism; 0 preserves default")
	flag.IntVar(&c.YieldEvery, "yield-every", 0, "Call Gosched every N increments; 0 disables")
	flag.DurationVar(&c.Delay, "delay", 0, "Optional delay after each increment")
	flag.DurationVar(&c.Timeout, "timeout", 0, "Timeout per trial; 0 disables")
	flag.Int64Var(&c.Seed, "seed", 42, "Seed used to shuffle mode execution order")
	flag.StringVar(&c.Format, "format", "human", "Output: human, json, jsonl, csv")
	flag.StringVar(&c.Output, "output", "", "Write output to file instead of stdout")
	flag.BoolVar(&c.Unsafe, "allow-unsafe", false, "Enable intentionally racy mode")
	flag.BoolVar(&c.Verify, "verify", false, "Exit nonzero if any safe trial fails")
	flag.BoolVar(&c.Quiet, "quiet", false, "Suppress per-trial lines in human format")
	flag.BoolVar(&c.Version, "version", false, "Print version")
	flag.Parse()
	if c.Version {
		return c, nil
	}
	if flag.NArg() != 0 {
		return c, errors.New("unexpected positional arguments")
	}
	if c.Workers < 1 || c.Iterations < 1 || c.Trials < 1 || c.Warmups < 0 || c.Shards < 1 || c.Buffer < 0 || c.GOMAXPROCS < 0 || c.YieldEvery < 0 || c.Delay < 0 || c.Timeout < 0 {
		return c, errors.New("invalid negative or zero configuration value")
	}
	if int64(c.Workers) > math.MaxInt64/int64(c.Iterations) {
		return c, errors.New("expected count exceeds int64")
	}
	switch c.Format {
	case "human", "json", "jsonl", "csv":
	default:
		return c, fmt.Errorf("unknown format %q", c.Format)
	}
	return c, nil
}

func selectModes(c Config) ([]string, error) {
	seen := map[string]bool{}
	var modes []string
	add := func(m string) {
		if !seen[m] {
			seen[m] = true
			modes = append(modes, m)
		}
	}
	for _, token := range strings.Split(c.Modes, ",") {
		m := strings.ToLower(strings.TrimSpace(token))
		switch m {
		case "safe":
			for _, x := range knownModes {
				if x != "unsafe" {
					add(x)
				}
			}
		case "all":
			for _, x := range knownModes {
				add(x)
			}
		default:
			valid := false
			for _, x := range knownModes {
				if x == m {
					valid = true
					break
				}
			}
			if !valid {
				return nil, fmt.Errorf("unknown mode %q", m)
			}
			add(m)
		}
	}
	if len(modes) == 0 {
		return nil, errors.New("no modes selected")
	}
	if seen["unsafe"] && !c.Unsafe {
		return nil, errors.New("unsafe mode requires --allow-unsafe; run with go run -race to observe race reports")
	}
	return modes, nil
}

func step(c Config, i int) {
	if c.YieldEvery > 0 && (i+1)%c.YieldEvery == 0 {
		runtime.Gosched()
	}
	if c.Delay > 0 {
		time.Sleep(c.Delay)
	}
}

func parallel(ctx context.Context, c Config, work func(worker, iteration int)) error {
	var wg sync.WaitGroup
	wg.Add(c.Workers)
	for w := 0; w < c.Workers; w++ {
		go func(id int) {
			defer wg.Done()
			for i := 0; i < c.Iterations; i++ {
				if i&255 == 0 {
					select {
					case <-ctx.Done():
						return
					default:
					}
				}
				work(id, i)
				step(c, i)
			}
		}(w)
	}
	wg.Wait()
	return ctx.Err()
}

func execute(ctx context.Context, c Config, mode string) (int64, error) {
	switch mode {
	case "unsafe":
		var counter int64
		err := parallel(ctx, c, func(_, _ int) { counter++ })
		return counter, err
	case "mutex":
		var mu sync.Mutex
		var counter int64
		err := parallel(ctx, c, func(_, _ int) { mu.Lock(); counter++; mu.Unlock() })
		return counter, err
	case "rwmutex":
		var mu sync.RWMutex
		var counter int64
		err := parallel(ctx, c, func(_, _ int) { mu.Lock(); counter++; mu.Unlock() })
		return counter, err
	case "atomic":
		var counter atomic.Int64
		err := parallel(ctx, c, func(_, _ int) { counter.Add(1) })
		return counter.Load(), err
	case "cas":
		var counter atomic.Int64
		err := parallel(ctx, c, func(_, _ int) {
			for {
				old := counter.Load()
				if counter.CompareAndSwap(old, old+1) {
					break
				}
			}
		})
		return counter.Load(), err
	case "sharded":
		shards := make([]atomic.Int64, c.Shards)
		err := parallel(ctx, c, func(w, _ int) { shards[w%len(shards)].Add(1) })
		var total int64
		for i := range shards {
			total += shards[i].Load()
		}
		return total, err
	case "local":
		counts := make([]int64, c.Workers)
		err := parallel(ctx, c, func(w, _ int) { counts[w]++ })
		var total int64
		for _, v := range counts {
			total += v
		}
		return total, err
	case "channel":
		updates := make(chan struct{}, c.Buffer)
		done := make(chan int64, 1)
		go func() {
			var total int64
			for range updates {
				total++
			}
			done <- total
		}()
		var wg sync.WaitGroup
		wg.Add(c.Workers)
		for w := 0; w < c.Workers; w++ {
			go func() {
				defer wg.Done()
				for i := 0; i < c.Iterations; i++ {
					select {
					case updates <- struct{}{}:
					case <-ctx.Done():
						return
					}
					step(c, i)
				}
			}()
		}
		wg.Wait()
		close(updates)
		return <-done, ctx.Err()
	default:
		return 0, fmt.Errorf("unsupported mode %q", mode)
	}
}

func trial(parent context.Context, c Config, mode string, n int) Result {
	expected := int64(c.Workers) * int64(c.Iterations)
	ctx := parent
	cancel := func() {}
	if c.Timeout > 0 {
		ctx, cancel = context.WithTimeout(parent, c.Timeout)
	}
	defer cancel()
	start := time.Now()
	observed, err := execute(ctx, c, mode)
	duration := time.Since(start)
	r := Result{Mode: mode, Trial: n, Workers: c.Workers, Iterations: c.Iterations, Expected: expected, Observed: observed, Lost: expected - observed, Correct: expected == observed && err == nil, DurationNS: duration.Nanoseconds(), TimedOut: errors.Is(err, context.DeadlineExceeded)}
	if err != nil {
		r.Error = err.Error()
	}
	if duration > 0 {
		r.Throughput = float64(observed) / duration.Seconds()
		r.NanosecondsPerOp = float64(duration.Nanoseconds()) / float64(max(1, int(observed)))
	}
	return r
}

func percentile(sorted []float64, p float64) float64 {
	if len(sorted) == 0 {
		return 0
	}
	if len(sorted) == 1 {
		return sorted[0]
	}
	position := p * float64(len(sorted)-1)
	low := int(math.Floor(position))
	high := int(math.Ceil(position))
	fraction := position - float64(low)
	return sorted[low]*(1-fraction) + sorted[high]*fraction
}

func summarize(mode string, rows []Result) Summary {
	s := Summary{Mode: mode, Trials: len(rows)}
	if len(rows) == 0 {
		return s
	}
	durations := make([]float64, 0, len(rows))
	for i, r := range rows {
		if r.Correct {
			s.Correct++
		} else {
			s.Incorrect++
		}
		if r.Error != "" {
			s.Errors++
		}
		s.TotalLost += r.Lost
		s.MeanThroughput += r.Throughput
		s.MeanNS += float64(r.DurationNS)
		durations = append(durations, float64(r.DurationNS))
		if i == 0 || r.DurationNS < s.MinNS {
			s.MinNS = r.DurationNS
		}
		if r.DurationNS > s.MaxNS {
			s.MaxNS = r.DurationNS
		}
	}
	s.MeanNS /= float64(len(rows))
	s.MeanThroughput /= float64(len(rows))
	for _, x := range durations {
		d := x - s.MeanNS
		s.StddevNS += d * d
	}
	s.StddevNS = math.Sqrt(s.StddevNS / float64(len(rows)))
	sort.Float64s(durations)
	s.MedianNS = percentile(durations, .5)
	s.P95NS = percentile(durations, .95)
	return s
}

func collect(ctx context.Context, c Config, modes []string) Report {
	report := Report{
		Version: version, Timestamp: time.Now().UTC().Format(time.RFC3339Nano),
		GoVersion: runtime.Version(), OS: runtime.GOOS, Arch: runtime.GOARCH,
		CPUs: runtime.NumCPU(), GOMAXPROCS: runtime.GOMAXPROCS(0),
		Config:  ConfigView{Modes: modes, Workers: c.Workers, Iterations: c.Iterations, Trials: c.Trials, Warmups: c.Warmups, Shards: c.Shards, Buffer: c.Buffer, YieldEvery: c.YieldEvery, Delay: c.Delay.String(), Timeout: c.Timeout.String(), Seed: c.Seed, Unsafe: c.Unsafe},
		Results: []Result{}, Summaries: []Summary{},
	}
	if c.Unsafe {
		report.Warnings = append(report.Warnings, "unsafe mode intentionally contains a data race; behavior is undefined by the Go memory model")
	}
	if c.Timeout > 0 {
		report.Warnings = append(report.Warnings, "timeouts are cooperative and may complete after the deadline")
	}
	order := append([]string(nil), modes...)
	rng := rand.New(rand.NewSource(c.Seed))
	rng.Shuffle(len(order), func(i, j int) { order[i], order[j] = order[j], order[i] })
	for _, mode := range order {
		if ctx.Err() != nil {
			break
		}
		for w := 0; w < c.Warmups; w++ {
			if ctx.Err() != nil {
				break
			}
			trial(ctx, c, mode, 0)
		}
		var rows []Result
		for n := 1; n <= c.Trials; n++ {
			if ctx.Err() != nil {
				break
			}
			r := trial(ctx, c, mode, n)
			report.Results = append(report.Results, r)
			rows = append(rows, r)
		}
		report.Summaries = append(report.Summaries, summarize(mode, rows))
	}
	sort.Slice(report.Summaries, func(i, j int) bool { return report.Summaries[i].Mode < report.Summaries[j].Mode })
	return report
}

func renderHuman(r Report, quiet bool) error {
	fmt.Printf("Race Condition Laboratory %s\n", r.Version)
	fmt.Printf("Go %s | %s/%s | CPUs %d | GOMAXPROCS %d\n", r.GoVersion, r.OS, r.Arch, r.CPUs, r.GOMAXPROCS)
	fmt.Printf("Workers %d | Iterations %d | Trials %d | Warmups %d\n", r.Config.Workers, r.Config.Iterations, r.Config.Trials, r.Config.Warmups)
	if !quiet {
		fmt.Println("\nTrials")
		fmt.Printf("%-10s %5s %12s %12s %10s %12s %12s\n", "Mode", "Trial", "Expected", "Observed", "Lost", "Duration", "Ops/sec")
		for _, x := range r.Results {
			fmt.Printf("%-10s %5d %12d %12d %10d %12s %12.0f\n", x.Mode, x.Trial, x.Expected, x.Observed, x.Lost, time.Duration(x.DurationNS), x.Throughput)
			if x.Error != "" {
				fmt.Printf("  error: %s\n", x.Error)
			}
		}
	}
	fmt.Println("\nSummary")
	fmt.Printf("%-10s %8s %8s %14s %14s %14s %12s\n", "Mode", "Correct", "Failed", "Median", "P95", "Stddev", "Ops/sec")
	for _, x := range r.Summaries {
		fmt.Printf("%-10s %8d %8d %14s %14s %14s %12.0f\n", x.Mode, x.Correct, x.Incorrect, time.Duration(x.MedianNS), time.Duration(x.P95NS), time.Duration(x.StddevNS), x.MeanThroughput)
	}
	for _, warning := range r.Warnings {
		fmt.Println("Warning:", warning)
	}
	return nil
}

func renderJSON(r Report) error {
	enc := json.NewEncoder(os.Stdout)
	enc.SetIndent("", "  ")
	return enc.Encode(r)
}

func renderJSONL(r Report) error {
	enc := json.NewEncoder(os.Stdout)
	for _, x := range r.Results {
		if err := enc.Encode(struct {
			Type   string `json:"type"`
			Result Result `json:"result"`
		}{"trial", x}); err != nil {
			return err
		}
	}
	for _, x := range r.Summaries {
		if err := enc.Encode(struct {
			Type    string  `json:"type"`
			Summary Summary `json:"summary"`
		}{"summary", x}); err != nil {
			return err
		}
	}
	return nil
}

func renderCSV(r Report) error {
	w := csv.NewWriter(os.Stdout)
	if err := w.Write([]string{"mode", "trial", "workers", "iterations", "expected", "observed", "lost", "correct", "duration_ns", "operations_per_second", "nanoseconds_per_operation", "timed_out", "error"}); err != nil {
		return err
	}
	for _, x := range r.Results {
		row := []string{x.Mode, strconv.Itoa(x.Trial), strconv.Itoa(x.Workers), strconv.Itoa(x.Iterations), strconv.FormatInt(x.Expected, 10), strconv.FormatInt(x.Observed, 10), strconv.FormatInt(x.Lost, 10), strconv.FormatBool(x.Correct), strconv.FormatInt(x.DurationNS, 10), strconv.FormatFloat(x.Throughput, 'f', 3, 64), strconv.FormatFloat(x.NanosecondsPerOp, 'f', 3, 64), strconv.FormatBool(x.TimedOut), x.Error}
		if err := w.Write(row); err != nil {
			return err
		}
	}
	w.Flush()
	return w.Error()
}

func run() error {
	c, err := parseConfig()
	if err != nil {
		return err
	}
	if c.Version {
		fmt.Println("race-condition", version)
		return nil
	}
	modes, err := selectModes(c)
	if err != nil {
		return err
	}
	if c.GOMAXPROCS > 0 {
		runtime.GOMAXPROCS(c.GOMAXPROCS)
	}
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()
	report := collect(ctx, c, modes)
	if c.Output != "" {
		f, err := os.Create(c.Output)
		if err != nil {
			return err
		}
		original := os.Stdout
		os.Stdout = f
		defer func() { os.Stdout = original }()
		switch c.Format {
		case "human":
			err = renderHuman(report, c.Quiet)
		case "json":
			err = renderJSON(report)
		case "jsonl":
			err = renderJSONL(report)
		case "csv":
			err = renderCSV(report)
		}
		closeErr := f.Close()
		if err != nil {
			return err
		}
		if closeErr != nil {
			return closeErr
		}
	} else {
		switch c.Format {
		case "human":
			err = renderHuman(report, c.Quiet)
		case "json":
			err = renderJSON(report)
		case "jsonl":
			err = renderJSONL(report)
		case "csv":
			err = renderCSV(report)
		}
		if err != nil {
			return err
		}
	}
	if c.Verify {
		for _, s := range report.Summaries {
			if s.Mode != "unsafe" && s.Incorrect > 0 {
				return fmt.Errorf("verification failed for mode %s", s.Mode)
			}
		}
	}
	if ctx.Err() != nil {
		return ctx.Err()
	}
	return nil
}

func main() {
	if err := run(); err != nil {
		fmt.Fprintln(os.Stderr, "error:", err)
		os.Exit(1)
	}
}
