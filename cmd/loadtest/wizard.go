package main

import (
	"bufio"
	"flag"
	"fmt"
	"os"
	"strconv"
	"strings"
	"time"
)

// shouldRunWizard reports whether to walk the user through setup interactively: they
// passed no flags AND stdin is a real terminal. Piped or redirected stdin (CI, tests,
// `echo ... |`) is not a char device, so those runs silently use the defaults instead
// of blocking forever on a prompt that will never be answered.
func shouldRunWizard() bool {
	if flag.NFlag() != 0 {
		return false
	}
	info, err := os.Stdin.Stat()
	if err != nil {
		return false
	}
	return info.Mode()&os.ModeCharDevice != 0
}

// runWizard fills cfg by prompting for each knob, starting from the defaults already
// in cfg. Pressing Enter keeps the shown [default]; invalid input re-prompts. Only the
// workload knobs are asked; wal/out/visibility/sweep/draintimeout keep their defaults
// (pass them as flags for advanced tuning).
func runWizard(cfg *config) {
	in := bufio.NewScanner(os.Stdin)

	fmt.Println("No flags given — interactive setup. Press Enter to accept each [default].")
	fmt.Println("(Pass any flag to skip this, e.g. `go run ./cmd/loadtest -dur 30s`.)")
	fmt.Println()

	cfg.dur = askDuration(in, "duration", "how long producers + consumers run", cfg.dur)
	cfg.producers = askInt(in, "producers", "concurrent Enqueue goroutines", cfg.producers, 1)
	cfg.consumers = askInt(in, "consumers", "concurrent consumer goroutines", cfg.consumers, 1)
	cfg.failRate = askFloat(in, "failrate", "chance [0-1] a delivery fails to ack", cfg.failRate, 0, 1)
	cfg.crashShare = askFloat(in, "crashshare", "of failures, fraction [0-1] that crash AFTER the effect (rest Retry before it)", cfg.crashShare, 0, 1)
	cfg.maxRetries = askInt(in, "maxretries", "retries before a task is sent to the DLQ", cfg.maxRetries, 0)
	cfg.label = askString(in, "label", "tag recorded with the results (optional)", cfg.label)

	fmt.Println()
}

// prompt prints the knob's description and a "name [default]:" line, then returns the
// trimmed input. An empty return (blank line or EOF) means "use the default".
func prompt(in *bufio.Scanner, name, desc, def string) string {
	fmt.Printf("  %s — %s\n", name, desc)
	fmt.Printf("  %s [%s]: ", name, def)
	if !in.Scan() {
		fmt.Println() // EOF (Ctrl-D): move off the prompt line, caller uses default
		return ""
	}
	return strings.TrimSpace(in.Text())
}

func askDuration(in *bufio.Scanner, name, desc string, def time.Duration) time.Duration {
	for {
		s := prompt(in, name, desc, def.String())
		if s == "" {
			return def
		}
		d, err := time.ParseDuration(s)
		if err != nil || d <= 0 {
			fmt.Printf("    ! need a positive duration like 30s or 2m (got %q)\n", s)
			continue
		}
		return d
	}
}

func askInt(in *bufio.Scanner, name, desc string, def, min int) int {
	for {
		s := prompt(in, name, desc, strconv.Itoa(def))
		if s == "" {
			return def
		}
		n, err := strconv.Atoi(s)
		if err != nil || n < min {
			fmt.Printf("    ! need a whole number >= %d (got %q)\n", min, s)
			continue
		}
		return n
	}
}

func askFloat(in *bufio.Scanner, name, desc string, def, min, max float64) float64 {
	for {
		s := prompt(in, name, desc, strconv.FormatFloat(def, 'g', -1, 64))
		if s == "" {
			return def
		}
		f, err := strconv.ParseFloat(s, 64)
		if err != nil || f < min || f > max {
			fmt.Printf("    ! need a number in [%g, %g] (got %q)\n", min, max, s)
			continue
		}
		return f
	}
}

func askString(in *bufio.Scanner, name, desc, def string) string {
	if s := prompt(in, name, desc, def); s != "" {
		return s
	}
	return def
}
