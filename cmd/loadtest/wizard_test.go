package main

import (
	"bufio"
	"os"
	"strings"
	"testing"
	"time"
)

// scan wraps a canned input string as a scanner, as if typed at the prompt.
func scan(input string) *bufio.Scanner { return bufio.NewScanner(strings.NewReader(input)) }

// quiet redirects the prompt output to /dev/null for the duration of a test so the
// wizard's printed prompts don't clutter `go test` output.
func quiet(t *testing.T) {
	t.Helper()
	old := os.Stdout
	f, err := os.OpenFile(os.DevNull, os.O_WRONLY, 0)
	if err != nil {
		t.Fatalf("open devnull: %v", err)
	}
	os.Stdout = f
	t.Cleanup(func() { os.Stdout = old; f.Close() })
}

func TestAskInt(t *testing.T) {
	quiet(t)
	if got := askInt(scan("\n"), "n", "d", 4, 1); got != 4 {
		t.Errorf("blank should take default: got %d want 4", got)
	}
	if got := askInt(scan("12\n"), "n", "d", 4, 1); got != 12 {
		t.Errorf("valid: got %d want 12", got)
	}
	// below min, then non-numeric, then a valid value
	if got := askInt(scan("0\nabc\n7\n"), "n", "d", 4, 1); got != 7 {
		t.Errorf("should re-prompt past bad input: got %d want 7", got)
	}
}

func TestAskFloat(t *testing.T) {
	quiet(t)
	if got := askFloat(scan("\n"), "f", "d", 0.2, 0, 1); got != 0.2 {
		t.Errorf("blank should take default: got %v want 0.2", got)
	}
	if got := askFloat(scan("0.75\n"), "f", "d", 0.2, 0, 1); got != 0.75 {
		t.Errorf("valid: got %v want 0.75", got)
	}
	// out of range, then valid
	if got := askFloat(scan("1.5\n0.5\n"), "f", "d", 0.2, 0, 1); got != 0.5 {
		t.Errorf("should reject out-of-range: got %v want 0.5", got)
	}
}

func TestAskDuration(t *testing.T) {
	quiet(t)
	if got := askDuration(scan("\n"), "d", "x", 10*time.Second); got != 10*time.Second {
		t.Errorf("blank should take default: got %v want 10s", got)
	}
	if got := askDuration(scan("2m\n"), "d", "x", 10*time.Second); got != 2*time.Minute {
		t.Errorf("valid: got %v want 2m", got)
	}
	// unparseable, then valid
	if got := askDuration(scan("soon\n30s\n"), "d", "x", 10*time.Second); got != 30*time.Second {
		t.Errorf("should re-prompt past bad input: got %v want 30s", got)
	}
}

func TestAskString(t *testing.T) {
	quiet(t)
	if got := askString(scan("\n"), "l", "x", "baseline"); got != "baseline" {
		t.Errorf("blank should take default: got %q want baseline", got)
	}
	if got := askString(scan("after-minheap\n"), "l", "x", "baseline"); got != "after-minheap" {
		t.Errorf("typed value: got %q want after-minheap", got)
	}
}
