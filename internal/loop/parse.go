package loop

import (
	"fmt"
	"strconv"
	"strings"
	"time"
)

const Usage = "Usage: /loop every <dur> x<n>|for <dur> <prompt>\n" +
	"Example: /loop every 2m x15 check PR status\n" +
	"Limits: every 30s–1h, x2–60, for 1m–4h, prompt up to 1000 characters. List with /loops, stop with /loop stop <id>."

func ParseRequest(args string) (Request, error) {
	var req Request
	fields := strings.Fields(args)
	if len(fields) < 4 || fields[0] != "every" {
		return req, fmt.Errorf("loop: bad syntax")
	}
	interval, err := time.ParseDuration(fields[1])
	if err != nil {
		return req, fmt.Errorf("loop: bad interval: %w", err)
	}
	req.Interval = interval
	rest := fields[2:]
	switch {
	case strings.HasPrefix(rest[0], "x"):
		n, err := strconv.Atoi(strings.TrimPrefix(rest[0], "x"))
		if err != nil {
			return req, fmt.Errorf("loop: bad count: %w", err)
		}
		req.Count = n
		rest = rest[1:]
	case rest[0] == "for" && len(rest) >= 3:
		length, err := time.ParseDuration(rest[1])
		if err != nil {
			return req, fmt.Errorf("loop: bad duration: %w", err)
		}
		req.Length = length
		rest = rest[2:]
	default:
		return req, fmt.Errorf("loop: need x<n> or for <dur>")
	}
	req.Prompt = strings.Join(rest, " ")
	if err := check(req); err != nil {
		return Request{}, err
	}
	return req, nil
}
