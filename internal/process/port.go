package process

import (
	"fmt"
	"strconv"
	"strings"
)

// portAllocator hands out free ports from a fixed inclusive range.
type portAllocator struct {
	lo, hi int
	used   map[int]bool
}

func newPortAllocator(lo, hi int) *portAllocator {
	return &portAllocator{lo: lo, hi: hi, used: make(map[int]bool)}
}

// Acquire returns the lowest free port in the range, marking it used.
func (p *portAllocator) Acquire() (int, error) {
	for port := p.lo; port <= p.hi; port++ {
		if !p.used[port] {
			p.used[port] = true
			return port, nil
		}
	}
	return 0, fmt.Errorf("process: no available port in range %d-%d", p.lo, p.hi)
}

// Release returns a port to the free pool.
func (p *portAllocator) Release(port int) {
	delete(p.used, port)
}

// parsePortRange parses "lo-hi" into an inclusive port range.
func parsePortRange(s string) (int, int, error) {
	parts := strings.Split(s, "-")
	if len(parts) != 2 {
		return 0, 0, fmt.Errorf("process: invalid port_range %q (want \"lo-hi\")", s)
	}
	lo, err := strconv.Atoi(strings.TrimSpace(parts[0]))
	if err != nil {
		return 0, 0, fmt.Errorf("process: invalid port_range %q: %w", s, err)
	}
	hi, err := strconv.Atoi(strings.TrimSpace(parts[1]))
	if err != nil {
		return 0, 0, fmt.Errorf("process: invalid port_range %q: %w", s, err)
	}
	if lo <= 0 || hi < lo {
		return 0, 0, fmt.Errorf("process: invalid port_range %q (need 0 < lo <= hi)", s)
	}
	return lo, hi, nil
}
