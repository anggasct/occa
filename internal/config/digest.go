package config

import (
	"fmt"
	"strings"
	"time"
)

// DigestConfig is the optional webhooks.digest block: a once-a-day summary
// posted to one channel. An absent block turns the digest off.
type DigestConfig struct {
	At        string `yaml:"at"`
	Timezone  string `yaml:"timezone"`
	Platform  string `yaml:"platform"`
	ChannelID string `yaml:"channel_id"`

	// Hour, Minute and Location are At and Timezone, parsed at startup.
	Hour     int            `yaml:"-"`
	Minute   int            `yaml:"-"`
	Location *time.Location `yaml:"-"`
}

// NextAt returns the first digest time strictly after now.
func (d DigestConfig) NextAt(now time.Time) time.Time {
	loc := d.Location
	if loc == nil {
		loc = time.UTC
	}
	local := now.In(loc)
	next := time.Date(local.Year(), local.Month(), local.Day(), d.Hour, d.Minute, 0, 0, loc)
	if !next.After(local) {
		next = time.Date(local.Year(), local.Month(), local.Day()+1, d.Hour, d.Minute, 0, 0, loc)
	}
	return next
}

func validateDigest(d *DigestConfig) error {
	if d == nil {
		return nil
	}
	at := strings.TrimSpace(d.At)
	parsed, err := time.Parse("15:04", at)
	if err != nil {
		return fmt.Errorf("config: webhooks.digest.at must be HH:MM, got %q", d.At)
	}
	tz := strings.TrimSpace(d.Timezone)
	if tz == "" {
		return fmt.Errorf("config: webhooks.digest.timezone is required (IANA name, e.g. Asia/Jakarta)")
	}
	loc, err := time.LoadLocation(tz)
	if err != nil {
		return fmt.Errorf("config: webhooks.digest.timezone %q is not a known time zone", d.Timezone)
	}
	if strings.TrimSpace(d.Platform) == "" {
		return fmt.Errorf("config: webhooks.digest.platform is required")
	}
	if strings.TrimSpace(d.ChannelID) == "" {
		return fmt.Errorf("config: webhooks.digest.channel_id is required")
	}
	d.At = at
	d.Timezone = tz
	d.Platform = strings.TrimSpace(d.Platform)
	d.ChannelID = strings.TrimSpace(d.ChannelID)
	d.Hour = parsed.Hour()
	d.Minute = parsed.Minute()
	d.Location = loc
	return nil
}
