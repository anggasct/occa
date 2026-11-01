package webhook

import (
	"context"
	"fmt"
	"sort"
	"strings"
	"time"

	"github.com/anggasct/occa/internal/channel"
	"github.com/anggasct/occa/internal/store"
)

// inboxButtonRows is how many PRs get a button row in /inbox. Discord allows
// five action rows per message.
const inboxButtonRows = 5

// Inbox lists the PRs that wait on the operator: every blocked queue row,
// oldest first, in plain words, with Open · Retry · Merge · Cancel for the
// first five. The buttons are compact callbacks, so they work on Telegram.
func (s *Server) Inbox(ctx context.Context, msg channel.IncomingMessage) (string, []channel.Button, error) {
	if !s.operatorAllowed(msg.Platform, msg.UserID) {
		return "⛔ Not allowed.", nil, nil
	}
	rows, err := s.blockedRows(ctx)
	if err != nil {
		return "", nil, err
	}
	if len(rows) == 0 {
		return "📥 Nothing needs you right now.", nil, nil
	}
	now := time.Now()
	var b strings.Builder
	fmt.Fprintf(&b, "📥 Needs you — %d PR%s", len(rows), plural(len(rows)))
	var buttons []channel.Button
	for i, row := range rows {
		fmt.Fprintf(&b, "\n%d. %s — %s", i+1, row.PRKey, describeBlock(row.BlockedReason).Headline)
		if row.UpdatedAt > 0 {
			fmt.Fprintf(&b, " (%s ago)", ageText(now.Sub(time.Unix(row.UpdatedAt, 0))))
		}
		if i >= inboxButtonRows {
			continue
		}
		buttons = append(buttons, s.inboxButtons(row.PRKey, i+1)...)
	}
	if extra := len(rows) - inboxButtonRows; extra > 0 {
		fmt.Fprintf(&b, "\n…and %d more", extra)
	}
	return b.String(), buttons, nil
}

func (s *Server) blockedRows(ctx context.Context) ([]store.PRRun, error) {
	if s.prQueues == nil {
		return nil, nil
	}
	rows, err := s.prQueues.ListUnresolved(ctx)
	if err != nil {
		return nil, err
	}
	var blocked []store.PRRun
	for _, row := range rows {
		if row.Status == store.PRStatusBlocked {
			blocked = append(blocked, row)
		}
	}
	sort.SliceStable(blocked, func(i, j int) bool { return blocked[i].UpdatedAt < blocked[j].UpdatedAt })
	return blocked, nil
}

// inboxButtons is one button row for a blocked PR: a link to its thread when
// one is known, then Retry, Merge and Cancel naming the PR number.
func (s *Server) inboxButtons(prKey string, row int) []channel.Button {
	if s.tasks == nil {
		return nil
	}
	rec, ok := s.tasks.ResolvePRThread(prKey)
	if !ok {
		return nil
	}
	_, number, _ := splitPRKey(prKey)
	suffix := "#" + number
	var buttons []channel.Button
	if threadID := strings.TrimSpace(rec.ThreadID); threadID != "" {
		if ep, ok := s.reviewEndpointForPR(prKey); ok {
			if url := TaskThreadURL(ep.Platform, s.guildIDFor(ep.Platform, ep.ChannelID, threadID), threadID); url != "" {
				buttons = append(buttons, channel.Button{Label: "Open " + suffix, URL: url, Row: row})
			}
		}
	}
	return append(buttons, blockedActionButtons(rec.Repo, rec.Branch, suffix, row)...)
}

// ageText words how long ago something happened: minutes, hours, then days.
func ageText(d time.Duration) string {
	if d < time.Minute {
		return "just now"
	}
	if d < time.Hour {
		return fmt.Sprintf("%dm", int(d/time.Minute))
	}
	hours := int(d / time.Hour)
	if hours < 48 {
		return fmt.Sprintf("%dh", hours)
	}
	return fmt.Sprintf("%dd", hours/24)
}

func plural(n int) string {
	if n == 1 {
		return ""
	}
	return "s"
}
