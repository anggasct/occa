package webhook

import (
	"bytes"
	"context"
	"fmt"
	"log/slog"
	"os/exec"
	"slices"
	"strings"
	"time"

	"github.com/anggasct/occa/internal/relay"
	"github.com/anggasct/occa/internal/store"
)

// retryLane runs the retry effect shared by the operator command and the
// action API. modelOverride is carried into the synthetic re-run so
// retry_with can swap the lane model; the returned string is the operator-
// facing sentence.
func (s *Server) retryLane(ctx context.Context, prKey, modelOverride, prefix string) (string, error) {
	row, err := s.operatorRow(ctx, prKey)
	if err != nil || row == nil {
		return "Nothing to retry for " + prKey + " — no queue state.", nil
	}
	kind := s.inferRetryKind(ctx, row)
	if kind == "" || kind == "dispatch" || kind == "merged" {
		return "Nothing to retry for " + prKey + ".", nil
	}
	if err := s.prQueues.SetRetryState(ctx, prKey, 0, 0); err != nil {
		return "", err
	}
	if d := s.liveDispatcher(prKey); d != nil {
		if d.isBackingOff() {
			d.requestRetryNow()
			return "Retry count reset — firing now.", nil
		}
		if d.isActive() || d.queueLen() > 0 {
			return "Retry count reset — a run is already queued or running.", nil
		}
	}
	blocked := row.Status == store.PRStatusBlocked
	if blocked {
		if err := s.prQueues.Unblock(ctx, prKey); err != nil {
			return "", err
		}
		s.clearEscalated(prKey)
	}
	s.reopenPRSurface(prKey)
	var ran []store.Stage
	if blocked {
		ran = s.runWaitingInputs(ctx, prKey)
	}
	names := make([]string, 0, len(ran)+1)
	if stage, ok := stageForWorkflow(kind); !ok || !slices.Contains(ran, stage) {
		repo, number, _ := splitPRKey(prKey)
		deliveryID, enqErr := s.enqueueSyntheticRetryKind(ctx, repo, number, kind, prKey, prefix)
		if enqErr != nil {
			return "Retry reset, but re-run failed: " + enqErr.Error(), nil
		}
		if modelOverride != "" {
			s.modelOverrides.Store(deliveryID, modelOverride)
		}
		names = append(names, kind)
	}
	for _, stage := range ran {
		names = append(names, string(stage))
	}
	return "Re-running " + strings.Join(names, " and ") + " for " + prKey + ".", nil
}

func (s *Server) effectRetry(ctx context.Context, req FireActionRequest, v *actionView) (string, error) {
	return s.retryLane(ctx, v.prKey(), "", actionDeliveryPrefixFor(req))
}

func actionDeliveryPrefixFor(req FireActionRequest) string {
	if req.Surface == "chat" {
		return operatorRetryDeliveryPrefix
	}
	return actionDeliveryPrefix
}

func (s *Server) effectRetryWith(ctx context.Context, req FireActionRequest, v *actionView) (string, error) {
	profile := strings.TrimSpace(req.Params["profile"])
	if profile == "" {
		return "", fmt.Errorf("retry_with needs params.profile naming the model to run")
	}
	if _, err := relay.ParseModelRef(profile); err != nil {
		return "", fmt.Errorf("profile %q is not a valid model ref", profile)
	}
	tail, err := s.retryLane(ctx, v.prKey(), profile, actionDeliveryPrefixFor(req))
	if err != nil {
		return "", err
	}
	return tail + " with profile " + profile, nil
}

func (s *Server) effectUnblock(ctx context.Context, _ FireActionRequest, v *actionView) (string, error) {
	prKey := v.prKey()
	if s.prQueues != nil {
		if row, err := s.prQueues.Get(ctx, prKey); err == nil && row != nil && row.Status == store.PRStatusBlocked {
			if err := s.prQueues.Unblock(ctx, prKey); err != nil {
				return "", err
			}
		}
	}
	s.clearEscalated(prKey)
	s.reopenPRSurface(prKey)
	ran := s.runWaitingInputs(ctx, prKey)
	if len(ran) == 0 {
		return "blocker cleared — no queued input to run", nil
	}
	names := make([]string, 0, len(ran))
	for _, stage := range ran {
		names = append(names, string(stage))
	}
	return "blocker cleared — re-running " + strings.Join(names, " and "), nil
}

// cancelLane is the stop effect shared by /cancel, the Cancel button and the
// stop action: unblock, supersede waiting inputs, abort the live agent and
// drop queued work. It returns how many queued items were cleared.
func (s *Server) cancelLane(ctx context.Context, prKey string) (int, error) {
	if s.prQueues != nil {
		if row, err := s.prQueues.Get(ctx, prKey); err == nil && row != nil && row.Status == store.PRStatusBlocked {
			if uerr := s.prQueues.Unblock(ctx, prKey); uerr != nil {
				return 0, uerr
			}
			s.reopenPRSurface(prKey)
			s.clearEscalated(prKey)
			s.supersedeWaitingInputs(ctx, prKey, "cancelled by operator")
		}
	}
	d := s.liveDispatcher(prKey)
	repo, _, _ := splitPRKey(prKey)
	s.abortActiveSession(ctx, prKey, repo)
	dropped := 0
	if d != nil {
		d.requestCancel()
	} else if s.prQueues != nil {
		if n, err := s.prQueues.ClearQueue(ctx, prKey); err == nil {
			dropped = n
		}
		if err := s.prQueues.ClearActive(ctx, prKey); err != nil {
			return 0, err
		}
	}
	return dropped, nil
}

func (s *Server) effectStop(ctx context.Context, _ FireActionRequest, v *actionView) (string, error) {
	dropped, err := s.cancelLane(ctx, v.prKey())
	if err != nil {
		return "", err
	}
	if dropped > 0 {
		return fmt.Sprintf("%d queued item(s) cleared", dropped), nil
	}
	return "", nil
}

func (s *Server) effectSkipStage(ctx context.Context, req FireActionRequest, v *actionView) (string, error) {
	prKey := v.prKey()
	detail := "skipped by operator: " + req.Reason
	if v.latest != nil && !v.latest.State.Terminal() {
		var err error
		if v.latest.State == store.StageStatePending {
			err = s.actionRepo().EndPending(ctx, v.latest.ID, store.StageStateSkipped, detail)
		} else {
			err = s.actionRepo().EndRun(ctx, v.latest.ID, store.StageStateSkipped, detail)
		}
		if err != nil {
			return "", err
		}
	}
	if s.prQueues != nil {
		if row, err := s.prQueues.Get(ctx, prKey); err == nil && row != nil && row.Status == store.PRStatusBlocked {
			if err := s.prQueues.Unblock(ctx, prKey); err != nil {
				return "", err
			}
			s.clearEscalated(prKey)
			s.reopenPRSurface(prKey)
		}
	}
	if d := s.liveDispatcher(prKey); d != nil {
		repo, _, _ := splitPRKey(prKey)
		s.abortActiveSession(ctx, prKey, repo)
		d.requestSkip()
	}
	s.commentOnPR(ctx, v.item.Repo, v.item.Number, "⚙️ Stage skipped by operator — "+req.Reason)
	return "stage skipped — queue released", nil
}

func (s *Server) effectApprove(ctx context.Context, _ FireActionRequest, v *actionView) (string, error) {
	if err := s.actionRepo().SetGateResult(ctx, v.latest.ID, `{"decision":"approved"}`); err != nil {
		return "", err
	}
	if err := s.actionRepo().EndRun(ctx, v.latest.ID, store.StageStateDone, "approved by operator"); err != nil {
		return "", err
	}
	return "gate approved", nil
}

func (s *Server) effectReject(ctx context.Context, req FireActionRequest, v *actionView) (string, error) {
	if err := s.actionRepo().SetGateResult(ctx, v.latest.ID, `{"decision":"rejected"}`); err != nil {
		return "", err
	}
	if err := s.actionRepo().EndRun(ctx, v.latest.ID, store.StageStateFailed, "rejected by operator: "+req.Reason); err != nil {
		return "", err
	}
	repo, number, _ := splitPRKey(v.prKey())
	if err := s.enqueueAutoFixCI(ctx, repo, number, s.livePRHeadSHA(ctx, v.prKey()), []string{fixKindReview}); err != nil {
		return "", err
	}
	s.commentOnPR(ctx, v.item.Repo, v.item.Number, "⚙️ Rejected by operator — "+req.Reason)
	return "gate rejected — fix run queued", nil
}

func (s *Server) effectRecheck(ctx context.Context, _ FireActionRequest, v *actionView) (string, error) {
	prKey := v.prKey()
	state, err := s.resolveLivePRState(ctx, prKey)
	if err != nil {
		return "", fmt.Errorf("live state read failed: %w", err)
	}
	if v.hasCode(store.BlockerGitHubUnreachable) && s.prQueues != nil {
		if err := s.prQueues.Unblock(ctx, prKey); err != nil {
			return "", err
		}
		s.clearEscalated(prKey)
		s.reopenPRSurface(prKey)
		ran := s.runWaitingInputs(ctx, prKey)
		if len(ran) > 0 {
			return "GitHub reachable again — re-running queued input", nil
		}
		return "GitHub reachable again — blocker released", nil
	}
	return "live state is " + state, nil
}

func (s *Server) effectReReview(ctx context.Context, _ FireActionRequest, v *actionView) (string, error) {
	prKey := v.prKey()
	ep, ok := s.reviewEndpointForPR(prKey)
	if !ok {
		return "", refuseAction("No review endpoint is configured for this repository.")
	}
	state, err := s.newPRStateDecision(prKey).resolve(ctx)
	if err != nil || state == "" {
		return "", refuseAction("Could not read the live state of " + prKey + " — try again in a moment.")
	}
	if state == PRStateMerged || state == PRStateClosed {
		return "", refuseAction(prKey + " is " + state + " — nothing to review.")
	}
	trigger := s.reviewTrigger(ep)
	if trigger == "" {
		return "", refuseAction("The review endpoint has no comment trigger.")
	}
	repo, number, _ := splitPRKey(prKey)
	branch := s.branchForPR(prKey)
	// Leave the head unset: the run resolves whatever head is current when it
	// executes, so a review queued behind a fix reviews the post-fix head.
	body := internalReviewCommentBody(repo, number, trigger, s.prURLForPR(prKey), branch, "")
	item := dispatchItem{
		ep:          ep,
		body:        body,
		deliveryID:  fmt.Sprintf("%sre_review-%d", actionDeliveryPrefix, time.Now().UnixNano()),
		eventType:   "issue_comment",
		payloadHash: payloadHashOf(body),
	}
	key := WebhookExecutionKey{Repository: repo, Branch: branch}
	if _, admitted, why := s.enqueue(ep, key, item); !admitted {
		return "", refuseAction("A review could not be queued — " + why + ".")
	}
	tail := "Review queued for " + prKey + "."
	switch {
	case v.busy.activeKind != "":
		tail += " It runs after the current " + v.busy.activeKind + " run."
	case v.busy.queued > 0:
		tail += " It runs after the queued work."
	}
	return tail, nil
}

func (s *Server) effectFixCI(ctx context.Context, _ FireActionRequest, v *actionView) (string, error) {
	return s.effectFix(ctx, v, fixKindCI)
}

func (s *Server) effectFixReview(ctx context.Context, _ FireActionRequest, v *actionView) (string, error) {
	return s.effectFix(ctx, v, fixKindReview)
}

func (s *Server) effectFix(ctx context.Context, v *actionView, kind string) (string, error) {
	prKey := v.prKey()
	if s.prQueues != nil {
		if row, err := s.prQueues.Get(ctx, prKey); err == nil && row != nil && row.Status == store.PRStatusBlocked {
			if err := s.prQueues.Unblock(ctx, prKey); err != nil {
				return "", err
			}
		}
	}
	s.clearEscalated(prKey)
	s.reopenPRSurface(prKey)
	repo, number, _ := splitPRKey(prKey)
	if err := s.enqueueAutoFixCI(ctx, repo, number, s.livePRHeadSHA(ctx, prKey), []string{kind}); err != nil {
		return "", err
	}
	return kind + " fix queued", nil
}

func (s *Server) effectRebind(ctx context.Context, _ FireActionRequest, v *actionView) (string, error) {
	prKey := v.prKey()
	repo, number, _ := splitPRKey(prKey)
	envelope := WebhookEnvelope{"repository": repo, "pr_number": number}
	binding := s.resolvePRBinding(ctx, prKey, "pull_request", envelope)
	if strings.TrimSpace(binding.ThreadID) == "" {
		return "", fmt.Errorf("no thread found for %s — post a card first", prKey)
	}
	if s.prQueues != nil {
		if err := s.prQueues.Unblock(ctx, prKey); err != nil {
			return "", err
		}
	}
	s.clearEscalated(prKey)
	s.reopenPRSurface(prKey)
	return "binding re-established", nil
}

func (s *Server) effectMergeNow(ctx context.Context, req FireActionRequest, v *actionView) (string, error) {
	prKey := v.prKey()
	repo, number, _ := splitPRKey(prKey)
	deliveryID, err := s.enqueueSyntheticRetryKind(ctx, repo, number, "merge", prKey, actionDeliveryPrefix)
	if err != nil {
		return "", err
	}
	s.mergeOverrides.Store(deliveryID, req.Reason)
	return "merge override queued", nil
}

func (s *Server) effectCleanWorkspace(ctx context.Context, req FireActionRequest, v *actionView) (string, error) {
	if v.latest == nil || strings.TrimSpace(v.latest.WorkspacePath) == "" {
		return "", fmt.Errorf("no recorded workspace for %s", v.prKey())
	}
	mode := strings.TrimSpace(req.Params["mode"])
	if mode == "" {
		mode = "snapshot"
	}
	wt := v.latest.WorkspacePath
	switch mode {
	case "snapshot":
		if _, err := runGitIn(ctx, wt, "add", "-A"); err != nil {
			return "", fmt.Errorf("snapshot add failed: %w", err)
		}
		if _, err := runGitIn(ctx, wt, "stash", "push", "-u", "-m", "occa clean_workspace "+time.Now().UTC().Format("2006-01-02 15:04:05")); err != nil {
			return "", fmt.Errorf("snapshot failed: %w", err)
		}
	case "discard":
		if _, err := runGitIn(ctx, wt, "reset", "--hard"); err != nil {
			return "", fmt.Errorf("discard reset failed: %w", err)
		}
		if _, err := runGitIn(ctx, wt, "clean", "-fd"); err != nil {
			return "", fmt.Errorf("discard clean failed: %w", err)
		}
	default:
		return "", fmt.Errorf("mode must be snapshot or discard")
	}
	if s.prQueues != nil {
		if err := s.prQueues.Unblock(ctx, v.prKey()); err != nil {
			return "", err
		}
	}
	s.clearEscalated(v.prKey())
	s.reopenPRSurface(v.prKey())
	return "workspace " + mode + " done — blocker released", nil
}

func (s *Server) notifyActionThread(ctx context.Context, prKey, line string) {
	if line == "" {
		return
	}
	threadID := ""
	if s.prQueues != nil {
		if row, err := s.prQueues.Get(ctx, prKey); err == nil && row != nil {
			threadID = strings.TrimSpace(row.ThreadID)
		}
	}
	if threadID == "" {
		if rec, ok := s.tasks.ResolvePRThread(prKey); ok {
			threadID = strings.TrimSpace(rec.ThreadID)
		}
	}
	if threadID == "" {
		return
	}
	ep, ok := s.reviewEndpointForPR(prKey)
	if !ok {
		for _, candidate := range s.endpoints {
			repo, _, _ := splitPRKey(prKey)
			if normalizeRepoKey(candidate.Repository) == normalizeRepoKey(repo) {
				ep = candidate
				ok = true
				break
			}
		}
	}
	if !ok {
		return
	}
	if err := s.notifyThread(ctx, ep.Platform, threadTargetFor(ep.Platform, ep.ChannelID, threadID), line); err != nil {
		slog.Warn("action: thread line failed", "pr_key", prKey, "error", err)
	}
}

func (s *Server) commentOnPR(ctx context.Context, repo string, number int, body string) {
	if body = strings.TrimSpace(body); body == "" || number <= 0 {
		return
	}
	if _, err := runGH(ctx, "pr", "comment", fmt.Sprint(number), "--repo", repo, "--body", body); err != nil {
		slog.Warn("action: pr comment failed", "repo", repo, "number", number, "error", err)
	}
}

func runGitIn(ctx context.Context, dir string, args ...string) ([]byte, error) {
	full := append([]string{"-C", dir}, args...)
	cmd := exec.CommandContext(ctx, "git", full...)
	var stderr bytes.Buffer
	cmd.Stderr = &stderr
	out, err := cmd.Output()
	if err != nil {
		detail := strings.TrimSpace(stderr.String())
		if detail == "" {
			detail = err.Error()
		}
		return nil, fmt.Errorf("git %s: %s", args[0], detail)
	}
	return out, nil
}
