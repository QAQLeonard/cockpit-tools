package main

import (
	"context"
	"strings"
	"sync"
)

type requestPoolDiagnosticsKey struct{}

// Priority tiers and credential retries deliberately narrow the candidate set.
// Their failures describe an attempt, not the final health of the request pool.
// Keep the latest detail on the HTTP request until its final outcome is known.
type requestPoolDiagnostics struct {
	mu        sync.Mutex
	requestID string
	pending   *requestDiagnosticPayload
	finished  bool
	recovered bool
}

// The pool is selected as "mixed", while an actual credential result names
// its backend (for example "codex"). Report successful request-level recovery
// under the pool route too, without attributing it to any individual account.
// Existing hosts can then clear an older mixed-pool failure and broadcast only
// when health actually changes, rather than refreshing the UI on every request.
func emitRequestPoolRecovery(ctx context.Context, emitter *eventEmitter, payload requestDiagnosticPayload) {
	state, _ := ctx.Value(requestPoolDiagnosticsKey{}).(*requestPoolDiagnostics)
	if state == nil || emitter == nil || state.requestID != payload.RequestID {
		return
	}
	state.mu.Lock()
	defer state.mu.Unlock()
	if state.recovered {
		return
	}
	state.recovered = true
	success := true
	payload.Success = &success
	emitter.emit(payload)
}

func withRequestPoolDiagnostics(ctx context.Context, requestID string) context.Context {
	return context.WithValue(ctx, requestPoolDiagnosticsKey{}, &requestPoolDiagnostics{requestID: requestID})
}

func deferRequestPoolDiagnostic(ctx context.Context, emitter *eventEmitter, payload requestDiagnosticPayload) bool {
	state, _ := ctx.Value(requestPoolDiagnosticsKey{}).(*requestPoolDiagnostics)
	if state == nil || state.requestID != payload.RequestID {
		return false
	}
	state.mu.Lock()
	defer state.mu.Unlock()
	if !state.finished {
		state.pending = &payload
	}
	// Retain attempt-level evidence in logs without publishing a pool failure.
	attempt := payload
	attempt.Type = "auth_pool_attempt"
	emitter.emit(attempt)
	return true
}

func finishRequestPoolDiagnostics(ctx context.Context, emitter *eventEmitter, status int, message string) {
	state, _ := ctx.Value(requestPoolDiagnosticsKey{}).(*requestPoolDiagnostics)
	if state == nil {
		return
	}
	state.mu.Lock()
	defer state.mu.Unlock()
	if state.finished {
		return
	}
	state.finished = true
	pending := state.pending
	state.pending = nil
	if pending == nil || emitter == nil || !finalRequestPoolFailure(status, message) {
		return
	}
	emitter.emit(*pending)
}

func finalRequestPoolFailure(status int, message string) bool {
	if status == clientClosedRequestStatus || strings.TrimSpace(message) == "" {
		return false
	}
	if errorCategory(status, message, false) == "no_available_account" {
		return true
	}
	// Gin prefixes its collected errors with "Error #..."; the final error
	// can still be a structured local-selection response behind that prefix.
	if start := strings.Index(message, "{"); start >= 0 {
		return errorCategory(status, strings.TrimSpace(message[start:]), false) == "no_available_account"
	}
	return false
}
