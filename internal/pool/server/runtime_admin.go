package server

import (
	"context"
	"encoding/json"
	"net"
	"net/http"
	"os"
	"strconv"
	"strings"
	"time"
)

func runtimeAdminEnabled() bool {
	raw := strings.TrimSpace(strings.ToLower(os.Getenv("POOL_RUNTIME_ADMIN_ENABLED")))
	return raw == "1" || raw == "true" || raw == "yes"
}

func requestFromLocalNetwork(r *http.Request) bool {
	host, _, err := net.SplitHostPort(r.RemoteAddr)
	if err != nil {
		host = r.RemoteAddr
	}
	ip := net.ParseIP(host)
	return ip != nil && (ip.IsLoopback() || ip.IsPrivate())
}

func requestFromLoopback(r *http.Request) bool {
	host, _, err := net.SplitHostPort(r.RemoteAddr)
	if err != nil {
		host = r.RemoteAddr
	}
	ip := net.ParseIP(host)
	return ip != nil && ip.IsLoopback()
}

func writeSubmissionDrainJSON(w http.ResponseWriter, status int, report submissionDrainReport) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(report)
}

func handleSubmissionDrainAdmin(w http.ResponseWriter, r *http.Request) {
	if !runtimeAdminEnabled() || !requestFromLoopback(r) {
		http.NotFound(w, r)
		return
	}
	if r.Method == http.MethodGet {
		report, err := submissionDrainStatus(r.Context(), activeSubmissionJournal)
		if err != nil {
			writeSubmissionDrainJSON(w, http.StatusServiceUnavailable, report)
			return
		}
		writeSubmissionDrainJSON(w, http.StatusOK, report)
		return
	}
	if r.Method != http.MethodPost {
		w.Header().Set("Allow", http.MethodGet+", "+http.MethodPost)
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
		return
	}
	operationID := strings.TrimSpace(r.URL.Query().Get("operation_id"))
	switch strings.TrimSpace(strings.ToLower(r.URL.Query().Get("action"))) {
	case "pause":
		wait := defaultSubmissionDrainWait
		if raw := strings.TrimSpace(r.URL.Query().Get("timeout_ms")); raw != "" {
			ms, err := strconv.Atoi(raw)
			if err != nil || time.Duration(ms)*time.Millisecond < minimumSubmissionDrainWait || time.Duration(ms)*time.Millisecond > maximumSubmissionDrainWait {
				http.Error(w, "timeout_ms must be between 100 and 600000", http.StatusBadRequest)
				return
			}
			wait = time.Duration(ms) * time.Millisecond
		}
		ctx, cancel := context.WithTimeout(r.Context(), wait)
		report, err := waitSubmissionDrain(ctx, activeSubmissionJournal, operationID)
		cancel()
		if err != nil {
			status := http.StatusConflict
			if report.Classification == "submission_journal_unavailable" {
				status = http.StatusServiceUnavailable
			}
			writeSubmissionDrainJSON(w, status, report)
			return
		}
		writeSubmissionDrainJSON(w, http.StatusOK, report)
	case "resume":
		if err := activeSubmissionDrain.resume(operationID); err != nil {
			report, _ := submissionDrainStatus(r.Context(), activeSubmissionJournal)
			report.Classification = "drain_owner_mismatch"
			report.Message = err.Error()
			writeSubmissionDrainJSON(w, http.StatusConflict, report)
			return
		}
		report, err := submissionDrainStatus(r.Context(), activeSubmissionJournal)
		if err != nil {
			writeSubmissionDrainJSON(w, http.StatusServiceUnavailable, report)
			return
		}
		report.Classification = "resumed"
		report.Message = "candidate ingress and job publication resumed"
		writeSubmissionDrainJSON(w, http.StatusOK, report)
	default:
		http.Error(w, "action must be pause or resume", http.StatusBadRequest)
	}
}

func handleTemplateTTLRefreshAdmin(w http.ResponseWriter, r *http.Request) {
	if !runtimeAdminEnabled() || !requestFromLocalNetwork(r) {
		http.NotFound(w, r)
		return
	}
	if r.Method != http.MethodPost {
		w.Header().Set("Allow", http.MethodPost)
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
		return
	}
	raw := strings.TrimSpace(r.URL.Query().Get("ms"))
	ms, err := strconv.Atoi(raw)
	if err != nil || ms <= 0 {
		http.Error(w, "invalid ms", http.StatusBadRequest)
		return
	}
	interval := time.Duration(ms) * time.Millisecond
	setTemplateTTLRefreshOverride(interval)
	publishBlockTimingControllerRuntime()
	w.Header().Set("Content-Type", "application/json")
	_ = json.NewEncoder(w).Encode(map[string]any{
		"ok":                      true,
		"template_ttl_refresh_ms": int(configuredTemplateTTLRefresh() / time.Millisecond),
	})
}

func handleBlockCandidateJobAgeAdmin(w http.ResponseWriter, r *http.Request) {
	if !runtimeAdminEnabled() || !requestFromLocalNetwork(r) {
		http.NotFound(w, r)
		return
	}
	if r.Method != http.MethodPost {
		w.Header().Set("Allow", http.MethodPost)
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
		return
	}
	raw := strings.TrimSpace(r.URL.Query().Get("ms"))
	ms, err := strconv.Atoi(raw)
	if err != nil || ms < 0 {
		http.Error(w, "invalid ms", http.StatusBadRequest)
		return
	}
	setBlockCandidateJobAgeOverride(time.Duration(ms) * time.Millisecond)
	publishBlockTimingControllerRuntime()
	w.Header().Set("Content-Type", "application/json")
	_ = json.NewEncoder(w).Encode(map[string]any{
		"ok":                         true,
		"block_candidate_job_age_ms": int(maxBlockCandidateJobAge(nil) / time.Millisecond),
	})
}

func handleAllowMultipleBlockCandidatesAdmin(w http.ResponseWriter, r *http.Request) {
	if !runtimeAdminEnabled() || !requestFromLocalNetwork(r) {
		http.NotFound(w, r)
		return
	}
	if r.Method != http.MethodPost {
		w.Header().Set("Allow", http.MethodPost)
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
		return
	}
	raw := strings.TrimSpace(strings.ToLower(r.URL.Query().Get("enabled")))
	var value bool
	switch raw {
	case "1", "true", "yes", "on":
		value = true
	case "0", "false", "no", "off":
		value = false
	case "reset":
		setAllowMultipleBlockCandidatesOverride(false, false)
		publishBlockTimingControllerRuntime()
		w.Header().Set("Content-Type", "application/json")
		_ = json.NewEncoder(w).Encode(map[string]any{
			"ok":                        true,
			"override_enabled":          false,
			"allow_multiple_block_jobs": shouldAllowMultipleBlockCandidatesPerJob(nil),
		})
		return
	default:
		http.Error(w, "invalid enabled", http.StatusBadRequest)
		return
	}
	setAllowMultipleBlockCandidatesOverride(value, true)
	publishBlockTimingControllerRuntime()
	w.Header().Set("Content-Type", "application/json")
	_ = json.NewEncoder(w).Encode(map[string]any{
		"ok":                        true,
		"override_enabled":          true,
		"allow_multiple_block_jobs": shouldAllowMultipleBlockCandidatesPerJob(nil),
	})
}
