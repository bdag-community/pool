package server

import (
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"
)

func TestTemplateTTLRefreshAdminDisabledByDefault(t *testing.T) {
	t.Setenv("POOL_RUNTIME_ADMIN_ENABLED", "")
	req := httptest.NewRequest(http.MethodPost, "/admin/template-ttl-refresh?ms=2000", nil)
	req.RemoteAddr = "127.0.0.1:12345"
	rec := httptest.NewRecorder()

	handleTemplateTTLRefreshAdmin(rec, req)

	if rec.Code != http.StatusNotFound {
		t.Fatalf("status = %d, want 404", rec.Code)
	}
}

func TestTemplateTTLRefreshAdminUpdatesOverride(t *testing.T) {
	previousOverride := templateTTLRefreshOverride.Load()
	t.Cleanup(func() {
		templateTTLRefreshOverride.Store(previousOverride)
	})
	templateTTLRefreshOverride.Store(0)
	t.Setenv("POOL_RUNTIME_ADMIN_ENABLED", "true")
	req := httptest.NewRequest(http.MethodPost, "/admin/template-ttl-refresh?ms=2000", nil)
	req.RemoteAddr = "127.0.0.1:12345"
	rec := httptest.NewRecorder()

	handleTemplateTTLRefreshAdmin(rec, req)

	if rec.Code != http.StatusOK {
		t.Fatalf("status = %d body=%q, want 200", rec.Code, rec.Body.String())
	}
	if got := time.Duration(templateTTLRefreshOverride.Load()); got != 2*time.Second {
		t.Fatalf("override = %s, want 2s", got)
	}
	if !strings.Contains(rec.Body.String(), `"template_ttl_refresh_ms":2000`) {
		t.Fatalf("response body = %q, want updated TTL", rec.Body.String())
	}
}

func TestTemplateTTLRefreshAdminRequiresPost(t *testing.T) {
	t.Setenv("POOL_RUNTIME_ADMIN_ENABLED", "true")
	req := httptest.NewRequest(http.MethodGet, "/admin/template-ttl-refresh?ms=2000", nil)
	req.RemoteAddr = "127.0.0.1:12345"
	rec := httptest.NewRecorder()

	handleTemplateTTLRefreshAdmin(rec, req)

	if rec.Code != http.StatusMethodNotAllowed {
		t.Fatalf("status = %d, want 405", rec.Code)
	}
}

func TestBlockCandidateJobAgeAdminUpdatesOverride(t *testing.T) {
	previousEnabled := blockCandidateJobAgeOverrideEnabled.Load()
	previousMS := blockCandidateJobAgeOverrideMS.Load()
	t.Cleanup(func() {
		blockCandidateJobAgeOverrideMS.Store(previousMS)
		blockCandidateJobAgeOverrideEnabled.Store(previousEnabled)
	})
	setBlockCandidateJobAgeOverride(-time.Millisecond)
	t.Setenv("POOL_RUNTIME_ADMIN_ENABLED", "true")
	req := httptest.NewRequest(http.MethodPost, "/admin/block-candidate-job-age?ms=2500", nil)
	req.RemoteAddr = "127.0.0.1:12345"
	rec := httptest.NewRecorder()

	handleBlockCandidateJobAgeAdmin(rec, req)

	if rec.Code != http.StatusOK {
		t.Fatalf("status = %d body=%q, want 200", rec.Code, rec.Body.String())
	}
	if got := maxBlockCandidateJobAge(&Config{MaxBlockCandidateJobAgeMS: 1200}); got != 2500*time.Millisecond {
		t.Fatalf("maxBlockCandidateJobAge() = %s, want 2500ms", got)
	}
	if !strings.Contains(rec.Body.String(), `"block_candidate_job_age_ms":2500`) {
		t.Fatalf("response body = %q, want updated age", rec.Body.String())
	}
}

func TestBlockCandidateJobAgeAdminAllowsDisable(t *testing.T) {
	previousEnabled := blockCandidateJobAgeOverrideEnabled.Load()
	previousMS := blockCandidateJobAgeOverrideMS.Load()
	t.Cleanup(func() {
		blockCandidateJobAgeOverrideMS.Store(previousMS)
		blockCandidateJobAgeOverrideEnabled.Store(previousEnabled)
	})
	t.Setenv("POOL_RUNTIME_ADMIN_ENABLED", "true")
	req := httptest.NewRequest(http.MethodPost, "/admin/block-candidate-job-age?ms=0", nil)
	req.RemoteAddr = "127.0.0.1:12345"
	rec := httptest.NewRecorder()

	handleBlockCandidateJobAgeAdmin(rec, req)

	if rec.Code != http.StatusOK {
		t.Fatalf("status = %d body=%q, want 200", rec.Code, rec.Body.String())
	}
	if got := maxBlockCandidateJobAge(&Config{MaxBlockCandidateJobAgeMS: 1200}); got != 0 {
		t.Fatalf("maxBlockCandidateJobAge() = %s, want disabled guard", got)
	}
}

func TestAllowMultipleBlockCandidatesAdminUpdatesOverride(t *testing.T) {
	previousEnabled := allowMultipleBlockOverrideEnabled.Load()
	previousValue := allowMultipleBlockOverrideValue.Load()
	t.Cleanup(func() {
		allowMultipleBlockOverrideValue.Store(previousValue)
		allowMultipleBlockOverrideEnabled.Store(previousEnabled)
	})
	setAllowMultipleBlockCandidatesOverride(false, false)
	t.Setenv("POOL_RUNTIME_ADMIN_ENABLED", "true")
	req := httptest.NewRequest(http.MethodPost, "/admin/allow-multiple-block-candidates?enabled=true", nil)
	req.RemoteAddr = "127.0.0.1:12345"
	rec := httptest.NewRecorder()

	handleAllowMultipleBlockCandidatesAdmin(rec, req)

	if rec.Code != http.StatusOK {
		t.Fatalf("status = %d body=%q, want 200", rec.Code, rec.Body.String())
	}
	if !shouldAllowMultipleBlockCandidatesPerJob(&Config{}) {
		t.Fatal("allow multiple override should be enabled")
	}
	if !strings.Contains(rec.Body.String(), `"allow_multiple_block_jobs":true`) {
		t.Fatalf("response body = %q, want enabled value", rec.Body.String())
	}
}

func TestAllowMultipleBlockCandidatesAdminReset(t *testing.T) {
	previousEnabled := allowMultipleBlockOverrideEnabled.Load()
	previousValue := allowMultipleBlockOverrideValue.Load()
	t.Cleanup(func() {
		allowMultipleBlockOverrideValue.Store(previousValue)
		allowMultipleBlockOverrideEnabled.Store(previousEnabled)
	})
	setAllowMultipleBlockCandidatesOverride(true, true)
	t.Setenv("POOL_RUNTIME_ADMIN_ENABLED", "true")
	req := httptest.NewRequest(http.MethodPost, "/admin/allow-multiple-block-candidates?enabled=reset", nil)
	req.RemoteAddr = "127.0.0.1:12345"
	rec := httptest.NewRecorder()

	handleAllowMultipleBlockCandidatesAdmin(rec, req)

	if rec.Code != http.StatusOK {
		t.Fatalf("status = %d body=%q, want 200", rec.Code, rec.Body.String())
	}
	if shouldAllowMultipleBlockCandidatesPerJob(&Config{}) {
		t.Fatal("reset should return to Config default false")
	}
	if !shouldAllowMultipleBlockCandidatesPerJob(&Config{AllowMultipleBlockCandidatesPerJob: true}) {
		t.Fatal("reset should expose Config value again")
	}
}
