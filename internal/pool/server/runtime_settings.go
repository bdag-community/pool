package server

import (
	"log"
	"os"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"
)

const (
	dynamicBlockCandidateAgeIncreaseMargin = 500 * time.Millisecond
	dynamicBlockCandidateAgeDecreaseStep   = 250 * time.Millisecond
	defaultDynamicBlockCandidateAgeMax     = 8 * time.Second
	absoluteDynamicBlockCandidateAgeMax    = 30 * time.Second
)

var (
	blockCandidateJobAgeOverrideEnabled atomic.Bool
	blockCandidateJobAgeOverrideMS      atomic.Int64
	allowMultipleBlockOverrideEnabled   atomic.Bool
	allowMultipleBlockOverrideValue     atomic.Bool
	recentStaleGraceOverrideEnabled     atomic.Bool
	recentStaleGraceOverrideMS          atomic.Int64
	blockCandidateAgeControlMu          sync.Mutex

	gbtMinIntervalOnce         sync.Once
	gbtMinIntervalConfigured   time.Duration
	gbtPressureIntervalOnce    sync.Once
	gbtPressureInterval        time.Duration
	gbtPressureWindowOnce      sync.Once
	gbtPressureWindow          time.Duration
	templateTTLRefreshOnce     sync.Once
	templateTTLRefreshDur      time.Duration
	templateTTLRefreshOverride atomic.Int64
	templatePressureUntil      atomic.Int64
	blockCandidateAgeMaxOnce   sync.Once
	blockCandidateAgeMaxDur    time.Duration
	wsBackoffOnce              sync.Once
	wsBackoffConfigured        time.Duration
)

func shouldAllowMultipleBlockCandidatesPerJob(cfg *Config) bool {
	if allowMultipleBlockOverrideEnabled.Load() {
		return allowMultipleBlockOverrideValue.Load()
	}
	return cfg != nil && cfg.AllowMultipleBlockCandidatesPerJob
}

func setAllowMultipleBlockCandidatesOverride(value bool, enabled bool) {
	allowMultipleBlockOverrideValue.Store(value)
	allowMultipleBlockOverrideEnabled.Store(enabled)
}

func currentRecentStaleBlockCandidateSubmitGrace(cfg *Config) time.Duration {
	if recentStaleGraceOverrideEnabled.Load() {
		ms := recentStaleGraceOverrideMS.Load()
		if ms <= 0 {
			return 0
		}
		return time.Duration(ms) * time.Millisecond
	}
	if cfg == nil || cfg.RecentStaleBlockCandidateSubmitGraceMS <= 0 {
		return 0
	}
	return time.Duration(cfg.RecentStaleBlockCandidateSubmitGraceMS) * time.Millisecond
}

func setRecentStaleBlockCandidateSubmitGraceOverride(grace time.Duration, enabled bool) {
	if grace < 0 {
		grace = 0
	}
	recentStaleGraceOverrideMS.Store(int64(grace / time.Millisecond))
	recentStaleGraceOverrideEnabled.Store(enabled)
}

func recordBlockCandidateAgeFeedback(cfg *Config, reason string, jobAge time.Duration, now time.Time) {
	if cfg == nil || !cfg.AutoTuneBlockCandidateJobAge || jobAge <= 0 {
		return
	}
	base := configuredBlockCandidateJobAge(cfg)
	if base <= 0 {
		return
	}
	if now.IsZero() {
		now = time.Now()
	}
	current := maxBlockCandidateJobAge(cfg)
	if current <= 0 {
		current = base
	}

	var target time.Duration
	switch reason {
	case "expired", "old-template-sequence":
		target = jobAge + dynamicBlockCandidateAgeIncreaseMargin
		if target <= current {
			return
		}
	case "tip-overdue", "stale-parent":
		if current <= base {
			return
		}
		target = current - dynamicBlockCandidateAgeDecreaseStep
	default:
		return
	}

	if target < base {
		target = base
	}
	maxAge := configuredDynamicBlockCandidateAgeMax()
	if target > maxAge {
		target = maxAge
	}
	if target == current {
		return
	}

	blockCandidateAgeControlMu.Lock()
	defer blockCandidateAgeControlMu.Unlock()

	current = maxBlockCandidateJobAge(cfg)
	if current <= 0 {
		current = base
	}
	if reason == "expired" || reason == "old-template-sequence" {
		if target <= current {
			return
		}
	} else if target >= current {
		return
	}

	setBlockCandidateJobAgeOverride(target)
	log.Printf("[CONTROL] block candidate job age adjusted reason=%s job_age=%s old=%s new=%s",
		reason, jobAge.Round(time.Millisecond), current.Round(time.Millisecond), target.Round(time.Millisecond))
}

func configuredDynamicBlockCandidateAgeMax() time.Duration {
	blockCandidateAgeMaxOnce.Do(func() {
		blockCandidateAgeMaxDur = defaultDynamicBlockCandidateAgeMax
		raw := strings.TrimSpace(os.Getenv("POOL_AUTO_TUNE_BLOCK_CANDIDATE_MAX_AGE_MS"))
		if raw == "" {
			return
		}
		ms, err := strconv.Atoi(raw)
		if err != nil || ms <= 0 {
			log.Printf("Invalid POOL_AUTO_TUNE_BLOCK_CANDIDATE_MAX_AGE_MS=%q; using %s", raw, defaultDynamicBlockCandidateAgeMax)
			return
		}
		configured := time.Duration(ms) * time.Millisecond
		if configured < minGBTMinInterval {
			configured = minGBTMinInterval
		}
		if configured > absoluteDynamicBlockCandidateAgeMax {
			log.Printf("POOL_AUTO_TUNE_BLOCK_CANDIDATE_MAX_AGE_MS=%d above cap %s; using %s", ms, absoluteDynamicBlockCandidateAgeMax, absoluteDynamicBlockCandidateAgeMax)
			configured = absoluteDynamicBlockCandidateAgeMax
		}
		blockCandidateAgeMaxDur = configured
		log.Printf("[CONTROL] block candidate job age auto-tune max set to %s", configured)
	})
	return blockCandidateAgeMaxDur
}

func maxBlockCandidateJobAge(cfg *Config) time.Duration {
	if blockCandidateJobAgeOverrideEnabled.Load() {
		ms := blockCandidateJobAgeOverrideMS.Load()
		if ms <= 0 {
			return 0
		}
		return time.Duration(ms) * time.Millisecond
	}
	return configuredBlockCandidateJobAge(cfg)
}

func configuredBlockCandidateJobAge(cfg *Config) time.Duration {
	if cfg == nil || cfg.MaxBlockCandidateJobAgeMS <= 0 {
		return 0
	}
	return time.Duration(cfg.MaxBlockCandidateJobAgeMS) * time.Millisecond
}

func setBlockCandidateJobAgeOverride(age time.Duration) {
	if age < 0 {
		blockCandidateJobAgeOverrideMS.Store(0)
		blockCandidateJobAgeOverrideEnabled.Store(false)
		return
	}
	blockCandidateJobAgeOverrideMS.Store(int64(age / time.Millisecond))
	blockCandidateJobAgeOverrideEnabled.Store(true)
}

func configuredGBTMinInterval() time.Duration {
	gbtMinIntervalOnce.Do(func() {
		gbtMinIntervalConfigured = defaultGBTMinInterval
		raw := strings.TrimSpace(os.Getenv("POOL_GBT_MIN_INTERVAL_MS"))
		if raw == "" {
			return
		}
		ms, err := strconv.Atoi(raw)
		if err != nil || ms <= 0 {
			log.Printf("Invalid POOL_GBT_MIN_INTERVAL_MS=%q; using %s", raw, defaultGBTMinInterval)
			return
		}
		configured := time.Duration(ms) * time.Millisecond
		if configured < minGBTMinInterval {
			log.Printf("POOL_GBT_MIN_INTERVAL_MS=%d below minimum %s; using %s", ms, minGBTMinInterval, minGBTMinInterval)
			configured = minGBTMinInterval
		}
		gbtMinIntervalConfigured = configured
		log.Printf("[POLL] getBlockTemplate minimum interval set to %s", configured)
	})
	return gbtMinIntervalConfigured
}

func configuredGBTPressureInterval() time.Duration {
	gbtPressureIntervalOnce.Do(func() {
		gbtPressureInterval = defaultGBTPressureInterval
		raw := strings.TrimSpace(os.Getenv("POOL_GBT_PRESSURE_INTERVAL_MS"))
		if raw == "" {
			return
		}
		ms, err := strconv.Atoi(raw)
		if err != nil || ms <= 0 {
			log.Printf("Invalid POOL_GBT_PRESSURE_INTERVAL_MS=%q; using %s", raw, defaultGBTPressureInterval)
			return
		}
		configured := time.Duration(ms) * time.Millisecond
		if configured < minGBTMinInterval {
			log.Printf("POOL_GBT_PRESSURE_INTERVAL_MS=%d below minimum %s; using %s", ms, minGBTMinInterval, minGBTMinInterval)
			configured = minGBTMinInterval
		}
		gbtPressureInterval = configured
		log.Printf("[POLL] getBlockTemplate pressure interval set to %s", configured)
	})
	return gbtPressureInterval
}

func configuredGBTPressureWindow() time.Duration {
	gbtPressureWindowOnce.Do(func() {
		gbtPressureWindow = defaultGBTPressureWindow
		raw := strings.TrimSpace(os.Getenv("POOL_GBT_PRESSURE_WINDOW_SECONDS"))
		if raw == "" {
			return
		}
		seconds, err := strconv.Atoi(raw)
		if err != nil || seconds < 0 {
			log.Printf("Invalid POOL_GBT_PRESSURE_WINDOW_SECONDS=%q; using %s", raw, defaultGBTPressureWindow)
			return
		}
		gbtPressureWindow = time.Duration(seconds) * time.Second
	})
	return gbtPressureWindow
}

func configuredTemplateTTLRefresh() time.Duration {
	if override := templateTTLRefreshOverride.Load(); override > 0 {
		return time.Duration(override)
	}
	templateTTLRefreshOnce.Do(func() {
		templateTTLRefreshDur = defaultTemplateTTLDur
		raw := strings.TrimSpace(os.Getenv("POOL_TEMPLATE_TTL_REFRESH_MS"))
		if raw == "" {
			return
		}
		ms, err := strconv.Atoi(raw)
		if err != nil || ms <= 0 {
			log.Printf("Invalid POOL_TEMPLATE_TTL_REFRESH_MS=%q; using %s", raw, defaultTemplateTTLDur)
			return
		}
		configured := time.Duration(ms) * time.Millisecond
		if configured < minGBTMinInterval {
			log.Printf("POOL_TEMPLATE_TTL_REFRESH_MS=%d below minimum %s; using %s", ms, minGBTMinInterval, minGBTMinInterval)
			configured = minGBTMinInterval
		}
		if configured > maxFastDAGTemplateTTLDur {
			// BDAG currently advances around once per second. Letting an env
			// override stretch same-parent refresh to many seconds caused six
			// X100s to keep hashing stale work during transient WS/RPC gaps. Cap
			// this path so a bad runtime value cannot silently lower paid blocks.
			log.Printf("POOL_TEMPLATE_TTL_REFRESH_MS=%d above fast-DAG cap %s; using %s", ms, maxFastDAGTemplateTTLDur, maxFastDAGTemplateTTLDur)
			configured = maxFastDAGTemplateTTLDur
		}
		templateTTLRefreshDur = configured
		log.Printf("[POLL] same-parent template refresh TTL set to %s", configured)
	})
	return templateTTLRefreshDur
}

func setTemplateTTLRefreshOverride(interval time.Duration) {
	if interval <= 0 {
		templateTTLRefreshOverride.Store(0)
		return
	}
	if interval < minGBTMinInterval {
		interval = minGBTMinInterval
	}
	templateTTLRefreshOverride.Store(int64(interval))
}

func currentGBTPollInterval() time.Duration {
	normal := configuredGBTMinInterval()
	if !templatePressureActive(time.Now()) {
		return normal
	}
	pressure := configuredGBTPressureInterval()
	if pressure <= 0 || pressure >= normal {
		return normal
	}
	return pressure
}

func recordTemplatePressure(reason string) {
	window := configuredGBTPressureWindow()
	if window <= 0 {
		return
	}
	templatePressureUntil.Store(time.Now().Add(window).UnixNano())
	clearCachedBlockTemplate()
	logTemplateRefresh("template pressure: "+reason, miningWork.currentTemplateSeq(), CurrentParent())
}

func clearTemplatePressure() {
	templatePressureUntil.Store(0)
}

func templatePressureActive(now time.Time) bool {
	until := templatePressureUntil.Load()
	return until > 0 && now.Before(time.Unix(0, until))
}

func configuredWSUnsupportedBackoff() time.Duration {
	wsBackoffOnce.Do(func() {
		wsBackoffConfigured = defaultWSUnsupportedBackoff
		raw := strings.TrimSpace(os.Getenv("POOL_WS_UNSUPPORTED_BACKOFF_SECONDS"))
		if raw == "" {
			return
		}
		seconds, err := strconv.Atoi(raw)
		if err != nil || seconds < 0 {
			log.Printf("Invalid POOL_WS_UNSUPPORTED_BACKOFF_SECONDS=%q; using %s", raw, defaultWSUnsupportedBackoff)
			return
		}
		wsBackoffConfigured = time.Duration(seconds) * time.Second
	})
	return wsBackoffConfigured
}
