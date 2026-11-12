package server

import (
	"log"
	"strings"
	"time"
)

const (
	defaultDuplicateMACLaneStaleGrace = 5 * time.Second
	defaultDuplicateMACLaneSweep      = time.Second
	duplicateMACLaneCurrentJobReason  = "duplicate_current_job"
)

type macLaneSnapshot struct {
	client *client
	health poolClientJobHealth
}

func startDuplicateMACLaneCleanup(cfg *Config, shutdown <-chan struct{}) {
	if !duplicateMACLaneCleanupEnabled(cfg) {
		return
	}
	interval := duplicateMACLaneSweepInterval(cfg)
	go func() {
		ticker := time.NewTicker(interval)
		defer ticker.Stop()
		for {
			select {
			case <-shutdown:
				return
			case now := <-ticker.C:
				cleanupDuplicateMACLanes(cfg, now, CurrentParent(), "duplicate-mac-lane-janitor")
			}
		}
	}()
}

func cleanupDuplicateMACLanes(cfg *Config, now time.Time, currentParent, trigger string) int {
	if !duplicateMACLaneCleanupEnabled(cfg) {
		return 0
	}
	if now.IsZero() {
		now = time.Now()
	}
	currentParent = normalizeHex(currentParent)
	if currentParent == "" {
		currentParent = normalizeHex(CurrentParent())
	}

	groups := duplicateMACLaneGroups(now, currentParent)
	closed := 0
	grace := duplicateMACLaneStaleGrace(cfg)
	for mac, lanes := range groups {
		if len(lanes) < 2 {
			continue
		}
		keep := preferredMACLane(lanes)
		if keep == nil || !keep.health.Ready {
			clearDuplicateMACStaleObservations(lanes)
			continue
		}
		for _, lane := range lanes {
			if lane.client == nil || lane.client == keep.client {
				continue
			}
			closeReason := duplicateMACLaneCloseReason(lane.health, keep.health)
			if closeReason == "" {
				markDuplicateMACLaneStale(lane.client, false, now, grace)
				continue
			}
			staleFor, eligible := markDuplicateMACLaneStale(lane.client, true, now, grace)
			if !eligible {
				continue
			}
			if duplicateMACLaneCloses != nil {
				duplicateMACLaneCloses.WithLabelValues(
					cleanJobPropagationLabel(closeReason),
					normalizeMetricMAC(mac),
				).Inc()
			}
			log.Printf("[MAC LANE] closing duplicate lane trigger=%s mac=%s close_lane=%s close_addr=%s close_reason=%s stale_for=%s close_seq=%d close_job=%s keep_lane=%s keep_addr=%s keep_seq=%d keep_job=%s current_parent=%s close_parent=%s keep_parent=%s",
				emptyAsUnknown(trigger),
				mac,
				emptyAsUnknown(lane.health.LaneID),
				emptyAsUnknown(lane.health.Address),
				emptyAsUnknown(closeReason),
				staleFor.Round(time.Millisecond),
				lane.health.TemplateSeq,
				emptyAsUnknown(lane.health.CurrentJobID),
				emptyAsUnknown(keep.health.LaneID),
				emptyAsUnknown(keep.health.Address),
				keep.health.TemplateSeq,
				emptyAsUnknown(keep.health.CurrentJobID),
				shortHash(currentParent),
				shortHash(lane.health.Parent),
				shortHash(keep.health.Parent),
			)
			closeClientConnection(lane.client)
			closed++
		}
	}
	if duplicateMACLaneDuplicateGroups != nil {
		duplicateMACLaneDuplicateGroups.Set(float64(len(groups)))
	}
	return closed
}

func normalizeMetricMAC(mac string) string {
	normalized := normalizeASICMAC(mac)
	if normalized == "" {
		return "unknown"
	}
	return normalized
}

func duplicateMACLaneGroups(now time.Time, currentParent string) map[string][]macLaneSnapshot {
	groups := map[string][]macLaneSnapshot{}
	for _, c := range snapshotClients() {
		if clientIsClosed(c) {
			continue
		}
		health := snapshotClientJobHealth(c, now, currentParent)
		mac := normalizeASICMAC(health.ASICMAC)
		if mac == "" {
			continue
		}
		groups[mac] = append(groups[mac], macLaneSnapshot{client: c, health: health})
	}
	for mac, lanes := range groups {
		if len(lanes) < 2 {
			delete(groups, mac)
		}
	}
	return groups
}

func preferredMACLane(lanes []macLaneSnapshot) *macLaneSnapshot {
	var best *macLaneSnapshot
	for i := range lanes {
		lane := &lanes[i]
		if !lane.health.Ready {
			continue
		}
		if best == nil || lanePreferredOver(lane.health, best.health) {
			best = lane
		}
	}
	return best
}

func lanePreferredOver(candidate, current poolClientJobHealth) bool {
	if candidate.TemplateSeq != current.TemplateSeq {
		return candidate.TemplateSeq > current.TemplateSeq
	}
	if candidate.CurrentJobAgeMs != current.CurrentJobAgeMs {
		return candidate.CurrentJobAgeMs < current.CurrentJobAgeMs
	}
	return strings.Compare(candidate.Address, current.Address) < 0
}

func duplicateMACLaneClosable(health poolClientJobHealth) bool {
	if !health.Authorized || !health.Subscribed {
		return false
	}
	switch health.ReasonCode {
	case poolJobHealthStaleCurrentJob, poolJobHealthInvalidatedCurrentJob, poolJobHealthExpiredCurrentJob:
		return true
	default:
		return false
	}
}

func duplicateMACLaneCloseReason(lane, keep poolClientJobHealth) string {
	if duplicateMACLaneClosable(lane) {
		return lane.ReasonCode
	}
	if duplicateMACReadyLaneClosable(lane, keep) {
		return duplicateMACLaneCurrentJobReason
	}
	return ""
}

func duplicateMACReadyLaneClosable(lane, keep poolClientJobHealth) bool {
	if !lane.Authorized || !lane.Subscribed || !lane.Ready || !keep.Ready {
		return false
	}
	if lane.CurrentJobID == "" || keep.CurrentJobID == "" || lane.CurrentJobID != keep.CurrentJobID {
		return false
	}
	if lane.TemplateSeq == 0 || lane.TemplateSeq != keep.TemplateSeq {
		return false
	}
	laneParent := normalizeHex(lane.Parent)
	keepParent := normalizeHex(keep.Parent)
	if laneParent == "" || keepParent == "" || laneParent != keepParent {
		return false
	}
	if lane.Address != "" && keep.Address != "" && lane.Address == keep.Address {
		return false
	}
	return true
}

func clearDuplicateMACStaleObservations(lanes []macLaneSnapshot) {
	for _, lane := range lanes {
		if lane.client != nil {
			markDuplicateMACLaneStale(lane.client, false, time.Time{}, 0)
		}
	}
}

func markDuplicateMACLaneStale(c *client, stale bool, now time.Time, grace time.Duration) (time.Duration, bool) {
	if c == nil {
		return 0, false
	}
	if now.IsZero() {
		now = time.Now()
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	if !stale {
		c.duplicateMACStaleSince = time.Time{}
		return 0, false
	}
	if c.duplicateMACStaleSince.IsZero() {
		c.duplicateMACStaleSince = now
		return 0, grace <= 0
	}
	staleFor := now.Sub(c.duplicateMACStaleSince)
	if staleFor < 0 {
		staleFor = 0
	}
	return staleFor, staleFor >= grace
}

func clientIsClosed(c *client) bool {
	if c == nil {
		return true
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.closed
}

func duplicateMACLaneCleanupEnabled(cfg *Config) bool {
	return cfg != nil && cfg.EnableDuplicateMACLaneCleanup
}

func duplicateMACLaneStaleGrace(cfg *Config) time.Duration {
	if cfg != nil && cfg.DuplicateMACLaneStaleGraceMS > 0 {
		return time.Duration(cfg.DuplicateMACLaneStaleGraceMS) * time.Millisecond
	}
	return defaultDuplicateMACLaneStaleGrace
}

func duplicateMACLaneSweepInterval(cfg *Config) time.Duration {
	grace := duplicateMACLaneStaleGrace(cfg)
	if grace <= 0 {
		return defaultDuplicateMACLaneSweep
	}
	interval := grace / 2
	if interval < defaultDuplicateMACLaneSweep {
		return defaultDuplicateMACLaneSweep
	}
	if interval > 5*time.Second {
		return 5 * time.Second
	}
	return interval
}
