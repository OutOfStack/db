package tiered

import "time"

// Stats is a point-in-time snapshot of engine counters, used for observability
// and tests.
type Stats struct {
	Keys        int
	LiveBytes   int64
	DiskBytes   int64
	Segments    int
	Hits        uint64
	Misses      uint64
	Compactions uint64
}

// Stats returns a snapshot of engine metrics.
func (e *Engine) Stats() Stats {
	e.mu.Lock()
	defer e.mu.Unlock()
	return Stats{
		Keys:        e.keyCount(),
		LiveBytes:   e.liveBytes,
		DiskBytes:   e.store.diskBytes(),
		Segments:    len(e.store.sizes),
		Hits:        e.hits,
		Misses:      e.misses,
		Compactions: e.compactions,
	}
}

// maintenanceLoop periodically compacts a reclaimable segment and logs stats.
func (e *Engine) maintenanceLoop(interval time.Duration) {
	ticker := time.NewTicker(interval)
	defer ticker.Stop()
	for {
		select {
		case <-e.done:
			return
		case <-ticker.C:
			e.Compact()
			e.logStats()
		}
	}
}

func (e *Engine) logStats() {
	s := e.Stats()
	total := s.Hits + s.Misses
	var hitRate float64
	if total > 0 {
		hitRate = float64(s.Hits) / float64(total)
	}
	e.logger.Info("Tiered engine stats",
		"keys", s.Keys, "live_bytes", s.LiveBytes, "disk_bytes", s.DiskBytes,
		"segments", s.Segments, "cache_hit_rate", hitRate, "compactions", s.Compactions)
}

// Compact runs one compaction pass immediately, independent of the background
// interval: it reclaims the oldest sealed segment whose dead-bytes ratio exceeds
// the threshold, rewriting its live records into the active segment. Useful for
// operators and for deterministic tests.
//
// ponytail: whole compaction runs under the engine mutex, so it stalls writes
// while a segment is rewritten. Correct and simple; go two-phase (read sealed
// segments lock-free, swap keydir pointers under a short lock) if write-latency
// spikes during compaction ever matter.
func (e *Engine) Compact() {
	e.mu.Lock()
	defer e.mu.Unlock()

	seg, ok := e.pickCompactible()
	if !ok {
		return
	}
	if err := e.compactSegment(seg); err != nil {
		e.logger.Error("Compaction failed", "segment", seg, "error", err)
		return
	}
	e.compactions++
}

// pickCompactible returns the oldest sealed segment past the dead-bytes
// threshold. The active segment is never chosen: it is still being appended to.
func (e *Engine) pickCompactible() (uint32, bool) {
	for _, seg := range e.store.segments() {
		if seg == e.store.activeSeg {
			continue
		}
		size := e.store.sizes[seg]
		if size == 0 {
			continue
		}
		dead := size - e.segLive[seg]
		if float64(dead)/float64(size) >= e.threshold {
			return seg, true
		}
	}
	return 0, false
}

// compactSegment rewrites the live records of seg into the active segment and
// deletes seg. A record is live iff the keydir still points to its exact
// (segment, offset); everything else is a superseded overwrite or a tombstone.
func (e *Engine) compactSegment(seg uint32) error {
	var writeErr error
	scanErr := e.store.scanSegment(seg, false, func(rec decoded, recPos int64) {
		if writeErr != nil || rec.tombstone {
			return
		}
		location, ok := e.lookup(rec.table, rec.key)
		if !ok || location.seg != seg || location.valPos != valPosFor(recPos, rec.table, rec.key) {
			return // dead: overwritten or deleted since it was written
		}
		newRec := encodeRecord(rec.table, rec.key, rec.value, false)
		newSeg, newRecPos, err := e.store.append(newRec)
		if err != nil {
			writeErr = err
			return
		}
		// Net-zero for liveBytes: the rewritten record encodes identically.
		e.dropLive(rec.table, rec.key)
		e.setLoc(newSeg, rec.table, rec.key, len(rec.value), newRecPos, int64(len(newRec)))
	})
	if scanErr != nil {
		return scanErr
	}
	if writeErr != nil {
		return writeErr
	}
	// The rewritten records must be durable before the only other copy is
	// unlinked: a crash between the two would lose keys that were already durable.
	// This is unconditional except under SyncNo, where durability is opted out of.
	if err := e.store.syncActive(); err != nil {
		return err
	}
	if err := e.store.removeSegment(seg); err != nil {
		return err
	}
	delete(e.segLive, seg)
	return nil
}
