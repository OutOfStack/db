package tiered

import (
	"os"
	"time"
)

// Stats is a point-in-time snapshot of engine counters, used for observability and tests.
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
	e.mu.RLock()
	defer e.mu.RUnlock()
	return Stats{
		Keys:        e.keyCount(),
		LiveBytes:   e.liveBytes,
		DiskBytes:   e.store.diskBytes(),
		Segments:    len(e.store.sizes),
		Hits:        e.hits.Load(),
		Misses:      e.misses.Load(),
		Compactions: e.compactions.Load(),
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

// Compact runs one compaction pass immediately, independent of the background interval: it reclaims the oldest sealed
// segment whose dead-bytes ratio exceeds the threshold, rewriting its live records into the active segment. Useful for
// operators and for deterministic tests.
func (e *Engine) Compact() {
	seg, file, ok := e.beginCompaction()
	if !ok {
		return
	}
	defer e.endCompaction()

	var rewriteErr error
	_, scanErr := scanPinnedSegment(seg, file, false, func(rec decoded, recPos int64) {
		if rewriteErr != nil {
			return
		}
		e.mu.Lock()
		if !e.closed {
			rewriteErr = e.rewriteRecord(seg, rec, recPos)
		}
		e.mu.Unlock()
	})
	e.store.unpin(seg)
	if scanErr != nil || rewriteErr != nil {
		if scanErr == nil {
			scanErr = rewriteErr
		}
		e.logger.Error("Compaction failed", "segment", seg, "error", scanErr)
		return
	}
	if err := e.finishCompaction(seg); err != nil {
		e.logger.Error("Compaction failed", "segment", seg, "error", err)
	}
}

// beginCompaction claims the compaction slot and picks a segment to reclaim. Only one pass runs at a time: two passes
// over the same segment would let one delete the file the other is still reading.
func (e *Engine) beginCompaction() (uint32, *os.File, bool) {
	e.mu.Lock()
	defer e.mu.Unlock()

	if e.compacting || e.closed {
		return 0, nil, false
	}
	seg, ok := e.pickCompactible()
	if !ok {
		return 0, nil, false
	}
	file, ok := e.store.pin(seg)
	if !ok {
		return 0, nil, false
	}
	e.compacting = true
	e.compactWG.Add(1)
	return seg, file, true
}

func (e *Engine) endCompaction() {
	e.mu.Lock()
	e.compacting = false
	e.mu.Unlock()
	e.compactWG.Done()
}

// pickCompactible returns the oldest sealed segment past the dead-bytes threshold. The active segment is never chosen:
// it is still being appended to.
func (e *Engine) pickCompactible() (uint32, bool) {
	for _, seg := range e.store.segments() {
		if seg == e.store.activeSeg {
			continue
		}
		size := e.store.dataSize(seg)
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

// rewriteRecord copies one still-live record into the active segment. A record is live iff the keydir points at its
// exact (segment, offset); everything else is a superseded overwrite or a tombstone.
func (e *Engine) rewriteRecord(seg uint32, rec decoded, recPos int64) error {
	if rec.tombstone {
		return e.keepTombstone(seg, rec)
	}
	location, ok := e.lookup(rec.table, rec.key)
	if !ok || location.seg != seg || location.valPos != valPosFor(recPos, rec.table, rec.key) {
		return nil // dead: overwritten or deleted since it was written
	}
	newRec := encodeRecord(rec.table, rec.key, rec.value, false)
	newSeg, newRecPos, err := e.store.append(newRec)
	if err != nil {
		return err
	}
	e.dropLive(rec.table, rec.key)
	e.setLoc(newSeg, rec.table, rec.key, len(rec.value), newRecPos, int64(len(newRec)))
	return nil
}

// finishCompaction makes the rewrites durable and unlinks the reclaimed segment.
func (e *Engine) finishCompaction(seg uint32) error {
	e.mu.Lock()
	defer e.mu.Unlock()

	if e.closed {
		return nil
	}
	// The rewritten records must be durable before the only other copy is unlinked: a crash between the two would lose
	// keys that were already durable. This is unconditional except under SyncNo, where durability is opted out of.
	if err := e.store.syncActive(); err != nil {
		return err
	}
	if err := e.store.removeSegment(seg); err != nil {
		return err
	}
	delete(e.segLive, seg)
	delete(e.segSets, seg)
	e.compactions.Add(1)
	return nil
}

// keepTombstone carries a delete forward when dropping it could resurrect a key. An older segment may still hold the
// SET this tombstone buried; once the tombstone's segment is unlinked, recovery would scan that SET with nothing after
// it and bring the key back. Rewriting into the active segment keeps the delete newer than any surviving value.
func (e *Engine) keepTombstone(seg uint32, rec decoded) error {
	if _, live := e.lookup(rec.table, rec.key); live {
		return nil // a later SET already superseded this delete
	}
	if !e.buriedBefore(hashKey(rec.table, rec.key), seg) {
		return nil // nothing older left to resurrect, so the delete goes away with its segment
	}
	_, _, err := e.store.append(encodeRecord(rec.table, rec.key, "", true))
	return err
}
