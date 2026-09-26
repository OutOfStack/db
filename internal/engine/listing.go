package engine

import (
	"iter"
	"slices"

	"github.com/OutOfStack/db/internal/protocol"
)

// CollectSorted gathers names into a sorted slice, provided the RESP array reply carrying them fits in maxBytes. The
// budget is checked while collecting: iteration stops at the first name that takes the encoded elements past it, so an
// oversized listing holds at most maxBytes worth of names and is never sorted. It is then refused with TOOLARGE rather
// than truncated, because a silently shortened listing looks exactly like a complete one. maxBytes <= 0 disables the
// bound.
//
// Both engines list through here, so the same limit means the same thing whichever one serves the command.
func CollectSorted(names iter.Seq[string], maxBytes int) ([]string, error) {
	if maxBytes <= 0 {
		return slices.Sorted(names), nil
	}
	var collected []string
	size := 0
	for name := range names {
		size += protocol.BulkStringSize(name)
		if size > maxBytes {
			return nil, listingTooLarge(maxBytes)
		}
		collected = append(collected, name)
	}
	if size+protocol.ArrayHeaderSize(len(collected)) > maxBytes {
		return nil, listingTooLarge(maxBytes)
	}
	slices.Sort(collected)
	return collected, nil
}

func listingTooLarge(maxBytes int) error {
	return protocol.NewError(protocol.CodeTooLarge,
		"listing exceeds the %d-byte reply limit (network.max_message_size); nothing was truncated", maxBytes)
}
