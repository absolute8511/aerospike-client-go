package types

import (
	"math"
	"time"
)

const (
	// citrusleaf epoc: Jan 01 2010 00:00:00 GMT
	CITRUSLEAF_EPOCH = 1262304000
)

func ServerTStamp(secsFromCitrusLeafEpoc uint32) int64 {
	return int64(CITRUSLEAF_EPOCH + secsFromCitrusLeafEpoc)
}

// TTL converts an Expiration time from citrusleaf epoc to TTL in seconds.
func TTL(secsFromCitrusLeafEpoc uint32) uint32 {
	switch secsFromCitrusLeafEpoc {
	// don't convert magic values
	case 0: // when set to don't expire, this value is returned
		return math.MaxUint32
	default:
		ts := ServerTStamp(secsFromCitrusLeafEpoc)
		now := time.Now().Unix()
		if ts > now {
			return uint32(ts - now)
		}
		return 1
	}
}
