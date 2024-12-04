package segmentstore

import "os"

type SegmentId = int64

type Segment struct {
	id SegmentId
	fd *os.File
}

func (segment *Segment) Close() error {
	return segment.fd.Close()
}
