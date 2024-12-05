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

func (segment *Segment) Write(buf []byte) error {
	_, err := segment.fd.Write(buf)

	if err != nil {
		return err
	}
	return nil
}
