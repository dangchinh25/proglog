package log

import (
	"bytes"
	api "dangchinh25/proglog/api/v1"
	"io"

	"github.com/golang/protobuf/proto"
	"github.com/hashicorp/raft"
)

// enforce fsm to interface raft.FSM
var _ raft.FSM = (*fsm)(nil)

type fsm struct {
	log *Log // Store the actual log data
}

// Apply read the Raft entry/log and execute the actual request/command.
// Raft call this method with raft.Log read from the LogStore
func (f *fsm) Apply(record *raft.Log) interface{} {
	buf := record.Data
	reqType := RequestType(buf[0])
	switch reqType {
	case AppendRequestType:
		return f.applyAppend(buf[1:])
	}
	return nil
}

// handle Append request type
func (f *fsm) applyAppend(b []byte) interface{} {
	var req api.ProduceRequest
	err := proto.Unmarshal(b, &req)
	if err != nil {
		return err
	}
	offset, err := f.log.Append(req.Record)
	if err != nil {
		return err
	}
	return &api.ProduceResponse{Offset: offset}
}

// Snapshot returns an FSMSnapshot that represents a snapshot of the FSM's state.
// In thes case the state is the FSM's log, so call Reader() to returns an io.Reader that will read all the log's data
// Raft automatically calls Snapshot according to a configured SnapshotInterval and Snapshot threshold.
func (f *fsm) Snapshot() (raft.FSMSnapshot, error) {
	r := f.log.Reader()
	return &snapshot{reader: r}, nil
}

// Restore a FSM from a snapshot
func (f *fsm) Restore(r io.ReadCloser) error {
	b := make([]byte, lenWidth)
	var buf bytes.Buffer
	for i := 0; ; i++ {
		_, err := io.ReadFull(r, b)
		if err == io.EOF {
			break
		} else if err != nil {
			return err
		}
		size := int64(enc.Uint64(b))
		if _, err = io.CopyN(&buf, r, size); err != nil {
			return err
		}
		record := &api.Record{}
		if err = proto.Unmarshal(buf.Bytes(), record); err != nil {
			return err
		}
		if i == 0 {
			f.log.Config.Segment.InitialOffset = record.Offset
			if err := f.log.Reset(); err != nil {
				return err
			}
		}
		if _, err = f.log.Append(record); err != nil {
			return err
		}
		buf.Reset()
	}
	return nil
}
