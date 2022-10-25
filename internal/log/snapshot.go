package log

import (
	"io"

	"github.com/hashicorp/raft"
)

var _ raft.FSMSnapshot = (*snapshot)(nil)

// snapshot allow Raft to compact its log so it doesn't store logs whose command Raft has applied already.
// Allow Raft to bootstrap new servers more efficiently than if the leader had to replicate its entire log again and again.
type snapshot struct {
	reader io.Reader
}

// Persiist write the state to some sink that depend on the snapshot store we configured.
func (s *snapshot) Persist(sink raft.SnapshotSink) error {
	if _, err := io.Copy(sink, s.reader); err != nil {
		_ = sink.Cancel()
		return err
	}
	return sink.Close()
}

func (s *snapshot) Release() {}
