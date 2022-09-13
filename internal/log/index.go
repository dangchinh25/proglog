package log

import (
	"io"
	"os"

	"github.com/tysonmote/gommap"
)

var (
	offWidth uint64 = 4
	posWidth uint64 = 8
	entWidth uint64 = offWidth + posWidth
)

type index struct {
	file *os.File
	mmap gommap.MMap
	size uint64
}

func newIndex(f *os.File, c Config) (*index, error) {
	idx := &index{
		file: f,
	}
	fi, err := os.Stat(f.Name())
	if err != nil {
		return nil, err
	}
	idx.size = uint64(fi.Size())
	if err = os.Truncate(f.Name(), int64(c.Segment.MaxIndexBytes)); err != nil {
		return nil, err
	}
	if idx.mmap, err = gommap.Map(idx.file.Fd(), gommap.PROT_READ|gommap.PROT_WRITE, gommap.MAP_SHARED); err != nil {
		return nil, err
	}
	return idx, nil
}

func (i *index) Read(off int64) (storeOffset uint32, storePos uint64, err error) {
	if i.size == 0 {
		return 0, 0, io.EOF
	}
	var indexOff uint32
	if off == -1 {
		indexOff = uint32((i.size / entWidth) - 1)
	} else {
		indexOff = uint32(off)
	}
	indexPos := uint64(indexOff) * entWidth
	if i.size < indexPos+entWidth {
		return 0, 0, io.EOF
	}
	storeOffset = enc.Uint32(i.mmap[indexPos : indexPos+offWidth])
	storePos = enc.Uint64(i.mmap[indexPos+offWidth : indexPos+entWidth])
	return storeOffset, storePos, nil
}

func (i *index) Write(storeOffset uint32, storePos uint64) error {
	if uint64(len(i.mmap)) < i.size+entWidth {
		return io.EOF
	}
	enc.PutUint32(i.mmap[i.size:i.size+offWidth], storeOffset)
	enc.PutUint64(i.mmap[i.size+offWidth:i.size+entWidth], storePos)
	i.size += uint64(entWidth)
	return nil
}

func (i *index) Name() string {
	return i.file.Name()
}

func (i *index) Close() error {
	if err := i.mmap.Sync(gommap.MS_ASYNC); err != nil {
		return err
	}
	if err := i.file.Sync(); err != nil {
		return err
	}
	if err := i.file.Truncate(int64(i.size)); err != nil {
		return err
	}
	return i.file.Close()
}
