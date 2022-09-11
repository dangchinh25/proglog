package log

import (
	"io/ioutil"
	"os"
	"testing"

	"github.com/stretchr/testify/require"
)

var (
	data       = []byte("hello world")
	dataLength = uint64(len(data) + lenWidth)
	numTest    = 4
)

func TestStoreAppendRead(t *testing.T) {
	f, err := ioutil.TempFile("", "store_append_read_test")
	require.NoError(t, err)
	defer os.Remove(f.Name())

	s, err := newStore(f)
	require.NoError(t, err)

	testAppend(t, s)
	testRead(t, s)
	testReadAt(t, s)

	s, err = newStore(f)
	require.NoError(t, err)
	testRead(t, s)
}

func testAppend(t *testing.T, s *store) {
	t.Helper()
	for i := 1; i < numTest; i++ {
		numBytesWritten, pos, err := s.Append(data)
		require.NoError(t, err)
		require.Equal(t, pos+numBytesWritten, dataLength*uint64(i))
	}
}

func testRead(t *testing.T, s *store) {
	t.Helper()
	var pos uint64
	for i := 1; i < numTest; i++ {
		result, err := s.Read(pos)
		require.NoError(t, err)
		require.Equal(t, data, result)
		pos += dataLength
	}
}

func testReadAt(t *testing.T, s *store) {
	t.Helper()
	for i, off := 1, int64(0); i < numTest; i++ {
		b := make([]byte, lenWidth)
		numBytesRead, err := s.ReadAt(b, off)
		require.NoError(t, err)
		require.Equal(t, lenWidth, numBytesRead)
		off += int64(numBytesRead)

		size := enc.Uint64(b)
		b = make([]byte, size)
		numBytesRead, err = s.ReadAt(b, off)
		require.NoError(t, err)
		require.Equal(t, data, b)
		require.Equal(t, int(size), numBytesRead)
		off += int64(numBytesRead)
	}
}

func TestStoreClose(t *testing.T) {
	f, err := ioutil.TempFile("", "store_close_test")
	require.NoError(t, err)
	defer os.Remove(f.Name())

	s, err := newStore(f)
	require.NoError(t, err)
	_, _, err = s.Append(data)
	require.NoError(t, err)

	f, beforeSize, err := openFile(f.Name())
	require.NoError(t, err)

	err = s.Close()
	require.NoError(t, err)

	_, afterSize, err := openFile(f.Name())
	require.NoError(t, err)
	require.True(t, afterSize > beforeSize)
}

func openFile(name string) (file *os.File, size int64, err error) {
	f, err := os.OpenFile(name, os.O_RDWR|os.O_CREATE|os.O_APPEND, 0644)
	if err != nil {
		return nil, 0, err
	}
	fi, err := f.Stat()
	if err != nil {
		return nil, 0, err
	}
	return f, fi.Size(), nil
}
