package fio

import (
	"os"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/assert"
)

func destroyFile(path string) {
	if err := os.RemoveAll(path); err != nil {
		panic(err)
	}
}

func TestNewFileIO(t *testing.T) {
	path := filepath.Join("../testdata", "a.data")
	fd, err := NewFileIO(path)
	defer destroyFile(path)
	assert.Nil(t, err)
	assert.NotNil(t, fd)

	path1 := filepath.Join("../zwaeqweqweqweqw", "a.data")
	fd1, err1 := NewFileIO(path1)
	defer destroyFile(path1)
	assert.NotNil(t, err1)
	assert.Nil(t, fd1)
}

func TestFileIO_Read(t *testing.T) {
	path := filepath.Join("../testdata", "a.data")
	fd, err := NewFileIO(path)
	defer destroyFile(path)

	n, err := fd.Write([]byte("hello"))
	assert.Nil(t, err)
	assert.Equal(t, 5, n)

	n, err = fd.Write([]byte("mck"))
	assert.Nil(t, err)
	assert.Equal(t, 3, n)

	b1 := make([]byte, 5)
	n1, err1 := fd.Read(b1, 0)
	assert.Nil(t, err1)
	assert.Equal(t, 5, n1)

	b2 := make([]byte, 3)
	n2, err2 := fd.Read(b2, 5)
	assert.Nil(t, err2)
	assert.Equal(t, 3, n2)
}

func TestFileIO_Write(t *testing.T) {
	path := filepath.Join("../testdata", "a.data")
	fd, err := NewFileIO(path)
	defer destroyFile(path)

	n, err := fd.Write([]byte("hello"))
	assert.Nil(t, err)
	assert.Equal(t, 5, n)

	n, err = fd.Write([]byte("mck"))
	assert.Nil(t, err)
	assert.Equal(t, 3, n)
}

func TestFileIO_Sync(t *testing.T) {
	path := filepath.Join("../testdata", "a.data")
	fd, err := NewFileIO(path)
	defer destroyFile(path)

	err = fd.Sync()
	assert.Nil(t, err)
}

func TestFileIO_Close(t *testing.T) {
	path := filepath.Join("../testdata", "a.data")
	fd, err := NewFileIO(path)
	defer destroyFile(path)

	err = fd.Close()
	assert.Nil(t, err)
}
