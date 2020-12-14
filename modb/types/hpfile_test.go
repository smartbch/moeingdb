package types

import (
	"io"
	"os"
	"testing"

	"github.com/stretchr/testify/assert"
)

func newSlice(length int, value byte) []byte {
	res := make([]byte, length)
	for i := range res {
		res[i] = value
	}
	return res
}

func TestHPFile(t *testing.T) {
	os.RemoveAll("./test")
	os.Mkdir("./test", 0700)

	hpfile, err := NewHPFile(64, 128, "./test")
	assert.Equal(t, nil, err)
	slice0 := newSlice(44, 1)
	pos, err := hpfile.Append([][]byte{slice0})
	assert.Equal(t, nil, err)
	assert.Equal(t, int64(0), pos)
	assert.Equal(t, int64(44), hpfile.Size())

	slice1a := newSlice(16, 2)
	slice1b := newSlice(10, 3)
	slice1 := append(slice1a, slice1b...)
	pos, err = hpfile.Append([][]byte{slice1a, slice1b})
	assert.Equal(t, nil, err)
	assert.Equal(t, int64(44), pos)
	assert.Equal(t, int64(70), hpfile.Size())

	slice2a := newSlice(25, 4)
	slice2b := newSlice(25, 5)
	slice2 := append(slice2a, slice2b...)
	pos, err = hpfile.Append([][]byte{slice2a, slice2b})
	assert.Equal(t, nil, err)
	assert.Equal(t, int64(70), pos)
	assert.Equal(t, int64(120), hpfile.Size())

	check0 := make([]byte, len(slice0))
	err = hpfile.ReadAt(check0, 0, false)
	assert.Equal(t, nil, err)
	assert.Equal(t, slice0, check0)

	hpfile.Flush()

	check1 := make([]byte, len(slice1))
	err = hpfile.ReadAt(check1, 44, false)
	assert.Equal(t, nil, err)
	assert.Equal(t, slice1, check1)

	check2 := make([]byte, len(slice2))
	err = hpfile.ReadAt(check2, 70, false)
	assert.Equal(t, nil, err)
	assert.Equal(t, slice2, check2)

	slice3 := make([]byte, 16)
	pos, err = hpfile.Append([][]byte{slice3})
	assert.Equal(t, nil, err)
	assert.Equal(t, int64(120), pos)
	assert.Equal(t, int64(136), hpfile.Size())

	hpfile.Flush()
	hpfile.Close()

	hpfile, err = NewHPFile(64, 128, "./test")
	assert.Equal(t, nil, err)

	err = hpfile.ReadAt(check0, 0, false)
	assert.Equal(t, nil, err)
	assert.Equal(t, slice0, check0)

	err = hpfile.ReadAt(check1, 44, false)
	assert.Equal(t, nil, err)
	assert.Equal(t, slice1, check1)

	err = hpfile.ReadAt(check2, 70, false)
	assert.Equal(t, nil, err)
	assert.Equal(t, slice2, check2)

	check3 := make([]byte, len(slice3))
	err = hpfile.ReadAt(check3, 120, false)
	assert.Equal(t, nil, err)
	assert.Equal(t, slice3, check3)

	err = hpfile.PruneHead(64)
	assert.Equal(t, nil, err)

	err = hpfile.Truncate(120)
	assert.Equal(t, nil, err)
	assert.Equal(t, int64(120), hpfile.Size())

	err = hpfile.ReadAt(check3, 120, false)
	assert.Equal(t, io.EOF, err)

	hpfile.Flush()
	hpfile.Close()

	f, err := os.OpenFile("./test/haha", os.O_RDWR|os.O_CREATE, 0700)
	assert.Equal(t, nil, err)
	f.Close()

	_, err = NewHPFile(64, 128, "./test")
	assert.Equal(t, "haha does not match the pattern 'FileId-BlockSize'", err.Error())

	os.RemoveAll("./test/haha")
	f, err = os.OpenFile("./test/haha-100", os.O_RDWR|os.O_CREATE, 0700)
	assert.Equal(t, nil, err)
	f.Close()

	_, err = NewHPFile(64, 128, "./test")
	assert.Equal(t, "strconv.ParseInt: parsing \"haha\": invalid syntax", err.Error())

	os.RemoveAll("./test/haha-100")
	f, err = os.OpenFile("./test/23-100", os.O_RDWR|os.O_CREATE, 0700)
	assert.Equal(t, nil, err)
	f.Close()

	_, err = NewHPFile(64, 128, "./test")
	assert.Equal(t, "Invalid Size! 100!=128", err.Error())

	os.RemoveAll("./test")
}
