package syncdb

import (
	"os"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestInt64Array(t *testing.T) {
	ia := NewInt64Array()
	assert.Equal(t, int64(0), ia.Get(0))
	assert.Equal(t, int64(0), ia.Get(1))
	assert.Equal(t, int64(0), ia.Get(100))
	assert.Equal(t, int64(0), ia.Get(10000))
	ia.Set(0, 200)
	ia.Set(1, 201)
	ia.Set(100, 202)
	ia.Set(10000, 203)
	assert.Equal(t, int64(200), ia.Get(0))
	assert.Equal(t, int64(201), ia.Get(1))
	assert.Equal(t, int64(202), ia.Get(100))
	assert.Equal(t, int64(203), ia.Get(10000))
	assert.Equal(t, int64(0), ia.Get(200))
}

func TestSyncDB(t *testing.T) {
	assert.Equal(t, 0, Padding64(0))
	assert.Equal(t, 0, Padding64(64))
	assert.Equal(t, 0, Padding64(128))
	assert.Equal(t, 1, Padding64(127))
	assert.Equal(t, 63, Padding64(65))
	assert.Equal(t, 32, Padding64(32))
	b1 := strings.Repeat("A", 32)
	b2 := strings.Repeat("B", 65)
	b3 := strings.Repeat("C", 40)
	b4 := strings.Repeat("D", 120)

	os.RemoveAll("./test")
	db := NewSyncDB("./test")
	assert.Nil(t, db.Get(1))
	assert.Nil(t, db.Get(2))
	assert.Nil(t, db.Get(3))
	assert.Nil(t, db.Get(4))
	db.Set(1, []byte(b1))
	db.Set(2, []byte(b2))
	db.Set(3, []byte(b3))
	db.Set(4, []byte(b4))
	assert.Equal(t, b1, string(db.Get(1)))
	assert.Equal(t, b2, string(db.Get(2)))
	assert.Equal(t, b3, string(db.Get(3)))
	assert.Equal(t, b4, string(db.Get(4)))
	db.Close()

	db = NewSyncDB("./test")
	assert.Equal(t, b1, string(db.Get(1)))
	assert.Equal(t, b2, string(db.Get(2)))
	assert.Equal(t, b3, string(db.Get(3)))
	assert.Equal(t, b4, string(db.Get(4)))
	db.Close()
	os.RemoveAll("./test")
}
