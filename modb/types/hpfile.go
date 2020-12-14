package types

import (
	"fmt"
	"io"
	"io/ioutil"
	"os"
	"strconv"
	"strings"
	"sync"
	//"sync/atomic"
	//"github.com/dterei/gotsc"
)

var TotalWriteTime, TotalReadTime, TotalSyncTime uint64

const (
	BufferSize      = 16 * 1024 * 1024
	SmallBufferSize = 32 * 1024 //For UnitTest
	PreReadBufSize  = 256 * 1024
)

// Head prune-able file
type HPFile struct {
	fileMap        map[int]*os.File
	blockSize      int
	dirName        string
	largestID      int
	latestFileSize int64
	bufferSize     int
	buffer         []byte
	mtx            sync.RWMutex
	preReader      PreReader
}

func NewHPFile(bufferSize, blockSize int, dirName string) (HPFile, error) {
	res := HPFile{
		fileMap:    make(map[int]*os.File),
		blockSize:  blockSize,
		dirName:    dirName,
		bufferSize: bufferSize,
		buffer:     make([]byte, 0, bufferSize),
	}
	if blockSize%bufferSize != 0 {
		panic(fmt.Sprintf("Invalid blockSize 0x%x bufferSize 0x%x", blockSize, bufferSize))
	}
	fileInfoList, err := ioutil.ReadDir(dirName)
	if err != nil {
		return res, err
	}
	var idList []int
	for _, fileInfo := range fileInfoList {
		if fileInfo.IsDir() {
			continue
		}
		twoParts := strings.Split(fileInfo.Name(), "-")
		if len(twoParts) != 2 {
			return res, fmt.Errorf("%s does not match the pattern 'FileId-BlockSize'", fileInfo.Name())
		}
		id, err := strconv.ParseInt(twoParts[0], 10, 63)
		if err != nil {
			return res, err
		}
		if res.largestID < int(id) {
			res.largestID = int(id)
		}
		idList = append(idList, int(id))
		size, err := strconv.ParseInt(twoParts[1], 10, 63)
		if int64(blockSize) != size {
			return res, fmt.Errorf("Invalid Size! %d!=%d", size, blockSize)
		}
	}
	for _, id := range idList {
		fname := fmt.Sprintf("%s/%d-%d", dirName, id, blockSize)
		var err error
		if id == res.largestID { // will write to this latest file
			res.fileMap[id], err = os.OpenFile(fname, os.O_RDWR, 0700)
			if err == nil {
				res.latestFileSize, err = res.fileMap[id].Seek(0, os.SEEK_END)
			}
		} else {
			res.fileMap[id], err = os.Open(fname)
		}
		if err != nil {
			return res, err
		}
	}
	if len(idList) == 0 {
		fname := fmt.Sprintf("%s/%d-%d", dirName, 0, blockSize)
		var err error
		res.fileMap[0], err = os.OpenFile(fname, os.O_RDWR|os.O_CREATE, 0700)
		if err != nil {
			return res, err
		}
	}
	return res, nil
}

func (hpf *HPFile) InitPreReader() {
	hpf.preReader.Init()
}

func (hpf *HPFile) Size() int64 {
	return int64(hpf.largestID)*int64(hpf.blockSize) + hpf.latestFileSize
}

func (hpf *HPFile) Truncate(size int64) error {
	for size < int64(hpf.largestID)*int64(hpf.blockSize) {
		f := hpf.fileMap[hpf.largestID]
		err := f.Close()
		if err != nil {
			return err
		}
		fname := fmt.Sprintf("%s/%d-%d", hpf.dirName, hpf.largestID, hpf.blockSize)
		err = os.Remove(fname)
		if err != nil {
			return err
		}
		delete(hpf.fileMap, hpf.largestID)
		hpf.largestID--
	}
	size -= int64(hpf.largestID) * int64(hpf.blockSize)
	err := hpf.fileMap[hpf.largestID].Close()
	if err != nil {
		return err
	}
	fname := fmt.Sprintf("%s/%d-%d", hpf.dirName, hpf.largestID, hpf.blockSize)
	hpf.fileMap[hpf.largestID], err = os.OpenFile(fname, os.O_RDWR, 0700)
	if err != nil {
		return err
	}
	hpf.latestFileSize = size
	return hpf.fileMap[hpf.largestID].Truncate(size)
}

func (hpf *HPFile) Flush() {
	//start := gotsc.BenchStart()
	hpf.mtx.Lock()
	defer hpf.mtx.Unlock()
	hpf.flush()
}

func (hpf *HPFile) flush() {
	if len(hpf.buffer) != 0 {
		_, err := hpf.fileMap[hpf.largestID].Write(hpf.buffer)
		if err != nil {
			panic(err)
		}
		hpf.buffer = hpf.buffer[:0]
	}
	hpf.fileMap[hpf.largestID].Sync()
	//atomic.AddUint64(&TotalSyncTime, gotsc.BenchEnd() - start - tscOverhead)
}

func (hpf *HPFile) FlushAsync() {
	hpf.mtx.Lock()
	go func() {
		defer hpf.mtx.Unlock()
		hpf.flush()
	}()
}

func (hpf *HPFile) Close() error {
	hpf.mtx.Lock()
	defer hpf.mtx.Unlock()
	for _, file := range hpf.fileMap {
		err := file.Close()
		if err != nil {
			return err
		}
	}
	return nil
}

func (hpf *HPFile) ReadAt(buf []byte, off int64, withBuf bool) (err error) {
	hpf.mtx.RLock()
	defer hpf.mtx.RUnlock()
	if withBuf {
		return hpf.readAtWithBuf(buf, off)
	}
	//start := gotsc.BenchStart()
	fileID := off / int64(hpf.blockSize)
	pos := off % int64(hpf.blockSize)
	f, ok := hpf.fileMap[int(fileID)]
	if !ok {
		return fmt.Errorf("Can not find the file with id=%d (%d/%d)", fileID, off, hpf.blockSize)
	}
	_, err = f.ReadAt(buf, pos)
	//atomic.AddUint64(&TotalReadTime, gotsc.BenchEnd() - start - tscOverhead)
	return
}

// For continuous read
func (hpf *HPFile) readAtWithBuf(buf []byte, off int64) (err error) {
	fileID := off / int64(hpf.blockSize)
	pos := off % int64(hpf.blockSize)
	f, ok := hpf.fileMap[int(fileID)]
	if !ok {
		return fmt.Errorf("Can not find the file with id=%d (%d/%d)", fileID, off, hpf.blockSize)
	}

	hpf.preReader.mtx.Lock()
	defer hpf.preReader.mtx.Unlock()
	ok = hpf.preReader.TryRead(fileID, pos, buf)
	if ok {
		return nil
	}
	if len(buf) >= PreReadBufSize || int(pos)+len(buf) > hpf.blockSize {
		_, err = f.ReadAt(buf, pos)
		return
	}
	part := hpf.preReader.GetToFill(fileID, pos, pos+PreReadBufSize)
	n, err := f.ReadAt(part, pos)
	if err == io.EOF {
		hpf.preReader.end = pos + int64(n)
	} else if err != nil {
		return err
	}
	ok = hpf.preReader.TryRead(fileID, pos, buf)
	if !ok {
		panic("Cannot read data just fetched")
	}
	return nil
}

func (hpf *HPFile) Append(bufList [][]byte) (int64, error) {
	//start := gotsc.BenchStart()
	hpf.mtx.Lock()
	defer hpf.mtx.Unlock()
	f := hpf.fileMap[hpf.largestID]
	startPos := int64(hpf.largestID*hpf.blockSize) + hpf.latestFileSize
	for _, buf := range bufList {
		if len(buf) > hpf.bufferSize {
			panic("buf is too large")
		}
		hpf.latestFileSize += int64(len(buf))
		extraBytes := len(hpf.buffer) + len(buf) - hpf.bufferSize
		if extraBytes > 0 {
			hpf.buffer = append(hpf.buffer, buf[:len(buf)-extraBytes]...)
			buf = buf[len(buf)-extraBytes:]
			//pos, _ := f.Seek(0, os.SEEK_END)
			//fmt.Printf("Haha startPos %x: %x + %x > %x; real pos %x\n", startPos, len(hpf.buffer), len(buf), hpf.bufferSize, pos)
			_, err := f.Write(hpf.buffer)
			if err != nil {
				return 0, err
			}
			hpf.buffer = hpf.buffer[:0]
		}
		hpf.buffer = append(hpf.buffer, buf...)
	}
	overflowByteCount := hpf.latestFileSize - int64(hpf.blockSize)
	if overflowByteCount >= 0 {
		hpf.flush()
		hpf.largestID++
		fname := fmt.Sprintf("%s/%d-%d", hpf.dirName, hpf.largestID, hpf.blockSize)
		f, err := os.OpenFile(fname, os.O_RDWR|os.O_CREATE, 0700)
		if err != nil {
			return 0, err
		}
		if overflowByteCount != 0 {
			hpf.buffer = hpf.buffer[:overflowByteCount]
			for i := 0; i < int(overflowByteCount); i++ {
				hpf.buffer[i] = 0
			}
		}
		hpf.fileMap[hpf.largestID] = f
		hpf.latestFileSize = overflowByteCount
	}
	//atomic.AddUint64(&TotalWriteTime, gotsc.BenchEnd() - start - tscOverhead)
	return startPos, nil
}

func (hpf *HPFile) PruneHead(off int64) error {
	fileID := off / int64(hpf.blockSize)
	var idList []int
	for id, f := range hpf.fileMap {
		if id >= int(fileID) {
			continue
		}
		err := f.Close()
		if err != nil {
			return err
		}
		idList = append(idList, id)
	}
	for _, id := range idList {
		delete(hpf.fileMap, id)
		fname := fmt.Sprintf("%s/%d-%d", hpf.dirName, id, hpf.blockSize)
		err := os.Remove(fname)
		if err != nil {
			return err
		}
	}
	return nil
}

type PreReader struct {
	buf    []byte
	fileID int64
	start  int64
	end    int64
	mtx    sync.Mutex
}

func (pr *PreReader) Init() {
	pr.fileID = -1
	pr.buf = make([]byte, PreReadBufSize)
}

func (pr *PreReader) GetToFill(fileID, start, end int64) []byte {
	pr.fileID = fileID
	pr.start = start
	pr.end = end
	return pr.buf[:end-start]
}

func (pr *PreReader) TryRead(fileID, start int64, buf []byte) bool {
	if fileID == pr.fileID && pr.start <= start && start+int64(len(buf)) < pr.end {
		copy(buf, pr.buf[start-pr.start:])
		return true
	}
	return false
}
