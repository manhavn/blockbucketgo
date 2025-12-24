package blockbucketgo

import (
	"bytes"
	"crypto/md5"
	"encoding/hex"
	"fmt"
	"math"
	"os"
	"slices"
	"sort"
	"sync"
	"syscall"
)

const (
	cMaxDigitGroup uint8 = 249
	cStart         uint8 = 250
	cSizeKey       uint8 = 251
	cSumKey        uint8 = 252
	cSumMd5        uint8 = 253
	cSizeData      uint8 = 254
	cEnd           uint8 = 255
	cFirstSize     uint  = 128
)

type Bucket struct {
	reader *os.File
	writer *os.File
	fd     uintptr
	mu     sync.Mutex
}

type Item struct {
	Key  []byte
	Data []byte
}

type block struct {
	start    uint
	sizeKey  uint
	sumKey   uint
	sumMd5   uint
	sizeData uint
}

var emptyBlock = block{
	start:    0,
	sizeKey:  0,
	sumKey:   0,
	sumMd5:   0,
	sizeData: 0,
}

func (e *Bucket) Close() {
	if e.reader != nil {
		_ = e.reader.Close()
	}
	if e.writer != nil {
		_ = e.writer.Close()
	}
}

func New(path string) *Bucket {
	e := Bucket{}
	reader, err := os.OpenFile(path, os.O_CREATE|os.O_RDONLY, 0o644)
	if err != nil {
		return nil
	}
	e.reader = reader
	writer, err := os.OpenFile(path, os.O_CREATE|os.O_RDWR, 0o644)
	if err != nil {
		return nil
	}
	e.writer = writer
	e.fd = writer.Fd()
	return &e
}

func (e *Bucket) digitsToNumber(digits []byte) (n uint) {
	for _, x := range digits {
		var count int
		if x < 10 {
			count = 1
		} else if x < 100 {
			count = 2
		} else {
			count = 3
		}
		n = n*uint(math.Pow10(count)) + uint(x)
	}
	return
}

func (e *Bucket) Set(item Item) (int, error) {
	_ = syscall.Flock(int(e.fd), syscall.LOCK_EX)
	defer syscall.Flock(int(e.fd), syscall.LOCK_UN)
	e.mu.Lock()
	defer e.mu.Unlock()

	startListPoint, listBlockData := e.getListConfig()
	return e.setOneData(
		listBlockData,
		item.Key,
		item.Data,
		startListPoint,
	)
}

func (e *Bucket) getListConfig() (uint, []byte) {
	buffer := make([]byte, cFirstSize)
	_, _ = e.reader.ReadAt(buffer, 0)

	var startListData []byte
	var sizeListData []byte
	func() {
		var positionListCheck uint8
		for _, v := range buffer {
			if v == cEnd {
				positionListCheck += 1
				continue
			}
			if positionListCheck == 0 {
				startListData = append(startListData, v)
			} else if positionListCheck == 1 {
				sizeListData = append(sizeListData, v)
			} else {
				break
			}
		}
	}()
	startListPoint := e.digitsToNumber(startListData)
	var listBlockData []byte

	func() {
		if startListPoint < cFirstSize {
			startListPoint = cFirstSize
			listBlockData = []byte{}
		} else {
			sizeList := e.digitsToNumber(sizeListData)
			listBlockData = make([]byte, sizeList) // vec![0u8; size_list];
			_, _ = e.reader.ReadAt(listBlockData, int64(startListPoint))
			idx := slices.Index(listBlockData, cEnd)
			if idx > -1 {
				listBlockData = listBlockData[:idx]
			}
		}
	}()
	return startListPoint, listBlockData
}

func (e *Bucket) setOneData(
	listBlockData []byte,
	key []byte,
	data []byte,
	startListPoint uint,
) (int, error) {
	newListBlockData, newListBlockInfo := e.getNewListNotContainKey(listBlockData, key, true)
	sizeKey := uint(len(key))
	sizeData := uint(len(data))
	blockSize := sizeKey + sizeData
	var sumKey uint
	for i := 0; i < len(key); i++ {
		sumKey += uint(key[i])
	}
	var sumMd5 uint
	keyMd5 := e.md5(key)
	for i := 0; i < len(keyMd5); i++ {
		sumMd5 += uint(keyMd5[i])
	}
	listSpace := e.getListSpace(startListPoint, newListBlockInfo)
	startList, startBlock := e.getPerfectSpace(listSpace, startListPoint, blockSize)
	infoData := pushBlockToData([]byte{}, &block{
		start:    startBlock,
		sizeKey:  sizeKey,
		sumKey:   sumKey,
		sumMd5:   sumMd5,
		sizeData: sizeData,
	})
	listBlockDataWriter := append(newListBlockData, infoData...)
	if n, err := e.updateListBlock(startList, listBlockDataWriter); err != nil {
		return n, err
	}
	return e.writer.WriteAt(append(key, data...), int64(startBlock))
}

func (e *Bucket) md5(input []byte) []byte {
	hash := md5.New()
	hash.Write(input)
	return []byte(hex.EncodeToString(hash.Sum(nil)))
}

func (e *Bucket) getNewListNotContainKey(
	listBlockData []byte,
	key []byte,
	isReturnListInfo bool,
) (newListBlockData []byte, newListBlockInfo []block) {
	sizeKey := uint(len(key))
	var sumKey uint
	for i := 0; i < len(key); i++ {
		sumKey += uint(key[i])
	}
	var sumMd5 uint
	keyMd5 := e.md5(key)
	for i := 0; i < len(keyMd5); i++ {
		sumMd5 += uint(keyMd5[i])
	}

	blockInfo := emptyBlock
	var tmpGroup []byte
	for _, v := range listBlockData {
		switch v {
		case cStart:
			blockInfo.start = e.digitsToNumber(tmpGroup)
			tmpGroup = []byte{}
		case cSizeKey:
			blockInfo.sizeKey = e.digitsToNumber(tmpGroup)
			tmpGroup = []byte{}
		case cSumKey:
			blockInfo.sumKey = e.digitsToNumber(tmpGroup)
			tmpGroup = []byte{}
		case cSumMd5:
			blockInfo.sumMd5 = e.digitsToNumber(tmpGroup)
			tmpGroup = []byte{}
		case cSizeData:
			blockInfo.sizeData = e.digitsToNumber(tmpGroup)
			tmpGroup = []byte{}
			if blockInfo.sizeKey == sizeKey && blockInfo.sumKey == sumKey &&
				blockInfo.sumMd5 == sumMd5 {
				foundKey := e.pullKey(blockInfo)
				if bytes.Equal(foundKey, key) {
					continue
				}
			}
			newListBlockData = pushBlockToData(newListBlockData, &blockInfo)
			if isReturnListInfo {
				newListBlockInfo = append(newListBlockInfo, blockInfo)
			}
			blockInfo = emptyBlock
		case cEnd:
			break
		default:
			tmpGroup = append(tmpGroup, v)
		}
	}
	return newListBlockData, newListBlockInfo
}

func groupDigitsAppend(dst *[]byte, n uint) {
	// tách chữ số (tối đa ~20 digit với uint64)
	var digits [20]byte
	pos := 0

	if n == 0 {
		digits[0] = 0
		pos = 1
	} else {
		for n > 0 {
			digits[pos] = byte(n % 10)
			n /= 10
			pos++
		}
	}

	// đảo ngược
	for i, j := 0, pos-1; i < j; i, j = i+1, j-1 {
		digits[i], digits[j] = digits[j], digits[i]
	}

	i := 0
	for i < pos {
		if digits[i] == 0 {
			*dst = append(*dst, 0)
			i++
			continue
		}

		if i+2 < pos {
			v := uint16(digits[i])*100 +
				uint16(digits[i+1])*10 +
				uint16(digits[i+2])

			if v <= uint16(cMaxDigitGroup) {
				*dst = append(*dst, byte(v))
				i += 3
				continue
			}
		}

		if i+1 < pos {
			v := digits[i]*10 + digits[i+1]
			if v <= cMaxDigitGroup {
				*dst = append(*dst, v)
				i += 2
				continue
			}
		}

		*dst = append(*dst, digits[i])
		i++
	}
}

func pushBlockToData(buf []byte, b *block) []byte {
	groupDigitsAppend(&buf, b.start)
	buf = append(buf, cStart)

	groupDigitsAppend(&buf, b.sizeKey)
	buf = append(buf, cSizeKey)

	groupDigitsAppend(&buf, b.sumKey)
	buf = append(buf, cSumKey)

	groupDigitsAppend(&buf, b.sumMd5)
	buf = append(buf, cSumMd5)

	groupDigitsAppend(&buf, b.sizeData)
	buf = append(buf, cSizeData)

	return buf
}

func (e *Bucket) pullKey(info block) []byte {
	foundKey := make([]byte, info.sizeKey)
	_, _ = e.reader.ReadAt(foundKey, int64(info.start))
	return foundKey
}

func (e *Bucket) getListSpace(startListPoint uint, listBlockInfo []block) []block {
	var listStartBlock []uint
	mapStartBlock := map[uint]uint{}
	for i := 0; i < len(listBlockInfo); i++ {
		b := listBlockInfo[i]
		listStartBlock = append(listStartBlock, b.start)
		mapStartBlock[b.start] = b.sizeKey + b.sizeData
	}
	sort.Slice(listStartBlock, func(i, j int) bool {
		return listStartBlock[i] < listStartBlock[j]
	})
	currentPoint := cFirstSize
	var listSpace []block
	for i := 0; i < len(listStartBlock); i++ {
		start := listStartBlock[i]
		if currentPoint < start {
			listSpace = append(listSpace, block{
				start:    currentPoint,
				sizeKey:  0,
				sumKey:   0,
				sumMd5:   0,
				sizeData: start - currentPoint,
			})
		}
		currentPoint = start + mapStartBlock[start]
	}
	lastSpaceSize := startListPoint - currentPoint
	if lastSpaceSize > 0 {
		listSpace = append(listSpace, block{
			start:    currentPoint,
			sizeKey:  0,
			sumKey:   1, // space này là vị trí còn trống cuối cùng trước list
			sumMd5:   0,
			sizeData: lastSpaceSize,
		})
	}
	return listSpace
}

func (e *Bucket) getPerfectSpace(
	listSpace []block,
	startListPoint uint,
	blockSize uint,
) (uint, uint) {
	perfectBlockSize := blockSize
	var perfectFreeSize uint
	startBlock := startListPoint
	var isLastSpace bool
	for i := 0; i < len(listSpace); i++ {
		s := listSpace[i]
		if s.sizeData >= blockSize || s.sumKey == 1 {
			newSize := s.sizeData
			if newSize > perfectFreeSize ||
				(perfectFreeSize > newSize && startListPoint > s.start+s.sizeData) {
				perfectFreeSize = newSize
				startBlock = s.start
				isLastSpace = s.sumKey == 1
			}
		}
	}
	if perfectFreeSize >= perfectBlockSize {
		perfectBlockSize = 0
	}
	var startList uint
	if isLastSpace {
		startList = startBlock + blockSize
	} else {
		startList = startListPoint + perfectBlockSize
	}
	return startList, startBlock
}

func (e *Bucket) updateListBlock(start uint, listBlockData []byte) (n int, err error) {
	var firstBlockData []byte
	groupDigitsAppend(&firstBlockData, start)
	firstBlockData = append(firstBlockData, cEnd)
	groupDigitsAppend(&firstBlockData, uint(len(listBlockData)))
	firstBlockData = append(firstBlockData, cEnd)

	n, err = e.writer.WriteAt(append(listBlockData, cEnd), int64(start))
	if err != nil {
		return n, err
	}
	n, err = e.writer.WriteAt(firstBlockData, 0)
	return n, err
}

func (e *Bucket) Get(key []byte) ([]byte, []byte) {
	_, listBlockData := e.getListConfig()
	return e.getOneData(listBlockData, key)
}

func (e *Bucket) getOneData(listBlockData []byte, key []byte) (rsKey []byte, rsData []byte) {
	sizeKey := uint(len(key))
	var sumKey uint
	for i := 0; i < len(key); i++ {
		sumKey += uint(key[i])
	}
	var sumMd5 uint
	keyMd5 := e.md5(key)
	for i := 0; i < len(keyMd5); i++ {
		sumMd5 += uint(keyMd5[i])
	}
	blockInfo := emptyBlock
	var tmpGroup []byte
	for _, v := range listBlockData {
		switch v {
		case cStart:
			blockInfo.start = e.digitsToNumber(tmpGroup)
			tmpGroup = []byte{}
		case cSizeKey:
			blockInfo.sizeKey = e.digitsToNumber(tmpGroup)
			tmpGroup = []byte{}
		case cSumKey:
			blockInfo.sumKey = e.digitsToNumber(tmpGroup)
			tmpGroup = []byte{}
		case cSumMd5:
			blockInfo.sumMd5 = e.digitsToNumber(tmpGroup)
			tmpGroup = []byte{}
		case cSizeData:
			blockInfo.sizeData = e.digitsToNumber(tmpGroup)
			tmpGroup = []byte{}
			if blockInfo.sizeKey == sizeKey && blockInfo.sumKey == sumKey &&
				blockInfo.sumMd5 == sumMd5 {
				foundKey, foundData := e.pullData(blockInfo)
				if bytes.Equal(foundKey, key) {
					rsKey = foundKey
					rsData = foundData
					break
				}
			}
			blockInfo = emptyBlock
		case cEnd:
			break
		default:
			tmpGroup = append(tmpGroup, v)
		}
	}
	return
}

func (e *Bucket) pullData(info block) ([]byte, []byte) {
	foundKey := make([]byte, info.sizeKey)
	foundData := make([]byte, info.sizeData)
	_, err := e.writer.ReadAt(foundKey, int64(info.start))
	if err != nil {
		return nil, nil
	}
	_, err = e.writer.ReadAt(foundData, int64(info.start+info.sizeKey))
	if err != nil {
		return nil, nil
	}
	return foundKey, foundData
}

func (e *Bucket) Delete(key []byte) (int, error) {
	_ = syscall.Flock(int(e.fd), syscall.LOCK_EX)
	defer syscall.Flock(int(e.fd), syscall.LOCK_UN)
	e.mu.Lock()
	defer e.mu.Unlock()

	startListPoint, listBlockData := e.getListConfig()
	return e.deleteOneData(
		listBlockData,
		key,
		startListPoint,
	)
}

func (e *Bucket) deleteOneData(listBlockData []byte, key []byte, startListPoint uint) (int, error) {
	newListBlockData, _ := e.getNewListNotContainKey(listBlockData, key, false)
	return e.updateListBlock(startListPoint, newListBlockData)
}

func (e *Bucket) SetMany(listData []Item) int {
	_ = syscall.Flock(int(e.fd), syscall.LOCK_EX)
	defer syscall.Flock(int(e.fd), syscall.LOCK_UN)
	e.mu.Lock()
	defer e.mu.Unlock()

	startListPoint, listBlockData := e.getListConfig()
	return e.setManyData(
		listBlockData,
		listData,
		startListPoint,
	)
}

func (e *Bucket) setManyData(
	listBlockData []byte,
	listData []Item,
	startListPoint uint,
) (count int) {
	newListBlockData, newListBlockInfo := e.getNewListNotContainListKey(
		listBlockData,
		&listData,
		true,
	)

	var listBlockInsertPosition []string
	var minSizeBlock uint
	var listConfigInsert []block

	var maxSizeBlock uint
	for i := 0; i < len(listData); i++ {
		item := listData[i]
		key := item.Key
		data := item.Data
		sizeKey := uint(len(key))
		sizeData := uint(len(data))
		blockSize := sizeKey + sizeData
		if minSizeBlock == 0 || blockSize < minSizeBlock {
			minSizeBlock = blockSize
		}
		if blockSize > maxSizeBlock {
			maxSizeBlock = blockSize
		}
		var sumKey uint
		for i := 0; i < len(key); i++ {
			sumKey += uint(key[i])
		}
		var sumMd5 uint
		keyMd5 := e.md5(key)
		for i := 0; i < len(keyMd5); i++ {
			sumMd5 += uint(keyMd5[i])
		}
		listConfigInsert = append(listConfigInsert, block{
			start:    uint(i),
			sizeKey:  sizeKey,
			sumKey:   sumKey,
			sumMd5:   sumMd5,
			sizeData: sizeData,
		})
		listBlockInsertPosition = append(
			listBlockInsertPosition,
			fmt.Sprintf("%d%d%d%d", sizeKey, sumKey, sumMd5, sizeData),
		)
	}

	sort.Slice(listConfigInsert, func(i, j int) bool {
		itemA := listConfigInsert[i]
		itemB := listConfigInsert[j]
		ka := itemA.sizeKey + itemA.sizeData
		kb := itemB.sizeKey + itemB.sizeData
		return ka > kb
	})

	startListBlock := startListPoint
	var listInfoData []byte
	selected := map[uint]bool{}
	type writeDataTemp struct {
		start uint
		key   []byte
		data  []byte
	}
	var listWriteData []writeDataTemp
	var totalLastSpaceUsed uint
	mapBlockInsert := map[string]block{}
	listSpace := e.getListSpace(startListPoint, newListBlockInfo)
	for i := 0; i < len(listSpace); i++ {
		s := listSpace[i]
		var thisSpaceUsed uint
		if s.sizeData < minSizeBlock && s.sumKey == 0 {
			continue
		}
		for j := 0; j < len(listConfigInsert); j++ {
			c := listConfigInsert[j]
			if selected[c.start] {
				continue
			}
			blockSize := c.sizeKey + c.sizeData
			if s.sizeData >= blockSize+thisSpaceUsed || s.sumKey == 1 {
				selected[c.start] = true

				startBlock := s.start + thisSpaceUsed
				item := listData[c.start]
				key := item.Key
				data := item.Data
				e.addToMapSort(&mapBlockInsert, c, startBlock)
				listWriteData = append(listWriteData, writeDataTemp{
					start: startBlock,
					key:   key,
					data:  data,
				})
				thisSpaceUsed += blockSize
			}
			if s.sumKey == 1 {
				startListBlock = s.start
				totalLastSpaceUsed += blockSize
			}
		}
	}

	for i := 0; i < len(listConfigInsert); i++ {
		c := listConfigInsert[i]
		if selected[c.start] {
			continue
		}
		selected[c.start] = true
		blockSize := c.sizeKey + c.sizeData
		startBlock := startListBlock + totalLastSpaceUsed
		item := listData[c.start]
		key := item.Key
		data := item.Data
		e.addToMapSort(&mapBlockInsert, c, startBlock)
		listWriteData = append(listWriteData, writeDataTemp{
			start: startBlock,
			key:   key,
			data:  data,
		})
		totalLastSpaceUsed += blockSize
	}

	for i := 0; i < len(listBlockInsertPosition); i++ {
		blockData := mapBlockInsert[listBlockInsertPosition[i]]
		infoData := pushBlockToData([]byte{}, &blockData)
		listInfoData = append(listInfoData, infoData...)
	}

	_, err := e.updateListBlock(startListBlock+totalLastSpaceUsed, append(
		newListBlockData, listInfoData...))
	if err != nil {
		return
	}
	for i := 0; i < len(listWriteData); i++ {
		item := listWriteData[i]
		if _, err = e.writer.WriteAt(append(item.key, item.data...), int64(item.start)); err == nil {
			count++
		}
	}
	return
}

func (e *Bucket) getNewListNotContainListKey(
	listBlockData []byte,
	listData *[]Item,
	isReturnListInfo bool,
) (newListBlockData []byte, newListBlockInfo []block) {
	listSkipCheck := map[string]bool{}
	blockInfo := emptyBlock
	var tmpGroup []byte
	for _, v := range listBlockData {
		switch v {
		case cStart:
			blockInfo.start = e.digitsToNumber(tmpGroup)
			tmpGroup = []byte{}
		case cSizeKey:
			blockInfo.sizeKey = e.digitsToNumber(tmpGroup)
			tmpGroup = []byte{}
		case cSumKey:
			blockInfo.sumKey = e.digitsToNumber(tmpGroup)
			tmpGroup = []byte{}
		case cSumMd5:
			blockInfo.sumMd5 = e.digitsToNumber(tmpGroup)
			tmpGroup = []byte{}
		case cSizeData:
			blockInfo.sizeData = e.digitsToNumber(tmpGroup)
			tmpGroup = []byte{}

			isFoundKey := false
			for i := 0; i < len(*listData); i++ {
				curKey := (*listData)[i]
				curKeyMd5 := string(e.md5(curKey.Key))
				if listSkipCheck[curKeyMd5] {
					continue
				}
				key := curKey.Key
				sizeKey := uint(len(key))
				var sumKey uint
				for i := 0; i < len(key); i++ {
					sumKey += uint(key[i])
				}
				var sumMd5 uint
				keyMd5 := e.md5(key)
				for i := 0; i < len(keyMd5); i++ {
					sumMd5 += uint(keyMd5[i])
				}

				if blockInfo.sizeKey == sizeKey && blockInfo.sumKey == sumKey &&
					blockInfo.sumMd5 == sumMd5 {
					foundKey := e.pullKey(blockInfo)
					if bytes.Equal(foundKey, key) {
						// success
						isFoundKey = true
						listSkipCheck[curKeyMd5] = true
						break
					}
				}
			}
			if isFoundKey {
				continue
			}
			newListBlockData = pushBlockToData(newListBlockData, &blockInfo)
			if isReturnListInfo {
				newListBlockInfo = append(newListBlockInfo, blockInfo)
			}

			blockInfo = emptyBlock
		case cEnd:
			break
		default:
			tmpGroup = append(tmpGroup, v)
		}
	}
	return nil, nil
}

func (e *Bucket) List(limit uint8) []Item {
	_, listBlockData := e.getListConfig()
	return e.getListData(listBlockData, limit)
}

func (e *Bucket) getListData(listBlockData []byte, limit uint8) []Item {
	var result []Item
	blockInfo := emptyBlock
	var tmpGroup []byte
	var current uint8 = 0
	for i := 0; i < len(listBlockData); i++ {
		if current >= limit {
			break
		}
		v := listBlockData[i]
		switch v {
		case cStart:
			blockInfo.start = e.digitsToNumber(tmpGroup)
			tmpGroup = []byte{}
		case cSizeKey:
			blockInfo.sizeKey = e.digitsToNumber(tmpGroup)
			tmpGroup = []byte{}
		case cSumKey:
			blockInfo.sumKey = e.digitsToNumber(tmpGroup)
			tmpGroup = []byte{}
		case cSumMd5:
			blockInfo.sumMd5 = e.digitsToNumber(tmpGroup)
			tmpGroup = []byte{}
		case cSizeData:
			blockInfo.sizeData = e.digitsToNumber(tmpGroup)
			tmpGroup = []byte{}

			foundKey, foundData := e.pullData(blockInfo)
			sizeFoundKey := uint(len(foundKey))
			if sizeFoundKey == blockInfo.sizeKey {
				var sumFoundKey uint
				for i := 0; i < len(foundKey); i++ {
					sumFoundKey += uint(foundKey[i])
				}

				if sumFoundKey == blockInfo.sumKey {
					var sumFoundMd5 uint
					keyMd5 := e.md5(foundKey)
					for i := 0; i < len(keyMd5); i++ {
						sumFoundMd5 += uint(keyMd5[i])
					}
					if sumFoundMd5 == blockInfo.sumMd5 {
						// success
						result = append(result, Item{
							Key:  foundKey,
							Data: foundData,
						})
						current += 1
					}
				}
			}

			blockInfo = emptyBlock
		case cEnd:
			break
		default:
			tmpGroup = append(tmpGroup, v)
		}
	}
	return result
}

func (e *Bucket) addToMapSort(mapBlockInsert *map[string]block, c block, startBlock uint) {
	(*mapBlockInsert)[fmt.Sprintf("%d%d%d%d", c.sizeKey, c.sumKey, c.sumMd5, c.sizeData)] = block{
		start:    startBlock,
		sizeKey:  c.sizeKey,
		sumKey:   c.sumKey,
		sumMd5:   c.sumMd5,
		sizeData: c.sizeData,
	}
}

func (e *Bucket) ListNext(limit uint8, skip uint) []Item {
	_, listBlockData := e.getListConfig()
	return e.getListNextData(listBlockData, limit, skip)
}

func (e *Bucket) getListNextData(listBlockData []byte, limit uint8, skip uint) []Item {
	var result []Item
	blockInfo := emptyBlock
	var tmpGroup []byte
	var current uint8 = 0
	var currentSkip uint = 0
	for i := 0; i < len(listBlockData); i++ {
		if current >= limit {
			break
		}
		v := listBlockData[i]
		switch v {
		case cStart:
			blockInfo.start = e.digitsToNumber(tmpGroup)
			tmpGroup = []byte{}
		case cSizeKey:
			blockInfo.sizeKey = e.digitsToNumber(tmpGroup)
			tmpGroup = []byte{}
		case cSumKey:
			blockInfo.sumKey = e.digitsToNumber(tmpGroup)
			tmpGroup = []byte{}
		case cSumMd5:
			blockInfo.sumMd5 = e.digitsToNumber(tmpGroup)
			tmpGroup = []byte{}
		case cSizeData:
			blockInfo.sizeData = e.digitsToNumber(tmpGroup)
			tmpGroup = []byte{}

			foundKey, foundData := e.pullData(blockInfo)
			sizeFoundKey := uint(len(foundKey))
			if sizeFoundKey == blockInfo.sizeKey {
				var sumFoundKey uint
				for i := 0; i < len(foundKey); i++ {
					sumFoundKey += uint(foundKey[i])
				}

				if sumFoundKey == blockInfo.sumKey {
					var sumFoundMd5 uint
					keyMd5 := e.md5(foundKey)
					for i := 0; i < len(keyMd5); i++ {
						sumFoundMd5 += uint(keyMd5[i])
					}
					if sumFoundMd5 == blockInfo.sumMd5 {
						// success
						if currentSkip < skip {
							currentSkip += 1
						} else {
							result = append(result, Item{
								Key:  foundKey,
								Data: foundData,
							})
							current += 1
						}
					}
				}
			}

			blockInfo = emptyBlock
		case cEnd:
			break
		default:
			tmpGroup = append(tmpGroup, v)
		}
	}
	return result
}

func (e *Bucket) FindNext(key []byte, limit uint8, onlyAfterKey bool) []Item {
	_, listBlockData := e.getListConfig()
	return e.getFindNextData(
		listBlockData,
		key,
		limit,
		onlyAfterKey,
	)
}

func (e *Bucket) getFindNextData(
	listBlockData []byte,
	key []byte,
	limit uint8,
	onlyAfterKey bool,
) []Item {
	var result []Item

	sizeCurrentKey := uint(len(key))
	var sumCurrentKey uint
	for i := 0; i < len(key); i++ {
		sumCurrentKey += uint(key[i])
	}
	var sumCurrentMd5 uint
	keyCurrentMd5 := e.md5(key)
	for i := 0; i < len(keyCurrentMd5); i++ {
		sumCurrentMd5 += uint(keyCurrentMd5[i])
	}
	checkIsBegin := false

	blockInfo := emptyBlock
	var tmpGroup []byte
	var current uint8 = 0
	for i := 0; i < len(listBlockData); i++ {
		if current >= limit {
			break
		}
		v := listBlockData[i]
		switch v {
		case cStart:
			blockInfo.start = e.digitsToNumber(tmpGroup)
			tmpGroup = []byte{}
		case cSizeKey:
			blockInfo.sizeKey = e.digitsToNumber(tmpGroup)
			tmpGroup = []byte{}
		case cSumKey:
			blockInfo.sumKey = e.digitsToNumber(tmpGroup)
			tmpGroup = []byte{}
		case cSumMd5:
			blockInfo.sumMd5 = e.digitsToNumber(tmpGroup)
			tmpGroup = []byte{}
		case cSizeData:
			blockInfo.sizeData = e.digitsToNumber(tmpGroup)
			tmpGroup = []byte{}
			if !checkIsBegin && blockInfo.sizeKey == sizeCurrentKey &&
				blockInfo.sumKey == sumCurrentKey &&
				blockInfo.sumMd5 == sumCurrentMd5 {
				foundKey := e.pullKey(blockInfo)
				checkIsBegin = bytes.Equal(foundKey, key)
			}
			if checkIsBegin {
				foundKey, foundData := e.pullData(blockInfo)
				sizeFoundKey := uint(len(foundKey))
				if sizeFoundKey == blockInfo.sizeKey {
					var sumFoundKey uint
					for i := 0; i < len(foundKey); i++ {
						sumFoundKey += uint(foundKey[i])
					}
					if sumFoundKey == blockInfo.sumKey {
						var sumFoundMd5 uint
						keyMd5 := e.md5(foundKey)
						for i := 0; i < len(keyMd5); i++ {
							sumFoundMd5 += uint(keyMd5[i])
						}
						if sumFoundMd5 == blockInfo.sumMd5 {
							if !onlyAfterKey || current > 0 {
								result = append(result, Item{
									Key:  foundKey,
									Data: foundData,
								})
							}
							current += 1
						}
					}
				}
			}

			blockInfo = emptyBlock
		case cEnd:
			break
		default:
			tmpGroup = append(tmpGroup, v)
		}
	}
	return result
}

func (e *Bucket) DeleteTo(key []byte, alsoDeleteTheFoundBlock bool) error {
	_ = syscall.Flock(int(e.fd), syscall.LOCK_EX)
	defer syscall.Flock(int(e.fd), syscall.LOCK_UN)
	e.mu.Lock()
	defer e.mu.Unlock()

	startListPoint, listBlockData := e.getListConfig()
	return e.deleteToData(
		startListPoint,
		listBlockData,
		alsoDeleteTheFoundBlock,
		key,
	)
}

func (e *Bucket) deleteToData(
	startListPoint uint,
	listBlockData []byte,
	alsoDeleteTheFoundBlock bool,
	key []byte,
) error {
	var thisFoundIndex int
	var thisFoundFinishIndex int
	lastFoundBlockInfo := emptyBlock

	var blockControlIndexBefore int
	blockInfo := emptyBlock
	var tmpGroup []byte

	sizeCurrentKey := uint(len(key))
	var sumCurrentKey uint
	for i := 0; i < len(key); i++ {
		sumCurrentKey += uint(key[i])
	}
	var sumCurrentMd5 uint
	keyCurrentMd5 := e.md5(key)
	for i := 0; i < len(keyCurrentMd5); i++ {
		sumCurrentMd5 += uint(keyCurrentMd5[i])
	}

	for i := 0; i < len(listBlockData); i++ {
		v := listBlockData[i]
		switch v {
		case cStart:
			blockInfo.start = e.digitsToNumber(tmpGroup)
			tmpGroup = []byte{}
		case cSizeKey:
			blockInfo.sizeKey = e.digitsToNumber(tmpGroup)
			tmpGroup = []byte{}
		case cSumKey:
			blockInfo.sumKey = e.digitsToNumber(tmpGroup)
			tmpGroup = []byte{}
		case cSumMd5:
			blockInfo.sumMd5 = e.digitsToNumber(tmpGroup)
			tmpGroup = []byte{}
		case cSizeData:
			blockInfo.sizeData = e.digitsToNumber(tmpGroup)
			tmpGroup = []byte{}
			if blockInfo.sizeKey == sizeCurrentKey && blockInfo.sumKey == sumCurrentKey &&
				blockInfo.sumMd5 == sumCurrentMd5 {
				foundKey := e.pullKey(blockInfo)
				if bytes.Equal(foundKey, key) {
					// success
					lastFoundBlockInfo = blockInfo
					thisFoundIndex = blockControlIndexBefore + 1
					thisFoundFinishIndex = i + 1
				}
			}
			blockControlIndexBefore = i
			blockInfo = emptyBlock
		case cEnd:
			break
		default:
			tmpGroup = append(tmpGroup, v)
		}
	}
	if thisFoundIndex == 0 || lastFoundBlockInfo.start == 0 || lastFoundBlockInfo.sizeKey == 0 ||
		lastFoundBlockInfo.sumKey == 0 ||
		lastFoundBlockInfo.sumMd5 == 0 {
		return nil
	}
	var err error
	if alsoDeleteTheFoundBlock {
		_, err = e.updateListBlock(
			startListPoint,
			listBlockData[thisFoundFinishIndex:],
		)
	} else {
		_, err = e.updateListBlock(
			startListPoint,
			listBlockData[thisFoundIndex:],
		)
	}
	return err
}
