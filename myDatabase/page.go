package myDatabase

import (
	"bytes"
	"encoding/binary"
	"log"
	"math/bits"
)

const PAGE_SIZE = 4096
const SLOT_SIZE = 4
const BITMAP_SIZE = 64
const BITMAP_OFFSET = 13
const PAGE_HEADER_SIZE = 13 + BITMAP_SIZE

const (
	PAGE_FLAG_DIRTY     = 1 << 0
	PAGE_FLAG_TOMBSTONE = 1 << 1
	PAGE_FLAG_OVERFLOW  = 1 << 2
	PAGE_FLAG_LOOSE     = 1 << 3 //Not compact
)

type PageHeader struct {
	PageId          uint32
	RowCount        uint16
	PageLSN         uint64
	FreeSpaceOffset uint16
	OverflowPageId  uint32
	Flags           uint8
	SlotBitMap      [64]byte
}

type Page struct {
	data [PAGE_SIZE]byte
}

type Slot struct {
	offset uint16
	length uint16
}

type RowId struct{
	PageId uint32
	SlotId uint16
}

func (pg *Page) setFlag(fg uint8) {
	pg.data[12] |= fg
}

func (pg *Page) clearFlag(fg uint8) {
	pg.data[12] &^= fg
}

func (pg *Page) checkFlag(fg uint8) bool {
	return (pg.data[12] & fg) != 0
}

func (pg *Page) checkFlags(dirty, tombstone, overflow, loose uint8) (bool, bool, bool, bool) {
	return (pg.data[12] & dirty) != 0, (pg.data[12] & tombstone) != 0, (pg.data[12] & overflow) != 0, (pg.data[12] & loose) != 0
}

func (p *Page) KillSlotIndex(slot int) {
	byteIndex := slot / 8
	bitIndex := slot % 8
	p.data[BITMAP_OFFSET+byteIndex] |= (1 << bitIndex)
}

func (p *Page) SlotDead(slot int) bool {
	byteIndex := slot / 8
	bitIndex := slot % 8
	return (p.data[BITMAP_OFFSET+byteIndex] & (1 << bitIndex)) != 0
}

func new_page() *Page {
	return &Page{}
}

func (p *Page) Init(pageId uint32) {
	header := PageHeader{
		PageId:          pageId,
		RowCount:        0,
		FreeSpaceOffset: uint16(PAGE_HEADER_SIZE),
		OverflowPageId:  0,
	}

	p.Write_header(&header)
}

func (page *Page) Write_header(header *PageHeader) {
	binary.LittleEndian.PutUint32(page.data[0:4], header.PageId)
	binary.LittleEndian.PutUint16(page.data[4:6], header.RowCount)
	binary.LittleEndian.PutUint16(page.data[6:8], header.FreeSpaceOffset)
	binary.LittleEndian.PutUint32(page.data[8:12], header.OverflowPageId)
	page.data[12] = header.Flags
	copy(page.data[13:77], header.SlotBitMap[:])

}

func (page *Page) Read_header() PageHeader {
	pageData := page.data
	return PageHeader{
		PageId:          binary.LittleEndian.Uint32(pageData[0:4]),
		RowCount:        binary.LittleEndian.Uint16(pageData[4:6]),
		FreeSpaceOffset: binary.LittleEndian.Uint16(pageData[6:8]),
		OverflowPageId:  binary.LittleEndian.Uint32(pageData[8:12]),
		Flags:           pageData[12],
		SlotBitMap: [64]byte(pageData[13:77]),
	}
}

func (page *Page) Write_slot(slot_index int, slot Slot) {
	pos := PAGE_SIZE - ((uint16(slot_index) + 1) * SLOT_SIZE)
	binary.LittleEndian.PutUint16(page.data[pos:pos+2], slot.offset)
	binary.LittleEndian.PutUint16(page.data[pos+2:pos+4], slot.length)
}

func (page *Page) Read_slot(slot_index int) Slot {
	pos := PAGE_SIZE - ((uint16(slot_index) + 1) * SLOT_SIZE)

	return Slot{
		offset: binary.LittleEndian.Uint16(page.data[pos : pos+2]),
		length: binary.LittleEndian.Uint16(page.data[pos+2 : pos+4]),
	}
}

func (page *Page) Free_space() int {
	header := page.Read_header()

	slot_start := PAGE_SIZE - int(header.RowCount*SLOT_SIZE)

	return slot_start - int(header.FreeSpaceOffset)
}

func (page *Page) Insert_row(row []byte) (*RowId, bool) {
	row_length := len(row)
	needed := int(row_length + SLOT_SIZE)
	if needed > page.Free_space(){
     return nil, false
   }

	header := page.Read_header()
	
	//slot indices start from zero while row counts are really starting from 1 on the header
	//thus can use the current to refer to index and later use it's increment to update the row rowCount
	//in the header
	copy(page.data[header.FreeSpaceOffset : header.FreeSpaceOffset+uint16(len(row))], row)
	slot := Slot{
		offset: uint16(header.FreeSpaceOffset),
		length: uint16(row_length),
	}

	page.Write_slot(int(header.RowCount), slot)

	rowId := RowId{
		PageId: page.Read_header().PageId,
		SlotId: header.RowCount,
	}

	header.RowCount += 1
	header.FreeSpaceOffset += uint16(needed)

	page.Write_header(&header)
	return &rowId, true
}

func (page *Page) WalkPage(target []byte) (int, bool){
   header := page.Read_header()

   targetLen := uint16(len(target))
	 for s :=0; s<= int(header.RowCount); s++{
	    slot := page.Read_slot(s)
			if slot.length != targetLen{
			  continue
			}
      
			data := page.Read_row(s)
			if bytes.Equal(data, target){
			  return s, true
			}
	 }

	 return -1, false
}

func (page *Page) Read_row(slot_index int) []byte{
	slot := page.Read_slot(slot_index)

	return []byte(page.data[slot.offset:slot.offset+slot.length])
}

func (pg *Page) Delete_row(slot_index int) {
	pg.KillSlotIndex(slot_index)
	pg.setFlag(PAGE_FLAG_LOOSE)
}

func (pg *Page) Compact_slots(tempPage *Page){
	header := pg.Read_header()
	log.Printf("Compacting slots of %v", header.PageId)

	cursor := 0
	totalSlots :=int(header.RowCount)
	for chunk:=0; chunk<8; chunk++{
		word := binary.LittleEndian.Uint64(header.SlotBitMap[chunk*8: (chunk+1)*8])

		for word !=0{
			tz := bits.TrailingZeros(uint(word))
			dead_index := chunk*8 +tz

			if dead_index > totalSlots{
				break
		  }

		  for cursor <totalSlots{
				rowData := pg.Read_row(cursor) 
				_,ok := tempPage.Insert_row(rowData)

				if !ok{
					log.Printf("Temp page lacks enough space for compaction, break...")
					break
				}
				
			  cursor++
		   }

		log.Println("Done![===], compacted the slots of %v", header.PageId)
	   }
  }
}

func (pg *Page) UpdateRowInPlace(rowId *RowId, data []byte) bool{
   slot := pg.Read_slot(int(rowId.SlotId))
	 dataLen := len(data)
	 if int(slot.length) < dataLen{
		 return false
	 }
	 newOffset := int(slot.offset) + dataLen
   copy(pg.data[slot.offset:newOffset], data)
	 return true
 }
