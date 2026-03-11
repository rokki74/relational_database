package myDatabase

import (
	"encoding/binary"
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
	pageId          uint32
	rowCount        uint16
	freeSpaceOffset uint16
	overflowPageId  uint32
	flags           uint8
	slotBitMap      [64]byte
}

type Page struct {
	data [PAGE_SIZE]byte
}

type Slot struct {
	offset uint16
	length uint16
}

type RowId struct{
	PageId uint32,
	SlotId uint16,
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

func (p *Page) killSlotIndex(slot int) {
	byteIndex := slot / 8
	bitIndex := slot % 8
	p.data[BITMAP_OFFSET+byteIndex] |= (1 << bitIndex)
}

func (p *Page) isSlotDead(slot int) bool {
	byteIndex := slot / 8
	bitIndex := slot % 8
	return (p.data[BITMAP_OFFSET+byteIndex] & (1 << bitIndex)) != 0
}

func new_page() *Page {
	return &Page{}
}

func (p *Page) init(pageId uint32) {
	header := PageHeader{
		pageId:          pageId,
		rowCount:        0,
		freeSpaceOffset: uint16(PAGE_HEADER_SIZE),
		overflowPageId:  0,
	}

	p.write_header(&header)
}

func (page *Page) write_header(header *PageHeader) {
	binary.LittleEndian.PutUint32(page.data[0:4], header.pageId)
	binary.LittleEndian.PutUint16(page.data[4:6], header.rowCount)
	binary.LittleEndian.PutUint16(page.data[6:8], header.freeSpaceOffset)
	binary.LittleEndian.PutUint32(page.data[8:12], header.overflowPageId)
	page.data[12] = header.flags
	copy(page.data[13:77], header.slotBitMap[:])

}

func (page *Page) read_header() PageHeader {
	pageData := page.data
	return PageHeader{
		pageId:          binary.LittleEndian.Uint32(pageData[0:4]),
		rowCount:        binary.LittleEndian.Uint16(pageData[4:6]),
		freeSpaceOffset: binary.LittleEndian.Uint16(pageData[6:8]),
		overflowPageId:  binary.LittleEndian.Uint32(pageData[8:12]),
		flags:           pageData[12],
		slotBitMap: [64]byte(pageData[13:77]),
	}
}

func (page *Page) write_slot(slot_index int, slot Slot) {
	pos := PAGE_SIZE - ((uint16(slot_index) + 1) * SLOT_SIZE)
	binary.LittleEndian.PutUint16(page.data[pos:pos+2], slot.offset)
	binary.LittleEndian.PutUint16(page.data[pos+2:pos+4], slot.length)
}

func (page *Page) read_slot(slot_index int) Slot {
	pos := PAGE_SIZE - ((uint16(slot_index) + 1) * SLOT_SIZE)

	return Slot{
		offset: binary.LittleEndian.uint16(page.data[pos : pos+2]),
		length: binary.LittleEndian.uint16(page.data[pos+2 : pos+4]),
	}
}

func (page *Page) free_space() int {
	header := page.read_header()

	slot_start := PAGE_SIZE - int(header.rowCount*SLOT_SIZE)

	return slot_start - int(header.freeSpaceOffset)
}

func (page *Page) insert_row(row string) (*RowID, bool) {
	row_length := len(row)
	needed := int(row_length + SLOT_SIZE)
	if needed > page.free_space() }
     return nil, false
   }

	header := page.read_header()
	//slot indices start from zero while row counts are really starting from 1 on the header
	//thus can use the current to refer to index and later use it's increment to update the row rowCount
	//in the header
	copy(page.data[header.freeSpaceOffset : header.freeSpaceOffset+len(row)], row)
	slot := Slot{
		offset: uint16(header.freeSpaceOffset),
		length: uint16(row_length),
	}

	page.write_slot(int(header.rowCount), slot)

	rowId :=RowID{
		PageId: page.pageId,
		SlotId: header.rowCount,
	}

	header.rowCount += 1
	header.freeSpaceOffset += needed

	page.write_header(&header)
	return &rowId, true
}

func (page *Page) read_row(slot_index int) string {
	slot := page.read_slot(slot_index)

	return string(page.data[slot.offset:slot.length])
}

func (pg *Page) delete_row(slot_index int) {
	pg.killSlotIndex(slot_index)
	pg.setFlag(PAGE_FLAG_LOOSE)
}


