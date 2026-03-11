package myDatabase

type Frame struct{
	FramePage: Page,
	PinCount: uint32,
	Dirty: bool.,
}

type BufferKey{
	TableId: uint16,
	PageId: uint32,
}

type BufferPool struct{
	pager: Pager,
	/*pageId is the best key in frames map as it is helps not to hold copies of same page in the map
	thus map[pageId]Frame is the best course here*/
	frames: map[BufferKey]Frame,
	tablesMap: map[uint16]string,
	capacity: uint,
}

func (bf *BufferPool) fetch_page(pageId uint32, tableId uint16) Page{
	bufKey := BufferKey{tableId, pageId}

	frame,ok = bf.frames[bufKey]
	if !ok{
		tableFileName, present := bf.tablesMap[tableId]
		if !present{
			log.Printf("The table Id %v i not available on the map!",tableId)
			return
		}
		page,found  := bf.pager.GetPage(tableFileName, pageId)
		if !found{
			return
		}

		frm :=Frame{
			FramePage: page,
			PinCount: 1,
			Dirty: false,
		}

		//add it to BufferPool
    Pool[BufferKey{tableId, pageId}] = frm
	}

	frame.pinCount +=1
	return frame.page
}

func (bf *BufferPool) SavePage(tableId uint16, page Page){
	tableFileName, ok := bf.tablesMap[tableId]
	if !ok{
		return
	}

	bufferKey := BufferKey{tableId, page.PageId}
  
	frm := Frame{
		FramePage: Page,
    PinCount: 1,
    Dirty: true,
	}

	Pool[bufferKey] = frm
}

func (bf *BufferPool) evict_pages(){
	
	for key, val := range bf.frames{

	     if val.pin_count == 0{
				 delete(bf.frames, key)

			//persist just to make sure disk doesn't lag far behind after evictions
			 }else if val.Dirty {
					if bf.Pager.PersistPage(key.TableId, key.PageId){
					   val.Dirty = false
					}
      }
  }
}	

func (bf *BufferPool) DeleteTableById(tableId uint16){

	[]bufkeys := map.keys(bf.frames)
	for key := bufkeys{
		if key.TableId == tableId{
			bf.frames = delete(bf.frames, key)
		}
	}
  
	tableFileName, ok := bf.tablesMap[bf.tablesMap[tableId]]
	if !ok{
		log.Printf("TableId %v is currently invalid in the map")
		return
	}
	
	bf.pager.DeleteTable(tableFileName)
	bf.tablesMap = delete(bf.tablesMap, tableId)

}

func (bf *BufferPool) MarkDirty(tableId uint16, pageId uint32{
	bufKey := BufferKey{tableId, pageId}
	bf.frames[bufKey].Dirty = true
}

