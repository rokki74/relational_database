package myDatabase

type Frame struct{
	FramePage: Page,
	PinCount: uint32,
	Dirty: bool.,
}

type BufferKey{
	FileName: string,
	PageId: uint32,
}

type BufferPool struct{
	pager: Pager,
	/*pageId is the best key in frames map as it is helps not to hold copies of same page in the map
	thus map[pageId]Frame is the best course here*/
	frames: map[BufferKey]Frame,
	capacity: uint,
	tablesMap *Database_Manager.tablesMap
}

func (bf *BufferPool) fetch_page(pageId uint32, fileName string) Page{
	bufKey := BufferKey{fileName, pageId}

	frame,ok = bf.frames[bufKey]
		page,found  := bf.pager.GetPage(fileName, pageId)
		if !found{
			return
		}

		frm :=Frame{
			FramePage: page,
			PinCount: 1,
			Dirty: false,
		}

		//add it to BufferPool
    Pool[BufferKey{fileName, pageId}] = frm

	frame.pinCount +=1
	return frame.page
}

func (bf *BufferPool) SavePage(fileName string, page Page){
	bufferKey := BufferKey{fileName, page.PageId}
  
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
					if bf.Pager.WritePage(key.FileName, key.PageId){
					   val.Dirty = false
					}
      }
  }
}	

func (bf *BufferPool) DeleteTableName(fileName string){

	[]bufkeys := map.keys(bf.frames)
	for key := bufkeys{
		if key.FileName == fileName{
			bf.frames = delete(bf.frames, key)
		}
	}
	
	bf.pager.DeleteTable(fileName)
	bf.tablesMap = delete(bf.tablesMap, fileName)

}

func (bf *BufferPool) MarkDirty(fileName string, pageId uint32{
	bufKey := BufferKey{fileName, pageId}
	bf.frames[bufKey].Dirty = true
}

func (bf *BufferPool) FittingPage(tableFile string, length uint8){

}

