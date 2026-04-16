package myDatabase

import(
	"log"
	"encoding/binary"
)

type Frame struct{
	FramePage Page
	PinCount uint32
	Dirty bool
}

type BufferKey struct{
	FileName string
	PageId uint32
}

type BufferPool struct{
	Pager *Pager
	/*pageId is the best key in frames map as it is helps not to hold copies of same page in the map
	thus map[pageId]Frame is the best course here*/
	frames map[BufferKey]Frame
	capacity uint
	Fsm *FSMManager
}

func (bf *BufferPool) FetchPage(pageId uint32, fileName string) (*Page, bool){
	bufKey := BufferKey{fileName, pageId}

	frame,ok := bf.frames[bufKey]
	if !ok{
		page,found  := bf.Pager.GetPage(fileName, pageId)
		if !found{
			return nil, false
		}

		frm :=Frame{
			FramePage: page,
			PinCount: 1,
			Dirty: false,
		}

		//add it to BufferPool
		bf.frames[BufferKey{fileName, pageId}] = frm
		return &page, true
  }

	frame.PinCount +=1
	return &frame.FramePage, true
}

func (bf *BufferPool) SavePage(fileName string, page Page){
	log.Printf("save page hit..")
	header := page.Read_header()
	bufferKey := BufferKey{fileName, header.PageId}
  
	frm := Frame{
		FramePage: page,
    PinCount: 1,
    Dirty: true,
	}
  log.Printf("bufferKey: %v", bufferKey)
	if bf.frames ==nil{
		log.Printf("the buffer frames not initialized yet, initializing the map..")
		bf.frames = make(map[BufferKey]Frame)
	}
	log.Printf("setting the buffer frame..")
	bf.frames[bufferKey] = frm
	log.Printf("save page was a success")
}

func (bf *BufferPool) FlushTable(tablePath string, tb *Table){
	log.Printf("flushing the whole table to disk..")
	if tb.LastPageId <1{
		log.Printf("Flush page found less than one pages for the table, flushing only one..")
			log.Printf("No in-mem or disk page found for the table: %v, so saving and persisting it's first", tb.TableName)
			page := Page{}
			page.Init(0)
			bf.Pager.WritePage(tablePath, page)
			bf.SavePage(tablePath, page)
			return
	  }

	log.Printf("Flush page found more pages flushing all..")
	for pgId :=uint32(0); pgId <=tb.LastPageId;pgId++{
		 page,ok := bf.FetchPage(pgId, tablePath)
		 if ok{
		    bf.Pager.WritePage(tablePath, *page)
		 }
	 }
}

func (bf *BufferPool) evict_pages(){
	for key, val := range bf.frames{

	     if val.PinCount == 0{
				 delete(bf.frames, key)

			//persist just to make sure disk doesn't lag far behind after evictions
			 }else if val.Dirty {
				 if bf.Pager.WritePage(key.FileName, val.FramePage){
					 val.Dirty = false
				 }
      }
  }
}	

func (bf *BufferPool) DeleteTableName(fileName string){
	for key, _ := range bf.frames{
		if key.FileName == fileName{
			delete(bf.frames, key)
		}
	}
	
	bf.Pager.DeleteTable(fileName)
	//catalog really needs to be talked to, i've left it behind for so long!
}

func (bf *BufferPool) MarkDirty(fileName string, pageId uint32){
	bufKey := BufferKey{fileName, pageId}
	datum, ok := 	bf.frames[bufKey]
	if ok{
		datum.Dirty = true
		bf.frames[bufKey] = datum
		return
  }

	log.Printf("pageId[%v] of table[%v] marked dirty..", pageId, fileName)
}

func (bf *BufferPool) FittingPage(tb *Table, length uint16) (uint32, *Page, bool){
	log.Printf("FittingPage hit..\n finding fsm path to get free page of atleast size[%v]",length)
	fsmPath, prsnt := tb.Db.GetFsmPath(tb.TableName)
	if !prsnt{
		log.Printf("problem, cannot find fsmPath, returning..")
		return 0, nil, false
	}
	log.Printf("fsmPath found at FittingPage func, path[%v]",fsmPath)
	log.Printf("Looking at Fsm table records next..")
	if bf.Fsm.TablesRecorded ==nil{
		bf.Fsm.TablesRecorded = make(map[string]uint32, 0)
	}
	lastFramePageId,ok := bf.Fsm.TablesRecorded[tb.TableName]
	if !ok{
		log.Printf("The fsm records not found for the table %v",tb.TableName)
		return 0, nil, false
	}

	//Just remembered i need to scan from the last fsm page as it shall be the freshest then
	for pId :=lastFramePageId; pId>=0; pId--{//i have to placeit here though uint32 is always >=0
		fsmPage, prsnt := bf.FetchPage(pId, fsmPath)
		if !prsnt{
			continue
		}

		header := fsmPage.Read_header()
		for s:=0; s<=int(header.RowCount); s++{
			row := fsmPage.Read_row(s)
			var pageId uint32
			var freeBytes uint16
			binary.LittleEndian.PutUint32(row[0:4], pageId)
			binary.LittleEndian.PutUint16(row[4:6], freeBytes)

			if freeBytes >= length{
				log.Printf("FOUND PAGE ID[%v] to have accomodating free bytes returning..", pageId)
				return pageId, fsmPage, true
			}

		}
	}
	return 0, &Page{}, false
}

func (bf *BufferPool) FlushAll(){
	for key, val := range bf.frames{

	     if val.PinCount == 0{
				 delete(bf.frames, key)

			//persist just to make sure disk doesn't lag far behind after evictions
			 }else if val.Dirty {
				 if bf.Pager.WritePage(key.FileName, val.FramePage){
					 val.Dirty = false
				 }
      }

			delete(bf.frames, key)
  }

}

func (bf *BufferPool) AllocatePage(table *Table) *Page{
  pg := &Page{}
	allocId := table.LastPageId + 1
	pg.Init(allocId)

	return pg
}

