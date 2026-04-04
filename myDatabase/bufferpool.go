package myDatabase

import(
	"log"
	"strings"
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
	Pager Pager
	/*pageId is the best key in frames map as it is helps not to hold copies of same page in the map
	thus map[pageId]Frame is the best course here*/
	frames map[BufferKey]Frame
	capacity uint
	fsm *FSMManager
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
	header := page.Read_header()
	bufferKey := BufferKey{fileName, header.PageId}
  
	frm := Frame{
		FramePage: page,
    PinCount: 1,
    Dirty: true,
	}

	bf.frames[bufferKey] = frm
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
	if !ok{
		return
	}

	datum.Dirty = true
}

func (bf *BufferPool) FittingPage(filepath string, length uint16) (uint32, bool){
	parts := strings.Split(filepath, ".tbl") 
	fsmFile := parts[0]+".fsm"

	for pId :=uint32(0); pId<= bf.fsm.LastFsmPageId; pId++{
  	fsmData, exists := bf.fsm.Data[pId]
	  if !exists{
	  	continue
	  }

		tbls, okay := fsmData.Tbls[fsmFile]
		if !okay{
			continue
		}

		for pageId, bts := range tbls.TblPages{
			if bts >= length{
				log.Printf("FOUND PAGE ID[%v] to have accomodating free bytes returning..", pageId)
				return pageId, true
			}
		}
	}
	return 0, false
}

func (bf *BufferPool) UpdateFsm(filePath string, remainingSpace uint16){

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

