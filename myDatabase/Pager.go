package myDatabase
/*A TableFileHeader i mistakenly deleted, nvim oversights i shall have to reimplement it otherwise the tables shall never
too much on the plate i guess i bite more than i could chew almost giving up but never quiting ths system halfway never
*/
import (
	"log"
	"os"
)

const tempPageId = -1
//Page Cleaner within Page Manager
//Sequentially load pages into mem check whether they have invalidated slots if not evict from
//this memory and keep only ones with invalid pages, do this using a thread to fetch while
//adding while another does the vacuum job on them so a queue is the best, the
//fetcher thread adds to the back of the queue while the vacuum thread picks from front,
//wait isn't this literally like a pipe but with storage then can a go channel serve this purpose??
//But i discovered the mkfifo command that makes a pipe for passing things between processes but once then it closes when received it was mkfifo my_pipe then input>my_pipe then on receiving end cat < my_pipe
func (pg *Page) clean_page(){
   pg.compact_slots()
}

type Pager struct{
	db Database_Manager
	dbdbPath db.dbPath
}

func (pgr *Pager) SaveTable(table *Table){
	dbPath := pgr.dbPath
	//I shall later cross check if really os.Open creates a file if it didn't exist otherwise this code should be so functioning
	f, err := os.Open(dbPath + table.TableName+".tbl")
	if err != nil{
		log.Printf("The table file for table: %v couldn't be created",table.TableName)
	}

	//shall complete the logic later to write to the .tbl file and the fileheader etc
	f.WriteAt(b []byte, off int64)
}

func (pgr *Pager) WritePage(tableFileName string, pageId uint32) bool{
	dbPath := pgr.ddbPath
	f, err := os.Open(dbPath+tableFileName)
	if err != nil{
		log.Print("Error could not write page")
		return false
	}

	f.write(page)
	return true
}

func (pgr *Pager) GetPage(tableFileName string, pageId uint32) Page{
	dbPath := pgr.dbdbPath
	f, err := os.Open(dbPath+tableFileName)
	if err != nil{
		log.Print("Error could not write page")
		return false
	}

	page := Page{}
	f.ReadAt(page, pageId*4096)
	return page
}

func (pg *Page) compact_slots(temp_page *Page){
	
	log.Println("Compacting slots of %v", pg.pageId) 
	header := pg.read_header()

	cursor := 0
	totalSlots := int(header.rowCount)
	for chunk:=0; chunk < 8; chunk++{
		word :=binary.LittleEndian.Uint64(
			header.slotBitMap[chunk*8: (chunk+1)*8]
		)
    
		for word !=0{
				tz : bits.TrailingZeros64(word)
				dead_index := chunk*8 + tz 

				if dead_index > totalSlots{
					break
				}

				for cursor<dead_index{
					temp_page.insert_row(pg.read_row(cursor))
					cursor++
				}

				cursor = dead_index+1

				//clear lowest set bit 
				word &= word -1
	  }
  }

	for cursor<totalSlots{
		temp_page.insert_row(pg.read_row(cursor))
		cursor ++
	}

	 log.Println("Done![===] compacting slots of %v", pg.pageId) 
}

func (pgr *Pager) DeleteTable(tableFileName string) bool{
	fileName := pgr.dbPath +tableFileName+".tbl"
	err := os.Remove(filename)
	if err !nil{
		log.Printf("Could not delete the table and it's file %v", TableFileName)
		return false
	}

	return true
}

