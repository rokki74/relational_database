package myDatabase

/*A TableFileHeader i mistakenly deleted, nvim oversights i shall have to reimplement it otherwise the tables shall never
too much on the plate i guess i bite more than i could chew almost giving up but never quiting ths system halfway never
*/
import (
	"encoding/binary"
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
func (pg *Page) Clean_page(){
   pg.Compact_slots()
}

type Pager struct{
	db Database_Manager
}

//This was supposed to be handled by catalog, to cross check later with catalog
func (pgr *Pager) SaveTable(table *Table, filePath string) bool{
	//I shall later cross check if really os.Open creates a file if it didn't exist otherwise this code should be so functioning
	f, err := os.Open(filePath)
	defer f.Close()
	if err != nil{
		log.Printf("The table file for table: %v couldn't be created", table.TableName)
		return false 
	}

  page := new_page()
	page.Init(0)
	page.Insert_row(table.TableName)
	header := page.Read_header()
	binary.LittleEndian.PutUint32(page.data[header.freeSpaceOffset:header.freeSpaceOffset+4], uint32(table.LastPageId))
	binary.LittleEndian.PutUint32(page.data[:])
	
	//shall complete the logic later to write to the .tbl file and the fileheader etc
	f.WriteAt(page.data, 0)
	return true
}

func (pgr *Pager) WritePage(tableFileName string, page Page) bool{
	f, err := os.OpenFile(tableFileName, os.O_CREATE|os.O_WRONLY, 0666)
	defer f.Close()
	if err != nil{
		log.Print("Error could not write page")
		return false
	}

  header := page.Read_header()
	f.WriteAt(page.data,int64(header.PageId*4096))
	return true
}

func (pgr *Pager) GetPage(tableFileName string, pageId uint32) (Page, bool){
	f, err := os.Open(tableFileName)
	defer f.Close()
	if err != nil{
		log.Print("Error could not write page")
		return Page{}, false
	}

	page := Page{}
	f.ReadAt(page.data, int64(pageId*4096))
	return page, true
}

func (pgr *Pager) DeleteTable(tableFileName string) bool{
	err := os.Remove(tableFileName)
	if err != nil{
		log.Printf("Could not delete the table and it's file %v", tableFileName)
		return false
	}

	return true
}

