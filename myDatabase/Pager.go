package myDatabase

/*A TableFileHeader i mistakenly deleted, nvim oversights i shall have to reimplement it otherwise the tables shall never
too much on the plate i guess i bite more than i could chew almost giving up but never quiting ths system halfway never
*/
import (
	"log"
	"os"
	"io"
)

const tempPageId = -1
const PageSize = 4096
//Page Cleaner within Page Manager
//Sequentially load pages into mem check whether they have invalidated slots if not evict from
//this memory and keep only ones with invalid pages, do this using a thread to fetch while
//adding while another does the vacuum job on them so a queue is the best, the
//fetcher thread adds to the back of the queue while the vacuum thread picks from front,
//wait isn't this literally like a pipe but with storage then can a go channel serve this purpose??
//But i discovered the mkfifo command that makes a pipe for passing things between processes but once then it closes when received it was mkfifo my_pipe then input>my_pipe then on receiving end cat < my_pipe
func (pg *Page) Clean_page(){
	//for pages compaction etc 
}
type Pager struct{
	db Database_Manager
}



func (pgr *Pager) WritePage(tableFileName string, page *Page) bool {
	log.Printf("pager hit, writing page..")
	f, err := os.OpenFile(tableFileName, os.O_CREATE|os.O_RDWR, 0666)
	if err != nil {
		log.Printf("Error opening file for write: %v", err)
		return false
   }
	defer f.Close()

	header := page.Read_header()

	log.Printf("DEBBUGGING THE PAGE DATA ROWS..")
	for s :=0;s<int(header.RowCount);s++{
		datum := page.Read_row(s)
		log.Printf("data inside row %v :DATUM[%v]", s,datum)
		
		log.Printf("string repr %v :DATUM[%v]", s,datum)
	} 

	offset := int64(header.PageId * PageSize)

	bytesWritten, err := f.WriteAt(page.data[:], offset)
	log.Printf("bytes written by pager: %v", bytesWritten)
	if err != nil {
		log.Printf("Error writing page to disk: %v", err)
		return false
	}
	if bytesWritten != PageSize {
		log.Printf("Fatal: Partial write! Expected %d, wrote %d", PageSize, bytesWritten)
		return false
	}

	f.Sync()
	log.Printf("page written to disk successfully!")
	return true
}

func (pgr *Pager) GetPage(tableFileName string, pageId uint32) (*Page, bool) {
	// uniformity of file descriptors? -- needs me to later look at this Pager.go
	f, err := os.OpenFile(tableFileName, os.O_CREATE|os.O_RDWR, 0666)
	if err != nil {
		log.Printf("Error opening file for read: %v", err)
		return &Page{}, false
	}
	defer f.Close()

	page := Page{}
	offset := int64(pageId * PageSize)

	bytesRead, err := f.ReadAt(page.data[:], offset)
	
	//Page is brand new (past the end of the current file)
	if err == io.EOF {
		// bufferPool asked an unpersisted page returning zeroed one
		return &page, true 
	}
	
	if err != nil {
		log.Printf("Error reading page from disk: %v", err)
		return &Page{}, false
	}

	if bytesRead != PageSize {
		log.Printf("Warning: Partial read! Expected %d, read %d", PageSize, bytesRead)
	}

	log.Printf("bytes read back by Pager: %v", bytesRead)
	return &page, true
}

func (pgr *Pager) DeleteTable(tableFileName string) bool{
	err := os.Remove(tableFileName)
	if err != nil{
		log.Printf("Could not delete the table and it's file %v", tableFileName)
		return false
	}

	return true
}
