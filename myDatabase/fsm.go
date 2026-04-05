//This shall track all free pages and use an appen strategy while vacuum can later get rid upto a certain checkpoint

package myDatabase

import(
  "encoding/binary"
	"os"
)

type FSMManager struct{
  TablesRecorded map[string]uint32//val: lastFramePageId 
}

func NewFsmManager() *FSMManager{
	return &FSMManager{ TablesRecorded: make(map[string]uint32, 0)}
}

func (fsm *FSMManager) FillFsms(db *Database_Manager, tableNames []string){
	for _, tblName := range tableNames{
		 fsmPath, exists := db.GetObjectPath(tblName, FSMTYPE)
		 if !exists{
		   continue
		 }
		 var lastFramePageId uint32 = 0

		 f, e := os.Open(fsmPath)
		 if e != nil{
		   continue
		 }
		 i := uint32(0)
		 for{
		   buf := make([]byte, 0)
		   n, err := f.ReadAt(buf, int64(i)*4096)
			 if err != nil{
			   if n>0{
				   lastFramePageId = i
					 break
				 }
			 }

			 i += 1
		 }

		 fsm.TablesRecorded[tblName] = lastFramePageId
	 }
}

func UpdateFSM(fsmPage *Page, tablePgId uint32, writtenLen uint16){
    header := fsmPage.Read_header()

		for s := 0; s<=int(header.RowCount); s++{
		    row := fsmPage.Read_row(s)
				pageId := binary.LittleEndian.Uint32(row[0:4])
				freeBytes :=binary.LittleEndian.Uint16(row[4:6])

        
				fsmPage.Delete_row(s)
				freeBytes += writtenLen
				binary.LittleEndian.PutUint32(row[0:4], pageId)
				binary.LittleEndian.PutUint16(row[4:6], freeBytes)
				fsmPage.Insert_row(row)
		}
}

