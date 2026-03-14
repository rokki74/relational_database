//This shall track all free pages and use an appen strategy while vacuum can later get rid upto a certain checkpoint

package myDatabase

import(
	"log"
)

type FSMManager struct{
   FsmFile: string
}

type FSMData struct{
	PageId uint32,
	FreeBytes uint16,
	NextFsmPage uint32,
}

func (fsm *FSMManager) Save(fsmPage Page){
	pager := Pager{}
	pager.WritePage(fsm.FsmFile, fsmPage)
}

func (fsm *FSMManager) fetchFreeTableFsm(tableFile string, neededLength uint64){
	
}



