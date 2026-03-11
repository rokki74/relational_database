//This shall track all free pages and use an appen strategy while vacuum can later get rid upto a certain checkpoint

package myDatabase

import(
	"log"
)

type FSMManager struct{
   FsmFile: *os.File
}

func (fsm *FSMManager) save(fsmPageId uint32){
   offset := fsmPageId * 4096

	 f := fsm.FsmFile
	 f.WriteAt(offset)
}



