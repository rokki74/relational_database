//This shall track all free pages and use an appen strategy while vacuum can later get rid upto a certain checkpoint

package myDatabase

type FSMData struct{
	FSMPageId uint32
	FSMMap map[uint32]uint16
}

type FSMManager struct{
	FsmFile string
	FSMData *FSMData
}

