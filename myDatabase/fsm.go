//This shall track all free pages and use an appen strategy while vacuum can later get rid upto a certain checkpoint

package myDatabase

type FSMData struct{
	TblPages map[uint32]uint16
}

type FSM struct{
  Tbls map[string]FSMData
}

type FSMManager struct{
 LastFsmPageId uint32
 Data map[uint32]FSM 
}

