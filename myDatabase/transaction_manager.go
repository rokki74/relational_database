package myDatabase

import(
	"log"
)

type TxnType int
const (
	BEGIN TxnType = iota
	INSERT 
	UPDATE 
	DELETE 
	COMMIT
	ROLLBACK
)

type TxnState int 
const (
	TxnActive TxnState = iota
	TxnCommited
	TxnAborted
)

type Transaction struct{
	ID uint8
	State TxnState
	StartLSN uint32 
}

type TransactionManager struct{
	nextTxnId uint8
	ActiveTxns map[uint8]*Transaction
	DbManager *Database_Manager 
	lockMngr *LockManager
}

type LockType int 
const (
	SHARED LockType = iota
	EXCLUSIVE
)

type Lock struct{
	LockId uint8
	LockType LockType
	Holders map[uint8]struct{}
}

type ResourceType uint8
const (
	TableRes ResourceType = iota
	IndexRes
)

type ResourceKey struct {
	  ResourceType ResourceType
    ResourceName string
    ResourcePageId uint32
}

type LockManager struct{
	lockTable map[ResourceKey]Lock
}

func (tm *TransactionManager) Begin() *Transaction{
	txnId := tm.nextTxnId+1

	txn := &Transaction{
		ID: txnId,
		State: TxnActive,
	}

	tm.ActiveTxns[txnId] = txn

	return txn
}

func (tm *TransactionManager) Commit(txn *Transaction){
	logs, present := tm.GetTxnLogs(txn.ID)
	if !present{
		return
	}

	for _, log := range logs{
	  tm.DbManager.WAL.FlushLog(log, tm.DbManager)
  }
	txn.State = TxnCommited

	tm.lockMngr.ReleaseLocks(txn.ID)
	delete(tm.ActiveTxns, txn.ID)
}

func (tm *TransactionManager) Abort(txn *Transaction){
	delete(tm.ActiveTxns, txn.ID)

	logs, exist := tm.GetTxnLogs(txn.ID)
	if !exist{
		txn.State = TxnAborted
   	tm.lockMngr.ReleaseLocks(txn.ID)

		return
	}

	for l :=len(logs) - 1; l >=0; l--{
		tm.DbManager.WAL.Undo(logs[l], tm.DbManager)
	}

	txn.State = TxnAborted
	tm.lockMngr.ReleaseLocks(txn.ID)
}

func (lm *LockManager) LockResource(resK ResourceKey, lck Lock, holderTxn uint8){
	existentLock, exists := lm.lockTable[resK]
	if !exists{
		lm.lockTable[resK] = lck
		return
	}

	if lck.LockType == EXCLUSIVE{
		log.Printf("Another similar transaction is going on!")
		return
	}

	if lck.LockType == existentLock.LockType{
		type EmptyStruct struct{}
		existentLock.Holders[holderTxn] = EmptyStruct{}
		return
	} 
}

func (lm *LockManager) ReleaseLocks(holderTxn uint8){
	for resKey, lck := range lm.lockTable{
		delete(lck.Holders, holderTxn)
		if len(lck.Holders) <= 0{
			delete(lm.lockTable, resKey)
		}
	}
}


