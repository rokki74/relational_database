package myDatabase

import(
	"fmt"
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
	ID: uint8
	State: TxnState
	StartLSN: uint32 
	Locks: []Lock
}

type TransactionManager struct{
	nextTxnId: uint8,
	activeTxns: map[uint8]*Transaction
	wal: *wal
	bufferPoool: *bufferPool 
	lockMngr: *LockManager
}

func (tm *TransactionManager) Begin() *Transaction{
	txnId := tm.nextTxnId++

	txn := &Transaction{
		ID: txnId,
		State: TxnActive,
	}

	tm.activeTxns[txnId] = txn
	tm.wal.LogBegin(txnId)

	return txn
}

func (tm *TransactionManager) Commit(txn *Transaction){
	tm.wal.LogCommit(txn.ID)

	tm.wal.Flush()
	txn.State = TxnCommited

	tm.lockMngr.ReleaseLocks(txn.ID)
	delete(tm.activeTxns, txn.ID)
}

func (tm *TransactionManager) Abort(txn *Transaction){
	logs := tm.wal.GetTxnLogs(txn.Id)

	for l :=len(logs) - 1; l >=0; l--{
		tm.wal.Undo(logs[l])
	}

	tm.wal.LogAbort(txn.ID)
	txn.State = TxnAborted
	tm.lockMngr.ReleaseLocks(txn.ID)
}

type LockType int 
const (
	SHARED LockType = iota
	EXCLUSIVE
)

type Lock struct{
	Type LockType
	Resource Resource 
}

type LockManager struct{

}


