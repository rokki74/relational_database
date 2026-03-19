package myDatabase

import(
	"fmt"
)

/* REFERENCE FOR IMPL
The wal shld check that txn 12 has no commit record therefore undoes it wal.Undo(12) maintaining consistency
Begin txn 12
    ↓
LogBegin
    ↓
Insert tuple
    ↓
LogInsert
    ↓
Insert tuple
    ↓
LogInsert
    ↓
Commit
    ↓
LogCommit
    ↓
Flush WAL
    ↓
Release locks

So the transaction manager integrates with:
table.Insert(txn, tuple)
index.Insert(txn, key, rowPointer)
bufferPool.MarkDirty(txn)

tuple := BuildTuple(values)

tuple.txnID = txnID

pageID,slot := table.Insert(tuple)

wal.LogInsert(txnID,pageID,slot,tuple)

for each log record:

    if INSERT:
        if pageLSN < logLSN
            redo

    if COMMIT:
        txnState = committed
*/

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

type LockManager struct{
	lockTable map[Resource]Lock
}

func (lm *LockManager) AddLock(id Transaction.Id){
	/*what really is a Transaction should it have resources it is using maybe the pages it is writing etc?
	so we just lock all its resources until it is committed/aborted? */
	//add the lock
	lm.lockTable
}

func (lm *LockManager) ReleaseLocks(id Transaction.ID){
	//remove
	delete(lm.lockTable, id)
}

