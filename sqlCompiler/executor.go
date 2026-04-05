package sqlCompiler

import (
	"log"
	"real_dbms/myDatabase/system"
	"real_dbms/myDatabase"
	"strings"
)

type Executor struct{
	session *system.Session
	syst *system.DBSystem
}

func (e *Executor) Runner(clientData string, c chan [][]string) [][]string{
	sql := strings.Split(clientData, ";")
	var results [][]string
	for i := 0; i < len(sql); i++{
	  lexer := NewLexer(sql[i])
		parser := NewParser(lexer)
		stmt := parser.ParseStatement(e)
		c <- e.Execute(stmt)
	}

	return results
}

func (e *Executor) Execute(stmt Statement) [][]string {
    switch s := stmt.(type) {

    case *SelectStmt:
			vals, ok := e.execSelect(s)
				if !ok{
					log.Printf("No data to select!, --debugging purposes")
					return nil
				}
				return vals
		case *InsertStmt:
        e.execInsert(s)
        return nil
		case *DeleteStmt:
			  e.execDelete(s)
			  return nil
		case *UpdateStmt:
			  e.execUpdate(s)
				return nil
		case *CreateDBStmt:
			 e.execCreateDB(s)
		case *CreateTBLStmt:
			 e.execCreateTbl(s)
		case *CreateIDXStmt:
			 e.execCreateIDX(s)
    default:
        log.Printf("unsupported statement")
				return nil
    }
		return nil
}

func (e *Executor) execSelect(stmt *SelectStmt) ([][]string, bool) {
		db, ok := e.syst.GetDatabase(e.session.CurrentDB.Dbname)
		if !ok{
			log.Printf("Database unavailable for table deletion operation!")
			return nil, false
		}
    table, exists := db.GetTable(stmt.Table)
		if !exists{
			log.Printf("Table does not exist")
			return nil, false
		}

    var results [][]string

    // full table scan (start simple)
		for pageID := uint32(0); pageID <= table.LastPageId; pageID++ {
        tablePath, okay := db.GetTablePath(table.TableName)
				if !okay{
					return nil, false
				}
        page, present := db.BufferPool.FetchPage(pageID, tablePath)
        if !present{
					log.Printf("Page not found! [TablePath: %v, PageId: %v] ", tablePath, pageID)
					return nil, false
				}
				header := page.Read_header()
        for slot := 0; slot < int(header.RowCount); slot++ {

            if !page.IsAlive(slot) {
                continue
            }

            tuple := page.Read_row(slot)

            if stmt.Where != nil {
                if !e.evalExpr(stmt.Where, tuple) {
                    continue
                }
            }

            row := e.project(stmt.Columns, tuple)
            results = append(results, row)
        }
    }
    return results, true
}

func (e *Executor) execInsert(stmt *InsertStmt) {
		db, ok := e.syst.GetDatabase(e.session.CurrentDB.Dbname)
		if !ok{
			log.Printf("Database unavailable for table deletion operation!")
			return
		}
    table, exists := db.GetTable(stmt.Table)
		if !exists{
			log.Printf("Table does not exist")
			return
		}

    // 1. Evaluate values
    values := e.evalInsertValues(stmt.Values)

    // 2. Encode tuple
		tupleBytes := table.EncodeTuple(stmt.Column, values)

    // 3. Find page with space (FSM)
		tblPath := db.GetObjPath(table.TableName, TABLETYPE)
		fsmPath := db.GetObjPath(table.TableName, FSMTYPE)
    pageID, fsmPage, availed := db.BufferPool.FittingPage(fsmPath, uint16(len(tupleBytes)))
		if !availed{
			return
		}
    
		page, found := db.BufferPool.FetchPage(pageID, tblPath)
		if found{
			page.Insert_row(tupleBytes)
			db.BufferPool.fsm.UpdateFSM(&fsmPage, pageID, uint16(stmt.Values))
			db.BufferPool.SavePage(fsmPath, fsmPage)
		}

    // fallback: allocate new page
		pg := db.BufferPool.AllocatePage(&table)
    db.BufferPool.SavePage(tblPath, *pg)
    // 5. Insert into page
    slot := page.Insert_row(tupleBytes)

    // 6. WAL logging
    db.WAL.LogInsert(pageID, slot, tupleBytes)

    // 7. Update indexes
    e.db.UpdateIndexesUpdateIndexes(table, values, pageID, slot)
}

func (e *Executor) evalExpr(expr Expr, tuple Tuple) bool {

    switch ex := expr.(type) {

    case *BinaryExpr:
        left := e.evalValue(ex.Left, tuple)
        right := e.evalValue(ex.Right, tuple)

        switch ex.Op {

        case EQ:
            return left == right

        case NEQ:
            return left != right

        case LT:
            return left < right

        case GT:
            return left > right

        case LTE:
            return left <= right

        case GTE:
            return left >= right

        case AND:
            return e.evalExpr(ex.Left, tuple) &&
                   e.evalExpr(ex.Right, tuple)

        case OR:
            return e.evalExpr(ex.Left, tuple) ||
                   e.evalExpr(ex.Right, tuple)
        }

    default:
        log.Printf("invalid expression")
    }

    return false
}

func (e *Executor) evalValue(expr Expr, tuple Tuple) string {

    switch ex := expr.(type) {

    case *Identifier:
        return tuple.Get(ex.Name)

    case *NumberLiteral:
        return ex.Value

    case *StringLiteral:
        return ex.Value

    default:
        log.Printf("invalid value expression")
    }
}

func (e *Executor) project(columns []string, tuple Tuple) []string {

    var row []string

    for _, col := range columns {
        row = append(row, tuple.Get(col))
    }

    return row
}

func (e *Executor) evalInsertValues(exprs []Expr) []string {
    var values []string

    for _, expr := range exprs {
        v := e.evalValue(expr, nil) // no tuple needed
        values = append(values, v)
    }

    return values
}

func (e Executor) execDelete(stmt *DeleteStmt){
	db, ok := e.syst.GetDatabase(e.session.CurrentDB.Dbname)
	if !ok{
		log.Printf("Database unavailable for table deletion operation!")
		return
	}
	table, exists := db.GetTable(stmt.Table)
	if !exists{
		log.Printf("Database does not exist, cannot delete")
		return
	}

	for pageId := uint32(0); pageId <= table.LastPageId; pageId++{
		page := db.BufferPool.FetchPage(pageId, db.GetTablePath(table.TableName))
		header := page.read_header()

		for s :=0; s<header.rowCount; s++{
			if page.SlotDead(s){continue}

			rowId := RowId{pageId, s}

			if stmt.Where !=nil{
				if !e.evalExpr(stmt.Where, rowId){
					continue
				}
			}

			page.KillSlotIndex(s)
			db.WAL.LogDelete(pageId, slot)
		}
	}
}


func (e *Executor) execUpdate(stmt *UpdateStmt) {
		db, ok := e.syst.GetDatabase(e.session.CurrentDB.Dbname)
		if !ok{
			log.Printf("Database unavailable for table deletion operation!")
			return
		}
    table, exists := db.GetTable(stmt.Table)
		if !exists{
			log.Printf("Cannot update, --Database doesn't exist!")
			return
		}

    for pageID := uint32(0); pageID <=table.LastPageId; pageID++ {

        page := db.BufferPool.FetchPage(pageID)

        for slot := 0; slot < page.NumSlots(); slot++ {

            if !page.IsAlive(slot) {
                continue
            }

            oldTuple := page.ReadTuple(slot)

            if stmt.Where != nil {
                if !e.evalExpr(stmt.Where, oldTuple) {
                    continue
                }
            }

            // 1. Build updated tuple
            newValues := e.applyUpdate(stmt, oldTuple)

            newTupleBytes := table.EncodeTupleFromMap(newValues)

            // 2. Decide: in-place or move
            if page.CanFitInPlace(slot, len(newTupleBytes)) {

                page.UpdateTuple(slot, newTupleBytes)

                db.WAL.LogUpdateInPlace(pageID, slot, newTupleBytes)

            } else {

                // mark old as deleted
                page.MarkDeleted(slot)
                db.WAL.LogDelete(pageID, slot)

                // insert new tuple
                newPageID := db.FSM.FindPageWithSpace(table.ID, len(newTupleBytes))
                if newPageID == -1 {
                    newPageID = db.AllocatePage(table.ID)
                }

                newPage := db.BufferPool.Fetch(newPageID)
                newSlot := newPage.InsertTuple(newTupleBytes)

                db.WAL.LogInsert(newPageID, newSlot, newTupleBytes)

                // update indexes
                e.updateIndexes(table, newValues, newPageID, newSlot)
            }
        }
    }
}

func (e *Executor) applyUpdate(stmt *UpdateStmt, tuple Tuple) map[string]string {

    result := make(map[string]string)

    // copy old values
    for k, v := range tuple.Values {
        result[k] = v
    }

    // apply SET clause
    for col, expr := range stmt.Set {
        result[col] = e.evalValue(expr, tuple)
    }

    return result
}

func (e *Executor) execCreateDB(stmt Statement){
	dbName := stmt.DBName
	e.syst.CreateDatabase(dbName)
}

func (e *Executor) execCreateTbl(stmt Statement){
  dbMngr, exists := e.syst.GetDatabase(e.session.CurrentDB.Dbname)
	if !exists{
		log.Printf("Unable to create table in a non-existent database!")
		return
	}
  dbMngr.CreateTable(stmt.TBLName)
}

func (e *Executor) execCreateIDX(stmt Statement){
   dbMngr, exists := e.syst.GetDatabase(e.session.CurrentDB.Dbname)
	 if !exists{
		 log.Printf("Can't create an index on non-existent database")
		 return
	 }

	 table, ok := dbMngr.GetTable(stmt.ParentTableName)
	 if !ok{
		 log.Printf("Table not available, might be deleted! %v", stmt.ParentTableName)
	 }

	 table.CreateIndex(stmt.IDXName, stmt.Columns)
}

