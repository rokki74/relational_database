package sqlCompiler

import (
	"log"
	"real_dbms/myDatabase"
	"strconv"
	"strings"
	"encoding/binary"
)

var TABLEResource uint8 = 0 
var INDEXResource uint8 = 1

type Executor struct{
	CurrentDB *myDatabase.Database_Manager
	Syst *myDatabase.DBSystem
}

type TupData struct{
	Type myDatabase.ColumnType
	Value string
}

type Tuple struct{
	Tup map[string]TupData
}

func (e *Executor) Runner(clientData string, c chan [][]string) [][]string{
	sql := strings.Split(clientData, ";")
	var results [][]string
	for i := 0; i < len(sql); i++{
	  lexer := NewLexer(sql[i])
		parser := NewParser(lexer)
		stmt := parser.ParseStatement()
		c <- e.Execute(stmt)
	}

	return results
}

func (e *Executor) Execute(stmt Statement) [][]string {
    switch s := stmt.(type) {
    case *UseStmt:
			e.execUseStmt(s)
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

//I highly suspect this to also be errenous just don't know where exactly so i'm trying to look at it
//let's run it again see what happens
func (e *Executor) execUseStmt(stmt *UseStmt){
  log.Printf("executing the use statement, trying to get the db from system catalog")
	dbMngr, ok := e.Syst.GetDatabase(stmt.DBName)
	if !ok{
		log.Printf("Database unavailable for the executor to use in its operation!")
		return
	}

	log.Printf("Get database was a success, adding it as the current session of the executor, DBNAME: %v", dbMngr.Dbname)
	e.CurrentDB = dbMngr

	log.Printf("checking whether the e.CurrentDB is set: CurrentDB[%v]", e.CurrentDB.Dbname)
}

func (e *Executor) execSelect(stmt *SelectStmt) ([][]string, bool) {
	 //I think checking this way might be the problem so how do i do this? can't directly check if it is a nil??
	 //Okay let me print the e.CurrentDB and really see
	 log.Printf("e.CurrentDB is: %v", e.CurrentDB)
			db := e.CurrentDB
			if _, prsnt := e.Syst.GetDatabase(db.Dbname); !prsnt{
				log.Printf("Database doesn't exist for real")
				return nil, false
			}			
			table, exists := db.GetTable(stmt.TBLName)
			if !exists{
				log.Printf("Table does not exist")
				return nil, false
			}

			var results [][]string
			tablePath := db.GetTablePath(table.TableName)
			for pageID := uint32(0); pageID <= table.LastPageId; pageID++ {
					page, present := db.BufferPool.FetchPage(pageID, tablePath)
					if !present{
						log.Printf("Page not found! [TablePath: %v, PageId: %v] ", tablePath, pageID)
						continue
					}
					header := page.Read_header()
					for s := 0; s < int(header.RowCount); s++ {
							if !page.SlotDead(s) {
									continue
							}

							tupleBs := page.Read_row(s)
							//Build tuple
							tuple := e.buildTup(table.TableSchema, tupleBs)
							tp := rowByteIntoTuple(table.TableSchema, tupleBs)
							if stmt.Where != nil {
									if !e.evalExpr(stmt.Where, *tp) {
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
	  log.Printf("Insert stmt hit..")
		db := e.CurrentDB

    log.Printf("Scouting at executor to see whether the db struct has died/idempotent..\n dbName is: ")
		log.Printf(db.Dbname)
		 //a crude test here for the time being and see 
			if _, prsnt := e.Syst.GetDatabase(db.Dbname); !prsnt{
				log.Printf("Critical, the database is really not set or unavailable, yeah")
				return
			}

    log.Printf("Scouting at executor to see whether the db struct has died/idempotent..\n dbName is: ")
		log.Printf(db.Dbname)

			table, exists := db.GetTable(stmt.TBLName)
			if !exists{
				log.Printf("Table does not exist, trying without")
			}

    log.Printf("Scouting at executor to see whether the db struct has died/idempotent..\n dbName is: ")
		log.Printf(db.Dbname)

    tblPath := db.GetTablePath(table.TableName)
    log.Printf("tblPath at executor.go line 162: %v", tblPath)
		if tblPath == ""{
			log.Printf("risky, the tbltblPath is empty..")
		}
		log.Printf("Scouting at executor to see whether the db struct has died/idempotent..\n dbName is: ")
		log.Printf(db.Dbname)
		fsmPath, _ := db.GetFsmPath(table.TableName)

    log.Printf("Scouting to see whether the db struct has died/idempotent..\n dbName is: ")
		log.Printf(db.Dbname)

    log.Printf("Table found, proceeding with the insert")
    colTypes := make([]myDatabase.ColumnType, len(stmt.Columns))
		colNames := make([]string, len(stmt.Columns))
    for _, colName := range stmt.Columns{
			colType, _, prsnt := table.FindColumnTypeAndPos(colName)
			if !prsnt{
				continue
			}
			colTypes = append(colTypes, colType)
			colNames = append(colNames, colName)
		} 

    // 1. Evaluate values
    values := make([]string, 0)
		log.Printf("Evaluating the values for insert..")
		for _, val := range stmt.Values{
			values = append(values, e.evalValue(val, Tuple{}))
		}
    log.Printf("Done evaluating")
    // 2. Encode tupleBytes/serializedBs 
		tupleBytes := table.SerializeColumnValues(values, colTypes)

    log.Printf("Scouting at executor to see whether the db struct has died/idempotent..\n dbName is: ")
		log.Printf(db.Dbname)

    // 3. Find page with space (FSM)
		log.Printf("looking to find a fitting page..")
    pageID, fsmPage, availed := db.BufferPool.FittingPage(table.TableName, fsmPath, uint16(len(tupleBytes)))
		if availed{
			  log.Printf("A fitting page was found, using it..")
				page, got := db.BufferPool.FetchPage(pageID, tblPath)
				if got{
					rowId, _ := page.Insert_row(tupleBytes)
					db.BufferPool.Fsm.UpdateFSM(fsmPage, pageID, uint16(len(tupleBytes)))
					db.BufferPool.SavePage(fsmPath, *fsmPage)

					// 6. WAL logging
					db.WAL.LogInsert(table.TableName, myDatabase.ResourceType(TABLEResource), *rowId, tupleBytes)

					// 7. Update indexes
					//e.CurrentDB.UpdateIndexes(&table, *rowId, colNames)
          db.InsertIntoIndexes(&table, *rowId, tupleBytes)
					log.Printf("Insert was a success!")
				}

				log.Printf("Using fsm page was unsuccessful, switching..")
		}
   
		log.Printf("Fitting page not found, settling for the last page instead..")
		log.Printf("the last pageId that shall be used: %v", table.LastPageId)
		log.Printf("at executor.go line 221")
		page, found := db.BufferPool.FetchPage(table.LastPageId, tblPath)
		if !found{
			
			log.Printf("Insert was unsuccessful!, The BufferPool wouldn't get the lastPage of a table thru id[%v]", table.LastPageId)
			return
		}
    // 5. Insert into page
    rowId, possible := page.Insert_row(tupleBytes)
		if !possible{
			pg := db.BufferPool.AllocatePage(&table)
			rowId, done := pg.Insert_row(tupleBytes)
			if !done{
				log.Printf("Totally impossible to make an insert, this was an attempt on newly allocated page!")
				return
			}
			db.BufferPool.SavePage(tblPath, *pg)

			// 6. WAL logging
			db.WAL.LogInsert(table.TableName, myDatabase.ResourceType(TABLEResource), *rowId, tupleBytes)

			// 7. Update indexes
			e.CurrentDB.InsertIntoIndexes(&table, *rowId, tupleBytes)
			
			log.Printf("Insert was a success!")

			log.Printf("flushing for now..")
			db.BufferPool.FlushTable(tblPath, &table)
			return
		}

    // 6. WAL logging
    db.WAL.LogInsert(table.TableName, myDatabase.ResourceType(TABLEResource), *rowId, tupleBytes)

    // 7. Update indexes
    e.CurrentDB.InsertIntoIndexes(&table, *rowId, tupleBytes)

		log.Printf("Insert was a success!")
			log.Printf("flushing for now..")
			db.BufferPool.FlushTable(tblPath, &table)
}

func (e *Executor) evalExpr(expr Expr, tuple Tuple) bool {

    switch ex := expr.(type) {

    case *BinaryExpr:
        left := e.evalValue(ex.Left, tuple)
        right := e.evalValue(ex.Right, tuple)

				log.Printf("Just for the fun of it let me visualize how the evalExpr output looks like, here it is:\n %v\n", e.evalExpr(expr, tuple))
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
		/*val, ok := tuple.Get(ex.Value)
		if !ok {
			return ""
		}*/
		log.Printf("returning this for now but Identifier logic needed!")
		return ex.Name

	case *NumberLiteral:
		return ex.Value

	case *StringLiteral:
		return ex.Value
	}

	return ""
}


func (e *Executor) project(columns []string, tuple Tuple) []string {

    var row []string

    for _, col := range columns {
			  tupData, ok := tuple.Get(col)
				if !ok{
					continue
				}
        row = append(row, tupData.Value)
    }

    return row
}

func (e *Executor) evalInsertValues(exprs []Expr) []string {
    var values []string

    for _, expr := range exprs {
        v := e.evalValue(expr, Tuple{})
        values = append(values, v)
    }

    return values
}

func (e *Executor) execDelete(stmt *DeleteStmt){
	db := e.CurrentDB
		 //let's perform a crude test here for the time being and see 
			if _, prsnt := e.Syst.GetDatabase(db.Dbname); !prsnt{
				log.Printf("Critical, the database is really not set or unavailable, yeah")
				return
			}
	table, exists := db.GetTable(stmt.TBLName)
	if !exists{
		log.Printf("Database does not exist, cannot delete")
		return
	}

	for pageId := uint32(0); pageId <= table.LastPageId; pageId++{
		tablePath := db.GetTablePath(table.TableName)

		page, exists := db.BufferPool.FetchPage(pageId, tablePath) 
		if !exists{
			continue
		}
		header := page.Read_header()
    
		for s :=0; s<int(header.RowCount); s++{
			if page.SlotDead(s){continue}

			rowId := myDatabase.RowId{PageId:pageId, SlotId:uint16(s)}

			staleBytes := page.Read_row(s)
			if stmt.Where !=nil{
				tuple := Tuple{}
				tuple.Tup = make(map[string]TupData)
				stTuple := rowByteIntoTuple(table.TableSchema, staleBytes)
				if !e.evalExpr(stmt.Where, *stTuple){
					continue
				}
			}

      page.Delete_row(s)
			db.WAL.LogDelete(table.TableName, myDatabase.ResourceType(TABLEResource), rowId, staleBytes)

      db.DeleteFromIndexes(&table, rowId, staleBytes)
		}
	}
}


func (e *Executor) execUpdate(stmt *UpdateStmt) {
		db := e.CurrentDB
		 //let's perform a crude test here for the time being and see 
			if _, prsnt := e.Syst.GetDatabase(db.Dbname); !prsnt{
				log.Printf("Critical, the database is really not set or unavailable, yeah")
				return
			}
    table, exists := db.GetTable(stmt.TBLName)
		if !exists{
			log.Printf("Cannot update, --Database doesn't exist!")
			return
		}

    tablePath := db.GetTablePath(table.TableName)
    for pageID := uint32(0); pageID <=table.LastPageId; pageID++ {
        page, exists := db.BufferPool.FetchPage(pageID, tablePath)
				if !exists{
					continue
				}
        
				header := page.Read_header()
        for slot := 0; slot < int(header.RowCount); slot++ {
            if page.SlotDead(slot) {
							  log.Printf("slot found dead!")
                continue
            }

            oldBytes := page.Read_row(slot)
            oldTup := e.buildTup(table.TableSchema, oldBytes)
            tupl := rowByteIntoTuple(table.TableSchema, oldBytes)
            if stmt.Where != nil {
                if !e.evalExpr(stmt.Where, *tupl) {
                    continue
                }
            }

            // 1. Build updated tuple
            newTup := e.applyUpdate(stmt, oldTup)

            newTupleBytes := newTup.turnToBytes(table.TableSchema)


						rowId := myDatabase.RowId{pageID, uint16(slot)}
            // 2. Decide: in-place or move
						updated := page.UpdateRowInPlace(&rowId, newTupleBytes)
						if updated{
                db.WAL.LogUpdateInPlace(table.TableName, myDatabase.ResourceType(TABLEResource), rowId, newTupleBytes, oldBytes)
                // update indexes
                //e.CurrentDB.UpdateIndexes(table, rowId, newValues)
								db.UpdateIndexes(
											&table,
											rowId,
											oldBytes,
											newTupleBytes,
											stmt.GetUpdatedColumns(),
									)
							} else {
                // mark old as deleted
								forDeletion := page.Read_row(slot)
                page.Delete_row(slot)

                db.WAL.LogDelete(table.TableName, myDatabase.ResourceType(TABLEResource), rowId, forDeletion)
                db.DeleteFromIndexes(&table, rowId, oldBytes)

                // insert new tuple
								var freePage myDatabase.Page
								fsmPath,_ := db.GetFsmPath(table.TableName)
								pgId, fsmPage, fitting := db.BufferPool.FittingPage(table.TableName, fsmPath, uint16(len(newTupleBytes)))
								if fitting{
									fPage, exists := db.BufferPool.FetchPage(pgId, tablePath)
									if !exists{
										continue
									}
									freePage = *fPage
									rowId, _ := freePage.Insert_row(newTupleBytes)
                  db.WAL.LogInsert(table.TableName, myDatabase.ResourceType(TABLEResource), *rowId, newTupleBytes)
									db.BufferPool.Fsm.UpdateFSM(fsmPage, pgId, uint16(len(newTupleBytes)))
                // update indexes
                //e.CurrentDB.UpdateIndexes(table, *rowId, newValues)
                db.InsertIntoIndexes(&table, *rowId, newTupleBytes)
								}

								newPage := db.BufferPool.AllocatePage(&table)
                rowId, _ := newPage.Insert_row(newTupleBytes)
                 db.WAL.LogInsert(table.TableName, myDatabase.ResourceType(TABLEResource), *rowId, newTupleBytes)

                // update indexes
                //e.CurrentDB.UpdateIndexes(table, *rowId, newValues)
                db.InsertIntoIndexes(&table, *rowId, newTupleBytes)
            }
        }
    }
}

func (stmt *UpdateStmt) GetUpdatedColumns() []string {
	var cols []string
	for col := range stmt.Set {
		cols = append(cols, col)
	}
	return cols
}

func (tp *Tuple) Get(colName string) (TupData, bool){
	v, ok := tp.Tup[colName]
	if !ok{
		return TupData{}, false
	}

	return v, false
}

func (e *Executor) buildTup(schema myDatabase.Schema, rowBs []byte) Tuple{
 columns := schema.Columns 
 tuple := Tuple{}
 
 offset := 0
 for _, col := range columns{
	 switch col.ColumnType{
	   case myDatabase.BOOLEAN:
			 val := rowBs[offset]
			 if val ==0{
				 tuple.Tup[col.ColumnName] = TupData{myDatabase.BOOLEAN, "false"} 
			 }else{
				 tuple.Tup[col.ColumnName] = TupData{myDatabase.BOOLEAN, "true"} 
			 }
			 offset = offset + 1
	   case myDatabase.INT:
       val := rowBs[offset:offset+4]
       tuple.Tup[col.ColumnName] = TupData{myDatabase.INT, string(val)} 
			 offset = offset+4
		 case myDatabase.STRING:
			 val := rowBs[offset:offset+4]
			 tuple.Tup[col.ColumnName] = TupData{myDatabase.STRING, string(val)}
			 offset = offset+4
	 }
 }

 return tuple
}

func rowByteIntoTuple(schema myDatabase.Schema, rowBytes []byte) *Tuple{
	tuple := &Tuple{}
	offset := 0

	colTypes := make([]myDatabase.ColumnType, 0)

	round := 0
	for round<len(schema.Columns){
		colType := rowBytes[offset]
		colTypes = append(colTypes, myDatabase.ColumnType(colType))
		switch colType{
		 case 0:
			 value := rowBytes[offset]
				var col_value string
				if value ==1{
					col_value = "true"
				}else{
					col_value = "false"
				}
				offset += 1

				tpData := TupData{
							myDatabase.ColumnType(colType),
							col_value,
			   }

				 col := schema.Columns[round]
				 tuple.Tup[col.ColumnName] = tpData
				 if(colType == byte(col.ColumnType)){
					 log.Printf("Mahn a lie is happening here, colType had to be = col.ColumnType at rowByteIntoTuple in executor")
				 }
		 case 1:
				col_value := string(rowBytes[offset:offset+4])
				offset += 4

				tpData := TupData{
							myDatabase.ColumnType(colType),
							col_value,
			   }

				 col := schema.Columns[round]
				 tuple.Tup[col.ColumnName] = tpData
				 if(colType == byte(col.ColumnType)){
					 log.Printf("Mahn a lie is happening here, colType had to be = col.ColumnType at rowByteIntoTuple in executor")
				 }
		 case 2:
				var str_len uint16
				str_len = binary.LittleEndian.Uint16(rowBytes[offset:offset+2])
				offset += 2
				col_value := string(rowBytes[offset:offset+int(str_len)])

				tpData := TupData{
							myDatabase.ColumnType(colType),
							col_value,
			   }

				 col := schema.Columns[round]
				 tuple.Tup[col.ColumnName] = tpData
				 if(colType != byte(col.ColumnType)){
					 log.Printf("Mahn a lie is happening here, colType had to be = col.ColumnType at rowByteIntoTuple in executor")
				 }
			}
			round++
	}
	 return tuple
}

func (tp *Tuple) turnToBytes(schema myDatabase.Schema) []byte {

	var result []byte

	for _, col := range schema.Columns {

		val := tp.Tup[col.ColumnName]

		switch col.ColumnType {

		case myDatabase.INT:
			i, _ := strconv.Atoi(val.Value)
			buf := make([]byte, 4)
			binary.LittleEndian.PutUint32(buf, uint32(i))
			result = append(result, buf...)

		case myDatabase.STRING:
			str := []byte(val.Value)
			result = append(result, byte(len(str)))
			result = append(result, str...)
		}
	}

	return result
}

func (e *Executor) applyUpdate(stmt *UpdateStmt, tuple Tuple) Tuple {

	newTuple := Tuple{
		Tup: make(map[string]TupData),
	}

	// 1. Copy old values
	for col, data := range tuple.Tup {
		newTuple.Tup[col] = data
	}

	// 2. Apply updates
	for col, expr := range stmt.Set {

		oldVal, exists := newTuple.Tup[col]
		if !exists {
			continue
		}

		emptyTuple := Tuple{}
		emptyTuple.Tup = make(map[string]TupData,0)
		newVal := e.evalValue(expr, emptyTuple)

		newTuple.Tup[col] = TupData{
			Type:  oldVal.Type,
			Value: newVal,
		}
	}

	return newTuple
}

func (e *Executor) execCreateDB(stmt *CreateDBStmt){
	e.Syst.CreateDatabase(stmt.DBName)
}

func (e *Executor) execCreateTbl(stmt *CreateTBLStmt){
	log.Printf("execCreateTbl hit. Caution, the session might be nil..")

	log.Printf("checking whether the e.CurrentDB is set: CurrentDB[%v]", e.CurrentDB)
  db := e.CurrentDB
		 //let's perform a crude test here for the time being and see 
			if _, prsnt := e.Syst.GetDatabase(db.Dbname); !prsnt{
				log.Printf("Critical, the database is really not set or unavailable, yeah")
				return
			}
	log.Printf("The db does exist and now going to create the table!")
  db.CreateTable(stmt.TBLName, stmt.Columns)
}

func (e *Executor) execCreateIDX(stmt *CreateIDXStmt){
   dbMngr := e.CurrentDB
		 //let's perform a crude test here for the time being and see 
			if _, prsnt := e.Syst.GetDatabase(dbMngr.Dbname); !prsnt{
				log.Printf("Critical, the database is really not set or unavailable, yeah")
				return
			}
	 table, ok := dbMngr.GetTable(stmt.ParentTableName)
	 if !ok{
		 log.Printf("Table not available, might be deleted! %v", stmt.ParentTableName)
	 }

	 table.CreateIndex(stmt.IDXName, stmt.Columns)
}




