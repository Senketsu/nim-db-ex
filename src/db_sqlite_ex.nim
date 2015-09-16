import strutils, sqlite3
import db_sqlite
export db_sqlite

type
  RowNew* = tuple[hasData: bool,data: seq[string]] ## new Row type with a boolean
                                                   ## indicating if any data were
                                                   ## retrieved

proc newRow(L: int): RowNew =
  newSeq(result.data, L)
  for i in 0..L-1:
   result.data[i] = ""
  result.hasData = false

proc setRow(stmt: Pstmt, r: var RowNew, cols: cint) =
  for col in 0..cols-1:
    setLen(r.data[col], column_bytes(stmt, col)) # set capacity
    setLen(r.data[col], 0)
    let x = column_text(stmt, col)
    if not isNil(x):
     add(r.data[col], x)
     r.hasData = true

iterator fastRowsNew*(db: DbConn, query: SqlQuery,
                   args: varargs[string, `$`]): RowNew  {.tags: [FReadDb].} =
  ## Executes the query and iterates over the result dataset.
  ##
  ## This is very fast, but potentially dangerous.  Use this iterator only
  ## if you require **ALL** the rows.
  ##
  ## Breaking the fastRows() iterator during a loop will cause the next
  ## database query to raise an [EDb] exception ``unable to close due to ...``.
  var stmt = setupQuery(db, query, args)
  var L = (column_count(stmt))
  var result: RowNew = newRow(L)
  while step(stmt) == SQLITE_ROW:
    setRow(stmt, result, L)
    yield result
  if finalize(stmt) != SQLITE_OK: dbError(db)


proc getRowNew*(db: DbConn, query: SqlQuery,
             args: varargs[string, `$`]): RowNew {.tags: [FReadDb].} =
  ## retrieves a single row. If the query doesn't return any rows, this proc
  ## will return a Row with empty strings for each column.
  var stmt = setupQuery(db, query, args)
  var L = (column_count(stmt))
  result = newRow(L)
  if step(stmt) == SQLITE_ROW:
    setRow(stmt, result, L)
  if finalize(stmt) != SQLITE_OK: dbError(db)

proc getAllRowsNew*(db: DbConn, query: SqlQuery,
                 args: varargs[string, `$`]): seq[RowNew] {.tags: [FReadDb].} =
  ## executes the query and returns the whole result dataset.
  result = @[]
  for r in fastRowsNew(db, query, args):
    result.add(r)

iterator rowsNew*(db: DbConn, query: SqlQuery,
               args: varargs[string, `$`]): RowNew {.tags: [FReadDb].} =
  ## same as `FastRows`, but slower and safe.
  for r in fastRowsNew(db, query, args): yield r


proc getValueNew*(db: DbConn, query: SqlQuery,
               args: varargs[string, `$`]): tuple[hasData: bool,data: string] {.tags: [FReadDb].} =
  ## executes the query and returns the first column of the first row of the
  ## result dataset. Returns "" if the dataset contains no rows or the database
  ## value is NULL.
  result.hasData = false
  var stmt = setupQuery(db, query, args)
  if step(stmt) == SQLITE_ROW:
    let cb = column_bytes(stmt, 0)
    if cb == 0:
      result.data = ""
    else:
      result.data = newStringOfCap(cb)
      add(result.data, column_text(stmt, 0))
      result.hasData = true
  else:
    result.data = ""
  if finalize(stmt) != SQLITE_OK: dbError(db)

when not defined(testing) and isMainModule:
  var db = open("db.sql", "", "", "")
  exec(db, sql"create table tbl1(one varchar(10), two smallint)", [])
  exec(db, sql"insert into tbl1 values('hello!',10)", [])
  exec(db, sql"insert into tbl1 values('goodbye', 20)", [])
  #db.query("create table tbl1(one varchar(10), two smallint)")
  #db.query("insert into tbl1 values('hello!',10)")
  #db.query("insert into tbl1 values('goodbye', 20)")
  for r in db.rows(sql"select * from tbl1", []):
    echo(r[0], r[1])
  for r in db.instantRows(sql"select * from tbl1", []):
    echo(r[0], r[1])

  db_sqlite.close(db)
