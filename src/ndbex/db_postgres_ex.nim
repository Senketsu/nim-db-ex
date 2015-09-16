import strutils, postgres
import db_postgres
export db_postgres

type
  RowNew* = tuple[hasData: bool,data: seq[string]] ## new Row type with a boolean
                                                   ## indicating if any data were
                                                   ## retrieved
proc newRow(L: int): RowNew =
  newSeq(result.data, L)
  for i in 0..L-1:
   result.data[i] = ""
  result.hasData = false

proc setRow(res: PPGresult, r: var RowNew, line, cols: int32) =
  for col in 0..cols-1:
    setLen(r.data[col], 0)
    let x = pqgetvalue(res, line, col)
    if x.isNil:
      r.data[col] = nil
    else:
      add(r.data[col], x)
      r.hasData = true

iterator fastRowsNew*(db: DbConn, query: SqlQuery,
                   args: varargs[string, `$`]): RowNew {.tags: [FReadDB].} =
  ## executes the query and iterates over the result dataset. This is very
  ## fast, but potenially dangerous: If the for-loop-body executes another
  ## query, the results can be undefined. For Postgres it is safe though.
  var res = setupQuery(db, query, args)
  var L = pqnfields(res)
  var result: RowNew = newRow(L)
  for i in 0..pqntuples(res)-1:
    setRow(res, result, i, L)
    yield result
  pqclear(res)

iterator fastRowsNew*(db: DbConn, stmtName: SqlPrepared,
                   args: varargs[string, `$`]): RowNew {.tags: [FReadDB].} =
  ## executes the prepared query and iterates over the result dataset.
  var res = setupQuery(db, stmtName, args)
  var L = pqNfields(res)
  var result: RowNew = newRow(L)
  for i in 0..pqNtuples(res)-1:
    setRow(res, result, i, L)
    yield result
  pqClear(res)


proc getRowNew*(db: DbConn, query: SqlQuery,
             args: varargs[string, `$`]): RowNew {.tags: [FReadDB].} =
  ## retrieves a single row. If the query doesn't return any rows, this proc
  ## will return a Row with empty strings for each column.
  var res = setupQuery(db, query, args)
  var L = pqnfields(res)
  result = newRow(L)
  setRow(res, result, 0, L)
  pqclear(res)

proc getRowNew*(db: DbConn, stmtName: SqlPrepared,
             args: varargs[string, `$`]): RowNew {.tags: [FReadDB].} =
  var res = setupQuery(db, stmtName, args)
  var L = pqNfields(res)
  result = newRow(L)
  setRow(res, result, 0, L)
  pqClear(res)

proc getAllRowsNew*(db: DbConn, query: SqlQuery,
                 args: varargs[string, `$`]): seq[RowNew] {.tags: [FReadDB].} =
  ## executes the query and returns the whole result dataset.
  result = @[]
  for r in fastRowsNew(db, query, args):
    result.add(r)

proc getAllRowsNew*(db: DbConn, stmtName: SqlPrepared,
                 args: varargs[string, `$`]): seq[RowNew] {.tags: [FReadDB].} =
  ## executes the prepared query and returns the whole result dataset.
  result = @[]
  for r in fastRowsNew(db, stmtName, args):
    result.add(r)

iterator rowsNew*(db: DbConn, query: SqlQuery,
               args: varargs[string, `$`]): RowNew {.tags: [FReadDB].} =
  ## same as `fastRows`, but slower and safe.
  for r in items(getAllRowsNew(db, query, args)): yield r

proc getValueNew*(db: DbConn, query: SqlQuery,
               args: varargs[string, `$`]): tuple[hasData: bool,data: string] {.tags: [FReadDB].} =
  ## executes the query and returns the first column of the first row of the
  ## result dataset. Returns "" if the dataset contains no rows or the database
  ## value is NULL.
  var x = pqgetvalue(setupQuery(db, query, args), 0, 0)
  result.data = if isNil(x): "" else: $x
  result.hasData = if isNil(x): false else: true
