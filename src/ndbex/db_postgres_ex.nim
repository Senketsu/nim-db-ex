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

proc hasData*(rows: seq[db_postgres.Row]): bool =
  result = false
  for row in rows:
    for item in row:
      if item != "" and item != nil:
        result = true
        break

proc hasData*(row: db_postgres.Row): bool =
  result = false
  for item in row:
    if item != "" and item != nil:
      result = true
      break

proc hasData*(value: string): bool =
  result = false
  if value != "" and value != nil:
    result = true

proc dbFormat(formatstr: SqlQuery, args: varargs[string]): string =
  result = ""
  var a = 0
  if args.len > 0 and not string(formatstr).contains("?"):
    dbError("""parameter substitution expects "?" """)
  if args.len == 0:
    return string(formatstr)
  else:
    for c in items(string(formatstr)):
      if c == '?':
        if args[a] == nil:
          add(result, "NULL")
        else:
          add(result, dbQuote(args[a]))
        inc(a)
      else:
        add(result, c)


proc setupQuery(db: DbConn, query: SqlQuery,
                args: varargs[string]): PPGresult =
  result = pqexec(db, dbFormat(query, args))
  if pqResultStatus(result) != PGRES_TUPLES_OK: dbError(db)

proc setupQuery(db: DbConn, stmtName: SqlPrepared,
                 args: varargs[string]): PPGresult =
  var arr = allocCStringArray(args)
  result = pqexecPrepared(db, stmtName.string, int32(args.len), arr,
                          nil, nil, 0)
  deallocCStringArray(arr)
  if pqResultStatus(result) != PGRES_TUPLES_OK: dbError(db)


iterator fastRowsNew*(db: DbConn, query: SqlQuery,
                   args: varargs[string, `$`]): RowNew {.tags: [ReadDbEffect].} =
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
                   args: varargs[string, `$`]): RowNew {.tags: [ReadDbEffect].} =
  ## executes the prepared query and iterates over the result dataset.
  var res = setupQuery(db, stmtName, args)
  var L = pqNfields(res)
  var result: RowNew = newRow(L)
  for i in 0..pqNtuples(res)-1:
    setRow(res, result, i, L)
    yield result
  pqClear(res)


proc getRowNew*(db: DbConn, query: SqlQuery,
             args: varargs[string, `$`]): RowNew {.tags: [ReadDbEffect].} =
  ## retrieves a single row. If the query doesn't return any rows, this proc
  ## will return a Row with empty strings for each column.
  var res = setupQuery(db, query, args)
  var L = pqnfields(res)
  result = newRow(L)
  setRow(res, result, 0, L)
  pqclear(res)

proc getRowNew*(db: DbConn, stmtName: SqlPrepared,
             args: varargs[string, `$`]): RowNew {.tags: [ReadDbEffect].} =
  var res = setupQuery(db, stmtName, args)
  var L = pqNfields(res)
  result = newRow(L)
  setRow(res, result, 0, L)
  pqClear(res)

proc getAllRowsNew*(db: DbConn, query: SqlQuery,
                 args: varargs[string, `$`]): seq[RowNew] {.tags: [ReadDbEffect].} =
  ## executes the query and returns the whole result dataset.
  result = @[]
  for r in fastRowsNew(db, query, args):
    result.add(r)

proc getAllRowsNew*(db: DbConn, stmtName: SqlPrepared,
                 args: varargs[string, `$`]): seq[RowNew] {.tags: [ReadDbEffect].} =
  ## executes the prepared query and returns the whole result dataset.
  result = @[]
  for r in fastRowsNew(db, stmtName, args):
    result.add(r)

iterator rowsNew*(db: DbConn, query: SqlQuery,
               args: varargs[string, `$`]): RowNew {.tags: [ReadDbEffect].} =
  ## same as `fastRows`, but slower and safe.
  for r in items(getAllRowsNew(db, query, args)): yield r

proc getValueNew*(db: DbConn, query: SqlQuery,
               args: varargs[string, `$`]): tuple[hasData: bool,data: string] {.tags: [ReadDbEffect].} =
  ## executes the query and returns the first column of the first row of the
  ## result dataset. Returns "" if the dataset contains no rows or the database
  ## value is NULL.
  var x = pqgetvalue(setupQuery(db, query, args), 0, 0)
  result.data = if isNil(x): "" else: $x
  result.hasData = result.data.hasData()
