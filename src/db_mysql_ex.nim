import strutils, mysql
import db_mysql
export db_mysql

type
  RowNew* = tuple[hasData: bool,data: seq[string]] ## new Row type with a boolean
                                                   ## indicating if any data were
                                                   ## retrieved

proc newRow(L: int): RowNew =
  newSeq(result.data, L)
  for i in 0..L-1:
   result.data[i] = ""
  result.hasData = false


iterator fastRowsNew*(db: DbConn, query: SqlQuery,
                   args: varargs[string, `$`]): RowNew {.tags: [FReadDB].} =
  ## executes the query and iterates over the result dataset.
  ##
  ## This is very fast, but potentially dangerous.  Use this iterator only
  ## if you require **ALL** the rows.
  ##
  ## Breaking the fastRows() iterator during a loop will cause the next
  ## database query to raise an [EDb] exception ``Commands out of sync``.
  rawExec(db, query, args)
  var sqlres = mysql.useResult(db)
  if sqlres != nil:
    var L = int(mysql.numFields(sqlres))
    var result: RowNew = newRow(L)
    var row: cstringArray
    while true:
      row = mysql.fetchRow(sqlres)
      if row == nil: break
      for i in 0..L-1:
        setLen(result.data[i], 0)
        if row[i] == nil:
          result.data[i] = nil
        else:
          add(result.data[i], row[i])
          result.hasData = true
      yield result
    properFreeResult(sqlres, row)

proc getRowNew*(db: DbConn, query: SqlQuery,
             args: varargs[string, `$`]): RowNew {.tags: [FReadDB].} =
  ## executes the query and returns RowNew with hasData indicating if any data
  ## were retrieved
  result.hasData = false
  rawExec(db, query, args)
  var sqlres = mysql.useResult(db)
  if sqlres != nil:
    var L = int(mysql.numFields(sqlres))
    result = newRow(L)
    var row = mysql.fetchRow(sqlres)
    if row != nil:
      for i in 0..L-1:
        setLen(result.data[i], 0)
        if row[i] == nil:
          result.data[i] = nil
        else:
          add(result.data[i], row[i])
          result.hasData = true
    properFreeResult(sqlres, row)

proc getAllRowsNew*(db: DbConn, query: SqlQuery,
                 args: varargs[string, `$`]): seq[RowNew] {.tags: [FReadDB].} =
  ## executes the query and returns the whole result dataset with a boolean for
  ## each row indicating whether row has any data
  result = @[]
  rawExec(db, query, args)
  var sqlres = mysql.useResult(db)
  if sqlres != nil:
    var L = int(mysql.numFields(sqlres))
    var row: cstringArray
    var j = 0
    while true:
      row = mysql.fetchRow(sqlres)
      if row == nil: break
      setLen(result, j+1)
      result[j] = newRow(L)
      for i in 0..L-1:
        if row[i] == nil:
          result[j].data[i] = nil
        else:
          result[j].data[i] = $row[i]
          result[j].hasData = true
      inc(j)
    mysql.freeResult(sqlres)

iterator rowsNew*(db: DbConn, query: SqlQuery,
               args: varargs[string, `$`]): RowNew {.tags: [FReadDB].} =
  ## same as `fastRows`, but slower and safe.
  for r in items(getAllRowsNew(db, query, args)): yield r

proc getValueNew*(db: DbConn, query: SqlQuery,
               args: varargs[string, `$`]): tuple[hasData: bool,data: string] {.tags: [FReadDB].} =
  ## executes the query and returns the first column of the first row of the
  ## result dataset. Returns "" if the dataset contains no rows or the database
  ## value is NULL.
  result.data = getRowNew(db, query, args).data[0]
  # result.data = row.data[0]
  result.hasData = result.data.hasData()

