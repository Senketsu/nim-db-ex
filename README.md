# nim-db-ex
A small extension to Nim's db_* modules

## About:
This library is a small extension for Nim's db_* modules that uses new type **RowNew**  
Importing those extensions will also import Nim's db_* modules.

### Contents:
Adds:
* New type **RowNew** `tuple[hasData: bool,data: seq[string]]`
* Convenience proc `hasData()` for **string**, **Row** & **seq[Row]**
* Convenience proc `getValueNew()` - returns `tuple[hasData: bool, data: string]`
* Basic procedures for the new type:  
    `getRowNew()`, `getAllRowsNew()`,  & iterators `fastRowsNew()`, `rowsNew`


