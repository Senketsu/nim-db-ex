# nim-db-ex
An extension modules for Nim's db_* modules

## About:
These are convenience modules that use new type 'RowNew'.
* `tuple[hasData: bool,data: seq[string]]`
Each Row retrieving procedure has its equivalent for type 'RowNew' with a suffix 'New'
* e.g: getRowNew() ,getAllRowsNew() ,fastRowsNew() ...
Contains proc `getValueNew()` to return `tuple[hasData: bool, data: string]`

These modules import & export their parent modules db_*

