# Getl
## About
Groovy ETL (Getl) - open source project on Groovy, developed since 2012 to automate loading and processing data from 
different sources. 

Getl is a replacement for classic ETLs for projects where you need to quickly and load a lot of data from a variety of
file sources with complex formats and RDBMS tables. Getl does not need to develop a process for each table or file format. 
It is enough in its simple and powerful ETL language to create templates with the necessary logic for working with data 
and sources to the logic of the software architecture of your project and call them up, transferring which sources to 
work with. Getl is an intelligent Etl tool and independently creates the correct mapping between the copied data, 
leads to the necessary types and performs the task by compiling it on the fly into byte code for execution in Java.

For Micro Focus Vertica, Getl has enhanced functionality that allows implement sophisticated 
solutions for capturing, delivering and calculating data for this data warehouse.

## When do you need Getl?
* Copy data arrays between different JDBC compatible databases and file sources;
* Provide incremental data capture and upload changes to the data warehouse;
* Copy, process or delete files on file sources under specified conditions;
* In pilot projects, take data source tables, deploy compatible structures for the data warehouse and copy
data from the source to the data warehouse tables;
* Organize the launch and debugging of ETL processes between development, testing and production environments;
* To simplify testing, create models of reference data and files and automate data initialization for starting tasks 
from unit tests;
* To control the state of tables in the data warehouse, describe the time rules from filling and automate the process
monitoring data in tables;
* Develop your own patterns for process automation to capture, load and process data in simple etl language for your 
business logic used in the project according to your chosen data warehouse architecture.

## Links
* Getl is published in the [Central Maven](http://search.maven.org/#search%7Cga%7C1%7Cg%3A%22net.sourceforge.getl%22)
* The source code of the project is located on the GitHub project [getl](https://github.com/ascrus/getl)
* Basic English documentation available on [Wiki](https://github.com/ascrus/getl/wiki)
* The Russian manual is available in the project's [manual](https://github.com/ascrus/getl/tree/master/manuals/getl.rus.md) 
* Detailed Russian-language documentation is located on [GitHub Pages](https://github.com/ascrus/getl)
* On the basis of Getl, a patterns library was developed to facilitate the development of tasks for Micro Focus Vertica, 
source code posted on the GitHub project [getl.vertica](https://github.com/ascrus/getl.vertica)  
* Examples of working with Getl on the basis of H2 Database can be viewed on the GitHub project 
[Getl examples](https://github.com/ascrus/getl-examples)

## Examples
Registration of connections to Oracle and Vertica:
```groovy
package packet1

import groovy.transform.BaseScript
import groovy.transform.Field
import getl.lang.Getl

@BaseScript Getl main

oracleConnection('ora', true) {
    connectHost = 'oracle-host'
    connectDatabase = 'oradb'
    login = 'user'
    password = 'password'
}

verticaConnection('ver', true) {
    connectHost = 'vertica-host1'
    connectDatabase = 'verdb'
    extended.backupservernode = 'vertica-host2,vertica-host3'
    login = 'user'
    password = 'password'
}
```

Creating a table in Vertica based on Oracle table:
```groovy
oracleTable('ora:table1', true) {
    useConnection oracleConnection('ora')
    schemaName = 'user'
    tableName = 'table1'
}

verticaTable('ver.stage:table1', true) {
    useConnection verticaConnection('ver')
    schemaName = 'stage'
    tableName = 'table1'
    if (!exists) {
        setOracleFields oracleTable('ora:table1')
        create()
    }
}

verticaTable('ver.work:table1', true) {
    useConnection verticaConnection('ver')
    schemaName = 'public'
    tableName = 'table1'
    if (!exists)
        createLike verticaTable('ver.stage:table1')
}
```

Copying all rows from the Oracle table to the Vertica table by uploading to an temporary csv file and loading 
it through the COPY statement:
```groovy
etl.copyRows(oracleTable('ora:table1'), verticaTable('ver.stage:table1')) { bulkLoad = true }
```

Copying table data from a staging area into a working one:
```groovy
sql {
    useConnection verticaConnection('ver')
    exec '''
        /*:count_insert*/
        INSERT INTO public.table1 SELECT * FROM stage.table1;
        IF ({count_insert} > 0);
            ECHO Copied {count_insert} rows successfull.
        END IF;
        COMMIT;
    ''' 
}
```

Truncate staging table and purge working table:
```groovy
verticaTable('ver.stage:table1') { truncate() }
verticaTable('ver.prod:table1') { purgeTable() }
```

Unloading rows from the Vertica table to a csv file according to a specified condition:
```groovy
csv('file1') {
    useConnection csvConnection { path = '/tmp/unload' }
    fileName = 'data.table1'
    extenstion = 'csv'
    fieldDelimiter = '|'
    codePage = 'utf-8'
    header = true
}

verticaTable('ver.work:table1') {
    readOpts {
        where = 'field1 = CURRENT_DATE'
        order = ['field2', 'field3']
    }
}

etl.copyRows(verticaTable('ver.work:table1'), csv('file1'))
```

Copying a file via ssh to another server:
```groovy
files('unload_files', true) {
    rootPath = '/tmp/unload'
}

sftp('ssh1', true) {
    host = 'ssh-host'
    login = 'user'
    password = 'password'
    rootPath = '/csv'
}

fileman.copier(files('unload_files'), sftp('ssh1')) {
    useSourcePath { mask = 'data.{table}.csv' }
}
```
