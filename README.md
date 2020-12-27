# Getl
## About
Groovy ETL (Getl) - open source project on Groovy, developed since 2012 to automate loading and processing data from 
different sources. 

## When do you need Getl?
* Copying datasets between RDBMS, file and cloud sources;
* Capturing and delivering data increment from sources to the data warehouse;
* Copying and processing files from local and external file sources;
* Rapid development of pilot projects for data warehouses (converting the structure of sources to warehouse tables, multi-threaded reloading of data from sources to warehouse tables)
* Organization of development, testing and prodiuction stands for ETL projects;
* Automation of testing etl processes;
* Automating data health monitoring in a data warehouse;
* Centralization of storage of descriptions of data sources and their structures in the repository;
* Simplify the development of data processing patterns.

## Supported RDBMS
IBM DB2, FireBird, H2 Database, Hadoop Hive, Cloudera Impala, MS SQLServer, MySql, IBM Netezza, NetSuite, Oracle, PostgreSql, Micro Focus Vertica.

## Supported file sources
CSV, MS Excel, Json, XML, Yaml.

## Supported cloud sources
Kafka, SalesForce.

## Supported file systems
Local file systems Windows and Linux, FTP, SFTP, Hadoop HDFS.

## Operations with RDBMS data sources
* Creating and dropping tables and temporary tables;
* Creating indexes;
* Managing table partitions (only Vertica)
* Reading records from tables and queries;
* Adding, modifying and deleting records in tables;
* Batch loading CSV files into tables (only H2, Vertica, Hive and Impala);
* Execution of SQL queries.

## Operations with file sources
* Reading records from files;
* Saving records to files (only CSV and Json);
* Splitting files into portions when recording (only CSV);
* Working with files compressed in Gz;
* Deleting files.

## Working with cloud sources
* Reading records from datasets;
* Saving records to datasets (only Kafka).

## Working with file systems
* Creating and deleting directories;
* Uploading and uploading a file from the server to a local directory;
* Renaming and moving files at source
* Executing OS command at source (only local file systems and SFTP).

## Getl language operators
* ETL:
    * Copy records from source to source or multiple sources at the same time
    * Reading records from source with error handling;
    * Saving records to a source or multiple sources at the same time;
* ELT:
    * Executing scripts in Getl stored procedure language;
    * Copying records between tables of the same RDBMS;
* File systems:
    * Copying a hierarchy of directories and their files between two file systems;
    * Clearing the hierarchy of directories and their files in the file system;
    * Multi-threaded parsing of files from the file system;
* Object repository:
    * Saving and reading repository object descriptions from repository files;
    * Processing repository objects by name group or mask;
* Configuration and resources:
    * Loading and saving configuration files;
    * Working with loaded configuration content;
* Logging and profiling:
    * Logging messages, events and errors;
    * Profiling the running time of commands and code blocks;
* Workflow management:
    * Controlling permission to start processes;
    * Automatic cloning and freeing of repository objects in threads;
    * Automatically freeing temporary repository objects upon termination of their processes;
    * Process and application termination Commands.

## Links
* Getl jar is published in the [Central Maven](http://search.maven.org/#search%7Cga%7C1%7Cg%3A%22net.sourceforge.getl%22)
* The source code of the project is located on the GitHub project [getl](https://github.com/ascrus/getl)
* Basic Russian documentation available on [Wiki](https://github.com/ascrus/getl/wiki)
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
