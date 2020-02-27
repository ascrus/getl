# Welcome to the GETL wiki!

**GETL** - based package in Groovy, which automates the work of loading and transforming data. His name is an acronym for "Groovy ETL".

GETL is a set of libraries of pre-built classes and objects that can be used to solve problems unpacking, transform and load data into programs written in Groovy, or Java, as well as from any software that supports the work with Java classes.

GETL taken into account when developing ideas and following requirements:
* The simpler the class hierarchy, the easier solution;
* The data structures tend to change over time, or not be known in advance, working with them must be maintained;
* All routine work ETL should be automated wherever possible;
* Compiling the code on the fly bail speed and reserve for the optimization;
* Sophisticated class hierarchy guarantee easy connection of other open source solutions.

# Getl DSL
<p>The implemented language extension makes it easier to work with data sources, file systems and simplifies the development of data processing processes.</p>
<p>Object-oriented coding is not required to work with data objects or file systems. All work is supported at the level of a simple scripting language. The language has built-in support for all types of data sources and file systems of the GETL classes.</p>
<p>To simplify the development of complex logic, support has been added for the local repository of the described data sources and file systems.</p>
<p>The language has added support for nested script calls for developing patterns of logic for working with data sources.</p>
<p>To extend the functionality of intermediate calculations, there is support for tables in the in-memory database and temporary text files.</p>
<p>To create parallelization of data processing, support for multi-threaded user code has been implemented. Connections and data sources that are registered in the repository are automatically cloned to work in threads.</p>

## The following data sources are supported
* CSV files
* Excel files
* H2 database
* Hadoop Hive
* Hadoop Impala
* JSON files
* MSSQL Server
* MySql
* Netezza
* Oracle
* PostgreSql
* Vertica
* XML files

## The following file systems are supported
* local and sharing files
* ftp
* ssh file (sftp)
* hadoop hdfs

## Documentation
Full Russian language documentation will be posted later on www.easydata.ru site. In the future, the basic part of the documentation will be translated into English and posted on Githab site.

## Example project:
https://github.com/ascrus/getl-examples