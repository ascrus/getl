/*
 GETL - based package in Groovy, which automates the work of loading and transforming data. His name is an acronym for "Groovy ETL".

 GETL is a set of libraries of pre-built classes and objects that can be used to solve problems unpacking,
 transform and load data into programs written in Groovy, or Java, as well as from any software that supports
 the work with Java classes.

 Copyright (C) 2013-2017  Alexsey Konstantonov (ASCRUS)

 This program is free software: you can redistribute it and/or modify
 it under the terms of the GNU Lesser General Public License as published by
 the Free Software Foundation, either version 3 of the License, or
 (at your option) any later version.

 This program is distributed in the hope that it will be useful,
 but WITHOUT ANY WARRANTY; without even the implied warranty of
 MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 GNU General Public License for more details.

 You should have received a copy of the GNU General Public License and
 GNU Lesser General Public License along with this program.
 If not, see <http://www.gnu.org/licenses/>.
*/

package getl.hive

import getl.csv.*
import getl.data.*
import getl.driver.Driver
import getl.exception.ExceptionGETL
import getl.files.*
import getl.jdbc.*
import getl.proc.Flow
import getl.tfs.TFS
import getl.utils.*

/**
 * Hive driver class
 * @author Alexsey Konstantinov
 */
class HiveDriver extends JDBCDriver {
    HiveDriver() {
        super()

        tablePrefix = ''
        fieldPrefix = ''

        connectionParamBegin = ';'
        connectionParamJoin = ';'

        localTemporaryTablePrefix = 'TEMPORARY'

        defaultTransactionIsolation = java.sql.Connection.TRANSACTION_READ_UNCOMMITTED

        syntaxPartitionKeyInColumns = false

        methodParams.register('createDataset',
                ['clustered', 'skewed', 'rowFormat', 'storedAs', 'location', 'tblproperties',
                 'fieldsTerminated', 'nullDefined', 'select'])
        methodParams.register('openWrite', ['overwrite'])
        methodParams.register('bulkLoadFile', ['overwrite', 'hdfsHost', 'hdfsPort', 'hdfsLogin',
                                               'hdfsDir', 'processRow', 'files', 'fileMask'])
    }

    @Override
    public List<Driver.Support> supported() {
        return super.supported() +
                [Driver.Support.LOCAL_TEMPORARY, Driver.Support.BLOB] -
                [Driver.Support.TRANSACTIONAL, Driver.Support.PRIMARY_KEY, Driver.Support.NOT_NULL_FIELD,
                 Driver.Support.DEFAULT_VALUE, Driver.Support.COMPUTE_FIELD]
    }

    @Override
    public List<Driver.Operation> operations() {
        return super.operations() +
                [Driver.Operation.CLEAR, Driver.Operation.DROP, Driver.Operation.EXECUTE, Driver.Operation.CREATE,
                 Driver.Operation.BULKLOAD]
    }

    @Override
    public String defaultConnectURL () {
        return 'jdbc:hive2://{host}/{database}'
    }

    @Override
    public Map getSqlType () {
        Map res = super.getSqlType()
        res.STRING.name = 'string'
        res.STRING.useLength = JDBCDriver.sqlTypeUse.NEVER
        res.BLOB.name = 'binary'

        return res
    }

    @Override
    protected String createDatasetAddColumn(Field f, boolean useNativeDBType) {
        return (!f.isPartition)?super.createDatasetAddColumn(f, useNativeDBType):null
    }

    @Override
    protected String createDatasetExtend(Dataset dataset, Map params) {
        def sb = new StringBuilder()

        def partitionFields = [] as List<Field>
        dataset.field.each { Field f ->
            if (f.isPartition) partitionFields << f
        }

        if (!partitionFields.isEmpty()) {
            partitionFields.sort(true) { Field a, Field b -> (a.ordPartition?:999999999) <=> (b.ordPartition?:999999999) }
            def partitionCols = [] as List<String>
            partitionFields.each { Field f ->
                partitionCols << generateColumnDefinition(f, false)
            }
            sb << "PARTITIONED BY (${partitionCols.join(', ')})"
            sb << '\n'
        }

        if (params.clustered != null) {
            if (!(params.clustered instanceof Map)) throw new ExceptionGETL('Required map type for parameter "clustered"')
            def clustered = params.clustered as Map

            def allowed = ['by', 'sortedBy', 'intoBuckets']
            def unknown = clustered.keySet().toList() - allowed
            if (!unknown.isEmpty()) throw new ExceptionGETL("Found unknown parameter for \"clustered\" option: $unknown")

            if (clustered.by == null) throw new ExceptionGETL('Required value for parameter "clustered.by"')
            if (!(clustered.by instanceof List)) throw new ExceptionGETL('Required list type for parameter "clustered.by"')
            def by = clustered.by as List
            sb << "CLUSTERED BY (${by.join(', ')})"

            if (clustered.sortedBy != null) {
                if (!(clustered.sortedBy instanceof List)) throw new ExceptionGETL('Required list type for parameter "clustered.sorterBy"')
                def sortedBy = clustered.sortedBy as List
                sb << " SORTED BY (${sortedBy.join(', ')})"
            }

            if (clustered.intoBuckets != null) {
                if (!(clustered.intoBuckets instanceof Integer)) throw new ExceptionGETL('Required integer type for parameter "clustered.intoBuckets"')
                def intoBuckets = clustered.intoBuckets as Integer
                sb << " INTO $intoBuckets BUCKETS"
            }

            sb << '\n'
        }

        if (params.skewed != null) {
            if (!(params.skewed instanceof Map)) throw new ExceptionGETL('Required map type for parameter "skewed"')
            def skewed = params.skewed as Map

            def allowed = ['by', 'on', 'storedAsDirectories']
            def unknown = skewed.keySet().toList() - allowed
            if (!unknown.isEmpty()) throw new ExceptionGETL("Found unknown parameter for \"skewed\" option: $unknown")

            if (skewed.by == null) throw new ExceptionGETL('Required value for parameter "skewed.by"')
            if (!(skewed.by instanceof List)) throw new ExceptionGETL('Required list type for parameter "skewed.by"')
            def by = skewed.by as List
            sb << "SKEWED BY (${by.join(', ')})"

            if (skewed.on == null) throw new ExceptionGETL('Required value for parameter "skewed.on"')
            if (!(skewed.on instanceof List)) throw new ExceptionGETL('Required list type for parameter "skewed.on"')
            def on = skewed.on as List
            def onCols = [] as List<String>
            on.each { onCol ->
                if (!(onCol instanceof List)) throw new ExceptionGETL('Required list type for item by "skewed.on"')
                onCols << "(${(onCol as List).join(', ')})"
            }
            sb << " ON ${onCols.join(', ')}"

            if (BoolUtils.IsValue(skewed.storedAsDirectories)) sb << ' STORED AS DIRECTORIES'

            sb << '\n'
        }

        if (params.rowFormat != null) {
            sb << "ROW FORMAT ${params.rowFormat}"
            sb << '\n'
        }

        if (params.fieldsTerminated != null) {
            sb << "FIELDS TERMINATED BY '${params.fieldsTerminated}'"
            sb << '\n'
        }

        if (params.nullDefined != null) {
            sb << "NULL DEFINED AS '${params.nullDefined}'"
            sb << '\n'
        }

        if (params.storedAs != null) {
            sb << "STORED AS ${params.storedAs}"

            sb << '\n'
        }

        if (params.location != null) {
            sb << "LOCATION '${params.location}'"

            sb << '\n'
        }

        if (params.tblproperties != null) {
            if (!(params.tblproperties instanceof Map)) throw new ExceptionGETL('Required map type for parameter "tblproperties"')
            def tblproperties = params.tblproperties as Map
            def props = [] as List<String>
            tblproperties.each { k, v ->
                props << "\"$k\"=\"$v\""
            }
            sb << "TBLPROPERTIES(${props.join(', ')})"

            sb << '\n'
        }

        if (params.select != null) {
            sb << "AS"
            sb << '\n'
            sb << "${params.select}"

            sb << '\n'
        }

        return sb.toString()
    }

    @Override
    protected String syntaxInsertStatement(Dataset dataset, Map params) {
        String into = (BoolUtils.IsValue(params.overwrite))?'OVERWRITE':'INTO'
        return ((dataset.fieldListPartitions.isEmpty()))?
                "INSERT $into {table} ({columns}) VALUES({values})":
                "INSERT $into {table} PARTITION ({partition}) VALUES({values})"
    }

    @Override
    public void bulkLoadFile(CSVDataset source, Dataset dest, Map bulkParams, Closure prepareCode) {
        def params = bulkLoadFilePrepare(source, dest as JDBCDataset, bulkParams, prepareCode)
        def conHive = dest.connection as HiveConnection

        def overwrite = BoolUtils.IsValue(bulkParams.overwrite)
        def hdfsHost = ListUtils.NotNullValue([bulkParams.hdfsHost, conHive.hdfsHost])
        def hdfsPort = ListUtils.NotNullValue([bulkParams.hdfsPort, conHive.hdfsPort])
        def hdfsLogin = ListUtils.NotNullValue([bulkParams.hdfsLogin, conHive.hdfsLogin])
        def hdfsDir = ListUtils.NotNullValue([bulkParams.hdfsDir, conHive.hdfsDir])
        def processRow = bulkParams.processRow as Closure

        if (hdfsHost == null) throw new ExceptionGETL('Required parameter "hdfsHost"')
        if (hdfsLogin == null) throw new ExceptionGETL('Required parameter "hdfsLogin"')
        if (hdfsDir == null) throw new ExceptionGETL('Required parameter "hdfsDir"')

        def files = [] as List<String>
        if (bulkParams.files != null) {
            files.addAll(bulkParams.files)
        }
        else if (bulkParams.fileMask != null) {
            def fm = new FileManager(rootPath: (source.connection as CSVConnection).path)
            fm.connect()
            try {
                fm.list(bulkParams.fileMask).each { Map f -> files << f.filename }
            }
            finally {
                fm.disconnect()
            }
        }
        else {
            files << source.fileName
        }

        // Describe temporary local csv file
        def tempFile = TFS.dataset()
        tempFile.header = false
        tempFile.fieldDelimiter = '\u0001'
        tempFile.rowDelimiter = '\n'
        tempFile.quoteMode = CSVDataset.QuoteMode.NORMAL
        tempFile.codePage = 'utf-8'
        tempFile.escaped = false
        tempFile.nullAsValue = '<NULL>'

        def loadFields = [] as List<String>
        def partFields = [] as List<String>

        // Add not partition fields to temp file from destination dataset
        dest.field.each { Field f ->
            if (f.isAutoincrement || f.isReadOnly || f.compute) return
            if (f.isPartition) return
            def n = f.copy()
            tempFile.field << n
            loadFields << n.name
        }

        // Add partition fields to temp file from destination dataset
        dest.fieldListPartitions.each { Field f ->
            def n = f.copy()
            n.isPartition = false
            tempFile.field << n
            loadFields << n.name
            partFields << n.name
        }

        // Copy source file to temp csv file
        def count = 0
        files.each { String fileName ->
            source.fileName = fileName
            count = count + new Flow().copy([source: source, dest: tempFile, inheritFields: false,
                                             dest_append: (count > 0)], processRow)
            println "$fileName: $count"
        }
        if (count == 0) return

        // Copy temp csv file to HDFS
        def fileMan = new HDFSManager(rootPath: hdfsDir, server: hdfsHost, port: hdfsPort, login: hdfsLogin,
                            localDirectory: (tempFile.connection as CSVConnection).path)
        fileMan.connect()
        try {
            fileMan.upload(tempFile.fileName)
        }
        finally {
            fileMan.disconnect()
        }

        try {
            def tempTable = new TableDataset(connection: dest.connection, tableName: "t_${tempFile.fileName}",
                    type: JDBCDataset.Type.LOCAL_TEMPORARY)
            tempTable.field = tempFile.field
            tempTable.create(rowFormat: 'DELIMITED', fieldsTerminated: '\\001', nullDefined: tempFile.nullAsValue)
            try {
                tempTable.connection
                        .executeCommand(command: "LOAD DATA INPATH '${fileMan.rootPath}/${tempFile.fileName}' INTO TABLE ${tempTable.tableName}")
                tempTable.connection
                        .executeCommand(command: "FROM ${tempTable.tableName} INSERT ${(overwrite) ? 'OVERWRITE' : 'INTO'} ${(dest as JDBCDataset).fullNameDataset()}" +
                        (!partFields.isEmpty() ? " PARTITION(${partFields.join(', ')})" : '') + " SELECT ${loadFields.join(', ')}")
            }
            finally {
                tempTable.drop()
            }
        }
        finally {
            fileMan.connect()
            fileMan.removeFile(tempFile.fileName)
            fileMan.disconnect()
        }
    }
}