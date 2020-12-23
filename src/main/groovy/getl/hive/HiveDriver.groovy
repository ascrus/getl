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
import groovy.transform.CompileStatic

/**
 * Hive driver class
 * @author Alexsey Konstantinov
 */
class HiveDriver extends JDBCDriver {
    HiveDriver() {
        super()

        tablePrefix = '`'
        fieldPrefix = '`'

        caseObjectName = "LOWER"
        connectionParamBegin = ';'
        connectionParamJoin = ';'

        localTemporaryTablePrefix = 'TEMPORARY'

        defaultTransactionIsolation = java.sql.Connection.TRANSACTION_READ_UNCOMMITTED

        syntaxPartitionKeyInColumns = false

        defaultSchemaName = 'default'

        methodParams.register('createDataset',
                ['clustered', 'skewed', 'rowFormat', 'storedAs', 'location', 'tblproperties',
                 'fieldsTerminated', 'nullDefined', 'select'])
        methodParams.register('openWrite', ['overwrite'])
        methodParams.register('bulkLoadFile', ['overwrite', 'hdfsHost', 'hdfsPort', 'hdfsLogin',
                                               'hdfsDir', 'processRow', 'expression', 'files', 'fileMask'])
    }

    @SuppressWarnings("UnnecessaryQualifiedReference")
    @Override
    List<Driver.Support> supported() {
        return super.supported() +
                [Driver.Support.LOCAL_TEMPORARY, Driver.Support.DATE, Driver.Support.BOOLEAN, Driver.Support.EXTERNAL,
                 Driver.Support.CREATEIFNOTEXIST, Driver.Support.DROPIFEXIST, Driver.Support.BULKLOADMANYFILES] -
                [Driver.Support.PRIMARY_KEY, Driver.Support.NOT_NULL_FIELD,
                 Driver.Support.DEFAULT_VALUE, Driver.Support.COMPUTE_FIELD]
    }

    @SuppressWarnings("UnnecessaryQualifiedReference")
    @Override
    List<Driver.Operation> operations() {
        return super.operations() +
                [Driver.Operation.TRUNCATE, Driver.Operation.DROP, Driver.Operation.EXECUTE,
                 Driver.Operation.CREATE, Driver.Operation.BULKLOAD] -
                [Driver.Operation.READ_METADATA, Driver.Operation.UPDATE, Driver.Operation.DELETE]
    }

    @Override
    String defaultConnectURL () {
        return 'jdbc:hive2://{host}/{database}'
    }

    @SuppressWarnings("UnnecessaryQualifiedReference")
    @Override
    Map getSqlType () {
        Map res = super.getSqlType()
        res.STRING.name = 'string'
        res.STRING.useLength = JDBCDriver.sqlTypeUse.NEVER
        res.DOUBLE.name = 'double'
        /*res.BLOB.name = 'binary'
        res.BLOB.useLength = JDBCDriver.sqlTypeUse.NEVER*/

        return res
    }

    @Override
    protected String createDatasetAddColumn(Field f, Boolean useNativeDBType) {
        return (!f.isPartition)?super.createDatasetAddColumn(f, useNativeDBType):null
    }

    @Override
    protected String createDatasetExtend(JDBCDataset dataset, Map params) {
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

        if (params.clustered != null && !(params.clustered as Map).isEmpty()) {
            def clustered = params.clustered as Map

            def allowed = ['by', 'sortedBy', 'intoBuckets']
            def unknown = clustered.keySet().toList() - allowed
            if (!unknown.isEmpty()) throw new ExceptionGETL("Found unknown parameter for \"clustered\" option: $unknown")

            if (clustered.by == null) throw new ExceptionGETL('Required value for parameter "clustered.by"')
            if (!(clustered.by instanceof List)) throw new ExceptionGETL('Required list type for parameter "clustered.by"')
            def by = clustered.by as List
            if (by.isEmpty()) throw new ExceptionGETL('Required value for parameter "clustered.by"')
            sb << "CLUSTERED BY (${by.join(', ')})"

            if (clustered.sortedBy != null) {
                if (!(clustered.sortedBy instanceof List)) throw new ExceptionGETL('Required list type for parameter "clustered.sorterBy"')
                def sortedBy = clustered.sortedBy as List
                if (!sortedBy.isEmpty()) sb << " SORTED BY (${sortedBy.join(', ')})"
            }

            if (clustered.intoBuckets != null) {
                if (!(clustered.intoBuckets instanceof Integer)) throw new ExceptionGETL('Required integer type for parameter "clustered.intoBuckets"')
                def intoBuckets = clustered.intoBuckets as Integer
                sb << " INTO $intoBuckets BUCKETS"
            }

            sb << '\n'
        }

        if (params.skewed != null && !(params.skewed as Map).isEmpty()) {
            def skewed = params.skewed as Map

            def allowed = ['by', 'on', 'storedAsDirectories']
            def unknown = skewed.keySet().toList() - allowed
            if (!unknown.isEmpty()) throw new ExceptionGETL("Found unknown parameter for \"skewed\" option: $unknown")

            if (skewed.by == null) throw new ExceptionGETL('Required value for parameter "skewed.by"')
            if (!(skewed.by instanceof List)) throw new ExceptionGETL('Required list type for parameter "skewed.by"')
            if ((skewed.by as List).isEmpty()) throw new ExceptionGETL('Required value for parameter "skewed.by"')
            def by = skewed.by as List
            sb << "SKEWED BY (${by.join(', ')})"

            if (skewed.on == null) throw new ExceptionGETL('Required value for parameter "skewed.on"')
            if (!(skewed.on instanceof List)) throw new ExceptionGETL('Required list type for parameter "skewed.on"')
            def on = skewed.on as List
            if (on.isEmpty()) throw new ExceptionGETL('Required value for parameter "skewed.on"')
            def onCols = [] as List<String>
            on.each { onCol ->
                if (!(onCol instanceof List)) throw new ExceptionGETL('Required list type for item by "skewed.on"')
                onCols << "(${(onCol as List).join(', ')})".toString()
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
            if (!tblproperties.isEmpty()) {
                def props = [] as List<String>
                tblproperties.each { k, v ->
                    props << "\"$k\"=\"$v\"".toString()
                }
                sb << "TBLPROPERTIES(${props.join(', ')})"

                sb << '\n'
            }
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
    protected String syntaxInsertStatement(JDBCDataset dataset, Map params) {
        String into = (BoolUtils.IsValue([params.overwrite, dataset.directives('write').overwrite]))?'OVERWRITE':'INTO'
        return ((dataset.fieldListPartitions.isEmpty()))?
                "INSERT $into TABLE {table} ({columns}) VALUES({values})":
                "INSERT $into TABLE {table} ({columns}) PARTITION ({partition}) VALUES({values})"
    }

    @Override
    void bulkLoadFile(CSVDataset source, Dataset dest, Map bulkParams, Closure prepareCode) {
        def table = dest as TableDataset
        bulkParams = bulkLoadFilePrepare(source, table, bulkParams, prepareCode)
        def conHive = dest.connection as HiveConnection

        //noinspection GroovyUnusedAssignment
        def overwriteTable = BoolUtils.IsValue(bulkParams.overwrite)
        def hdfsHost = ListUtils.NotNullValue([bulkParams.hdfsHost, conHive.hdfsHost])
        def hdfsPort = ListUtils.NotNullValue([bulkParams.hdfsPort, conHive.hdfsPort]) as Integer
        def hdfsLogin = ListUtils.NotNullValue([bulkParams.hdfsLogin, conHive.hdfsLogin])
        def hdfsDir = ListUtils.NotNullValue([bulkParams.hdfsDir, conHive.hdfsDir])
        def processRow = bulkParams.processRow as Closure

        def expression = ListUtils.NotNullValue([bulkParams.expression, [:]]) as Map<String, Object>
        expression.each { String fieldName, expr ->
            if (dest.fieldByName(fieldName) == null) throw new ExceptionGETL("Unknown field \"$fieldName\" in \"expression\" parameter")
        }

        if (hdfsHost == null) throw new ExceptionGETL('Required parameter "hdfsHost"')
        if (hdfsLogin == null) throw new ExceptionGETL('Required parameter "hdfsLogin"')
        if (hdfsDir == null) throw new ExceptionGETL('Required parameter "hdfsDir"')

        List<String> files = []
        if (bulkParams.files != null && !(bulkParams.files as List).isEmpty()) {
            files.addAll(bulkParams.files as List)
        }
        else if (bulkParams.fileMask != null) {
            def fm = new FileManager(rootPath: (source.connection as CSVConnection).currentPath())
            fm.connect()
            try {
                fm.list(bulkParams.fileMask as String).each { Map f -> files.add((f.filepath as String) + '/' + (f.filename as String))}
            }
            finally {
                fm.disconnect()
            }
        }
        else {
            files << source.fullFileName()
        }

        // Describe temporary table
        def tempFile = TFS.dataset()
        def tempFileName = new File(tempFile.fullFileName()).name
        tempFile.header = false
        tempFile.fieldDelimiter = '\u0001'
        tempFile.rowDelimiter = '\n'
        tempFile.quoteMode = CSVDataset.QuoteMode.COLUMN
        tempFile.codePage = 'utf-8'
        tempFile.escaped = true
        tempFile.nullAsValue = '<NULL>'

        def loadFields = [] as List<String>
        def partFields = [] as List<String>

        // Add not partition fields to temp file from destination dataset
        dest.field.each { Field f ->
            if (f.isAutoincrement || f.isReadOnly || f.compute) return
            if (f.isPartition) return
            def n = f.copy()
            tempFile.field << n

            def exprValue = expression.get(n.name.toLowerCase())
            if (exprValue == null) {
                loadFields << fieldPrefix + n.name + fieldPrefix
            }
            else {
                loadFields << exprValue.toString()
            }
        }

        // Add partition fields to temp file from destination dataset
        dest.fieldListPartitions.each { Field f ->
            def n = f.copy()
            n.isPartition = false
            tempFile.field << n

            partFields << fieldPrefix + n.name + fieldPrefix

            def exprValue = expression.get(n.name.toLowerCase())
            if (exprValue == null) {
                loadFields << fieldPrefix + n.name + fieldPrefix
            }
            else {
                loadFields << exprValue.toString()
            }
        }

        // Copy source file to temp csv file
        def count = 0L
        def csvCon = source.connection.cloneConnection() as CSVConnection
        csvCon.extension = null
        def csvFile = source.cloneDataset(csvCon) as CSVDataset
        csvFile.extension = null
        files.each { fileName ->
            def file = new File(fileName)
            if (!file.exists())
                throw new ExceptionGETL("File $fileName not found!")

            csvCon.path = file.parent
            csvFile.fileName = file.name

            count += new Flow().copy([source: csvFile, dest: tempFile, inheritFields: false, dest_append: (count > 0)], processRow)
        }
        if (count == 0) return

        // Copy temp csv file to HDFS
        def fileMan = new HDFSManager(rootPath: hdfsDir, server: hdfsHost, port: hdfsPort, login: hdfsLogin,
                            localDirectory: (tempFile.connection as CSVConnection).currentPath())
        fileMan.connect()
        try {
            fileMan.upload(tempFileName)
        }
        finally {
            fileMan.disconnect()
        }

        try {
            def tempTable = new TableDataset(connection: dest.connection, tableName: "t_${tempFile.fileName}",
                    type: JDBCDataset.Type.LOCAL_TEMPORARY)
            tempTable.field = tempFile.field
            tempTable.create(rowFormat: 'DELIMITED', fieldsTerminated: '\\001', nullDefined: tempFile.nullAsValue())
            try {
                tempTable.connection
                        .executeCommand(isUpdate: true, command: "LOAD DATA INPATH '${fileMan.rootPath}/${tempFileName}' INTO TABLE ${tempTable.tableName}")
                /*def countRow = */tempTable.connection
                        .executeCommand(isUpdate: true, command: "FROM ${tempTable.tableName} INSERT ${(overwriteTable)?'OVERWRITE':'INTO'} ${(dest as JDBCDataset).fullNameDataset()}" +
                        (!partFields.isEmpty() ? " PARTITION(${partFields.join(', ')})" : '') + " SELECT ${loadFields.join(', ')}")
                source.readRows = count
                dest.writeRows = count
                dest.updateRows = count
            }
            finally {
                tempTable.drop()
            }
        }
        finally {
            tempFile.drop()
            fileMan.connect()
            try {
                fileMan.removeFile(tempFileName)
            }
            finally {
                fileMan.disconnect()
            }
        }
    }

    static Map<String, Object> tableExtendedInfo(TableDataset table) {
        Map<String, Object> res = [:]
        def sql = 'SHOW TABLE EXTENDED'
        if (table.schemaName != null) sql += " IN ${table.schemaName}"
        sql += " LIKE '${table.tableName}'"
        def query = new QueryDataset(connection: table.connection, query: sql)
        query.eachRow { Map r ->
            def s = r.tab_name as String
            def i = s.indexOf(':')
            if (i == -1) return
            def name = s.substring(0, i)
            def value = s.substring(i + 1).trim()
            switch (name) {
                case 'columns':
                    def m = value =~ /.+([\\{].+[\\}])/
                    if (m.size() != 1) {
//                        value = null
                        return
                    }
                    def descCols = (m[0] as List)[1] as String
                    def cols = descCols.substring(1, descCols.length() - 1).split(', ')
                    List<Map<String, String>> pc = []
                    cols.each { String col ->
                        col = col.trim()
                        def x = col.indexOf(' ')
                        String colName, colType
                        if (x != -1) {
                            colName = col.substring(x + 1)
                            colType = col.substring(0, x)
                            /* todo: need detailing logic */
                            Map<String, String> cr = [name: colName, type: colType]
                            pc << cr
                        }
                    }
                    value = pc
                    break
                case 'partitioned':
                    value = Boolean.valueOf(value as String)
                    break
                case 'partitionColumns':
                    def m = value =~ /.+([\\{].+[\\}])/
                    if (m.size() != 1) {
//                        value = null
                        return
                    }
                    def descCols = (m[0] as List)[1] as String
                    def cols = descCols.substring(1, descCols.length() - 1).split(', ')
                    List<Map<String, String>> pc = []
                    cols.each { String col ->
                        col = col.trim()
                        def x = col.indexOf(' ')
                        String colName = '', colType = ''
                        if (x != -1) {
                            colName = col.substring(x + 1)
                            colType = col.substring(0, x)
                        }
                        Map<String, String> cr = [name: colName, type: colType]

                        pc << cr
                    }
                    value = pc
                    break
            }

            res.put(name, value)
        }

        return res
    }

    @Override
    List<Field> fields(Dataset dataset) {
        List<Field> res = super.fields(dataset)
        if (dataset instanceof TableDataset) {
            def ext = tableExtendedInfo(dataset)
            if (BoolUtils.IsValue(ext.partitioned) && ext.partitionColumns != null) {
                def partNum = 0
                ext.partitionColumns.each { Map<String, String> col ->
                    partNum++
                    def name = col.name.toLowerCase()
                    def f = res.find { it.name == name }
                    if (f != null) {
                        f.isPartition = true
                        f.ordPartition = partNum
                    }
                }
            }
        }

        return res
    }

    @CompileStatic
    protected void saveBatch (Dataset dataset, WriterParams wp) {
        try {
            super.saveBatch(dataset, wp)
        }
        catch (AssertionError e) {
            Logs.Dump(e, getClass().name, dataset.toString(), "operation:${wp.operation}, batch size: ${wp.batchSize}, query:\n${wp.query}\n\nstatement: ${wp.statement}")
            throw e
        }
    }

    @Override
    String getNowFunc() { 'CURRENT_TIMESTAMP' }
}