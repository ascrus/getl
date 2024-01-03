//file:noinspection DuplicatedCode
package getl.hive

import getl.csv.*
import getl.data.*
import getl.driver.Driver
import getl.exception.ConnectionError
import getl.exception.DatasetError
import getl.files.*
import getl.jdbc.*
import getl.proc.Flow
import getl.tfs.TFS
import getl.utils.*
import groovy.transform.CompileStatic
import groovy.transform.InheritConstructors

/**
 * Hive driver class
 * @author Alexsey Konstantinov
 */
@InheritConstructors
class HiveDriver extends JDBCDriver {
    @Override
    protected void registerParameters() {
        super.registerParameters()

        methodParams.register('createDataset', ['clustered', 'skewed', 'rowFormat', 'storedAs', 'location', 'tblproperties',
                                                'serdeproperties', 'select', 'fieldsTerminatedBy', 'linesTerminatedBy', 'escapedBy', 'nullDefinedAs'])
        methodParams.register('openWrite', ['overwrite'])
        methodParams.register('bulkLoadFile', ['overwrite', 'hdfsHost', 'hdfsPort', 'hdfsLogin', 'hdfsDir', 'processRow', 'expression',
                                               'files', 'fileMask'])
    }

    @Override
    protected void initParams() {
        super.initParams()

        tablePrefix = '`'
        fieldPrefix = '`'

        //caseObjectName = "LOWER"
        connectionParamBegin = ';'
        connectionParamJoin = ';'

        createViewTypes = ['CREATE']

        localTemporaryTablePrefix = 'TEMPORARY'

        defaultTransactionIsolation = java.sql.Connection.TRANSACTION_READ_UNCOMMITTED

        syntaxPartitionKeyInColumns = false
        defaultSchemaFromConnectDatabase = true

        defaultSchemaName = 'default'

        sqlExpressions.now = 'CURRENT_TIMESTAMP'
        sqlExpressions.sysDualTable = '(SELECT 1 AS row_num) AS dual'

        sqlTypeMap.STRING.name = 'string'
        sqlTypeMap.STRING.useLength = sqlTypeUse.NEVER
        sqlTypeMap.DOUBLE.name = 'double'
        /*sqlTypeMap.BLOB.name = 'binary'
        sqlTypeMap.BLOB.useLength = JDBCDriver.sqlTypeUse.NEVER*/

        ruleNameNotQuote = '(?i)^[a-z]+[a-z0-9_]*$'

        driverSqlKeywords.addAll(['METHOD'])
    }

    @SuppressWarnings("UnnecessaryQualifiedReference")
    @Override
    List<Driver.Support> supported() {
        return super.supported() +
                [Driver.Support.LOCAL_TEMPORARY, Driver.Support.DATE, Driver.Support.EXTERNAL, Driver.Support.BULKLOADMANYFILES,
                 Driver.Support.CREATEIFNOTEXIST, Driver.Support.DROPIFEXIST, Support.CREATESCHEMAIFNOTEXIST, Support.DROPSCHEMAIFEXIST,
                 Support.DROPVIEWIFEXISTS, Support.CREATEVIEWIFNOTEXISTS, Support.DROPINDEXIFEXIST] -
                [Driver.Support.PRIMARY_KEY, Driver.Support.NOT_NULL_FIELD, Driver.Support.DEFAULT_VALUE, Driver.Support.COMPUTE_FIELD,
                 Driver.Support.CHECK_FIELD, Driver.Support.SELECT_WITHOUT_FROM, Driver.Support.TRANSACTIONAL]
    }

    @SuppressWarnings("UnnecessaryQualifiedReference")
    @Override
    List<Driver.Operation> operations() {
        return super.operations() + [Driver.Operation.BULKLOAD] - [Driver.Operation.READ_METADATA, Driver.Operation.UPDATE, Driver.Operation.DELETE]
    }

    @Override
    String defaultConnectURL () {
        return 'jdbc:hive2://{host}/{database}'
    }

    /** Current Hive connection */
    @SuppressWarnings('unused')
    HiveConnection getCurrentHiveConnection() { connection as HiveConnection }

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
                partitionCols.add(createDatasetAddColumn(f))
            }
            sb << "PARTITIONED BY (${partitionCols.join(', ')})"
            sb << '\n'
        }

        if (params.clustered != null && !(params.clustered as Map).isEmpty()) {
            def clustered = params.clustered as Map

            def allowed = ['by', 'sortedBy', 'intoBuckets']
            def unknown = clustered.keySet().toList() - allowed
            if (!unknown.isEmpty())
                throw new DatasetError(dataset, 'invalid parameters {unknown} for clustered option', [unknown: unknown])

            if (clustered.by == null)
                throw new DatasetError(dataset, 'not set "clustered.by"')
            def clusterBy = ConvertUtils.Object2List(clustered.by)
            if (clusterBy.isEmpty())
                throw new DatasetError(dataset, 'not set "clustered.by"')
            sb << "CLUSTERED BY (${clusterBy.join(', ')})"

            if (clustered.sortedBy != null) {
                def sortedBy = ConvertUtils.Object2List(clustered.sortedBy)
                if (!sortedBy.isEmpty())
                    sb << " SORTED BY (${sortedBy.join(', ')})"
            }

            if (clustered.intoBuckets != null) {
                def intoBuckets = ConvertUtils.Object2Int(clustered.intoBuckets)
                sb << " INTO $intoBuckets BUCKETS"
            }

            sb << '\n'
        }

        if (params.skewed != null && !(params.skewed as Map).isEmpty()) {
            def skewed = params.skewed as Map

            def allowed = ['by', 'on', 'storedAsDirectories']
            def unknown = skewed.keySet().toList() - allowed
            if (!unknown.isEmpty())
                throw new DatasetError(dataset, 'unknown parameters "{unknown}" for "skewed"', [unknown: unknown])

            if (skewed.by == null)
                throw new DatasetError(dataset, 'not set "skewed.by"')
            def skewedBy = ConvertUtils.Object2List(skewed.by)
            if (skewedBy.isEmpty())
                throw new DatasetError(dataset, 'not set "skewed.by"')
            sb << "SKEWED BY (${skewedBy.join(', ')})"

            if (skewed.on == null)
                throw new DatasetError(dataset, 'not set "skewed.on"')
            def skewedOn = ConvertUtils.Object2List(skewed.on)
            if (skewedOn.isEmpty())
                throw new DatasetError(dataset, 'not set "skewed.on"')
            def onCols = [] as List<String>
            skewedOn.each { onCol ->
                def l = ConvertUtils.Object2List(onCol)
                if (l.isEmpty())
                    throw new DatasetError(dataset, 'not set list type for item by "skewed.on"')
                onCols << "(${l.join(', ')})".toString()
            }
            sb << " ON ${onCols.join(', ')}"

            if (BoolUtils.IsValue(skewed.storedAsDirectories))
                sb << ' STORED AS DIRECTORIES'

            sb << '\n'
        }

        if (params.rowFormat != null) {
            def rowFormat = (params.rowFormat as String).trim().toUpperCase()
            sb << "ROW FORMAT $rowFormat\n"

            if (rowFormat == 'DELIMITED') {
                if (params.fieldsTerminatedBy != null)
                    sb << "FIELDS TERMINATED BY '${params.fieldsTerminatedBy}'\n"

                if (params.linesTerminatedBy != null)
                    sb << "LINES TERMINATED BY '${params.linesTerminatedBy}'\n"

                if (params.escapedBy != null)
                    sb << "ESCAPED BY '${params.escapedBy}'\n"

                if (params.nullDefinedAs != null)
                    sb << "NULL DEFINED AS '${params.nullDefinedAs}'\n"
            }
        }

        if (params.storedAs != null) {
            sb << "STORED AS ${params.storedAs}\n"
        }

        if (params.serdeproperties != null) {
            def sp = params.serdeproperties as Map<String, Object>
            if (!sp.isEmpty()) {
                sb << 'WITH SERDEPROPERTIES ('
                sb << sp.collect { k, v -> '"' + k + '"="' + v + '"' }.join(', ')
                sb << ')\n'
            }
        }

        if (params.location != null)
            sb << "LOCATION '${params.location}'\n"

        if (params.tblproperties != null) {
            def tblproperties = ConvertUtils.Object2Map(params.tblproperties)
            if (!tblproperties.isEmpty()) {
                def props = [] as List<String>
                tblproperties.each { k, v ->
                    props << "\"$k\"=\"$v\"".toString()
                }
                sb << "TBLPROPERTIES(${props.join(', ')})\n"
            }
        }

        if (params.select != null) {
            sb << "AS\n"
            sb << "${params.select}\n"
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
        if (source.nullAsValue() != null) /* TODO: Проверить, что действительно не поддерживает nullAsValue */
            throw new DatasetError(dest, 'bulk load files not support "nullAsValue" from csv files')

        def table = dest as TableDataset
        bulkParams = bulkLoadFilePrepare(source, table, bulkParams, prepareCode)
        def con = table.connection as HiveConnection

        //noinspection GroovyUnusedAssignment
        def overwriteTable = BoolUtils.IsValue(bulkParams.overwrite)
        def hdfsHost = ListUtils.NotNullValue([bulkParams.hdfsHost, con.hdfsHost()])
        def hdfsPort = ListUtils.NotNullValue([bulkParams.hdfsPort, con.hdfsPort()]) as Integer
        def hdfsLogin = ListUtils.NotNullValue([bulkParams.hdfsLogin, con.hdfsLogin()])
        def hdfsDir = ListUtils.NotNullValue([bulkParams.hdfsDir, con.hdfsDir()])
        def processRow = bulkParams.processRow as Closure

        def expression = MapUtils.MapToLower(ListUtils.NotNullValue([bulkParams.expression, new HashMap<String, Object>()]) as Map<String, Object>)
        expression.each { String fieldName, expr ->
            def f = table.fieldByName(fieldName)
            if (f == null)
                throw new DatasetError(dest, 'invalid field "{field}" in "expression" parameter', [field: fieldName])
        }

        if (hdfsHost == null)
            throw new ConnectionError(connection, 'not set "hdfsHost"')
        if (hdfsLogin == null)
            throw new ConnectionError(connection, 'not set "hdfsLogin"')
        if (hdfsDir == null)
            throw new ConnectionError(connection, 'not set "hdfsDir"')

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
            files.add(source.fullFileName())
        }

        // Describe temporary table
        def tempFile = TFS.dataset().tap {
            header = false
            fieldDelimiter = '\u0002'
            quoteStr = '\u0001'
            rowDelimiter = '\n'
            quoteMode = CSVDataset.QuoteMode.NORMAL
            codePage = 'utf-8'
            escaped = false
            deleteOnEmpty = true

            field = table.field
            removeFields(expression.keySet().toList())
            movePartitionFieldsToLast()
            resetFieldToDefault(true, true, true, true)
        }
        def tempFileName = new File(tempFile.fullFileName()).name

        def tempTable = new HiveTable().tap {
            connection = con
            tableName = 'GETL_' + FileUtils.UniqueFileName()
            field = tempFile.field
            type = externalTable
            createOpts {
                rowFormat = 'DELIMITED'
                storedAs = 'TEXTFILE'
                fieldsTerminatedBy = '\\002'
                location = "$hdfsDir/$tableName"
            }
        }

        def insertFields = [] as List<String>
        table.field.each { Field f ->
            if (f.isPartition)
                return

            def fieldName = f.name.toLowerCase()
            String val

            def exprValue = expression.get(fieldName)
            if (exprValue == null)
                val = fieldPrefix + f.name + fieldPrefix
            else
                val = exprValue.toString()

            insertFields.add(val)
        }

        def partFields = [] as List<String>
        table.fieldListPartitions.each { Field f ->
            partFields.add(fieldPrefix + f.name + fieldPrefix)

            def fieldName = f.name.toLowerCase()
            String val

            def exprValue = expression.get(fieldName)
            if (exprValue == null)
                val =  fieldPrefix + f.name + fieldPrefix
            else
                val = exprValue.toString()

            insertFields.add(val)
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
                throw new DatasetError(dest, 'invalid file "{file}" in "files"', [file: fileName])

            csvCon.path = file.parent
            csvFile.fileName = file.name

            count += new Flow(connection.dslCreator).copy([source: csvFile, dest: tempFile, inheritFields: false,
                                                        dest_append: (count > 0)], processRow)
        }
        if (count == 0)
            return

        // Copy temp csv file to HDFS
        def fileMan = new HDFSManager(rootPath: hdfsDir, server: hdfsHost, port: hdfsPort, login: hdfsLogin,
                            localDirectory: (tempFile.connection as CSVConnection).currentPath())
        fileMan.connect()
        try {
            fileMan.createDir(tempTable.tableName)
            fileMan.changeDirectory(tempTable.tableName)
            fileMan.upload(tempFileName)
        }
        finally {
            fileMan.disconnect()
        }

        try {
            tempTable.create()
            try {
                tempTable.connection.executeCommand(isUpdate: true,
                        command: "FROM ${tempTable.tableName} INSERT ${(overwriteTable)?'OVERWRITE':'INTO'} ${table.fullNameDataset()}" +
                        (!partFields.isEmpty()?" PARTITION(${partFields.join(', ')})":'') + " SELECT ${insertFields.join(', ')}")
                source.readRows = count
                table.writeRows = count
                table.updateRows = count
            }
            finally {
                tempTable.drop()
            }
        }
        finally {
            tempFile.drop()

            fileMan.connect()
            try {
                fileMan.removeDir(tempTable.tableName, true)
            }
            finally {
                fileMan.disconnect()
            }
        }
    }

    static Map<String, Object> tableExtendedInfo(TableDataset table) {
        Map<String, Object> res = new HashMap<String, Object>()
        def sql = 'SHOW TABLE EXTENDED'
        if (table.schemaName() != null) sql += " IN ${table.schemaName()}"
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
            connection.logger.dump(e, getClass().name, dataset.toString(), "operation:${wp.operation}, batch size: ${wp.batchSize}, query:\n${wp.query}\n\nstatement: ${wp.statement}")
            throw e
        }
    }

    @Override
    void prepareCsvTempFile(Dataset source, CSVDataset csvFile) {
        super.prepareCsvTempFile(source, csvFile)
        csvFile.formatDateTime = 'yyyy-MM-dd HH:mm:ss.SSS'
        csvFile.formatTimestampWithTz = 'yyyy-MM-dd HH:mm:ss.SSSx'
    }
}