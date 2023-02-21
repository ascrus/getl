//file:noinspection DuplicatedCode
package getl.impala

import getl.csv.CSVConnection
import getl.csv.CSVDataset
import getl.data.Dataset
import getl.data.Field
import getl.driver.Driver
import getl.exception.DatasetError
import getl.exception.IOFilesError
import getl.exception.NotSupportError
import getl.exception.RequiredParameterError
import getl.files.FileManager
import getl.files.HDFSManager
import getl.jdbc.JDBCDataset
import getl.jdbc.JDBCDriver
import getl.jdbc.TableDataset
import getl.proc.Flow
import getl.tfs.TFS
import getl.utils.BoolUtils
import getl.utils.ConvertUtils
import getl.utils.FileUtils
import getl.utils.ListUtils
import getl.utils.MapUtils
import groovy.transform.CompileStatic
import groovy.transform.InheritConstructors
import java.sql.Connection

/**
 * Impala driver class
 * @author Alexsey Konstantinov
 */
@InheritConstructors
class ImpalaDriver extends JDBCDriver {
    @Override
    protected void registerParameters() {
        super.registerParameters()
        methodParams.register('createDataset',
                ['sortBy', 'rowFormat', 'storedAs', 'location', 'tblproperties', 'serdeproperties',
                 'select', 'fieldsTerminatedBy', 'linesTerminatedBy', 'escapedBy', 'nullDefinedAs'])
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

        syntaxPartitionKeyInColumns = false
        defaultSchemaFromConnectDatabase = true

        defaultTransactionIsolation = Connection.TRANSACTION_READ_UNCOMMITTED

        defaultSchemaName = 'default'

        sqlExpressions.sysDualTable = '(SELECT 1 AS row_num) AS dual'
    }

    @SuppressWarnings("UnnecessaryQualifiedReference")
    @Override
    List<Driver.Support> supported() {
        return super.supported() +
                [Driver.Support.CREATEIFNOTEXIST, Driver.Support.DROPIFEXIST, Support.CREATESCHEMAIFNOTEXIST, Support.DROPSCHEMAIFEXIST,
                 Driver.Support.EXTERNAL, Driver.Support.BULKLOADMANYFILES] -
                [Driver.Support.PRIMARY_KEY, Driver.Support.NOT_NULL_FIELD, Driver.Support.DEFAULT_VALUE, Driver.Support.COMPUTE_FIELD,
                 Driver.Support.CHECK_FIELD, Driver.Support.SELECT_WITHOUT_FROM, Driver.Support.TRANSACTIONAL]
    }

    @SuppressWarnings("UnnecessaryQualifiedReference")
    @Override
    List<Driver.Operation> operations() {
        return super.operations() + [Driver.Operation.BULKLOAD] - [Driver.Operation.READ_METADATA, Driver.Operation.UPDATE, Driver.Operation.DELETE]
    }

    @Override
    String defaultConnectURL() {
        return 'jdbc:impala://{host}/{database}'
    }

    @SuppressWarnings("UnnecessaryQualifiedReference")
    @Override
    Map<String, Map<String, Object>> getSqlType() {
        def res = super.getSqlType()
        res.STRING.name = 'string'
        res.STRING.useLength = JDBCDriver.sqlTypeUse.NEVER
        res.DOUBLE.name = 'double'
        /*res.BLOB.name = 'binary'
        res.BLOB.useLength = JDBCDriver.sqlTypeUse.NEVER*/

        return res
    }

    /** Current Impala connection */
    ImpalaConnection getCurrentImpalaConnection() { connection as ImpalaConnection }

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

        def sortBy = params.sortBy as List<String>
        if (sortBy != null && !(sortBy.isEmpty())) {
            sb << "SORT BY (${sortBy.join(', ')})"
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

        if (params.storedAs != null)
            sb << "STORED AS ${params.storedAs}\n"

        if (params.serdeproperties != null) {
            if (!(params.serdeproperties instanceof Map))
                throw new RequiredParameterError(dataset, 'serdeproperties', 'create')
            def serdeproperties = params.serdeproperties as Map
            if (!serdeproperties.isEmpty()) {
                def props = [] as List<String>
                serdeproperties.each { k, v ->
                    props << "\"$k\"=\"$v\"".toString()
                }
                sb << "WITH SERDEPROPERTIES(${props.join(', ')})"

                sb << '\n'
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
        String into = (BoolUtils.IsValue([params.overwrite, (dataset as TableDataset).params.overwrite]))?'OVERWRITE':'INTO'
        return ((dataset.fieldListPartitions.isEmpty()))?
                "INSERT $into TABLE {table} ({columns}) VALUES({values})":
                "INSERT $into TABLE {table} ({columns}) PARTITION ({partition}) VALUES({values})"
    }

    @CompileStatic
    protected void saveBatch(Dataset dataset, WriterParams wp) {
        try {
            super.saveBatch(dataset, wp)
        }
        catch (AssertionError e) {
            connection.logger.dump(e, getClass().name, dataset.toString(), "operation:${wp.operation}, batch size: ${wp.batchSize}, query:\n${wp.query}\n\nstatement: ${wp.statement}")
            throw e
        }
    }

    @Override
    String getSysDualTable() { currentImpalaConnection.dualTable?:sqlExpressionValue('sysDualTable') }

    @Override
    protected void initOpenWrite(JDBCDataset dataset, Map params, String query) {
        super.initOpenWrite(dataset, params, query)
        def compression = (params.compression as String)?.toLowerCase()
        if (compression == null) compression = 'none'
        dataset.currentJDBCConnection.executeCommand("set COMPRESSION_CODEC=$compression;")
    }

    @Override
    List<Field> fields(Dataset dataset) {
        def res = super.fields(dataset)
        res?.each {f ->
            if (f.typeName?.toUpperCase() == 'VARCHAR')
                f.typeName = 'STRING'
        }

        return res
    }

    @Override
    List<Field> prepareImportFields(Dataset dataset, Map importParams = new HashMap()) {
        def res = super.prepareImportFields(dataset, importParams)

        if (!(dataset instanceof ImpalaTable)) {
            res.each { field ->
                switch (field.type) {
                    case Field.dateFieldType:
                        field.type = Field.datetimeFieldType
                        break
                    case Field.timeFieldType:
                        field.type = Field.datetimeFieldType
                        break
                    case Field.timestamp_with_timezoneFieldType:
                        field.type = Field.datetimeFieldType
                        break
                }
            }
        }

        return res
    }

    @Override
    void bulkLoadFile(CSVDataset source, Dataset dest, Map bulkParams, Closure prepareCode) {
        if (source.nullAsValue() != null) /* TODO: Проверить, что действительно не поддерживает nullAsValue */
            throw new NotSupportError(dest, null, 'nullAsValue', 'bulkLoadFile')

        def table = dest as TableDataset
        bulkParams = bulkLoadFilePrepare(source, table, bulkParams, prepareCode)
        def con = table.connection as ImpalaConnection

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
                throw new DatasetError(dest, '#dataset.field_not_found', [field: fieldName, detail: 'expression'])
        }

        if (hdfsHost == null)
            throw new RequiredParameterError(dest, 'hdfsHost', 'bulkLoadFile')
        if (hdfsLogin == null)
            throw new RequiredParameterError(dest, 'hdfsLogin', 'bulkLoadFile')
        if (hdfsDir == null)
            throw new RequiredParameterError(dest, 'hdfsDir', 'bulkLoadFile')

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

        def tempTable = new ImpalaTable().tap {
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
                throw new IOFilesError(dest, '#io.file.not_found', [file: fileName])

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
                        command: "INSERT ${(overwriteTable)?'OVERWRITE':'INTO'} ${table.fullNameDataset()}" +
                                (!partFields.isEmpty()?" PARTITION(${partFields.join(', ')})":'') + " SELECT ${insertFields.join(', ')} FROM ${tempTable.tableName}")
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

    @Override
    void prepareCsvTempFile(Dataset source, CSVDataset csvFile) {
        super.prepareCsvTempFile(source, csvFile)
        csvFile.formatDateTime = 'yyyy-MM-dd HH:mm:ss.SSS'
        csvFile.formatTimestampWithTz = 'yyyy-MM-dd HH:mm:ss.SSSx'
    }
}