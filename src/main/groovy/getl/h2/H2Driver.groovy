//file:noinspection SqlNoDataSourceInspection
//file:noinspection DuplicatedCode
package getl.h2

import getl.driver.Driver
import getl.exception.DatasetError
import getl.exception.InternalError
import getl.jdbc.sub.BulkLoadMapping
import groovy.transform.InheritConstructors
import getl.csv.*
import getl.data.*
import getl.exception.ExceptionGETL
import getl.jdbc.*
import getl.utils.*

/**
 * H2 driver class
 * @author Alexsey Konstantinov
 */
@InheritConstructors
class H2Driver extends JDBCDriver {
	@Override
	protected void registerParameters() {
		super.registerParameters()
		methodParams.register('createDataset', ['transactional', 'not_persistent'])
		methodParams.register('bulkLoadFile', ['expression'])
		methodParams.register('dropSchema', ['cascade'])
	}

	@Override
	protected void initParams() {
		super.initParams()

		commitDDL = false
		allowBulkLoadExpressions = true
		caseObjectName = "UPPER"
		caseRetrieveObject = "UPPER"
		caseQuotedName = true
		defaultSchemaName = "PUBLIC"
		connectionParamBegin = ";"
		connectionParamJoin = ";"

		/* TODO: changed read fields from local temp tables */
//		supportLocalTemporaryRetrieveFields = false

		sqlExpressions.ddlAutoIncrement = 'AUTO_INCREMENT'
		sqlExpressions.ddlDropSchema = 'DROP SCHEMA{ %ifExists%} {schema}{ %cascade%}'
		sqlExpressions.changeSessionProperty = 'SET {name} {value}'
		sqlExpressions.escapedText = 'STRINGDECODE(\'{text}\')'
		sqlExpressions.convertTextToTimestamp = '(TIMESTAMP \'{value}\')'
		sqlExpressions.ddlChangeTypeColumnTable = 'ALTER TABLE {tableName} ALTER COLUMN {fieldName} SET DATA TYPE {typeName}'

		ruleEscapedText.put('\'', '\'\'')

		allowColumnsInDefinitionExpression = false
	}

	@SuppressWarnings('UnnecessaryQualifiedReference')
	@Override
	List<Driver.Support> supported() { /* TODO: H2 ARRAY NOT FULL */
		return super.supported() +
				[Support.GLOBAL_TEMPORARY, Support.LOCAL_TEMPORARY, Support.MEMORY,
				 Support.SEQUENCE, Support.BLOB, Support.CLOB, Support.INDEX, Support.INDEXFORTEMPTABLE, Support.COLUMN_CHANGE_TYPE,
				 Support.UUID, Support.TIME, Support.DATE, Support.TIMESTAMP_WITH_TIMEZONE, Support.AUTO_INCREMENT,
				 Support.DROPIFEXIST, Support.CREATEIFNOTEXIST, Driver.Support.CREATEINDEXIFNOTEXIST, Driver.Support.DROPINDEXIFEXIST,
				 Support.CREATESCHEMAIFNOTEXIST, Support.DROPSCHEMAIFEXIST, Support.DROPVIEWIFEXISTS, Support.CREATEVIEWIFNOTEXISTS,
				 Support.DROPSEQUENCEIFEXISTS, Support.CREATESEQUENCEIFNOTEXISTS
				 /*, Support.ARRAY*/] /*-
				[Support.ALTER_DEFAULT_VALUE]*/
	}

	@SuppressWarnings("UnnecessaryQualifiedReference")
	@Override
	List<Driver.Operation> operations() {
		return super.operations() + [Operation.BULKLOAD, Operation.MERGE]
	}

	@Override
	Boolean timestamptzReadAsTimestamp() { return true }

	@Override
	String defaultConnectURL() {
		def con = connection as H2Connection
		def url
		if (con.inMemory) {
			url = (con.connectHost != null)?"jdbc:h2:tcp://{host}/mem:{database}":"jdbc:h2:mem:{database}"
			if (con.connectDatabase == null)
				url = url.replace('{database}', 'memory_database')
		} else {
			url = (con.connectHost != null)?"jdbc:h2:tcp://{host}/{database}":"jdbc:h2://{database}"
		}

		return url
	}

	/** Current H2 connection */
	@SuppressWarnings('unused')
	H2Connection getCurrentH2Connection() { connection as H2Connection }

	@Override
	List<Object> retrieveObjects(Map params, Closure<Boolean> filter) {
		def p = MapUtils.Copy(params)
		def tableName = p.tableName
		if (tableName != null) {
			p.remove('tableName')
			def mask = p.get('tableMask')
			if (mask == null) {
				mask = [] as List<String>
				p.put('tableMask', mask)
			}
			mask << tableName
		}

		return super.retrieveObjects(p, filter)
	}

	@Override
	protected String createDatasetExtend(JDBCDataset dataset, Map params) {
		String result = ''
		def temporary = (dataset as JDBCDataset).isTemporaryTable
		if (BoolUtils.IsValue(params."not_persistent")) result += 'NOT PERSISTENT '
		if (temporary && BoolUtils.IsValue(params.transactional)) result += 'TRANSACTIONAL '

		return result
	}

	@Override
	void bulkLoadFile(CSVDataset source, Dataset dest, Map params, Closure prepareCode) {
		if (source.escaped())
			throw new DatasetError(source, 'escaped mode not support from bulk load file')

		def table = dest as TableDataset
		params = bulkLoadFilePrepare(source, table, params, prepareCode)
		def map = params.map as List<BulkLoadMapping>

		StringBuilder sb = new StringBuilder()

		List<String> fieldList = []
		List<String> csvFieldList = []
		List<String> headers = []
		List<String> fParams = []

		map.each { rule ->
			if (rule.sourceFieldName != null)
				headers.add(rule.sourceFieldName.toUpperCase().replace('\'', '\'\''))

			if (rule.destinationFieldName == null)
				return

			Field destField = table.fieldByName(rule.destinationFieldName)
			if (!destField.isReadOnly && destField.compute == null) {
				fieldList.add(fieldPrefix + rule.destinationFieldName.toUpperCase() + fieldPrefix)
				if (rule.expression != null)
					csvFieldList.add((rule.expression.length() > 0)?rule.expression:'NULL')
				else if (rule.sourceFieldName != null)
					csvFieldList.add(fieldPrefix + rule.sourceFieldName.toUpperCase() + fieldPrefix)
			}
		}

		def tableFields = fieldList.join(', ')
		def csvFields = csvFieldList.join(', ')
		def heads = (!source.isHeader()) ? "'" + headers.join(source.fieldDelimiter()) + "'" : "null"
		fParams.add("charset=${source.codePage()}")
		fParams.add("fieldSeparator=${source.fieldDelimiter()}")
		if (source.quoteStr() != null)
			fParams.add("fieldDelimiter=${StringUtils.EscapeJava(source.quoteStr())}")
		if (source.nullAsValue() != null)
			fParams.add("null=${StringUtils.EscapeJava(source.nullAsValue())}")
		def functionParams = fParams.join(" ")

		sb << """INSERT INTO ${fullNameDataset(table)} (
  $tableFields
)
SELECT 
  $csvFields 
FROM CSVREAD('{file_name}', ${heads}, '${functionParams}')
"""
        def sql = sb.toString()
		//println sb.toString()

		table.writeRows = 0
		table.updateRows = 0
		def loadFile = source.fullFileName()
		def count = executeCommand(sql.replace('{file_name}', FileUtils.ConvertToUnixPath(loadFile)), [isUpdate: true])
		source.readRows = count
		table.writeRows = count
		table.updateRows = count
	}
	
	@Override
	protected String unionDatasetMergeSyntax () {
'''MERGE INTO {target} ({fields})
  KEY ({keys})
    SELECT {values}
	FROM {source} s'''
	}

	@Override
	protected String sessionID() {
		String res = null
		def rows = sqlConnect.rows("SELECT SESSION_ID() AS session_id")
		if (!rows.isEmpty()) res = rows[0].session_id.toString()
		
		return res
	}

	@Override
	protected String openWriteMergeSql(JDBCDataset dataset, Map params, List<Field> fields, List<String> statFields) {
		def excludeFields = []
		fields.each { Field f ->
			if (f.isReadOnly) {
                excludeFields << f.name
            }
            else {
                statFields << f.name
            }
		}

		String res = """
MERGE INTO ${dataset.fullNameDataset()} (${GenerationUtils.SqlFields(dataset, fields, null, excludeFields).join(", ")})
VALUES(${GenerationUtils.SqlFields(dataset, fields, "?", excludeFields).join(", ")})
"""
		
		return res
	}

	@SuppressWarnings("UnnecessaryQualifiedReference")
	@Override
	void prepareField (Field field) {
		super.prepareField(field)

		if (field.typeName == null)
			return

		if (field.typeName.matches("(?i)UUID")) {
			field.type = Field.uuidFieldType
			field.dbType = java.sql.Types.VARCHAR
			field.length = 36
			field.precision = null
		}
		else if (field.typeName.matches("(?i)BLOB")) {
			field.type = Field.blobFieldType
			field.dbType = java.sql.Types.BLOB
			field.precision = null
		}
		else if (field.typeName.matches("(?i)CLOB")) {
			field.type = Field.textFieldType
			field.dbType = java.sql.Types.CLOB
			field.precision = null
		}
		else if (field.type == Field.arrayFieldType) {
			def match = field.typeName =~ /(?i)(\w+)\s+ARRAY.*/
			if (match.find())
				field.arrayType = match.group(1).toUpperCase()
		}
	}

	@Override
	String prepareReadField(Field field) {
		if (field.type == Field.timestamp_with_timezoneFieldType && field.columnClassName == 'org.h2.api.TimestampWithTimeZone')
			return 'org.h2.value.ValueTimestampTimeZone.fromDateValueAndNanos(({field} as org.h2.api.TimestampWithTimeZone).YMD, ({field} as org.h2.api.TimestampWithTimeZone).nanosSinceMidnight, ({field} as org.h2.api.TimestampWithTimeZone).timeZoneOffsetSeconds).getTimestamp(TimeZone.default)'

		return null
	}

	@Override
	void prepareCsvTempFile(Dataset source, CSVDataset csvFile) {
		super.prepareCsvTempFile(source, csvFile)
		csvFile.formatDateTime = 'yyyy-MM-dd HH:mm:ss.SSS'
		csvFile.formatTimestampWithTz = 'yyyy-MM-dd HH:mm:ss.SSSx'
		csvFile.blobAsPureHex = true
	}

	@Override
	void validCsvTempFile(Dataset source, CSVDataset csvFile) {
		super.validCsvTempFile(source, csvFile)
		if (!(csvFile.codePage().toLowerCase() in ['utf-8', 'utf8']))
			throw new DatasetError(csvFile, 'File must be encoded in utf-8 for bulk load to table {table}', [table: source])
		if (csvFile.isHeader())
			throw new DatasetError(csvFile, 'Header not allowed for bulk load to table {table}', [table: source])
		if (source.field.find { field -> field.type == Field.blobFieldType } != null) {
			if (!csvFile.blobAsPureHex())
				throw new DatasetError(csvFile, 'Blob fields is not allowed when used blob as not pure hex option for bulk load to table {table}', [table: source])
		}
	}

	@Override
	String type2sqlType(Field field, Boolean useNativeDBType) {
		if (field.type != Field.arrayFieldType || (useNativeDBType && field.typeName != null))
			return super.type2sqlType(field, useNativeDBType)

		if (field.arrayType == null)
			throw new ExceptionGETL('Not set "arrayType" for field "{field}"', [field: field.name])

		return "${field.arrayType} array" + (((field.length?:0) > 0)?"[${field.length}]":'')
	}

	@Override
	protected Map<String, Object> dropSchemaParams(String schemaName, Map<String, Object> dropParams) {
		def res = super.dropSchemaParams(schemaName, dropParams)
		if (BoolUtils.IsValue(dropParams.cascade))
			res.cascade = 'CASCADE'

		return res
	}

	static private final String ReadPrimaryKeyConstraintName = '''SELECT constraint_name
FROM information_schema.table_constraints
WHERE 
    constraint_type = 'PRIMARY KEY'
    AND Lower(table_schema) = '{schema}' 
    AND Lower(table_name) = '{table}' '''

	@Override
	String primaryKeyConstraintName(TableDataset table) {
		def q = new QueryDataset()
		q.connection = connection
		q.query = ReadPrimaryKeyConstraintName
		q.queryParams.schema = (table.schemaName()?:defaultSchemaName).toLowerCase()
		q.queryParams.table = table.tableName.toLowerCase()
		def r = q.rows()
		if (r.size() > 1)
			throw new InternalError(table, 'Error reading primary key from list of constraints', "${r.size()} records returned")

		return (!r.isEmpty())?r[0].constraint_name as String:null
	}
}