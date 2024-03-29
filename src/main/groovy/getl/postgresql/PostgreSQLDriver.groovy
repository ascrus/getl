//file:noinspection DuplicatedCode
package getl.postgresql

import getl.csv.CSVDataset
import getl.data.Dataset
import getl.data.Field
import getl.driver.Driver
import getl.exception.DatasetError
import getl.exception.ExceptionGETL
import getl.exception.InternalError
import getl.jdbc.*
import getl.jdbc.sub.BulkLoadMapping
import getl.proc.Flow
import getl.tfs.TFS
import getl.utils.BoolUtils
import getl.utils.StringUtils
import groovy.transform.CompileStatic
import groovy.transform.InheritConstructors
import org.postgresql.PGConnection

import java.sql.Types

/**
 * MySQL driver class
 * @author Alexsey Konstantinov
 *
 */
@InheritConstructors
class PostgreSQLDriver extends JDBCDriver {
	@Override
	protected void registerParameters() {
		super.registerParameters()
		methodParams.register('createDataset', ['unlogged'])
		methodParams.register('dropDataset', ['cascade'])
		methodParams.register('bulkLoadFile', ['format', 'where', 'forceNotNull', 'forceNull'])
	}

	@Override
	protected void initParams() {
		super.initParams()

		commitDDL = true
		transactionalDDL = true
		transactionalTruncate = true
		caseRetrieveObject = 'LOWER'
		caseQuotedName = false
		defaultSchemaName = 'public'

		sqlExpressions.ddlAutoIncrement = 'GENERATED BY DEFAULT AS IDENTITY'
		sqlExpressions.changeSessionProperty = 'SET {name} TO {value}'
		sqlExpressions.escapedText = 'E\'{text}\''
		sqlExpressions.ddlDrop = 'DROP {type} {%ifExists% }{tableName}{ %cascade%}'
		sqlExpressions.ddlDropConstraint = 'ALTER TABLE {tableName} DROP CONSTRAINT {constraintName} CASCADE'
		sqlExpressions.ddlDropColumnTable = 'ALTER TABLE {tableName} DROP COLUMN {fieldName} CASCADE'
		sqlExpressions.ddlChangeTypeColumnTable = 'ALTER TABLE {tableName} ALTER COLUMN {fieldName} SET DATA TYPE {typeName}'

		sqlTypeMap.BLOB.name = 'bytea'
		sqlTypeMap.BLOB.useLength = sqlTypeUse.NEVER
		sqlTypeMap.TEXT.name = 'text'
		sqlTypeMap.TEXT.useLength = sqlTypeUse.NEVER
    }

	/** Init rule for processing case sensitive fields */
	protected void initCaseSensitiveFields() {
		if (BoolUtils.IsValue(currentPostgreSQLConnection.caseSensitiveFields)) {
			ruleNameNotQuote = null
			caseObjectName = 'NONE'
		}
		else {
			ruleNameNotQuote = defaultRuleNameNotQuote
			caseObjectName = 'LOWER'
		}
	}

	@SuppressWarnings("UnnecessaryQualifiedReference")
	@Override
	List<Driver.Support> supported() {
		return super.supported() +
				[Support.LOCAL_TEMPORARY, Support.START_TRANSACTION,
				 Support.SEQUENCE, Support.CLOB, Support.INDEX, Support.INDEXFORTEMPTABLE,
				 Support.UUID, Support.TIME, Support.DATE, Support.COLUMN_CHANGE_TYPE,
				 Support.CREATEIFNOTEXIST, Support.DROPIFEXIST, Support.CREATEINDEXIFNOTEXIST, Support.DROPINDEXIFEXIST,
				 Support.CREATESCHEMAIFNOTEXIST, Support.DROPSCHEMAIFEXIST, Support.CREATESEQUENCEIFNOTEXISTS, Support.DROPSEQUENCEIFEXISTS, Support.DROPVIEWIFEXISTS,
				 Support.AUTO_INCREMENT, Support.BLOB, Support.TIMESTAMP_WITH_TIMEZONE, Support.ARRAY]
	}

	@SuppressWarnings("UnnecessaryQualifiedReference")
	@Override
	List<Driver.Operation> operations() {
		return super.operations() + [Driver.Operation.BULKLOAD, Driver.Operation.UNION]
	}
	
	@Override
	String defaultConnectURL () {
		return 'jdbc:postgresql://{host}/{database}'
	}

	@Override
	protected String sessionID() {
		String res = null
		//noinspection SqlNoDataSourceInspection
		def rows = sqlConnect.rows('SELECT pg_backend_pid() AS session_id')
		if (!rows.isEmpty()) res = rows[0].session_id.toString()

		return res
	}

	/** Current PostgreSQL connection */
	@SuppressWarnings('unused')
	PostgreSQLConnection getCurrentPostgreSQLConnection() { connection as PostgreSQLConnection }

	/** Enum types is loaded */
	private Boolean isLoadedEnumTypes = false
	/** List of enum types */
	private final Map<String, Integer> enumTypes = [:] as Map<String, Integer>

	@Override
	void connect() {
		super.connect()
		synchronized (enumTypes) {
			isLoadedEnumTypes = false
			enumTypes.clear()
		}
	}

	@Override
	void disconnect() {
		super.disconnect()
		synchronized (enumTypes) {
			isLoadedEnumTypes = false
			enumTypes.clear()
		}
	}

	@SuppressWarnings('SpellCheckingInspection')
	@Override
	List<Field> fields(Dataset dataset) {
		if (!isLoadedEnumTypes) {
			synchronized (enumTypes) {
				enumTypes.clear()
				new QueryDataset(connection: connection,
						query: '''SELECT t.typname AS type_name, Max(Length(e.enumlabel)) AS type_length 
								  FROM pg_catalog.pg_type t
								  	INNER JOIN pg_catalog.pg_enum e ON e.enumtypid = t.oid 
								  WHERE typtype = 'e'
								  GROUP BY t.typname
								   ''').eachRow { row ->
					enumTypes.put(row.type_name as String, row.type_length as Integer)
				}
				isLoadedEnumTypes = true
			}
		}

		return super.fields(dataset)
	}

	@SuppressWarnings("UnnecessaryQualifiedReference")
	@Override
	void prepareField (Field field) {
		super.prepareField(field)

		if (field.type == Field.Type.STRING && field.typeName != null) {
			def length = enumTypes.get(field.typeName)
			if (length != null) {
				field.length = length
				field.charOctetLength = length
				field.extended.put('postgresql_enum_type', field.typeName)
				return
			}
		}

		if (field.type == Field.Type.BLOB) {
			field.length = null
			field.precision = null
			return
		}

		//noinspection DuplicatedCode
		if (field.type == Field.Type.ARRAY) {
			if (field.typeName?.length() > 0 && field.typeName[0] == '_') {
				field.arrayType = field.typeName.substring(1)
				field.typeName = field.arrayType + ' array'
			}

			return
		}

		if (field.typeName != null) {
			if (field.typeName.matches("(?i)TIMESTAMPTZ")) {
				field.type = Field.timestamp_with_timezoneFieldType
				field.dbType = Types.TIMESTAMP_WITH_TIMEZONE
				//field.getMethod = '({field} as microsoft.sql.DateTimeOffset).timestamp'
				return
			}

			if (field.typeName.matches("(?i)UUID")) {
				field.type = Field.Type.UUID
				field.dbType = java.sql.Types.OTHER
				field.length = 36
				field.precision = null
				return
			}

			if (field.typeName.matches("(?i)TEXT")) {
				field.type = Field.Type.TEXT
				field.dbType = java.sql.Types.CLOB
				field.length = null
				field.precision = null
//				return
			}
		}
	}

	@Override
	String prepareFieldValueForInsert(Field field) {
		def res = super.prepareFieldValueForInsert(field)
		def enumName = field.extended.get('postgresql_enum_type')
		if (enumName != null)
			res = '?::"' + enumName + '"'

		return res
	}

	@Override
	Boolean blobReadAsObject(Field field = null) { return false }

	@Override
	String blobMethodWrite(String methodName) {
		return """void $methodName (java.sql.Connection con, java.sql.PreparedStatement stat, Integer paramNum, byte[] value) {
	if (value == null) { 
		stat.setNull(paramNum, java.sql.Types.BLOB) 
	}
	else {
    	try (def stream = new ByteArrayInputStream(value)) {
		  stat.setBinaryStream(paramNum, stream)
		}
	}
}"""
	}

	@Override
	Boolean textReadAsObject() { return false }

	@Override
	String textMethodWrite (String methodName) {
		return """void $methodName (java.sql.Connection con, java.sql.PreparedStatement stat, Integer paramNum, String value) {
	if (value == null) { 
		stat.setNull(paramNum, java.sql.Types.CLOB) 
	}
	else {
		stat.setString(paramNum, value)
	} 
}"""
	}

	@Override
	Boolean uuidReadAsObject() { return true }

	@Override
	Boolean timestamptzReadAsTimestamp() { return true }

	@Override
	String type2sqlType(Field field, Boolean useNativeDBType) {
		if (field.type == Field.stringFieldType) {
			def enumType = field.extended.get('postgresql_enum_type')
			if (enumType != null)
				return '"' + enumType + '"'
		}

		if ((field.type == Field.arrayFieldType)) {
			if (field.arrayType == null)
				throw new ExceptionGETL("It is required to specify the type of the array in \"arrayType\" for field \"${field.name}\"!")

			return "${field.arrayType} ARRAY" + ((field.length != null)?'[' + field.length.toString() + ']':'')
		}

		return super.type2sqlType(field, useNativeDBType)
	}

	@Override
	protected String tableTypeName(JDBCDataset dataset, Map params) {
		def res = super.tableTypeName(dataset, params)
		if (res == null && BoolUtils.IsValue(params.unlogged, BoolUtils.IsValue((dataset as TableDataset).createDirective.unlogged)))
			res = 'UNLOGGED'

		return res
	}

	@Override
	void prepareCsvTempFile(Dataset source, CSVDataset csvFile) {
		super.prepareCsvTempFile(source, csvFile)
		csvFile.arrayOpeningBracket = '{'
		csvFile.arrayClosingBracket = '}'
		csvFile.formatDateTime = 'yyyy-MM-dd HH:mm:ss.SSS'
		csvFile.formatTimestampWithTz = 'yyyy-MM-dd HH:mm:ss.SSSx'
		csvFile.blobAsPureHex = false
		csvFile.blobPrefix = '\\x'
	}

	@Override
	void validCsvTempFile(Dataset source, CSVDataset csvFile) {
		super.validCsvTempFile(source, csvFile)
		if (source.field.find { field -> field.type == Field.blobFieldType } != null) {
			if (csvFile.blobAsPureHex())
				throw new DatasetError(csvFile, 'Blob fields is not allowed when used blob as pure hex option for bulk load to table {table}', [table: source])
			if (csvFile.blobPrefix() != '\\x')
				throw new DatasetError(csvFile, 'Blob fields is not allowed when prefix not equals "\\x" for bulk load to table {table}', [table: source])
		}
	}

	@Override
	void bulkLoadFile(CSVDataset source, Dataset dest, Map params, Closure prepareCode) {
		TableDataset table = dest as TableDataset

		params = bulkLoadFilePrepare(source, table, params, prepareCode)
		def needConvert = false

		def pFormat = ((params.format as String)?:'csv').toLowerCase()
		if (!(pFormat in ['csv', 'text']))
			throw new ExceptionGETL("Unknown bulk load format \"$pFormat\"!")

		if (source.rowDelimiter() != '\n') {
			if (pFormat != 'csv')
				throw new ExceptionGETL('The line separator must be a line break character!')
			else
				needConvert = true
		}

		if (pFormat != 'csv' && source.isHeader())
			throw new ExceptionGETL('File header is only allowed for csv file format!')

		if (pFormat != 'csv' && source.quoteStr() != null)
			throw new ExceptionGETL('Field quotes are only allowed for csv file format!')

		if (source.escaped()) {
			if (pFormat != 'csv')
				throw new ExceptionGETL('Escape sequences are only allowed for csv file format!')

			needConvert = true
		}

		if (pFormat == 'csv' && (source.arrayOpeningBracket() != '{' || source.arrayClosingBracket() != '}'))
			needConvert = true

		def pWhere = params.where as String

		def pForceNotNull = params.forceNotNull as List<String>
		if (pForceNotNull != null && pFormat != 'csv')
			throw new ExceptionGETL('The "FORCE_NOT_NULL" parameter is only supported for csv file format!')
		def listForceNotNull = [] as List<String>
		pForceNotNull?.each { fieldName ->
			if (table.fieldByName(fieldName) == null)
				throw new ExceptionGETL("The FORCE_NOT_NULL parameter contains an unknown field \"$fieldName\"!")
			listForceNotNull.add(prepareFieldNameForSQL(fieldName, table))
		}

		def pForceNull = params.forceNotNull as List<String>
		if (pForceNull != null && pFormat != 'csv')
			throw new ExceptionGETL('The "FORCE_NULL" parameter is only supported for csv file format!')
		def listForceNull = [] as List<String>
		pForceNull?.each { fieldName ->
			if (table.fieldByName(fieldName) == null)
				throw new ExceptionGETL("The FORCE_NULL parameter contains an unknown field \"$fieldName\"!")
			listForceNull.add(prepareFieldNameForSQL(fieldName, table))
		}

		def processMap = params.map as List<BulkLoadMapping>
		def loadMap = new LinkedHashMap<String, String>()
		processMap.each { rule ->
			if (rule.destinationFieldName != null && rule.sourceFieldName != null) {
				loadMap.put(rule.destinationFieldName.toLowerCase(), rule.sourceFieldName.toLowerCase())
			}
		}

		def loadColumns = [] as List<String>
		CSVDataset bulkFile
		needConvert = needConvert || (processMap.size() != loadMap.size())
		if (needConvert) {
			def orig = source.cloneDatasetConnection()
			def tempFile = TFS.dataset()
			tempFile.tap {
				header = true
				escaped = false
				nullAsValue = '<NULL>'
				quoteStr = '"'
				isGzFile = true
				arrayOpeningBracket = '{'
				arrayClosingBracket = '}'
				blobAsPureHex = false
				blobPrefix = '\\x'

				table.field.each { field ->
					if (loadMap.containsValue(field.name.toLowerCase())) {
						addField(field.copy())
						loadColumns.add(field.name)
					}
				}
			}

			try {
				new Flow().copy(source: orig, dest: tempFile, map: loadMap)
			}
			catch (Throwable e) {
				if (tempFile.existsFile())
					tempFile.drop()
				throw e
			}
			bulkFile = tempFile
		}
		else {
			source.field.each { field ->
				def fieldName = field.name.toLowerCase()
				def loadField = loadMap.find {destFieldName, sourceFieldName -> (sourceFieldName == fieldName) }
				if (loadField != null)
					loadColumns.add(loadField.key)
			}
			bulkFile = source
		}

		def copyFields = [] as List<String>
		loadColumns.each { fieldName ->
			copyFields.add(prepareFieldNameForSQL(fieldName, table))
		}

		def opts = [] as List<String>
		opts.add("FORMAT $pFormat".toString())
		opts.add("DELIMITER '${StringUtils.EscapeJavaWithoutUTF(bulkFile.fieldDelimiter())}'".toString())
		if (bulkFile.nullAsValue() != null)
			opts.add("NULL '${bulkFile.nullAsValue()}'".toString())
		if (pFormat == 'csv') {
			if (bulkFile.isHeader())
				opts.add("HEADER true")
			if (bulkFile.quoteStr() != '"')
				opts.add("QUOTE AS '${StringUtils.EscapeJavaWithoutUTF(bulkFile.quoteStr())}'".toString())
			if (!listForceNotNull.isEmpty())
				opts.add("FORCE_NOT_NULL(${listForceNotNull.join(', ')})".toString())
			if (!listForceNull.isEmpty())
				opts.add("FORCE_NULL(${listForceNull.join(', ')})".toString())
		}
		opts.add("ENCODING 'UTF8'")

		StringBuilder sb = new StringBuilder()
		sb << """COPY ${fullNameDataset(table)} (
${copyFields.join(',\n')}
)
FROM STDIN
WITH (
  ${opts.join(',\n  ')}
)"""
		if (pWhere != null)
			sb << "\nWHERE $pWhere"

		def sql = sb.toString()
		saveToHistory(sql)
		//println sql

		def count = 0L
		try {
			def copyMan = (table.currentJDBCConnection.currentJDBCDriver.sqlConnect.connection as PGConnection).copyAPI
			try (def reader = bulkFile.currentCsvConnection.currentCSVDriver.getFileReader(bulkFile)) {
				count = copyMan.copyIn(sql, reader)
			}
		}
		catch (Exception e) {
			table.logger.dump(e, getClass().name + ".bulkLoad", table.toString(), "statement:\n${sql}")
			throw e
		}
		finally {
			if (needConvert)
				bulkFile.drop()
		}

		source.readRows = count
		table.writeRows = count
		table.updateRows = count
	}

	@Override
	protected String unionDatasetMergeSyntax () {
		return '''MERGE INTO {target} t
  USING {source} s ON {join}
  WHEN MATCHED THEN UPDATE SET 
    {set}
  WHEN NOT MATCHED THEN INSERT ({fields})
    VALUES ({values})'''
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

	@Override
	protected Map<String, Object> dropTableParams(JDBCDataset dataset, String type, Boolean validExists, Map<String, Object> params) {
		def res = super.dropTableParams(dataset, type, validExists, params)
		if (params.cascade == null && type == 'TABLE')
			res.cascade = 'CASCADE'

		return res
	}
}