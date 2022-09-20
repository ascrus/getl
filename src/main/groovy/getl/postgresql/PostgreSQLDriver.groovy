package getl.postgresql

import getl.csv.CSVDataset
import getl.data.Dataset
import getl.data.Field
import getl.driver.Driver
import getl.exception.ExceptionGETL
import getl.jdbc.*
import getl.jdbc.sub.BulkLoadMapping
import getl.proc.Flow
import getl.tfs.TFS
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

		sqlExpressions.ddlAutoIncrement = 'GENERATED BY DEFAULT AS IDENTITY'
		sqlExpressions.changeSessionProperty = 'SET {name} TO {value}'
    }

	@SuppressWarnings("UnnecessaryQualifiedReference")
	@Override
	List<Driver.Support> supported() {
		return super.supported() +
				[Driver.Support.GLOBAL_TEMPORARY, Driver.Support.LOCAL_TEMPORARY, Support.START_TRANSACTION,
				 Driver.Support.SEQUENCE, Driver.Support.CLOB, Driver.Support.INDEX, Support.INDEXFORTEMPTABLE,
				 Driver.Support.UUID, Driver.Support.TIME, Driver.Support.DATE,
				 Driver.Support.BOOLEAN, Driver.Support.CREATEIFNOTEXIST, Driver.Support.DROPIFEXIST,
				 Support.CREATESCHEMAIFNOTEXIST, Support.DROPSCHEMAIFEXIST, Driver.Support.AUTO_INCREMENT,
				 Driver.Support.BLOB, Driver.Support.TIMESTAMP_WITH_TIMEZONE, Driver.Support.ARRAY]
	}

	@SuppressWarnings("UnnecessaryQualifiedReference")
	@Override
	List<Driver.Operation> operations() {
		return super.operations() + [Driver.Operation.BULKLOAD]
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

	@SuppressWarnings("UnnecessaryQualifiedReference")
	@Override
	Map<String, Map<String, Object>> getSqlType () {
		def res = super.getSqlType()
		res.BLOB.name = 'bytea'
		res.BLOB.useLength = JDBCDriver.sqlTypeUse.NEVER
		res.TEXT.name = 'text'
		res.TEXT.useLength = JDBCDriver.sqlTypeUse.NEVER

		return res
	}

	@SuppressWarnings("UnnecessaryQualifiedReference")
	@Override
	void prepareField (Field field) {
		super.prepareField(field)

		if (field.type == Field.Type.BLOB) {
			field.length = null
			field.precision = null
			return
		}

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
		if ((field.type == Field.arrayFieldType)) {
			if (field.arrayType == null)
				throw new ExceptionGETL("It is required to specify the type of the array in \"arrayType\" for field \"${field.name}\"!")

			return "${field.arrayType} ARRAY" + ((field.length != null)?'[' + field.length.toString() + ']':'')
		}

		return super.type2sqlType(field, useNativeDBType)
	}

	@Override
	void prepareCsvTempFile(Dataset source, CSVDataset csvFile) {
		super.prepareCsvTempFile(source, csvFile)
		csvFile.arrayOpeningBracket = '{'
		csvFile.arrayClosingBracket = '}'
	}

	@Override
	void bulkLoadFile(CSVDataset source, Dataset dest, Map params, Closure prepareCode) {
		TableDataset table = dest as TableDataset

		params = bulkLoadFilePrepare(source, table, params, prepareCode)
		def needConvert = false

		def pFormat = ((params.format as String)?:'csv').toLowerCase()
		if (!(pFormat in ['csv', 'text']))
			throw new ExceptionGETL("Unknown bulk load format \"$pFormat\"!")

		def pFieldDelimiter = source.fieldDelimiter()

		if (source.rowDelimiter() != '\n') {
			if (pFormat != 'csv')
				throw new ExceptionGETL('The line separator must be a line break character!')
			else
				needConvert = true
		}

		def pNullAsValue = source.nullAsValue()

		def pHeader = source.isHeader()
		if (pHeader && pFormat != 'csv')
			throw new ExceptionGETL('File header is only allowed for csv file format!')

		def pQuoteStr = source.quoteStr()
		if (pFormat != 'csv' && pQuoteStr != null)
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
				fieldDelimiter = '|'
				rowDelimiter = '\n'
				escaped = false
				nullAsValue = '<NULL>'
				quoteStr = '"'
				isGzFile = true
				arrayOpeningBracket = '{'
				arrayClosingBracket = '}'

				table.field.each { field ->
					if (loadMap.containsValue(field.name.toLowerCase())) {
						addField(field.copy())
						loadColumns.add(field.name)
					}
				}
			}
			pHeader = true
			pFieldDelimiter = '|'
			pQuoteStr = '"'
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
		opts.add("FORMAT $pFormat")
		opts.add("DELIMITER '${StringUtils.EscapeJavaWithoutUTF(pFieldDelimiter)}'")
		if (pNullAsValue != null)
			opts.add("NULL '$pNullAsValue'")
		if (pFormat == 'csv') {
			if (pHeader)
				opts.add("HEADER true")
			if (pQuoteStr != '"')
				opts.add("QUOTE AS '${StringUtils.EscapeJavaWithoutUTF(pQuoteStr)}'")
			//opts.add("ESCAPE '\\'")
			if (!listForceNotNull.isEmpty())
				opts.add("FORCE_NOT_NULL(${listForceNotNull.join(', ')})")
			if (!listForceNull.isEmpty())
				opts.add("FORCE_NULL(${listForceNull.join(', ')})")
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
}