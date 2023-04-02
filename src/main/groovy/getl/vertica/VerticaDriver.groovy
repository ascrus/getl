//file:noinspection GrMethodMayBeStatic
//file:noinspection DuplicatedCode
package getl.vertica

import getl.csv.CSVConnection
import getl.exception.DatasetError
import getl.exception.IncorrectParameterError
import getl.jdbc.sub.BulkLoadMapping
import getl.oracle.OracleDriver
import getl.proc.Flow
import getl.vertica.opts.VerticaWriteSpec
import groovy.transform.CompileStatic
import getl.csv.CSVDataset
import getl.data.*
import getl.driver.Driver
import getl.exception.ExceptionGETL
import getl.utils.*
import getl.jdbc.*
import groovy.transform.InheritConstructors

import java.util.regex.Pattern

/**
 * Vertica driver class
 * @author Alexsey Konstantinov
 *
 */
@InheritConstructors
class VerticaDriver extends JDBCDriver {
	@Override
	protected void registerParameters() {
		super.registerParameters()

		methodParams.register('createDataset', ['orderBy', 'segmentedBy', 'unsegmented', 'partitionBy', 'checkPrimaryKey', 'privileges'])
		methodParams.register('eachRow', ['label', 'tablesample'])
		methodParams.register('openWrite', ['direct', 'label'])
		methodParams.register('bulkLoadFile',
				['loadMethod', 'rejectMax', 'enforceLength', 'compressed', 'exceptionPath', 'rejectedPath', 'escapeChar',
				 'location', 'formatDate', 'formatTime', 'formatDateTime', 'parser', 'streamName', 'files'])
		methodParams.register('unionDataset', ['direct'])
		methodParams.register('deleteRows', ['direct', 'label'])
		methodParams.register('createView', ['privileges'])
		methodParams.register('createSchema', ['authorization', 'privileges'])
		methodParams.register('dropSchema', ['cascade'])
	}

	@Override
	protected void initParams() {
		super.initParams()

		defaultSchemaName = 'public'
		tempSchemaName = 'v_temp_schema'
		allowExpressions = true
		lengthTextInBytes = true
        addPKFieldsToUpdateStatementFromMerge = true
		defaultBatchSize = 10000L

		sqlExpressions.ddlCreatePrimaryKey = 'PRIMARY KEY ({columns}) {check_pk}'
		sqlExpressions.ddlCreateView = '{create}{ %temporary%} VIEW{ %ifNotExists%} {name}{ %privileges% SCHEMA PRIVILEGES} AS\n{select}'
		sqlExpressions.ddlCreateSchema = 'CREATE SCHEMA{ %ifNotExists%} {schema}{ AUTHORIZATION %authorization%}{ DEFAULT %privileges% SCHEMA PRIVILEGES}'
		sqlExpressions.ddlDropSchema = 'DROP SCHEMA{ %ifExists%} {schema}{ %cascade%}'
		sqlExpressions.changeSessionProperty = 'SET {name} TO {value}'
		sqlExpressions.escapedText = 'E\'{text}\''

		ruleQuotedWords.add('NEW')
	}

    @Override
	Map<String, Map<String, Object>> getSqlType () {
        def res = super.getSqlType()
        res.DOUBLE.name = 'double precision'
        res.BLOB.name = 'varbinary'
        res.TEXT.name = 'long varchar'

        return res
    }

	@Override
	List<Support> supported() {
        return super.supported() +
				[Support.LOCAL_TEMPORARY, Support.GLOBAL_TEMPORARY, Support.SEQUENCE,
                 Support.BLOB, Support.CLOB, Support.UUID, Support.TIME, Support.DATE,
				 Support.TIMESTAMP_WITH_TIMEZONE, Support.CREATEIFNOTEXIST, Support.DROPIFEXIST,
				 Support.CREATESCHEMAIFNOTEXIST, Support.DROPSCHEMAIFEXIST,
				 Support.BULKLOADMANYFILES, Support.BULKESCAPED, Support.BULKGZ, Support.BULKNULLASVALUE,
				 Support.START_TRANSACTION, Support.LOCAL_TEMPORARY_VIEW /*, Support.ARRAY*/]
    }

	@SuppressWarnings("UnnecessaryQualifiedReference")
	@Override
	List<Driver.Operation> operations() {
		return super.operations() + [Driver.Operation.BULKLOAD]
    }

	@Override
	Boolean timestamptzReadAsTimestamp() { return true }

	@Override
	String defaultConnectURL () {
		return 'jdbc:vertica://{host}/{database}'
	}

	/** Current Vertica connection */
	@SuppressWarnings('unused')
	VerticaConnection getCurrentVerticaConnection() { connection as VerticaConnection }

	@Override
	protected List<Map> getIgnoreWarning () {
		List<Map> res = []
		res.add([errorCode: 4486, sqlState: '0A000'])

		return res
	}

	@Override
	String type2sqlType(Field field, Boolean useNativeDBType) {
		if (field.type != Field.arrayFieldType || (useNativeDBType && field.typeName != null))
			return super.type2sqlType(field, useNativeDBType)

		if (field.arrayType == null)
			throw new ExceptionGETL("It is required to specify the type of the array in \"arrayType\" for field \"${field.name}\"!")

		return "array[${field.arrayType}]" + (((field.length?:0) > 0)?"(${field.length})":'')
	}

	@Override
	protected String createDatasetExtend(JDBCDataset dataset, Map params) {
		def result = ''
		def temporary = (((dataset as JDBCDataset).type as JDBCDataset.Type) in
				[JDBCDataset.Type.GLOBAL_TEMPORARY, JDBCDataset.Type.LOCAL_TEMPORARY])
		if (temporary && BoolUtils.IsValue(params.onCommit))
			result += 'ON COMMIT PRESERVE ROWS\n'
		if (params.orderBy != null && !(params.orderBy as List).isEmpty())
			result += "ORDER BY ${(params.orderBy as List).join(", ")}\n"
		if (params.segmentedBy != null && BoolUtils.IsValue(params.unsegmented))
			throw new ExceptionGETL('Invalid segmented options')
		if (params.segmentedBy != null)
			result += "SEGMENTED BY ${params.segmentedBy}\n"
		else if (BoolUtils.IsValue(params.unsegmented))
			result += "UNSEGMENTED ALL NODES\n"
		if (params.partitionBy != null)
			result += "PARTITION BY ${params.partitionBy}\n"
		if (params.privileges != null)
			result += "${params.privileges} SCHEMA PRIVILEGES\n"

		return result
	}

	@Override
	String generateComputeDefinition(Field f) {
		return "DEFAULT USING ${f.compute}"
	}

	/**
	 * Convert text to unicode escape Vertica string
	 * @param value
	 * @return
	 */
	@CompileStatic
	static String EscapeString(String value) {
		def sb = new StringBuilder()
		for (Integer i = 0; i < value.length(); i++) {
			def ch = value.chars[i]
			def bt = value.bytes[i]
			if (ch < '\u0020'.chars[0] || ch > '\u007E'.chars[0])
				sb.append('\\' + StringUtils.AddLedZeroStr(Integer.toHexString(bt), 4))
			else if (ch == '\''.chars[0])
				sb.append('\'\'')
			else if (ch == '\\'.chars[0])
				sb.append('\\\\')
			else
				sb.append(ch)
		}

		return 'U&\'' + sb.toString() + '\''
	}

	@SuppressWarnings('GroovyFallthrough')
	String copyFormatField(TableDataset dest, Field field, String formatDate, String formatTime, String formatDateTime) {
		String res = null
		def destField = dest.fieldByName(field.name)
		def destFormat = destField?.format
		switch (field.type) {
			case Field.blobFieldType:
				res = "'hex'"
				break

			case Field.dateFieldType:
				def format = destFormat?:formatDate
				if (format != null && format.length() > 0)
					res = "'$format'"
				break

			case Field.timeFieldType:
				def format = destFormat?:formatTime
				if (format != null && format.length() > 0)
					res = "'$format'"
				break

			case Field.datetimeFieldType: case Field.timestamp_with_timezoneFieldType: /* TODO: добавить отдельно формат для часовых поясов */
				def format = destFormat?:formatDateTime
				if (format != null && format.length() > 0)
					res = "'$format'"
				break

			/* TODO: Добавить поддержку массивов */
			/*case Field.arrayFieldType:
				res = "$fieldName"
				break*/
		}

		return res
	}

	@Override
	Boolean supportBulkLoadAbortOnError() { true }

	@Override
	void bulkLoadFile(CSVDataset source, Dataset dest, Map bulkParams, Closure prepareCode) {
		def params = bulkLoadFilePrepare(source, dest as JDBCDataset, bulkParams, prepareCode)

		def rowDelimiterChar = source.rowDelimiter()
		if (rowDelimiterChar == '\r\n')
			rowDelimiterChar = '\n'
		if (rowDelimiterChar == null || rowDelimiterChar.length() != 1)
			throw new DatasetError(dest, '#vertica.invalid_row_delimiter')

		def escapeChar = params.escapeChar as String
		if (source.escaped() && escapeChar == null)
			escapeChar = '\\'
		else if (!source.escaped())
			escapeChar = null
		if (escapeChar != null && escapeChar.length() != 1)
			throw new DatasetError(dest, '#vertica.invalid_escape_char')

		def fieldDelimiterChar = source.fieldDelimiter()
		if (fieldDelimiterChar == null || fieldDelimiterChar.length() != 1)
			throw new DatasetError(dest, '#vertica.invalid_field_delimiter')

		def quoteStrChar = source.quoteStr()
		if (quoteStrChar == null || quoteStrChar.length() != 1)
			throw new DatasetError(dest, '#vertica.invalid_quote')

		def standardLoad = (source.escaped() || rowDelimiterChar != '\n')

		String parserText = '', fieldDelimiter = '', rowDelimiter = '', quoteStr = '', nullAsValue = '', escapeCharStr = ''
		def useExternalParser = (params.parser != null && !(params.parser as Map).isEmpty())
		if (useExternalParser) {
			String parserFunc = (params.parser as Map).function
			if (parserFunc == null)
				throw new ExceptionGETL('Required parser function name')
			def parserOptions = (params.parser as Map).options as Map<String, Object>
			if (parserOptions != null && !parserOptions.isEmpty()) {
				def ol = []
				parserOptions.each { String name, def value ->
					if (value instanceof String) {
						ol.add("$name='$value'")
					}
					else {
						ol.add("$name=$value")
					}
				}
				parserText = "\nWITH PARSER $parserFunc(${ol.join(', ')})"
			}
			else {
				parserText = "\nWITH PARSER $parserFunc()"
			}
			def useCsvOptions = BoolUtils.IsValue((params.parser as Map).useCsvOptions, true)
			if (useCsvOptions) {
				fieldDelimiter = "\nDELIMITER AS ${EscapeString(fieldDelimiterChar)}"
				rowDelimiter = "\nRECORD TERMINATOR ${EscapeString(rowDelimiterChar)}"
				quoteStr = "\nENCLOSED BY ${EscapeString(quoteStrChar)}"
				if (source.nullAsValue() != null)
					nullAsValue = "\nNULL AS ${EscapeString(source.nullAsValue())}"
				if (escapeChar != null)
					escapeCharStr = "\nESCAPE AS ${EscapeString(escapeChar)}"
				else
					escapeCharStr = '\nNO ESCAPE'
			}
		}
		else if (!standardLoad) {
			if (fieldDelimiterChar == '\u0001' && escapeChar == null)
				throw new ExceptionGETL('Field separator with unicode value 1 is not supported!')
			if (quoteStrChar == '\u0001' && escapeChar == null)
				throw new ExceptionGETL('Quote separator with unicode value 1 is not supported!')
			if (rowDelimiterChar == '\u0001' && escapeChar == null)
				throw new ExceptionGETL('Row separator with unicode value 1 is not supported!')
			if (source.nullAsValue() != null)
				throw new ExceptionGETL('Vertica driver not support nullAsValue option, when bulk loading not escaped file!')

			def opts = [
					'type=\'traditional\'',
					"delimiter = ${EscapeString(fieldDelimiterChar)}",
					"enclosed_by = ${EscapeString(quoteStrChar)}",
					"record_terminator = ${EscapeString(rowDelimiterChar)}",
					"escape = ${EscapeString(escapeChar?:'\u0001')}",
					"header=${source.isHeader()}",
					'trim=false'
			]

			parserText = "\nWITH PARSER public.fcsvparser(${opts.join(', ')})"
			/*if (source.nullAsValue() != null)
				nullAsValue = "\nNULL AS ${EscapeString(source.nullAsValue())}"*/
		}

		if (parserText.length() == 0) {
			if (source.fieldDelimiter() != null)
				fieldDelimiter = "\nDELIMITER AS ${EscapeString(fieldDelimiterChar)}"
			if (rowDelimiterChar != null)
				rowDelimiter = "\nRECORD TERMINATOR ${EscapeString(rowDelimiterChar)}"
			if (source.quoteStr() != null)
				quoteStr = "\nENCLOSED BY ${EscapeString(quoteStrChar)}"
			if (source.nullAsValue() != null)
				nullAsValue = "\nNULL AS ${EscapeString(source.nullAsValue())}"
			if (escapeChar != null)
				escapeCharStr = "\nESCAPE AS ${EscapeString(escapeChar)}"
			else
				escapeCharStr = '\nNO ESCAPE'
		}

		def header = source.isHeader()
		def isGzFile = source.isGzFile()

		def map = params.map as List<BulkLoadMapping>
		def loadMethod = params.loadMethod as String
		if (loadMethod != null && loadMethod.toLowerCase() == VerticaWriteSpec.AUTO)
			loadMethod = null
		def enforceLength = (!useExternalParser && BoolUtils.IsValue(params.enforceLength, true))
		def autoCommit = ListUtils.NotNullValue([BoolUtils.IsValue(params.autoCommit, null), dest.connection.tranCount == 0])
		String compressed = ListUtils.NotNullValue([params.compressed, (isGzFile?'GZIP':null)])

		def rejectMax = params.rejectMax as Long
		def abortOnError = BoolUtils.IsValue(params.abortOnError, true)
		String location = params.location as String
		String onNode = (location != null)?(' ON ' + location):''

		String exceptionPath = FileUtils.TransformFilePath(params.exceptionPath as String, connection.dslCreator)
		if (exceptionPath != null && FileUtils.FileExtension(exceptionPath).toLowerCase() == '.gz')
			exceptionPath = FileUtils.ExcludeFileExtension(exceptionPath)
		if (location == null && exceptionPath != null)
			new File(exceptionPath).delete()

		String rejectedPath = FileUtils.TransformFilePath(params.rejectedPath as String, connection.dslCreator)
		if (rejectedPath != null && FileUtils.FileExtension(rejectedPath).toLowerCase() == '.gz')
			rejectedPath = FileUtils.ExcludeFileExtension(rejectedPath)
		if (location == null && rejectedPath != null)
			new File(rejectedPath).delete()

		String streamName = params.streamName

		String fileName
		if (params.files != null) {
			if (!(params.files instanceof List))
				throw new ExceptionGETL('Parameter files must be of type List')
			def f = [] as List<String>
			(params.files as List<String>).each {
				def s = "'" + it + "'"
				s += onNode
				if (compressed != null)
					s += ' ' + compressed
				f.add(s)
			}
			fileName = f.join(', ')
		}
		else {
			fileName = "'${source.fullFileName().replace("\\", "/")}'"
			fileName += onNode
			if (compressed != null)
				fileName += ' ' + compressed
		}

		if (exceptionPath != null && location == null)
			FileUtils.ValidFilePath(exceptionPath)
		if (rejectedPath != null && location == null)
			FileUtils.ValidFilePath(rejectedPath)

		StringBuilder sb = new StringBuilder()
		sb.append("COPY ${fullNameDataset(dest)} (\n")

		def table = dest as TableDataset
		String formatDate = ListUtils.NotNullValue([params.formatDate, table.bulkLoadDirective.formatDate])
		String formatTime = ListUtils.NotNullValue([params.formatTime, table.bulkLoadDirective.formatTime])
		String formatDateTime = ListUtils.NotNullValue([params.formatDateTime, table.bulkLoadDirective.formatDateTime])

		// Find filled columns
		def filledCols = [] as List<String>
		def patternFilledCols = new HashMap<String, Pattern>()
		map.each { node ->
			if (node.sourceFieldName != null && (node.destinationFieldName == null || node.expression != null)) {
				def sourceFieldName = node.sourceFieldName.toLowerCase()
				filledCols.add(sourceFieldName)
				def pattern = Pattern.compile("(\\b${sourceFieldName}\\b)", Pattern.CASE_INSENSITIVE)
				patternFilledCols.put(sourceFieldName, pattern)
			}
		}

		// Replace filled columns in expressions
		map.findAll { node -> node.expression != null && node.expression.length() > 0 }.each { node ->
			def expr = node.expression
			patternFilledCols.each { sourceFieldName, pattern ->
				def matcher = pattern.matcher(expr)
				expr = matcher.replaceAll("_filled_$sourceFieldName")
			}
			node.expression = expr
		}

		def columns = [] as List<String>
		def options = [] as List<String>
		map.each { rule ->
			def sourceFieldName = rule.sourceFieldName?.toLowerCase()
			if (sourceFieldName != null) {
				def sourceField = source.fieldByName(sourceFieldName)
				if (sourceFieldName in filledCols) {
					def fillFieldName = table.sqlObjectName("_filled_$sourceFieldName")
					def sourceType = table.currentJDBCConnection.currentJDBCDriver.type2sqlType(sourceField, false)
					columns.add("  $fillFieldName FILLER $sourceType")

					def format = copyFormatField(table, sourceField, formatDate, formatTime, formatDateTime)
					if (format != null)
						options.add('  ' + table.sqlObjectName(fillFieldName) + ' format ' + format)
				} else if (sourceField.type == Field.blobFieldType) {
					def format = copyFormatField(table, sourceField, formatDate, formatTime, formatDateTime)
					if (format != null)
						options.add('  ' + table.sqlObjectName(sourceField.name) + ' format ' + format)
				}
			}

			if (rule.destinationFieldName != null) {
				def destFieldName = table.sqlObjectName(rule.destinationFieldName)
				if (rule.expression != null) {
					if (rule.expression.length() > 0)
						columns.add("  $destFieldName AS ${rule.expression}")
				}
				else
					columns.add("  $destFieldName")
			}
		}

		sb.append(columns.join(',\n'))
		sb.append('\n)\n')

		if (!options.isEmpty()) {
			sb.append('COLUMN OPTION (\n')
			sb.append(options.join(',\n'))
			sb.append('\n)\n')
		}

		sb.append("""FROM ${(location == null)?"LOCAL ":""}$fileName $parserText $fieldDelimiter$nullAsValue$quoteStr$rowDelimiter$escapeCharStr\n""")
		if (header && parserText.length() == 0)
			sb.append('SKIP 1\n')
		if (rejectMax != null)
			sb.append("REJECTMAX ${rejectMax}\n")
		if (exceptionPath != null)
			sb.append("EXCEPTIONS '${exceptionPath}'$onNode\n")

		def convertReject = (location == null && (
				((int)fieldDelimiterChar.charAt(0)) < 8 || ((int)rowDelimiterChar.charAt(0)) < 8 || ((int)quoteStrChar.charAt(0)) < 8))
		if (rejectedPath != null)
			sb.append("REJECTED DATA '${rejectedPath}'$onNode\n")

		if (enforceLength)
			sb.append('ENFORCELENGTH\n')
		if (abortOnError)
			sb.append('ABORT ON ERROR\n')
		if (loadMethod != null)
			sb.append("${loadMethod}\n")
		if (streamName != null)
			sb.append("STREAM NAME '$streamName'\n")
		if (!autoCommit)
			sb.append('NO COMMIT\n')

		def sql = sb.toString()
//		table.sysParams.sql = sql
		//println sql

		dest.writeRows = 0L
		dest.updateRows = 0L
		Long count = 0
		try {
			count = executeCommand(sql, [isUpdate: true])
		}
		finally {
			if (convertReject && rejectedPath != null) {
				def rejectCurFile = new File(rejectedPath)
				def rejectNewFile = new File(rejectedPath + '.getltemp')
				if (rejectCurFile.exists()) {
					def curFile = (source.cloneDatasetConnection() as CSVDataset).tap { f ->
						currentCsvConnection.path = rejectCurFile.parent
						f.fileName = rejectCurFile.name
						f.isGzFile = false
						f.readOpts.isSplit = false
						f.field = source.field
						f.field.each { it.type = it.stringFieldType }
						resetFieldToDefault()
					}

					def newFile = new CSVDataset().tap { f ->
						useConnection new CSVConnection().tap { con ->
							con.path = rejectNewFile.parent
							con.header = false
							con.fieldDelimiter = '\t'
							con.rowDelimiter = '\n'
							con.quoteStr = '"'
							con.escaped = false
							con.blobAsPureHex = false
							con.blobPrefix = '0x'
						}
						f.fileName = rejectNewFile.name
						f.field = source.field
						f.field.each { it.type = it.stringFieldType }
						resetFieldToDefault()
					}

					new Flow().copy(source: curFile, dest: newFile)
					rejectCurFile.delete()
					rejectNewFile.renameTo(rejectCurFile)
				}
			}
		}
		source.readRows = count
		dest.writeRows = count
		dest.updateRows = count
	}

	@Override
	protected String sessionID() {
		String res = null
		//noinspection SqlNoDataSourceInspection
		def rows = sqlConnect.rows('SELECT session_id FROM CURRENT_SESSION')
		if (!rows.isEmpty())
			res = rows[0].session_id as String

		return res
	}

	@Override
	protected Map unionDatasetMergeParams (JDBCDataset source, JDBCDataset target, Map procParams) {
		def res = super.unionDatasetMergeParams(source, target, procParams)
		res.direct = (procParams.direct != null && procParams.direct)?'/*+direct*/':''

		return res
	}

	@Override
	protected String unionDatasetMergeSyntax () {
        return '''MERGE {direct} INTO {target} t
  USING {source} s ON {join}
  WHEN MATCHED THEN UPDATE SET 
    {set}
  WHEN NOT MATCHED THEN INSERT ({fields})
    VALUES ({values})'''
	}

	@Override
	void sqlTableDirective(JDBCDataset dataset, Map params, Map dir) {
		super.sqlTableDirective(dataset, params, dir)
		if (params.limit != null && ConvertUtils.Object2Long(params.limit) > 0) {
			dir.afterOrderBy = ((dir.afterOrderBy != null)?(dir.afterOrderBy + '\n'):'') + "LIMIT ${params.limit}"
			params.limit = null
		}
		if (params.offs != null && ConvertUtils.Object2Long(params.offs) > 0) {
			dir.afterOrderBy = ((dir.afterOrderBy != null)?(dir.afterOrderBy + '\n'):'') + "OFFSET ${params.offs}"
			params.offs = null
		}
		Map<String, Object> dl = (((dataset as TableDataset).readDirective as Map<String, Object>)?:new HashMap<String, Object>()) + params as Map<String, Object>
        if (dl.label != null) {
            dir.afterselect = "/*+label(${dl.label})*/"
        }
		if (dl.tablesample != null) {
			dir.aftertable = "TABLESAMPLE(${dl.tablesample})"
		}
	}

	@SuppressWarnings("UnnecessaryQualifiedReference")
	@Override
	void prepareField(Field field) {
		super.prepareField(field)

		if (field.type == Field.Type.ARRAY) {
			if (field.typeName?.length() > 0 && field.typeName[0] == '_') {
				field.arrayType = field.typeName.substring(1)
				field.typeName = field.arrayType + ' array'
			}

			return
		}

		if (field.typeName != null) {
			if (field.typeName.matches("(?i)TIMESTAMPTZ")) {
				field.type = Field.Type.TIMESTAMP_WITH_TIMEZONE
				return
			}

			if (field.typeName.matches("(?i)UUID")) {
				field.type = Field.Type.UUID
				field.dbType = java.sql.Types.VARCHAR
				field.length = 36
				field.precision = null
			}
		}
	}

	@Override
	Boolean blobReadAsObject (Field field = null) { return false }

	@Override
	String blobMethodWrite (String methodName) {
		return """void $methodName (java.sql.Connection con, java.sql.PreparedStatement stat, Integer paramNum, byte[] value) {
	if (value == null) 
		stat.setNull(paramNum, java.sql.Types.BINARY) 
	else
		stat.setBytes(paramNum, value)
}"""
	}

	@Override
	Boolean textReadAsObject() { return false }

	static String writeHints(Map params) {
		def hints = [] as List<String>
		if (params.direct != null)
			hints.add((params.direct as String).toLowerCase())
		if (params.label != null)
			hints.add('label(' + params.label + ')')
		return (!hints.isEmpty())?('/*+' + hints.join(', ') + '*/'):''
	}

	@Override
	protected String syntaxInsertStatement(JDBCDataset dataset, Map params) {
		return "INSERT ${writeHints(params)} INTO {table} ({columns}) VALUES({values})"
	}

	@Override
	protected String syntaxUpdateStatement(JDBCDataset dataset, Map params) {
		def res = "UPDATE ${writeHints(params)} {table} SET {values} WHERE {keys}"
		if (params.where != null)
			res += " AND (${prepareWhereExpression(dataset, params)})"

		return res
	}

	@Override
	protected String syntaxDeleteStatement(JDBCDataset dataset, Map params){
		def res = "DELETE ${writeHints(params)} FROM {table} WHERE {keys}"
		if (params.where != null)
			res += " AND (${params.where})"
		return res
	}

	@Override
	void validCsvTempFile(Dataset source, CSVDataset csvFile) {
		super.validCsvTempFile(source, csvFile)
		if (!(csvFile.codePage().toLowerCase() in ['utf-8', 'utf8']))
			throw new DatasetError(csvFile, 'The file must be encoded in utf-8 for bulk load to table {table}', [table: source])

		if (csvFile.fieldDelimiter().length() > 1)
			throw new DatasetError(csvFile, 'The field delimiter must have only one character for bulk load to table {table}', [table: source])

		if (csvFile.quoteStr().length() > 1)
			throw new DatasetError(csvFile, 'The quote must have only one character for bulk load to table {table}', [table: source])

		if (csvFile.rowDelimiter().length() > 1 && csvFile.rowDelimiter() != '\r\n')
			throw new DatasetError(csvFile, 'The row delimiter must have only one character for bulk load to table {table}', [table: source])

		def isBlobs = (source.field.find { field -> field.type == Field.blobFieldType } != null)
		if (!csvFile.escaped() && csvFile.rowDelimiter() in ['\n', '\r\n']) {
			if (csvFile.nullAsValue() != null)
				throw new DatasetError(csvFile, 'When escaped is off, null as value is not allowed for bulk load to table {table}', [table: source])
			if (isBlobs)
				throw new DatasetError(csvFile, 'When escaped is off, blob fields is not allowed for bulk load to table {table}', [table: source])
		}
		if (isBlobs) {
			if (csvFile.blobAsPureHex())
				throw new DatasetError(csvFile, 'Blob fields is not allowed when used blob as pure hex option for bulk load to table {table}', [table: source])
			if (csvFile.blobPrefix() != '0x')
				throw new DatasetError(csvFile, 'Blob fields is not allowed when prefix not equals "0x" for bulk load to table {table}', [table: source])
		}
	}

	@Override
	protected Map deleteRowsHint(TableDataset dataset, Map procParams) {
		def res = super.deleteRowsHint(dataset, procParams)
		def direct = (procParams.direct?:dataset.writeDirective.direct) as String
		def label = procParams.label?:dataset.writeDirective.label
		def ad = [] as List<String>
		if (direct != null)
			ad.add(direct)
		if (label != null)
			ad.add("label(${label})")
		if (!ad.isEmpty())
			res.afterDelete = "/*+${ad.join(', ')}*/"
		return res
	}

	@Override
	List<Field> fields(Dataset dataset) {
		def ds = dataset as JDBCDataset
		def res = super.fields(ds)
		if (ds.type in [JDBCDataset.viewType, JDBCDataset.localTemporaryViewType]) {
			res.each {it.isNull = true }
		}
		return res
	}

	@Override
	protected void prepareCopyTableSource(TableDataset source, Map<String, Object> qParams ) {
		if (source.readDirective.label != null)
			qParams.after_select = "/*+label(${source.readDirective.label})*/"
		if (source.readDirective.tablesample != null)
			qParams.after_from = "TABLESAMPLE(${source.readDirective.tablesample})"
	}

	@Override
	protected void prepareCopyTableDestination(TableDataset dest, Map<String, Object> qParams ) {
		if (dest.writeDirective.direct != null && (dest.writeDirective.direct as String).toLowerCase() != VerticaWriteSpec.AUTO)
			qParams.after_insert = "/*+${dest.writeDirective.direct}*/"
	}

	@Override
	String generateDefaultDefinition(Field f) {
		String res
		def lookupType = (f.extended.lookup as String)?.toUpperCase()

		if (lookupType == null || lookupType == VerticaTable.lookupDefaultType)
			res = "DEFAULT (${f.defaultValue})"
		else if (lookupType == VerticaTable.lookupUsingType)
			res = "SET USING (${f.defaultValue})"
		else if (lookupType == VerticaTable.lookupDefaultUsingType)
			res = "DEFAULT USING (${f.defaultValue})"
		else
			throw new ExceptionGETL("Unknown lookup type $lookupType!")

		return res
	}

	/** Check default privileges type */
	private String checkPrivilegesType(String privileges, String objectName) {
		if (privileges == null)
			return null

		privileges = privileges.toUpperCase()
		if (!(privileges in ['INCLUDE', 'EXCLUDE']))
			throw new ExceptionGETL("Invalid default privilege option \"$privileges\" for \"$objectName\"!")

		return privileges
	}

	@Override
	protected Map<String, Object> createSchemaParams(String schemaName, Map<String, Object> createParams) {
		def res = super.createSchemaParams(schemaName, createParams)

		def authorization = createParams.authorization as String
		if (authorization != null)
			res.authorization = prepareObjectNameWithPrefix(authorization, '"')

		def privileges = createParams.privileges as String
		if (privileges != null)
			res.privileges = checkPrivilegesType(privileges, schemaName)

		return res
	}

	@Override
	protected Map<String, Object> dropSchemaParams(String schemaName, Map<String, Object> dropParams) {
		def res = super.dropSchemaParams(schemaName, dropParams)
		if (BoolUtils.IsValue(dropParams.cascade))
			res.cascade = 'CASCADE'

		return res
	}

	@Override
	protected Map<String, Object> createViewParams(ViewDataset dataset, Map<String, Object> createParams) {
		def res = super.createViewParams(dataset, createParams)

		def privileges = createParams.privileges as String
		if (privileges != null) {
			if (dataset.type != JDBCDataset.localTemporaryViewType)
				res.privileges = checkPrivilegesType(privileges, dataset.objectName)
			else
				throw new IncorrectParameterError(connection, '#vertica.temporary_view_privileges_not_support', 'privileges')
		}
		return res
	}

	/**
	 * Process field to suitable Vertica field types
	 */
	protected void prepareImportField(Field field) {
		if (field.type == Field.stringFieldType) {
			if ((field.length?:0) == 0)
				field.length = 255
		}
		else if (field.type in [Field.textFieldType, Field.blobFieldType]) {
			if (field.length <= 65000)
				field.length = 65000
		}
		else if (field.type == Field.numericFieldType) {
			if (field.length > 0 && (field.precision?:0) == 0) {
				if (field.length < 10) {
					field.type = Field.integerFieldType
					field.length = null
					field.precision = null
				} else if (field.length <= 20) {
					field.type = Field.bigintFieldType
					field.length = null
					field.precision = null
				}
			}
			else if (field.length == null) {
				field.type = Field.doubleFieldType
				field.length = null
				field.precision = null
			}
		}
	}

	/**
	 * Convert Oracle table fields to suitable Vertica field types
	 * @param field source field
	 */
	protected void prepareImportOracleField(Field field) {
		if (field.type == Field.dateFieldType)
			field.type = Field.datetimeFieldType
	}

	@Override
	protected Boolean lengthTextFieldsAsBytes() { true }

	/**
	 * Preparing import fields from another dataset<br><br>
	 * <b>Import options:</b><br>
	 * <ul>
	 *     <li> resetTypeName: reset the name of the field type</li>
	 *     <li>resetKey: reset primary key</li>
	 *     <li>resetNotNull: reset not null</li>
	 *     <li>resetDefault: reset default value</li>
	 *     <li>resetCheck: reset check expression</li>
	 *     <li>resetCompute: reset compute expression</li>
	 *</ul>
	 * @param dataset source
	 * @param importParams import options
	 * @return list of prepared field
	 */
	@Override
	List<Field> prepareImportFields(Dataset dataset, Map importParams = new HashMap()) {
		def res = super.prepareImportFields(dataset, importParams)

		if (!(dataset instanceof VerticaTable)) {
			def isOracle = (dataset.connection.driver instanceof OracleDriver)

			res.each { field ->
				prepareImportField(field)
				if (isOracle)
					prepareImportOracleField(field)
			}
		}

		return res
	}

	@Override
	void prepareCsvTempFile(Dataset source, CSVDataset csvFile) {
		super.prepareCsvTempFile(source, csvFile)
		csvFile.escaped = false
		csvFile.quoteStr = '\u0005'
		csvFile.fieldDelimiter = '\u0007'
		csvFile.rowDelimiter = '\u0006'
		csvFile.blobAsPureHex = false
		csvFile.blobPrefix = '0x'
	}

	@Override
	String generatePrimaryKeyDefinition(JDBCDataset dataset, Map params) {
		def defPrimary = GenerationUtils.SqlKeyFields(dataset, dataset.field, null, null)
		return sqlExpressionValue('ddlCreatePrimaryKey', [columns: defPrimary.join(","), check_pk: (BoolUtils.IsValue(params.checkPrimaryKey))?'ENABLED':'DISABLED'])
	}

	/*
	@Override
	Class sqlType2JavaClass(String sqlType) {
		Class res = super.sqlType2JavaClass(sqlType)
		if (res == null)
			return res

		switch (sqlType.toUpperCase()) {
			case 'INT': case 'INTEGER': case 'SMALLINT': case 'MEDIUMINT': case 'TINYINT': case 'INT2': case 'INT4': case 'SIGNED':
				res = Long
				break
		}

		return res
	}
	 */
}