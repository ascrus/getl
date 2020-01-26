/*
 GETL - based package in Groovy, which automates the work of loading and transforming data. His name is an acronym for "Groovy ETL".

 GETL is a set of libraries of pre-built classes and objects that can be used to solve problems unpacking,
 transform and load data into programs written in Groovy, or Java, as well as from any software that supports
 the work with Java classes.

 Copyright (C) EasyData Company LTD

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

package getl.vertica

import groovy.transform.CompileStatic
import groovy.transform.InheritConstructors

import getl.csv.CSVDataset
import getl.data.*
import getl.driver.Driver
import getl.exception.ExceptionGETL
import getl.utils.*
import getl.jdbc.*

/**
 * Vertica driver class
 * @author Alexsey Konstantinov
 *
 */
@InheritConstructors
class VerticaDriver extends JDBCDriver {
	VerticaDriver () {
		super()

		defaultSchemaName = 'public'
		tempSchemaName = 'v_temp_schema'

        addPKFieldsToUpdateStatementFromMerge = true

		methodParams.register('createDataset', ['orderBy', 'segmentedBy', 'unsegmented', 'partitionBy'])
        methodParams.register('eachRow', ['label', 'tablesample'])
		methodParams.register('openWrite', ['direct', 'label'])
		methodParams.register('bulkLoadFile',
				['loadMethod', 'rejectMax', 'enforceLength', 'compressed', 'exceptionPath', 'rejectedPath',
				 'expression', 'location', 'maskDate', 'maskTime', 'maskDateTime',
				 'parser', 'streamName', 'files'])
		methodParams.register('unionDataset', ['direct'])
		methodParams.register('deleteRows', ['direct', 'label'])
	}

    @Override
    Map getSqlType () {
        Map res = super.getSqlType()
        res.DOUBLE.name = 'double precision'
        res.BLOB.name = 'varbinary'
        res.TEXT.name = 'long varchar'

        return res
    }

	@SuppressWarnings("UnnecessaryQualifiedReference")
	@Override
	List<Driver.Support> supported() {
        return super.supported() +
				[Driver.Support.LOCAL_TEMPORARY, Driver.Support.GLOBAL_TEMPORARY, Driver.Support.SEQUENCE,
                 Driver.Support.BLOB, Driver.Support.CLOB, Driver.Support.UUID,
                 Driver.Support.TIME, Driver.Support.DATE, Driver.Support.TIMESTAMP_WITH_TIMEZONE, Driver.Support.BOOLEAN,
                 Driver.Support.CREATEIFNOTEXIST, Driver.Support.DROPIFEXIST, Driver.Support.BULKLOADMANYFILES]
    }

	@SuppressWarnings("UnnecessaryQualifiedReference")
	@Override
	List<Driver.Operation> operations() {
        return super.operations() +
                [Driver.Operation.CLEAR, Driver.Operation.DROP, Driver.Operation.EXECUTE, Driver.Operation.CREATE,
                 Driver.Operation.BULKLOAD]
    }

	@Override
	String defaultConnectURL () {
		return 'jdbc:vertica://{host}/{database}'
	}

	@Override
	protected List<Map> getIgnoreWarning () {
		List<Map> res = []
		res << [errorCode: 4486, sqlState: '0A000']

		return res
	}

	@Override
	protected String createDatasetExtend(Dataset dataset, Map params) {
		def result = ''
		def temporary = (((dataset as JDBCDataset).type as JDBCDataset.Type) in
				[JDBCDataset.Type.GLOBAL_TEMPORARY, JDBCDataset.Type.LOCAL_TEMPORARY])
		if (temporary && params.onCommit != null && params.onCommit) result += 'ON COMMIT PRESERVE ROWS\n'
		if (params.orderBy != null && !(params.orderBy as List).isEmpty()) result += "ORDER BY ${(params.orderBy as List).join(", ")}\n"
		if (params.segmentedBy != null && params.unsegmented != null) throw new ExceptionGETL('Invalid segmented options')
		if (params.segmentedBy != null) result += "SEGMENTED BY ${params.segmentedBy}\n"
		if (params.unsegmented != null && params.unsegmented) result += "UNSEGMENTED ALL NODES\n"
		if (params.partitionBy != null) result += "PARTITION BY ${params.partitionBy}\n"

		return result
	}

	/**
	 * Convert text to unicode escape Vertica string
	 * @param value
	 * @return
	 */
	@CompileStatic
	static String EscapeString(String value) {
		def sb = new StringBuilder()
		for (int i = 0; i < value.length(); i++) {
			def ch = value.chars[i]
			def bt = value.bytes[i]
			if (ch < '\u0020'.chars[0] || ch > '\u007E'.chars[0])
				sb << '\\' + StringUtils.AddLedZeroStr(Integer.toHexString(bt), 4)
			else if (ch == '\''.chars[0])
				sb << '\'\''
			else if (ch == '\\'.chars[0])
				sb << '\\\\'
			else
				sb << ch
		}

		return 'U&\'' + sb.toString() + '\''
	}

	@Override
	void bulkLoadFile(CSVDataset source, Dataset dest, Map bulkParams, Closure prepareCode) {
		def params = bulkLoadFilePrepare(source, dest as JDBCDataset, bulkParams, prepareCode)

		def rowDelimiterChar = source.rowDelimiter
		if (rowDelimiterChar == '\r\n') rowDelimiterChar = '\n'

		String parserText = '', fieldDelimiter = '', rowDelimiter = '', quoteStr = '', nullAsValue = ''
		if (params.parser != null && !(params.parser as Map).isEmpty()) {
			String parserFunc = (params.parser as Map).function
			if (parserFunc == null) throw new ExceptionGETL('Required parser function name')
			def parserOptions = (params.parser as Map).options as Map<String, Object>
			if (parserOptions != null && !parserOptions.isEmpty()) {
				def ol = []
				parserOptions.each { String name, def value ->
					if (value instanceof String) {
						ol << "$name='$value'"
					}
					else {
						ol << "$name=$value"
					}
				}
				parserText = "\nWITH PARSER $parserFunc(${ol.join(', ')})"
			}
			else {
				parserText = "\nWITH PARSER $parserFunc()"
			}
			boolean useCsvOptions = BoolUtils.IsValue((params.parser as Map).useCsvOptions, true)
			if (useCsvOptions) {
				if (source.params.fieldDelimiter != null) fieldDelimiter = "\nDELIMITER AS ${EscapeString(source.fieldDelimiter)}"
				if (source.params.rowDelimiter != null) rowDelimiter = "\nRECORD TERMINATOR ${EscapeString(rowDelimiterChar)}"
				if (source.params.quoteStr != null) quoteStr = "\nENCLOSED BY ${EscapeString(source.quoteStr)}"
				if (source.params.nullAsValue != null) nullAsValue = "\nNULL AS ${EscapeString(source.nullAsValue)}"
			}
		}
		else if (!source.escaped) {
			def opts = [
					'type=\'traditional\'',
					"delimiter = ${EscapeString(source.fieldDelimiter)}",
					"enclosed_by = ${EscapeString(source.quoteStr)}",
					"record_terminator = ${EscapeString(rowDelimiterChar)}",
					"escape = ${EscapeString('\u0001')}"
			]
			if (source.header) opts << 'header=\'true\''

			parserText = "\nWITH PARSER fcsvparser(${opts.join(', ')})"
			if (source.nullAsValue != null) nullAsValue = "\nNULL AS ${EscapeString(source.nullAsValue)}"
		}

		if (parserText.length() == 0) {
			if (source.fieldDelimiter == null || source.fieldDelimiter.length() != 1) throw new ExceptionGETL('Required one char field delimiter')
			if (rowDelimiterChar == null || rowDelimiterChar.length() != 1) throw new ExceptionGETL('Required one char row delimiter')
			if (source.quoteStr == null || source.quoteStr.length() != 1) throw new ExceptionGETL('Required one char quote str')

			if (source.fieldDelimiter != null) fieldDelimiter = "\nDELIMITER AS ${EscapeString(source.fieldDelimiter)}"
			if (rowDelimiterChar != null) rowDelimiter = "\nRECORD TERMINATOR ${EscapeString(rowDelimiterChar)}"
			if (source.quoteStr != null) quoteStr = "\nENCLOSED BY ${EscapeString(source.quoteStr)}"
			if (source.nullAsValue != null) nullAsValue = "\nNULL AS ${EscapeString(source.nullAsValue)}"
		}

		def header = source.header
		def isGzFile = source.isGzFile

		def map = params.map as List<Map>
		def expressions = (params.expression as Map<String, String>)?:[:]
		String loadMethod = ListUtils.NotNullValue([params.loadMethod, 'AUTO'])
		boolean enforceLength = (parserText == null && BoolUtils.IsValue(params.enforceLength, true))
		boolean autoCommit = ListUtils.NotNullValue([BoolUtils.IsValue(params.autoCommit, null), dest.connection.tranCount == 0])
		String compressed = ListUtils.NotNullValue([params.compressed, (isGzFile?'GZIP':null)])
		String exceptionPath = params.exceptionPath
		String rejectedPath = params.rejectedPath
		def rejectMax = params.rejectMax as Long
		boolean abortOnError = BoolUtils.IsValue(params.abortOnError, true)
		String location = params.location
		String onNode = (location != null)?(' ON ' + location):''

		String streamName = params.streamName

		String fileName
		if (params.files != null) {
			if (!(params.files instanceof List))
				throw new ExceptionGETL('Parameter files must be of type List')
			def f = []
			(params.files as List<String>).each {
				def s = "'" + it + "'"
				s += onNode
				if (compressed != null) s += ' ' + compressed
				f << s
			}
			fileName = f.join(', ')
		}
		else {
			fileName = "'${source.fullFileName().replace("\\", "/")}'"
			fileName += onNode
			if (compressed != null) fileName += ' ' + compressed
		}

		if (exceptionPath != null && location == null) FileUtils.ValidFilePath(exceptionPath)
		if (rejectedPath != null && location == null) FileUtils.ValidFilePath(rejectedPath)

		StringBuilder sb = new StringBuilder()
		sb << "COPY ${fullNameDataset(dest)} (\n"

		JDBCConnection con = dest.connection as JDBCConnection
		String formatDate = ListUtils.NotNullValue([params.maskDate, con.maskDate])
		String formatTime = ListUtils.NotNullValue([params.maskTime, con.maskTime])
		String formatDateTime = ListUtils.NotNullValue([params.maskDateTime, con.maskDateTime])

		List columns = []
		List options = []
		map.each { Map f ->
			if (f.field != null) {
				def fieldName = (dest as JDBCDataset).sqlObjectName((f.field as Field).name)
				columns << fieldName
				switch ((f.field as Field).type) {
					case Field.Type.BLOB:
						options << "$fieldName format 'hex'"
						break
					case Field.Type.DATE:
						if (f.format != null && f.format != '')
							options << "$fieldName format '${f.format}'"
						else
							if (formatDate != null) options << "$fieldName format '$formatDate'"
						break
					case Field.Type.TIME:
						if (f.format != null && f.format != '')
							options << "$fieldName format '${f.format}'"
						else
							if (formatTime != null) options << "$fieldName format '$formatTime'"
						break
					case Field.Type.DATETIME: case Field.Type.TIMESTAMP_WITH_TIMEZONE:
						if (f.format != null && f.format != '')
							options << "$fieldName format '${f.format}'"
						else
							if (formatDateTime != null) options << "$fieldName format '$formatDateTime'"
				}
			}
			else if (f.alias != null) {
				columns << f.alias
			}
			else {
				columns << "${fieldPrefix}__notfound__${f.column}${fieldPrefix} FILLER varchar(8000)"
			}
		}

		expressions.each { String col, String expr ->
			if (dest.fieldByName(col) == null) throw new ExceptionGETL("Expression field \"$col\" not found")
			if (expr != null) {
				col = (dest as JDBCDataset).sqlObjectName(col)
				columns << "$col AS $expr"
			}
		}

		sb << columns.join(',\n')
		sb << '\n)\n'

		if (!options.isEmpty()) {
			sb << 'COLUMN OPTION (\n'
			sb << options.join(',\n')
			sb << '\n)\n'
		}

		sb << """FROM ${(location == null)?"LOCAL ":""}$fileName $parserText $fieldDelimiter$nullAsValue$quoteStr$rowDelimiter
"""
		if (header && parserText.length() == 0) sb << 'SKIP 1\n'
		if (rejectMax != null) sb << "REJECTMAX ${rejectMax}\n"
		if (exceptionPath != null) sb << "EXCEPTIONS '${exceptionPath}'$onNode\n"
		if (rejectedPath != null) sb << "REJECTED DATA '${rejectedPath}'$onNode\n"
		if (enforceLength) sb << 'ENFORCELENGTH\n'
		if (abortOnError) sb << 'ABORT ON ERROR\n'
		sb << "${loadMethod}\n"
		if (streamName != null) sb << "STREAM NAME '$streamName'\n"
		if (!autoCommit) sb << 'NO COMMIT\n'

		def sql = sb.toString()
		dest.sysParams.sql = sql
		//println sql

		dest.writeRows = 0
		dest.updateRows = 0
		long count = executeCommand(sql, [isUpdate: true])
		source.readRows = count
		dest.writeRows = count
		dest.updateRows = count
	}

	@Override
	protected String sessionID() {
		String res = null
		def rows = sqlConnect.rows('SELECT session_id FROM CURRENT_SESSION')
		if (!rows.isEmpty()) res = rows[0].session_id as String

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
	protected String getChangeSessionPropertyQuery() { return 'SET {name} TO {value}' }

	@SuppressWarnings("DuplicatedCode")
	@Override
	void sqlTableDirective (Dataset dataset, Map params, Map dir) {
		super.sqlTableDirective(dataset, params, dir)
		if (params.limit != null) {
			dir.afterOrderBy = ((dir.afterOrderBy != null)?(dir.afterOrderBy + '\n'):'') + "LIMIT ${params.limit}"
			params.limit = null
		}
		if (params.offs != null) {
			dir.afterOrderBy = ((dir.afterOrderBy != null)?(dir.afterOrderBy + '\n'):'') + "OFFSET ${params.offs}"
			params.offs = null
		}
		Map<String, Object> dl = ((dataset as TableDataset).readDirective as Map)?:[:] + params
        if (dl.label != null) {
            dir.afterselect = "/*+label(${dl.label})*/"
        }
		if (dl.tablesample != null) {
			dir.aftertable = "TABLESAMPLE(${dl.tablesample})"
		}
	}

	@SuppressWarnings("UnnecessaryQualifiedReference")
	@Override
	void prepareField (Field field) {
		super.prepareField(field)

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
	boolean blobReadAsObject () { return false }

	@Override
	String blobMethodWrite (String methodName) {
		return """void $methodName (java.sql.Connection con, java.sql.PreparedStatement stat, int paramNum, byte[] value) {
	if (value == null) { 
		stat.setNull(paramNum, java.sql.Types.BINARY) 
	}
	else {
		def stream = new ByteArrayInputStream(value)
		stat.setBinaryStream(paramNum, stream, value.length)
		stream.close()
	}
}"""
	}

	@Override
	boolean textReadAsObject() { return false }

	static String writeHints(Map params) {
		def hints = [] as List<String>
		if (params.direct != null) hints << (params.direct as String).toLowerCase()
		if (params.label != null) hints << 'label(' + params.label + ')'
		return (!hints.isEmpty())?('/*+' + hints.join(', ') + '*/'):''
	}

	@Override
	protected String syntaxInsertStatement(Dataset dataset, Map params) {
		return "INSERT ${writeHints(params)} INTO {table} ({columns}) VALUES({values})"
	}

	@Override
	protected String syntaxUpdateStatement(Dataset dataset, Map params) {
		def res = "UPDATE ${writeHints(params)} {table} SET {values} WHERE {keys}"
		if (params.where != null) res += " AND (${params.where})"
		return res
	}

	@Override
	protected String syntaxDeleteStatement(Dataset dataset, Map params){
		def res = "DELETE ${writeHints(params)} FROM {table} WHERE {keys}"
		if (params.where != null) res += " AND (${params.where})"
		return res
	}

	@Override
	void prepareCsvTempFile(Dataset source, CSVDataset csvFile) {
		csvFile.header = true
		csvFile.escaped = (csvFile.field.find { it.type == Field.blobFieldType && source.fieldByName(it.name) != null } != null)
		csvFile.codePage = 'UTF-8'
		csvFile.nullAsValue = '\u000C'
		csvFile.fieldDelimiter = ','
		csvFile.rowDelimiter = '\n'
		csvFile.quoteStr = '"'
	}

	@Override
	void validCsvTempFile(Dataset source, CSVDataset csvFile) {
		if (!(csvFile.codePage.toLowerCase() in ['utf-8', 'utf8']))
			throw new ExceptionGETL('The file must be encoded in utf-8 for batch download!')

		if (csvFile.fieldDelimiter.length() > 1)
			throw new ExceptionGETL('The field delimiter must have only one character for bulk load!')

		if (csvFile.quoteStr.length() > 1)
			throw new ExceptionGETL('The quote must have only one character for bulk load!')

		if (csvFile.rowDelimiter.length() > 1 && csvFile.rowDelimiter != '\r\n')
			throw new ExceptionGETL('The row delimiter must have only one character for bulk load!')

		if (!csvFile.escaped) {
			def blobFields = csvFile.field.findAll { it.type == Field.blobFieldType && source.fieldByName(it.name) != null }
			if (blobFields != null && !blobFields.isEmpty()) {
				def blobNames = blobFields*.name
				throw new ExceptionGETL("When escaped is off, bulk loading with binary type fields is not allowed (fields: ${blobNames.join(', ')})!")
			}
		}
	}

	@Override
	protected Map deleteRowsHint(TableDataset dataset, Map procParams) {
		def res = super.deleteRowsHint(dataset, procParams)
		def direct = procParams.direct?:dataset.writeDirective.direct
		def label = procParams.label?:dataset.writeDirective.label
		def ad = []
		if (direct != null) ad << direct
		if (label != null) ad << "label(${label})"
		if (!ad.isEmpty()) res.afterDelete = "/*+${ad.join(', ')}*/"
		return res
	}
}