package getl.h2

import groovy.transform.InheritConstructors

import getl.csv.*
import getl.data.*
import getl.driver.Driver
import getl.exception.ExceptionGETL
import getl.jdbc.*
import getl.utils.*

/**
 * H2 driver class
 * @author Alexsey Konstantinov
 *
 */
@InheritConstructors
class H2Driver extends JDBCDriver {
	@Override
	protected void registerParameters() {
		super.registerParameters()
		methodParams.register("createDataset", ["transactional", "not_persistent"])
		methodParams.register('bulkLoadFile', ['expression'])
	}

	@Override
	protected void initParams() {
		super.initParams()

		commitDDL = false

		caseObjectName = "UPPER"
		defaultSchemaName = "PUBLIC"
		connectionParamBegin = ";"
		connectionParamJoin = ";"

		sqlExpressions.ddlAutoIncrement = 'AUTO_INCREMENT'
	}

	@SuppressWarnings("UnnecessaryQualifiedReference")
	@Override
	List<Driver.Support> supported() {
		return super.supported() +
				[Driver.Support.GLOBAL_TEMPORARY, Driver.Support.LOCAL_TEMPORARY, Driver.Support.MEMORY,
				 Driver.Support.SEQUENCE, Driver.Support.BLOB, Driver.Support.CLOB, Driver.Support.INDEX,
				 Driver.Support.UUID, Driver.Support.TIME, Driver.Support.DATE, Driver.Support.TIMESTAMP_WITH_TIMEZONE,
				 Driver.Support.BOOLEAN, Driver.Support.CREATEIFNOTEXIST, Driver.Support.DROPIFEXIST]
	}

	@SuppressWarnings("UnnecessaryQualifiedReference")
	@Override
	List<Driver.Operation> operations() {
		return super.operations() +
				[Driver.Operation.TRUNCATE, Driver.Operation.DROP, Driver.Operation.EXECUTE,
				 Driver.Operation.CREATE, Driver.Operation.BULKLOAD, Driver.Operation.MERGE]
	}

	@Override
	String defaultConnectURL() {
		def con = connection as H2Connection
		def url
		if (con.inMemory) {
			url = (con.connectHost != null) ? "jdbc:h2:tcp://{host}/mem:{database}" : "jdbc:h2:mem:{database}"
			if (con.connectDatabase == null) url = url.replace('{database}', 'memory_database')
		} else {
			url = (con.connectHost != null) ? "jdbc:h2:tcp://{host}/{database}" : "jdbc:h2://{database}"
		}

		return url
	}

	@Override
	List<Object> retrieveObjects (Map params, Closure<Boolean> filter) {
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
		String result = ""
		def temporary = (dataset as JDBCDataset).isTemporaryTable
		if (BoolUtils.IsValue(params."not_persistent")) result += "NOT PERSISTENT "
		if (temporary && BoolUtils.IsValue(params.transactional)) result += "TRANSACTIONAL "

		return result
	}

	@Override
	void bulkLoadFile(CSVDataset source, Dataset dest, Map params, Closure prepareCode) {
		if (params.compressed != null) throw new ExceptionGETL("H2 bulk load dont support compression files")

		params = bulkLoadFilePrepare(source, dest as JDBCDataset, params, prepareCode)

		def map = params.map as List<Map>

		Map<String, String> expression = (params.expression?:[:]) as Map<String, String>
		expression.each { String fieldName, String expr ->
			if (dest.fieldByName(fieldName) == null) throw new ExceptionGETL("Unknown field \"$fieldName\" in \"expression\" parameter")
		}

		StringBuilder sb = new StringBuilder()
		List<String> columns = []
		List<String> fields = []
		List<String> headers = []
		List<String> fParams = []
		map.each { Map<String, Object> m ->
			Field f = m.field as Field
			if (f != null && !f.isReadOnly) {
				headers << f.name.toUpperCase().replace('\'', '\'\'')
				columns << fieldPrefix + f.name.toUpperCase() + fieldPrefix
				def expr = expression.get(f.name.toLowerCase())
				if (expr == null) {
					fields << fieldPrefix + f.name.toUpperCase() + fieldPrefix
				} else {
					fields << expr
				}
			}
		}
		def cols = columns.join(', ')
		def csvFields = fields.join(', ')
		def heads = (!source.isHeader()) ? "'" + headers.join(source.fieldDelimiter()) + "'" : "null"
		fParams << "charset=${source.codePage()}".toString()
		fParams << "fieldSeparator=${source.fieldDelimiter()}".toString()
		if (source.quoteStr() != null) fParams << "fieldDelimiter=${StringUtils.EscapeJava(source.quoteStr())}".toString()
		def functionParams = fParams.join(" ")

		sb <<
"""INSERT INTO ${fullNameDataset(dest)} (
$cols
)
SELECT $csvFields 
FROM CSVREAD('{file_name}', ${heads}, '${functionParams}')
"""
        def sql = sb.toString()
		//println sb.toString()

        dest.writeRows = 0
		dest.updateRows = 0
		def loadFile = source.fullFileName()
		def count = executeCommand(sql.replace('{file_name}', loadFile), [isUpdate: true])
		source.readRows = count
		dest.writeRows = count
		dest.updateRows = count
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
		def rows = sqlConnect.rows("SELECT SESSION_ID() AS session_id;")
		if (!rows.isEmpty()) res = rows[0].session_id.toString()
		
		return res
	}

    @Override
    protected String getChangeSessionPropertyQuery() { return 'SET {name} {value}' }

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

		if (field.type == Field.timestamp_with_timezoneFieldType) {
			field.getMethod = 'org.h2.value.ValueTimestampTimeZone.fromDateValueAndNanos(({field} as org.h2.api.TimestampWithTimeZone).YMD, ({field} as org.h2.api.TimestampWithTimeZone).nanosSinceMidnight, ({field} as org.h2.api.TimestampWithTimeZone).timeZoneOffsetSeconds).getTimestamp(TimeZone.default)'
			return
		}

		if (field.typeName != null) {
			if (field.typeName.matches("(?i)UUID")) {
				field.type = Field.Type.UUID
				field.dbType = java.sql.Types.VARCHAR
				field.length = 36
				field.precision = null
				return
			}

			if (field.typeName.matches("(?i)BLOB")) {
				field.type = Field.Type.BLOB
				field.dbType = java.sql.Types.BLOB
				field.precision = null
				return
			}

			if (field.typeName.matches("(?i)CLOB")) {
				field.type = Field.Type.TEXT
				field.dbType = java.sql.Types.CLOB
				field.precision = null
//				return
			}
		}
	}

	@Override
	void prepareCsvTempFile(Dataset source, CSVDataset csvFile) {
		super.prepareCsvTempFile(source, csvFile)
		csvFile.escaped = false
	}

	@Override
	void validCsvTempFile(Dataset source, CSVDataset csvFile) {
		super.validCsvTempFile(source, csvFile)
		if (!(csvFile.codePage().toLowerCase() in ['utf-8', 'utf8']))
			throw new ExceptionGETL('The file must be encoded in utf-8 for batch download!')
		if (csvFile.isHeader())
			throw new ExceptionGETL('It is not allowed to use the header in the file for bulk load!')
	}
}