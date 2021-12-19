//file:noinspection SqlNoDataSourceInspection
package getl.h2

import getl.jdbc.sub.BulkLoadMapping
import groovy.transform.InheritConstructors
import getl.csv.*
import getl.data.*
import getl.exception.ExceptionGETL
import getl.jdbc.*
import getl.utils.*
import static getl.driver.Driver.Support
import static getl.driver.Driver.Operation

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
		methodParams.register('dropSchema', ['cascade'])
	}

	@Override
	protected void initParams() {
		super.initParams()

		commitDDL = false
		allowExpressions = true
		caseObjectName = "UPPER"
		caseQuotedName = true
		defaultSchemaName = "PUBLIC"
		connectionParamBegin = ";"
		connectionParamJoin = ";"

		/* TODO: changed read fields from local temp tables */
//		supportLocalTemporaryRetrieveFields = false

		sqlExpressions.ddlAutoIncrement = 'AUTO_INCREMENT'
		sqlExpressions.ddlDropSchema = 'DROP SCHEMA{ %ifExists%} {schema}{%cascade%}'
	}

	@Override
	List<Support> supported() {
		return super.supported() +
				[Support.GLOBAL_TEMPORARY, Support.LOCAL_TEMPORARY, Support.MEMORY,
				 Support.SEQUENCE, Support.BLOB, Support.CLOB, Support.INDEX,
				 Support.UUID, Support.TIME, Support.DATE, Support.TIMESTAMP_WITH_TIMEZONE,
				 Support.BOOLEAN, Support.DROPIFEXIST, Support.CREATEIFNOTEXIST,
				 Support.CREATESCHEMAIFNOTEXIST, Support.DROPSCHEMAIFEXIST, Support.AUTO_INCREMENT
				 /*,Driver.Support.ARRAY*/]
		/* TODO: H2 ARRAY NOT FULL */
	}

	@SuppressWarnings("UnnecessaryQualifiedReference")
	@Override
	List<Operation> operations() {
		return super.operations() + [Operation.BULKLOAD, Operation.MERGE]
	}

	@Override
	Boolean timestamptzReadAsTimestamp() { return true }

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
		if (params.compressed != null)
			throw new ExceptionGETL("H2 bulk load dont support compression files")

		params = bulkLoadFilePrepare(source, dest as JDBCDataset, params, prepareCode)
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

			Field destField = dest.fieldByName(rule.destinationFieldName)
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
		fParams << "charset=${source.codePage()}".toString()
		fParams << "fieldSeparator=${source.fieldDelimiter()}".toString()
		if (source.quoteStr() != null)
			fParams << "fieldDelimiter=${StringUtils.EscapeJava(source.quoteStr())}".toString()
		def functionParams = fParams.join(" ")

		sb <<
"""INSERT INTO ${fullNameDataset(dest)} (
  $tableFields
)
SELECT 
  $csvFields 
FROM CSVREAD('{file_name}', ${heads}, '${functionParams}')
"""
        def sql = sb.toString()
		//println sb.toString()

        dest.writeRows = 0
		dest.updateRows = 0
		def loadFile = source.fullFileName()
		def count = executeCommand(sql.replace('{file_name}', FileUtils.ConvertToUnixPath(loadFile)), [isUpdate: true])
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
		def rows = sqlConnect.rows("SELECT SESSION_ID() AS session_id")
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

	@Override
	String type2sqlType(Field field, Boolean useNativeDBType) {
		if (field.type != Field.arrayFieldType || (useNativeDBType && field.typeName != null))
			return super.type2sqlType(field, useNativeDBType)

		if (field.arrayType == null)
			throw new ExceptionGETL("It is required to specify the type of the array in \"arrayType\" for field \"${field.name}\"!")

		return "${field.arrayType} array" + (((field.length?:0) > 0)?"[${field.length}]":'')
	}

	@Override
	protected Map<String, Object> dropSchemaParams(String schemaName, Map<String, Object> dropParams) {
		def res = super.dropSchemaParams(schemaName, dropParams)
		if (BoolUtils.IsValue(dropParams.cascade))
			res.cascade = 'CASCADE'

		return res
	}
}