package getl.oracle

import getl.data.Dataset
import getl.exception.ExceptionGETL
import getl.jdbc.*
import groovy.transform.InheritConstructors
import getl.data.Field
import getl.driver.Driver
import getl.utils.*
import java.sql.Blob

/**
 * Oracle driver class
 * @author Alexsey Konstantinov
 *
 */
@InheritConstructors
class OracleDriver extends JDBCDriver {
	@Override
	protected void registerParameters() {
		super.registerParameters()

		methodParams.register("eachRow", ["scn", "timestamp", "hints", "usePartition"])
	}

	@Override
	protected void initParams() {
		super.initParams()

		caseObjectName = 'UPPER'
		caseRetrieveObject = 'UPPER'
		commitDDL = true
		transactionalDDL = true

		ruleNameNotQuote = '(?i)^[a-z]+[a-z0-9_]*$'
		ruleEscapedText.put('\'', '\'\'')
		ruleEscapedText.remove('\n')
		ruleEscapedText.remove('\\')

		sqlExpressions.convertTextToTimestamp = 'TO_TIMESTAMP(\'{value}\', \'yyyy-mm-dd hh24:mi:ss.FF\')'
		sqlExpressions.now = 'LOCALTIMESTAMP'
		sqlExpressions.sequenceNext = 'SELECT {value}.nextval id FROM dual'
		sqlExpressions.sysDualTable = 'DUAL'
		sqlExpressions.changeSessionProperty = 'ALTER SESSION SET {name} = \'{value}\''
		sqlExpressions.ddlCreateSequence = '''declare count_seq number; if_not_exists varchar(20) := '{%ifNotExists%}';
begin
    if (if_not_exists = 'IF NOT EXISTS') then
		select count(*) into count_seq from user_sequences where Upper(sequence_name)  = Upper('{name}');
		if (count_seq = 0) then
			execute immediate 'CREATE SEQUENCE {name}{ INCREMENT BY %increment%}{ MINVALUE %min%}{ MAXVALUE %max%}{ START WITH %start%}{ CACHE %cache%}{%CYCLE%}';
		end if;
	else
		execute immediate 'CREATE SEQUENCE {name}{ INCREMENT BY %increment%}{ MINVALUE %min%}{ MAXVALUE %max%}{ START WITH %start%}{ CACHE %cache%}{%CYCLE%}';
	end if;
end;'''
		sqlExpressions.ddlDropSequence = '''declare count_seq number; if_exists varchar(20) := '{%ifExists%}';
begin
    if (if_exists = 'IF EXISTS') then
		select count(*) into count_seq from user_sequences where Upper(sequence_name)  = Upper('{name}');
		if (count_seq = 1) then
			execute immediate 'DROP SEQUENCE {name}';
		end if;
	else
		execute immediate 'DROP SEQUENCE {name}';
	end if;
end;'''
	}

	@SuppressWarnings("UnnecessaryQualifiedReference")
	@Override
	List<Driver.Support> supported() {
		return super.supported() +
				[Support.GLOBAL_TEMPORARY, Support.SEQUENCE, Support.BLOB, Support.CLOB, Support.INDEX, Support.INDEXFORTEMPTABLE,
				 Support.TIMESTAMP_WITH_TIMEZONE, Support.START_TRANSACTION] -
				[Support.SELECT_WITHOUT_FROM, Support.BOOLEAN]
	}

	@SuppressWarnings("UnnecessaryQualifiedReference")
	@Override
	List<Driver.Operation> operations() {
        return super.operations() - [Driver.Operation.CREATE_SCHEMA]
	}

	@Override
	Boolean timestamptzReadAsTimestamp() { return true }

	/** Current Oracle connection */
	@SuppressWarnings('unused')
	OracleConnection getCurrentOracleConnection() { connection as OracleConnection }

	@Override
	void connect() {
		System.properties.setProperty('oracle.jdbc.fanEnabled', 'false')
		super.connect()
		defaultSchemaName = jdbcConnection.login
	}
	
	@Override
	String blobMethodWrite(String methodName) {
		return """void $methodName (java.sql.Connection con, java.sql.PreparedStatement stat, Integer paramNum, byte[] value) {
	if (value == null) { 
		stat.setNull(paramNum, java.sql.Types.BLOB) 
	}
	else {
		oracle.sql.BLOB blob = con.createBlob() as oracle.sql.BLOB
		def stream = blob.getBinaryOutputStream()
		try {
		  stream.write(value)
		}
		finally {
		  stream.close()
		}
		stat.setBlob(paramNum, blob)
	}
}"""
    }
	
	@Override
	String textMethodWrite(String methodName) {
		return """void $methodName (java.sql.Connection con, java.sql.PreparedStatement stat, Integer paramNum, String value) {
	if (value == null) { 
		stat.setNull(paramNum, java.sql.Types.CLOB) 
	}
	else {
		def clob = con.createClob()
		clob.setString(1, value)
		stat.setClob(paramNum, clob)
	} 
}"""
	}

	@SuppressWarnings("UnnecessaryQualifiedReference")
	@Override
	Map<String, Map<String, Object>> getSqlType() {
		def res = super.getSqlType()
		res.INTEGER.name = 'number(10)'
		res.BIGINT.name = 'number(19)'
		res.BLOB.useLength = JDBCDriver.sqlTypeUse.NEVER
		res.TEXT.useLength = JDBCDriver.sqlTypeUse.NEVER

		return res
	}
	
	@Override
	void sqlTableDirective(JDBCDataset dataset, Map params, Map dir) {
		super.sqlTableDirective(dataset, params, dir)
		Map<String, Object> dl = ((dataset as TableDataset).readDirective?:new HashMap<String, Object>()) + (params as Map<String, Object>)
		if (dl.scn != null) {
			Long scn
			if (dl.scn instanceof String)
				scn = ConvertUtils.Object2Long(dl.scn)
			else
				scn = dl.scn as Long
			dir.afteralias = "AS OF SCN $scn"
		}
		else if (dl.timestamp != null) {
			Date timestamp 
			def ts = convertDateTime2String(dl.timestamp)?:dl.timestamp.toString()
			dir.afteralias = "AS OF TIMESTAMP TO_TIMESTAMP('$ts', 'YYYY-MM-DD HH24:MI:SS.FF')"
		}
		
		if (dl.hints != null) {
			dir.afterselect = "/*+ ${dl.hints} */"
		}
		
		if (dl.usePartition != null) {
			dir.aftertable = "PARTITION (${dl.usePartition})"
		}
	}

	@SuppressWarnings("UnnecessaryQualifiedReference")
	@Override
	void prepareField(Field field) {
		super.prepareField(field)

		if (field.type == Field.numericFieldType) {
			if (field.columnClassName == 'java.lang.Double') {
				field.type = Field.doubleFieldType
				field.length = null
				field.precision = null
			}
			if (field.length == 0 && field.precision == -127) {
				field.length = 38
				field.precision = 6
			}
			else if (field.length == 19 && field.precision == 0) {
				field.type = Field.bigintFieldType
				field.length = null
				field.precision = null
			}
			else if (field.length == 10 && field.precision == 0) {
				field.type = Field.integerFieldType
				field.length = null
				field.precision = null
			}
		}

		if (field.typeName != null) {
			if (field.typeName.matches("(?i)DATE")) {
				field.type = Field.Type.DATETIME
				field.dbType = java.sql.Types.TIMESTAMP
				return
			}

			if (field.typeName.matches("(?i)TIMESTAMP[(]\\d+[)]") || 
					field.typeName.matches("(?i)TIMESTAMP")) {
				field.type = Field.Type.DATETIME
				field.dbType = java.sql.Types.TIMESTAMP
				return
			}
			
			if (field.typeName.matches("(?i)TIMESTAMP[(]\\d+[)] WITH TIME ZONE") ||
					field.typeName.matches("(?i)TIMESTAMP WITH TIME ZONE")) {
				field.type = Field.Type.TIMESTAMP_WITH_TIMEZONE
				field.dbType = java.sql.Types.TIMESTAMP_WITH_TIMEZONE
				return
			}
			
			if (field.typeName.matches("(?i)TIMESTAMP[(]\\d+[)] WITH LOCAL TIME ZONE") ||
					field.typeName.matches("(?i)TIMESTAMP WITH LOCAL TIME ZONE")) {
				field.type = Field.Type.TIMESTAMP_WITH_TIMEZONE
				field.dbType = java.sql.Types.TIMESTAMP_WITH_TIMEZONE
				return
			}
			
			if (field.typeName.matches("(?i)NCHAR")) {
				field.type = Field.Type.STRING
				field.dbType = java.sql.Types.NVARCHAR
				return
			}
			
			if (field.typeName.matches("(?i)NVARCHAR2")) {
				field.type = Field.Type.STRING
				field.dbType = java.sql.Types.NVARCHAR
				return
			}
			
			if (field.typeName.matches("(?i)LONG")) {
				field.type = Field.Type.STRING
				field.dbType = java.sql.Types.LONGVARCHAR
				return
			}
			
			if (field.typeName.matches("(?i)BINARY_FLOAT") || field.typeName.matches("(?i)BINARY_DOUBLE")) {
				field.type = Field.Type.DOUBLE
				field.dbType = java.sql.Types.DOUBLE
				return
			}
			
			if (field.typeName.matches("(?i)NCLOB")) {
				field.type = Field.Type.TEXT
				field.dbType = java.sql.Types.NCLOB
//				return
			}
		}
	}

	@Override
	String prepareReadField(Field field) {
		if (field.type == Field.integerFieldType)
			return '({field} as Number).toInteger()'

		if (field.type == Field.bigintFieldType)
			return '({field} as Number).toLong()'

		if (field.type == Field.doubleFieldType)
			return '({field} as Number).toDouble()'

		if (field.type == Field.blobFieldType) {
			if (field.columnClassName == 'oracle.sql.BLOB')
				return '({field} as oracle.sql.BLOB).getBytes((long)1, (int)(({field} as oracle.sql.BLOB).length()))'
			else if (field.columnClassName == 'oracle.jdbc.OracleBlob')
				return '({field} as oracle.jdbc.OracleBlob).getBytes((long)1, (int)(({field} as oracle.jdbc.OracleBlob).length()))'
			else {
				def classBlob = Class.forName(field.columnClassName)
				if (Blob.isAssignableFrom(classBlob))
					return '({field} as java.sql.Blob).getBytes((long)1, (int)(({field} as java.sql.Blob).length()))'
			}
		}

		if (field.type == Field.Type.ROWID)
			return 'new String({field}.bytes)'

		if (field.type in [Field.Type.DATE, Field.Type.DATETIME, Field.Type.TIMESTAMP_WITH_TIMEZONE] &&
				!(field.columnClassName in ['java.sql.Date', 'java.sql.Time', 'java.sql.Timestamp']) && field.typeName != null) {

			if (field.typeName.matches("(?i)DATE"))
				return '({field} as oracle.sql.DATE).timestampValue()'

			if (field.typeName.matches("(?i)TIMESTAMP[(]\\d+[)]") ||
					field.typeName.matches("(?i)TIMESTAMP"))
				return '({field} as oracle.sql.TIMESTAMP).timestampValue()'

			if (field.typeName.matches("(?i)TIMESTAMP[(]\\d+[)] WITH TIME ZONE") ||
					field.typeName.matches("(?i)TIMESTAMP WITH TIME ZONE"))
				return '({field} as oracle.sql.TIMESTAMPTZ).timestampValue(connection)'

			if (field.typeName.matches("(?i)TIMESTAMP[(]\\d+[)] WITH LOCAL TIME ZONE") ||
					field.typeName.matches("(?i)TIMESTAMP WITH LOCAL TIME ZONE"))
				//noinspection SpellCheckingInspection
				return '({field} as oracle.sql.TIMESTAMPLTZ).timestampValue(connection)'
		}

		return null
	}
	
	@Override
	String defaultConnectURL() {
		return 'jdbc:oracle:thin:@{host}:{database}'
	}

	@SuppressWarnings("UnnecessaryQualifiedReference")
	@Override
	String generateColumnDefinition(Field f, Boolean useNativeDBType) {
		return "${prepareFieldNameForSQL(f.name)} ${type2sqlType(f, useNativeDBType)}" +
				((isSupport(Driver.Support.DEFAULT_VALUE) && f.defaultValue != null)?" ${generateDefaultDefinition(f)}":"") +
				((isSupport(Driver.Support.PRIMARY_KEY) && !f.isNull)?" NOT NULL":"") +
				((isSupport(Driver.Support.COMPUTE_FIELD) && f.compute != null)?" ${generateComputeDefinition(f)}":"")
	}

	@Override
	String generateComputeDefinition(Field f) {
		return "COMPUTED BY ${f.compute}"
	}

	@SuppressWarnings(['SpellCheckingInspection', 'SqlNoDataSourceInspection'])
	@Override
	protected String sessionID() {
		String res = null
		def rows = sqlConnect.rows("select sys_context('userenv','sid') as session_id from dual")
		if (!rows.isEmpty()) res = rows[0].session_id.toString()

		return res
	}

    @Override
	protected String buildConnectURL() {
        JDBCConnection con = jdbcConnection

        def url = (con.connectURL != null)?con.connectURL:defaultConnectURL()
        if (url == null)
			return null

        if (url.indexOf('{host}') != -1) {
            if (con.connectHost == null)
				throw new ExceptionGETL('Need set property "connectHost"')
            def host = (con.connectHost.indexOf(':') == -1)?(con.connectHost + ':1521'):con.connectHost
            url = url.replace("{host}", host)
        }
        if (url.indexOf('{database}') != -1) {
            if (con.connectDatabase == null)
				throw new ExceptionGETL('Need set property "connectDatabase"')
            url = url.replace("{database}", con.connectDatabase)
        }

        return url
	}

	@Override
	List<Field> prepareImportFields(Dataset dataset, Map importParams = new HashMap()) {
		def res = super.prepareImportFields(dataset, importParams)
		if (!(dataset instanceof OracleTable)) {
			res.each { field ->
				if (field.type in [Field.dateFieldType, Field.timeFieldType])
					field.type = Field.datetimeFieldType
			}
		}

		return res
	}

	@Override
	Boolean blobReadAsObject(Field field = null) { return false }
}