package getl.oracle

import getl.exception.ExceptionGETL
import getl.jdbc.*
import getl.jdbc.opts.SequenceCreateSpec
import groovy.transform.InheritConstructors
import getl.data.Dataset
import getl.data.Field
import getl.driver.Driver
import getl.utils.*
import groovy.transform.Synchronized


/**
 * Oracle driver class
 * @author Alexsey Konstantinov
 *
 */
class OracleDriver extends JDBCDriver {
	OracleDriver () {
		super()
		caseObjectName = 'UPPER'
		commitDDL = true
		transactionalDDL = true

		methodParams.register("eachRow", ["scn", "timestamp", "hints", "usePartition"])
	}

	@SuppressWarnings("UnnecessaryQualifiedReference")
	@Override
	List<Driver.Support> supported() {
		return super.supported() +
				[Driver.Support.GLOBAL_TEMPORARY, Driver.Support.SEQUENCE, Driver.Support.BLOB,
				 Driver.Support.CLOB, Driver.Support.INDEX, /*Driver.Support.DATE, */Driver.Support.TIMESTAMP_WITH_TIMEZONE]
	}

	@SuppressWarnings("UnnecessaryQualifiedReference")
	@Override
	List<Driver.Operation> operations() {
        return super.operations() +
                [Driver.Operation.TRUNCATE, Driver.Operation.DROP, Driver.Operation.EXECUTE,
				 Driver.Operation.CREATE]
	}

	@Override
	void connect() {
		System.properties.setProperty('oracle.jdbc.fanEnabled', 'false')
		super.connect()
	}
	
	@Override
	String blobMethodWrite (String methodName) {
		return """void $methodName (java.sql.Connection con, java.sql.PreparedStatement stat, Integer paramNum, byte[] value) {
	if (value == null) { 
		stat.setNull(paramNum, java.sql.Types.BLOB) 
	}
	else {
		oracle.sql.BLOB blob = con.createBlob() as oracle.sql.BLOB
		def stream = blob.getBinaryOutputStream()
		stream.write(value)
		stream.close()
		stat.setBlob(paramNum, /*new javax.sql.rowset.serial.SerialBlob(blob)*/blob)
	}
}"""
    }
	
	@Override
	Boolean blobReadAsObject () { return false }
	
	@Override
	String textMethodWrite (String methodName) {
		return """void $methodName (java.sql.Connection con, java.sql.PreparedStatement stat, Integer paramNum, String value) {
	if (value == null) { 
		stat.setNull(paramNum, java.sql.Types.CLOB) 
	}
	else {
		def clob = con.createClob()
		clob.setString(1, value)
		stat.setClob(paramNum, /*new javax.sql.rowset.serial.SerialClob(clob)*/clob)
	} 
}"""
	}

	@SuppressWarnings("UnnecessaryQualifiedReference")
	@Override
	Map getSqlType () {
		Map res = super.getSqlType()
		res.BIGINT.name = 'number'
		res.BIGINT.useLength = JDBCDriver.sqlTypeUse.NEVER
		res.BLOB.name = 'raw'
		res.TEXT.useLength = JDBCDriver.sqlTypeUse.NEVER

		return res
	}
	
	@Override
	void sqlTableDirective (JDBCDataset dataset, Map params, Map dir) {
		super.sqlTableDirective(dataset, params, dir)
		Map<String, Object> dl = ((dataset as TableDataset).readDirective?:[:]) + (params as Map<String, Object>)
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
			if (dl.timestamp instanceof String)
				timestamp = DateUtils.ParseDate("yyyy-MM-dd HH:mm:ss", dl.timestamp)
			else
				timestamp = dl.timestamp as Date
			def ts = DateUtils.FormatDate("yyyy-MM-dd HH:mm:ss.sss", timestamp)
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
	void prepareField (Field field) {
		super.prepareField(field)
		
		if (field.type == Field.Type.NUMERIC) {
			if (field.length == 0 && field.precision == -127) {
				field.length = 38
				field.precision = 6
			}
			return
		}
		
		if (field.type == Field.Type.ROWID) {
			field.getMethod = "new String({field}.bytes)"
			return
		}
		
		if (field.typeName != null) {
			if (field.typeName.matches("(?i)DATE")) {
				field.type = Field.Type.DATETIME
//				field.getMethod = "new java.sql.Timestamp(({field} as oracle.sql.DATE).timestampValue().getTime())"
				return
			}

			if (field.typeName.matches("(?i)TIMESTAMP[(]\\d+[)]") || 
					field.typeName.matches("(?i)TIMESTAMP")) {
				field.type = Field.Type.DATETIME
				field.getMethod = "new java.sql.Timestamp(({field} as oracle.sql.TIMESTAMP).timestampValue().getTime())"
				return
			}
			
			if (field.typeName.matches("(?i)TIMESTAMP[(]\\d+[)] WITH TIME ZONE") ||
					field.typeName.matches("(?i)TIMESTAMP WITH TIME ZONE")) {
				field.type = Field.Type.TIMESTAMP_WITH_TIMEZONE
				field.getMethod = "new java.sql.Timestamp(({field} as oracle.sql.TIMESTAMPTZ).timestampValue(connection).getTime())"
				return
			}
			
			if (field.typeName.matches("(?i)TIMESTAMP[(]\\d+[)] WITH LOCAL TIME ZONE") ||
					field.typeName.matches("(?i)TIMESTAMP WITH LOCAL TIME ZONE")) {
				field.type = Field.Type.TIMESTAMP_WITH_TIMEZONE
				field.getMethod = "new java.sql.Timestamp(({field} as oracle.sql.TIMESTAMPTZ).timestampValue(connection, Calendar.instance).getTime())"
				return
			}
			
			if (field.typeName.matches("(?i)NCHAR")) {
				field.type = Field.Type.STRING
				return
			}
			
			if (field.typeName.matches("(?i)NVARCHAR2")) {
				field.type = Field.Type.STRING
				return
			}
			
			if (field.typeName.matches("(?i)LONG")) {
				field.type = Field.Type.STRING
				return
			}
			
			if (field.typeName.matches("(?i)BINARY_FLOAT") || field.typeName.matches("(?i)BINARY_DOUBLE")) {
				field.type = Field.Type.DOUBLE
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
	String defaultConnectURL () {
		return 'jdbc:oracle:thin:@{host}:{database}'
	}

	@Override
	protected String getChangeSessionPropertyQuery() { return 'ALTER SESSION SET {name} = \'{value}\'' }

	@SuppressWarnings("UnnecessaryQualifiedReference")
	@Override
	String generateColumnDefinition(Field f, Boolean useNativeDBType) {
		return "${prepareFieldNameForSQL(f.name)} ${type2sqlType(f, useNativeDBType)}" +
				((isSupport(Driver.Support.DEFAULT_VALUE) && f.defaultValue != null)?" DEFAULT ${f.defaultValue}":"") +
				((isSupport(Driver.Support.PRIMARY_KEY) && !f.isNull)?" NOT NULL":"") +
				((isSupport(Driver.Support.COMPUTE_FIELD) && f.compute != null)?" COMPUTED BY ${f.compute}":"")
	}

	@Override
	String getSysDualTable() { return 'DUAL' }

	@SuppressWarnings('SpellCheckingInspection')
	@Override
	protected String sessionID() {
		String res = null
		def rows = sqlConnect.rows("select sys_context('userenv','sid') as session_id from dual")
		if (!rows.isEmpty()) res = rows[0].session_id.toString()

		return res
	}

    @Override
	protected String buildConnectURL () {
        JDBCConnection con = jdbcConnection

        def url = (con.connectURL != null)?con.connectURL:defaultConnectURL()
        if (url == null) return null

        if (url.indexOf('{host}') != -1) {
            if (con.connectHost == null) throw new ExceptionGETL('Need set property "connectHost"')
            def host = (con.connectHost.indexOf(':') == -1)?(con.connectHost + ':1521'):con.connectHost
            url = url.replace("{host}", host)
        }
        if (url.indexOf('{database}') != -1) {
            if (con.connectDatabase == null) throw new ExceptionGETL('Need set property "connectDatabase"')
            url = url.replace("{database}", con.connectDatabase)
        }

        return url
	}

	/** Next value sequence sql script */
	@Override
	protected String sqlSequenceNext(String sequenceName) { "SELECT ${sequenceName}.nextval id FROM dual" }

	@Synchronized
	@Override
	protected void dropSequence(String name, Boolean ifExists) {
		if (ifExists) {
			def sql = """begin
	for x in (select sequence_name from user_sequences where Upper(sequence_name)  = '${name.toUpperCase()}')
	loop
		execute immediate 'drop sequence ' || x.sequence_name;
	end loop;
end;
	"""
			executeCommand(sql)
		}
		else {
			super.dropSequence(name, ifExists)
		}
	}

	@Synchronized
	@Override
	protected void createSequence(String name, Boolean ifNotExists, SequenceCreateSpec opts) {
		if (ifNotExists) {
			def attrs = createSequenceAttrs(opts).join(' ')
			def sql = """declare count_seq number;
begin
	select count(*) into count_seq from user_sequences where Upper(sequence_name)  = '${name.toUpperCase()}';
	if (count_seq = 0) then
		execute immediate 'create sequence $name $attrs';
	end if;
end;
	"""
			executeCommand(sql)
		}
		else {
			super.createSequence(name, ifNotExists, opts)
		}
	}

	@Override
	String getNowFunc() { 'LOCALTIMESTAMP' }
}