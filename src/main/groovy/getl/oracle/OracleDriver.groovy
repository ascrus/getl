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

package getl.oracle

import getl.exception.ExceptionGETL
import getl.jdbc.*
import groovy.transform.InheritConstructors
import getl.data.Dataset
import getl.data.Field
import getl.driver.Driver
import getl.utils.*


/**
 * Oracle driver class
 * @author Alexsey Konstantinov
 *
 */
@InheritConstructors
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
				 Driver.Support.CLOB, Driver.Support.INDEX, Driver.Support.DATE, Driver.Support.TIMESTAMP_WITH_TIMEZONE]
	}

	@SuppressWarnings("UnnecessaryQualifiedReference")
	@Override
	List<Driver.Operation> operations() {
        return super.operations() +
                [Driver.Operation.CLEAR, Driver.Operation.DROP, Driver.Operation.EXECUTE, Driver.Operation.CREATE]
	}
	
	@Override
	String blobMethodWrite (String methodName) {
		return """void $methodName (java.sql.Connection con, java.sql.PreparedStatement stat, int paramNum, byte[] value) {
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
	boolean blobReadAsObject () { return false }
	
	@Override
	String textMethodWrite (String methodName) {
		return """void $methodName (java.sql.Connection con, java.sql.PreparedStatement stat, int paramNum, String value) {
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
	void sqlTableDirective (Dataset dataset, Map params, Map dir) {
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
				field.type = Field.Type.DATE
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
				field.getMethod = "new java.sql.Timestamp(({field} as oracle.sql.TIMESTAMPLTZ).timestampValue(connection, Calendar.instance).getTime())"
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
	String generateColumnDefinition(Field f, boolean useNativeDBType) {
		return "${prepareFieldNameForSQL(f.name)} ${type2sqlType(f, useNativeDBType)}" +
				((isSupport(Driver.Support.DEFAULT_VALUE) && f.defaultValue != null)?" DEFAULT ${f.defaultValue}":"") +
				((isSupport(Driver.Support.PRIMARY_KEY) && !f.isNull)?" NOT NULL":"") +
				((isSupport(Driver.Support.COMPUTE_FIELD) && f.compute != null)?" COMPUTED BY ${f.compute}":"")
	}

	@Override
	String getSysDualTable() { return 'DUAL' }

	@Override
	protected String sessionID() {
		String res = null
		def rows = sqlConnect.rows("select sys_context('userenv','sid') as session_id from dual")
		if (!rows.isEmpty()) res = rows[0].session_id.toString()

		return res
	}

    @Override
	protected String buildConnectURL () {
        JDBCConnection con = connection as JDBCConnection

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
}
