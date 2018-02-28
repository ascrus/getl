/*
 GETL - based package in Groovy, which automates the work of loading and transforming data. His name is an acronym for "Groovy ETL".

 GETL is a set of libraries of pre-built classes and objects that can be used to solve problems unpacking,
 transform and load data into programs written in Groovy, or Java, as well as from any software that supports
 the work with Java classes.
 
 Copyright (C) 2013-2017  Alexsey Konstantonov (ASCRUS)

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

import groovy.transform.InheritConstructors
import getl.data.Dataset
import getl.data.Field
import getl.driver.Driver
import getl.jdbc.JDBCDriver
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

		methodParams.register("eachRow", ["scn", "timestamp", "hints", "usePartition"])
	}
	
	@Override
	public List<Driver.Support> supported() {
		return super.supported() +
				[Driver.Support.GLOBAL_TEMPORARY, Driver.Support.SEQUENCE, Driver.Support.BLOB,
				 Driver.Support.CLOB, Driver.Support.INDEX]
	}
	
	@Override
	public List<Driver.Operation> operations() {
        return super.operations() +
                [Driver.Operation.CLEAR, Driver.Operation.DROP, Driver.Operation.EXECUTE, Driver.Operation.CREATE]
	}
	
	@Override
	public String blobMethodWrite (String methodName) {
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
	public boolean blobReadAsObject () { return false }
	
	@Override
	public String textMethodWrite (String methodName) {
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
	
	@Override
	public Map getSqlType () {
		Map res = super.getSqlType()
		res.BIGINT.name = 'number'
		res.BIGINT.useLength = JDBCDriver.sqlTypeUse.NEVER
		res.BLOB.name = 'raw'
		res.TEXT.useLength = JDBCDriver.sqlTypeUse.NEVER

		return res
	}
	
	@Override
	public void sqlTableDirective (Dataset dataset, Map params, Map dir) {
		if (params."scn" != null) {
			Long scn
			if (params."scn" instanceof String) scn = ConvertUtils.Object2Long(params."scn") else scn = params."scn"
			dir."afteralias" = "AS OF SCN $scn"
		}
		else if (params."timestamp" != null) {
			Date timestamp 
			if (params."timestamp" instanceof String) timestamp = DateUtils.ParseDate("yyyy-MM-dd HH:mm:ss", params."timestamp") else timestamp = params."timestamp"
			def ts = DateUtils.FormatDate("yyyy-MM-dd HH:mm:ss.sss", timestamp)
			dir."afteralias" = "AS OF TIMESTAMP TO_TIMESTAMP('$ts', 'YYYY-MM-DD HH24:MI:SS.FF')"
		}
		
		if (params."hints" != null) {
			dir."afterselect" = "/*+ ${params."hints"} */"
		}
		
		if (params."usePartition" != null) {
			dir."aftertable" = "PARTITION (${params."usePartition"})"
		}
	}
	
	@Override
    public void prepareField (Field field) {
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
			if (field.typeName.matches("(?i)TIMESTAMP[(]\\d+[)]") || 
					field.typeName.matches("(?i)TIMESTAMP")) {
				field.type = Field.Type.DATETIME
				field.getMethod = "new java.sql.Timestamp(({field} as oracle.sql.TIMESTAMP).timestampValue().getTime())"
				return
			}
			
			if (field.typeName.matches("(?i)TIMESTAMP[(]\\d+[)] WITH TIME ZONE") ||
					field.typeName.matches("(?i)TIMESTAMP WITH TIME ZONE")) {
				field.type = Field.Type.DATETIME
				field.getMethod = "new java.sql.Timestamp(({field} as oracle.sql.TIMESTAMP).timestampValue(connection).getTime())"
				return
			}
			
			if (field.typeName.matches("(?i)TIMESTAMP[(]\\d+[)] WITH LOCAL TIME ZONE") ||
					field.typeName.matches("(?i)TIMESTAMP WITH LOCAL TIME ZONE")) {
				field.type = Field.Type.DATETIME
				field.getMethod = "new java.sql.Timestamp(({field} as oracle.sql.TIMESTAMP).timestampValue(connection, Calendar.getInstance()).getTime())"
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
				return
			}
		}
	}
	
	@Override
	public String defaultConnectURL () {
		return 'jdbc:oracle:thin:@{host}:{database}'
	}

	@Override
	protected String getChangeSessionPropertyQuery() { return 'ALTER SESSION SET {name} = \'{value}\'' }

	@Override
	public String generateColumnDefinition(Field f, boolean useNativeDBType) {
		return "${prepareFieldNameForSQL(f.name)} ${type2sqlType(f, useNativeDBType)}" +
				((isSupport(Driver.Support.DEFAULT_VALUE) && f.defaultValue != null)?" DEFAULT ${f.defaultValue}":"") +
				((isSupport(Driver.Support.PRIMARY_KEY) && !f.isNull)?" NOT NULL":"") +
				((isSupport(Driver.Support.COMPUTE_FIELD) && f.compute != null)?" COMPUTED BY ${f.compute}":"")
	}

	@Override
	public String getSysDualTable() { return 'DUAL' }

	@Override
	protected String sessionID() {
		String res = null
		def rows = sqlConnect.rows("select sys_context('userenv','sid') as session_id from dual")
		if (!rows.isEmpty()) res = rows[0].session_id.toString()

		return res
	}
}
