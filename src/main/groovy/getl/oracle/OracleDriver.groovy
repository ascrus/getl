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
		sqlType.BIGINT.name = 'number'

		methodParams.register("eachRow", ["scn", "timestamp", "hints", "usePartition"])
	}
	
	@Override
	public List<Driver.Support> supported() {
		return super.supported() +
				[Driver.Support.GLOBAL_TEMPORARY,
                 Driver.Support.SEQUENCE, Driver.Support.BLOB, Driver.Support.CLOB, Driver.Support.INDEX]
	}
	
	@Override
	public List<Driver.Operation> operations() {
        return super.operations() +
                [Driver.Operation.CLEAR, Driver.Operation.DROP, Driver.Operation.EXECUTE, Driver.Operation.CREATE,
                 Driver.Operation.BULKLOAD]
	}
	
	@Override
	public String blobClosureWrite () {
        return
            '{ byte[] value -> def blob = _getl_con.createBlob(); def stream = blob.getBinaryOutputStream(); stream.write(value); stream.close(); blob }'
    }
	
	@Override
	public boolean blobReadAsObject () { return false }
	
	@Override
	public String clobClosureWrite () { return '{ String value -> def clob = _getl_con.createClob(); clob.setString(1, value); clob }' }
	
	@Override
	public Map getSqlType () {
		Map res = super.getSqlType()
		res.BLOB.name = 'raw'
		res.TEXT.useLength = JDBCDriver.sqlTypeUse.NEVER
		
		return res
	}
	
	/**
	 * Prepare object name by prefix
	 * @param name
	 * @param prefix
	 * @return
	 */
	@Override
	public String prepareObjectNameWithPrefix(String name, String prefix, String prefixEnd = null) {
		if (name == null) return null
		
		String res
		
		def m = name =~ /([^a-zA-Z0-9_])/
		if (m.size() > 0) res = prefix + name + prefixEnd?:prefix else res = prefix + name.toUpperCase() + (prefixEnd?:prefix)
		
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
			field.getMethod = "(({field} != null)?new String({field}.bytes):null)"
			return
		}
		
		if (field.typeName != null) {
			if (field.typeName.matches("(?i)TIMESTAMP[(]\\d+[)]") || 
					field.typeName.matches("(?i)TIMESTAMP")) {
				field.type = Field.Type.DATETIME
				field.getMethod = "(({field} != null)?new java.sql.Timestamp({field}.timestampValue().getTime()):null)"
				return
			}
			
			if (field.typeName.matches("(?i)TIMESTAMP[(]\\d+[)] WITH TIME ZONE") ||
					field.typeName.matches("(?i)TIMESTAMP WITH TIME ZONE")) {
				field.type = Field.Type.DATETIME
				field.getMethod = "(({field} != null)?new java.sql.Timestamp({field}.timestampValue(connection).getTime()):null)"
				return
			}
			
			if (field.typeName.matches("(?i)TIMESTAMP[(]\\d+[)] WITH LOCAL TIME ZONE") ||
					field.typeName.matches("(?i)TIMESTAMP WITH LOCAL TIME ZONE")) {
				field.type = Field.Type.DATETIME
				field.getMethod = "(({field} != null)?new java.sql.Timestamp({field}.timestampValue(connection, Calendar.getInstance()).getTime()):null)"
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
}
