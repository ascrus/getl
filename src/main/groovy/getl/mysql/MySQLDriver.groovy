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

package getl.mysql

import getl.data.Field
import getl.driver.Driver
import getl.jdbc.*
import getl.utils.ListUtils
import groovy.transform.InheritConstructors

/**
 * MySQL driver class
 * @author Alexsey Konstantinov
 *
 */
@InheritConstructors
class MySQLDriver extends JDBCDriver {
	MySQLDriver () {
		super()

		connectionParamBegin = '?'
		connectionParamJoin = '&'

		tablePrefix = '`'
		fieldPrefix = '`'

        localTemporaryTablePrefix = 'TEMPORARY'
		supportLocalTemporaryRetrieveFields = false
	}

	@SuppressWarnings("UnnecessaryQualifiedReference")
	@Override
	List<Driver.Support> supported() {
		return super.supported() +
				[Driver.Support.LOCAL_TEMPORARY, Driver.Support.BLOB, Driver.Support.CLOB,
				 Driver.Support.INDEX, Driver.Support.TIME, Driver.Support.DATE, Driver.Support.BOOLEAN,
				 Driver.Support.CREATEIFNOTEXIST, Driver.Support.DROPIFEXIST]
	}

	@SuppressWarnings("UnnecessaryQualifiedReference")
	@Override
	List<Driver.Operation> operations() {
        return super.operations() +
                [Driver.Operation.CLEAR, Driver.Operation.DROP, Driver.Operation.EXECUTE, Driver.Operation.CREATE]
	}

	@Override
	protected Map getConnectProperty() {
		return [zeroDateTimeBehavior: 'convertToNull', useServerPrepStmts: false, rewriteBatchedStatements: true,
				serverTimezone: 'UTC']
	}

	@Override
	String defaultConnectURL () {
		return 'jdbc:mysql://{host}/{database}'
	}

	@Override
	protected String getChangeSessionPropertyQuery() { return 'SET {name} = {value}' }

	@SuppressWarnings("UnnecessaryQualifiedReference")
	@Override
	Map getSqlType () {
		Map res = super.getSqlType()
		res.BLOB.name = 'blob'
		res.BLOB.useLength = JDBCDriver.sqlTypeUse.NEVER
		res.TEXT.name = 'text'
		res.TEXT.useLength = JDBCDriver.sqlTypeUse.NEVER

		return res
	}

	@Override
	boolean blobReadAsObject () { return false }

	@Override
	String blobMethodWrite (String methodName) {
		return """void $methodName (java.sql.Connection con, java.sql.PreparedStatement stat, int paramNum, byte[] value) {
	if (value == null) { 
		stat.setNull(paramNum, java.sql.Types.BLOB) 
	}
	else {
		def stream = new ByteArrayInputStream(value)
		stat.setBinaryStream(paramNum, stream, value.length)
		stream.close()
	}
}"""
	}

	@Override
	boolean textReadAsObject () { return false }

	@Override
	String textMethodWrite (String methodName) {
		return """void $methodName (java.sql.Connection con, java.sql.PreparedStatement stat, int paramNum, String value) {
	if (value == null) { 
		stat.setNull(paramNum, java.sql.Types.CLOB) 
	}
	else {
		stat.setString(paramNum, value)
	} 
}"""
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

		if (field.typeName != null) {
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
	String getSysDualTable() { return 'DUAL' }

	@Override
	protected String sessionID() {
		String res = null
		def rows = sqlConnect.rows('SELECT connection_id() as session_id')
		if (!rows.isEmpty()) res = rows[0].session_id.toString()

		return res
	}

	@Override
	protected Map<String, String> prepareForRetrieveFields(TableDataset dataset) {
		def names = super.prepareForRetrieveFields(dataset)
		names.dbName = prepareObjectName(ListUtils.NotNullValue([dataset.dbName, (dataset.connection as MySQLConnection).connectDatabase, defaultDBName]) as String)
		return names
	}
}