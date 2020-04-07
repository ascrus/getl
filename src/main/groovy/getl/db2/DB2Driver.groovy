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

package getl.db2

import getl.data.Field
import getl.driver.Driver
import getl.jdbc.JDBCDriver
import groovy.transform.InheritConstructors


/**
 * MSSQL driver class
 * @author Alexsey Konstantinov
 *
 */
class DB2Driver extends JDBCDriver {
	DB2Driver () {
		super()
		
		caseObjectName = 'UPPER'
		connectionParamBegin = ':'
		connectionParamJoin = ';'
		connectionParamFinish = ';'
	}

	@SuppressWarnings("UnnecessaryQualifiedReference")
	@Override
	List<Driver.Support> supported() {
		return super.supported() +
				[Driver.Support.GLOBAL_TEMPORARY, Driver.Support.SEQUENCE, Driver.Support.BLOB, Driver.Support.CLOB,
				 Driver.Support.INDEX, Driver.Support.TIME, Driver.Support.DATE, /*Driver.Support.TIMESTAMP_WITH_TIMEZONE,*/
				 Driver.Support.BOOLEAN]
	}

	@SuppressWarnings("UnnecessaryQualifiedReference")
	@Override
	List<Driver.Operation> operations() {
		return super.operations() +
                [Driver.Operation.CLEAR, Driver.Operation.DROP, Driver.Operation.EXECUTE, Driver.Operation.CREATE]
	}

	@Override
    String defaultConnectURL () {
		return "jdbc:db2://{host}/{database}"
	}
	
	@Override
    void prepareField (Field field) {
		super.prepareField(field)
		
		if (field.typeName?.matches('(?i)CLOB')) {
			field.type= Field.Type.STRING
			field.getMethod = '{field}.getSubString(1, (int){field}.length())'
		}
		else if (field.typeName?.matches('(?i)XML')) {
			field.type= Field.Type.STRING
			field.getMethod = '{field}.getString()'
		} 
	}

	@Override
	protected String getChangeSessionPropertyQuery() { return 'SET {name} = {value}' }

	/** Next value sequence sql script */
	@Override
	protected String sqlSequenceNext(String sequenceName) { "SELECT NEXT VALUE FOR ${sequenceName} AS id" }
}