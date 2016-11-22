/*
 GETL - based package in Groovy, which automates the work of loading and transforming data. His name is an acronym for "Groovy ETL".

 GETL is a set of libraries of pre-built classes and objects that can be used to solve problems unpacking,
 transform and load data into programs written in Groovy, or Java, as well as from any software that supports
 the work with Java classes.
 
 Copyright (C) 2013-2016  Alexsey Konstantonov (ASCRUS)

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

import getl.data.Field;
import getl.driver.Driver
import getl.jdbc.JDBCDriver
import groovy.transform.InheritConstructors


/**
 * MSSQL driver class
 * @author Alexsey Konstantinov
 *
 */
@InheritConstructors
class DB2Driver extends JDBCDriver {
	DB2Driver () {
		super()
		
		caseObjectName = 'UPPER'
		connectionParamBegin = ':'
		connectionParamJoin = ';'
		connectionParamFinish = ';'
	}
	
	@Override
	public List<Driver.Operation> operations() {
		List<Driver.Operation> result = super.operations()
		result << Driver.Operation.CREATE
		result
	}
	
	@Override
	public List<Driver.Support> supported() {
		List<Driver.Support> result = super.supported()
		result << Driver.Support.INDEX
		result
	}
	
	@Override
	public String defaultConnectURL () {
		"jdbc:db2://{host}/{database}"
	}
	
	@Override
    public
    void prepareField (Field field) {
		super.prepareField(field)
		
		if (field.typeName?.matches('(?i)CLOB')) {
			field.type= Field.Type.STRING
			field.getMethod = "(({field} != null)?{field}.getSubString(1, (int){field}.length()):null)"
		}
		else if (field.typeName?.matches('(?i)XML')) {
			field.type= Field.Type.STRING
			field.getMethod = "(({field} != null)?{field}.getString():null)"
		} 
	}
}