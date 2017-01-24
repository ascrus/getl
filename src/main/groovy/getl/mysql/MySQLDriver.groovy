/*
 GETL - based package in Groovy, which automates the work of loading and transforming data. His name is an acronym for "Groovy ETL".

 GETL is a set of libraries of pre-built classes and objects that can be used to solve problems unpacking,
 transform and load data into programs written in Groovy, or Java, as well as from any software that supports
 the work with Java classes.
 
 Copyright (C) 2013-2015  Alexsey Konstantonov (ASCRUS)

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

import java.sql.PreparedStatement
import getl.driver.Driver
import getl.jdbc.JDBCDriver
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
	}
	
	@Override
	public List<Driver.Support> supported() {
		List<Driver.Support> result = super.supported()
        result << Driver.Support.BLOB
        result << Driver.Support.CLOB
		result << Driver.Support.TEMPORARY
		result << Driver.Support.INDEX
		return result
	}
	
	@Override
	public List<Driver.Operation> operations() {
		List<Driver.Operation> result = super.operations()
		result << Driver.Operation.BULKLOAD
		result << Driver.Operation.CREATE
		return result
	}
	
	@Override
	protected Map getConnectProperty() {
		return [zeroDateTimeBehavior: 'convertToNull']
	}
	
	@Override
	public String getTablePrefix () {
		return '`'
	}
	
	@Override
	public String getFieldPrefix () {
		return '`'
	}
	
	@Override
	public String defaultConnectURL () {
		return 'jdbc:mysql://{host}/{database}'
	}

	@Override
	protected String getChangeSessionPropertyQuery() { return 'SET {name} = {value}' }
}
