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

package getl.mysql

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

		tablePrefix = '`'
		fieldPrefix = '`'

        localTemporaryTablePrefix = 'LOCAL TEMPORARY'
	}
	
	@Override
	public List<Driver.Support> supported() {
		return super.supported() +
				[Driver.Support.LOCAL_TEMPORARY,
				 Driver.Support.SEQUENCE, Driver.Support.BLOB, Driver.Support.CLOB, Driver.Support.INDEX]
	}
	
	@Override
	public List<Driver.Operation> operations() {
        return super.operations() +
                [Driver.Operation.CLEAR, Driver.Operation.DROP, Driver.Operation.EXECUTE, Driver.Operation.CREATE]
	}
	
	@Override
	protected Map getConnectProperty() {
		return [zeroDateTimeBehavior: 'convertToNull']
	}

	@Override
	public String defaultConnectURL () {
		return 'jdbc:mysql://{host}/{database}'
	}

	@Override
	protected String getChangeSessionPropertyQuery() { return 'SET {name} = {value}' }
}
