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

import getl.utils.BoolUtils
import groovy.transform.InheritConstructors
import getl.jdbc.JDBCConnection

/**
 * MySQL connection class
 * @author Alexsey Konstantinov
 *
 */
class MySQLConnection extends JDBCConnection {
	MySQLConnection() {
		super(driver: MySQLDriver)
	}
	
	MySQLConnection(Map params) {
		super(new HashMap([driver: MySQLDriver]) + params?:[:])
		if (this.getClass().name == 'getl.mysql.MySQLConnection') methodParams.validation("Super", params?:[:])
	}

	/** Current MySQL connection driver */
	MySQLDriver getCurrentMySQLDriver() { driver as MySQLDriver }

	@Override
	protected void registerParameters () {
		super.registerParameters()
		methodParams.register('Super', ['usedOldDriver'])
	}
	
	@Override
	protected void onLoadConfig (Map configSection) {
		super.onLoadConfig(configSection)
		if (this.getClass().name == 'getl.mysql.MySQLConnection') methodParams.validation("Super", params)
	}

	/** Enable if a driver under version 6 is used. */
	Boolean getUsedOldDriver() { params.usedOldDriver as Boolean }
	/** Enable if a driver under version 6 is used. */
	void setUsedOldDriver(Boolean value) { params.usedOldDriver = value }
	
	@Override
	protected void doInitConnection () {
		super.doInitConnection()
		driverName = (!BoolUtils.IsValue(usedOldDriver))?'com.mysql.cj.jdbc.Driver':'com.mysql.jdbc.Driver'
	}
}