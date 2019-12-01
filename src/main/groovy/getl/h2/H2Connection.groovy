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

package getl.h2

import groovy.transform.InheritConstructors

import getl.jdbc.JDBCDriver
import getl.driver.Driver
import getl.jdbc.JDBCConnection
import getl.utils.*

/**
 * H2 connection class
 * @author Alexsey Konstantinov
 *
 */
@InheritConstructors
class H2Connection extends JDBCConnection {
	H2Connection() {
		super(driver: H2Driver)
		if (connectProperty.LOCK_TIMEOUT == null) connectProperty.LOCK_TIMEOUT = 10000
		connectProperty.CASE_INSENSITIVE_IDENTIFIERS = true
		connectProperty.ALIAS_COLUMN_NAME = true
	}
	
	H2Connection(Map params) {
		super(new HashMap([driver: H2Driver]) + params?:[:])
		if (connectProperty.LOCK_TIMEOUT == null) connectProperty.LOCK_TIMEOUT = 10000
		connectProperty.CASE_INSENSITIVE_IDENTIFIERS = true
		connectProperty.ALIAS_COLUMN_NAME = true
		if (this.getClass().name == 'getl.h2.H2Connection') methodParams.validation('Super', params?:[:])
	}

	/** Current H2 connection driver */
	H2Driver getCurrentH2Driver() { driver as H2Driver }
	
	@Override
	protected void registerParameters () {
		super.registerParameters()
		methodParams.register('Super', ['inMemory', 'exclusive'])
	}
	
	@Override
	protected void onLoadConfig (Map configSection) {
		super.onLoadConfig(configSection)
		if (this.getClass().name == 'getl.h2.H2Connection') methodParams.validation('Super', params)
	}
	
	@Override
	protected void doInitConnection () {
		super.doInitConnection()
		driverName = 'org.h2.Driver'
	}
	
	/** Enabled "in-memory" mode */
	Boolean getInMemory () { BoolUtils.IsValue(params.inMemory, false) }
	/** Enabled "in-memory" mode */
	void setInMemory (Boolean value) { params.inMemory = value }

    /** Exclusive connection */
	Integer getExclusive() { sessionProperty.exclusive as Integer }
	/** Exclusive connection */
	void setExclusive(Integer value) {
        if (connected && exclusive != value) (driver as JDBCDriver).changeSessionProperty('exclusive', value)
        sessionProperty.exclusive = value
    }
}