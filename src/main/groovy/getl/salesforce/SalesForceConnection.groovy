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

package getl.salesforce

import getl.data.Connection
import getl.exception.ExceptionGETL
import getl.lang.sub.UserLogins
import groovy.transform.InheritConstructors

/**
 * SalesForce Connection class
 * @author Dmitry Shaldin
 */
class SalesForceConnection extends Connection implements UserLogins {
    SalesForceConnection() {
		super(driver: SalesForceDriver)
	}

	SalesForceConnection(Map params) {
		super(new HashMap([driver: SalesForceDriver]) + params?:[:])

		if (this.getClass().name == 'getl.salesforce.SalesForceConnection') {
			methodParams.validation("Super", params?:[:])
		}
	}

	/** Current SalesForce connection driver */
	SalesForceDriver getCurrentSalesForceDriver() { driver as SalesForceDriver }

	@Override
	void initParams() {
		super.initParams()
		params.storedLogins = [:] as Map<String, String>
	}

	@Override
	protected void registerParameters() {
		super.registerParameters()
		methodParams.register('Super', ['login', 'password', 'connectURL', 'batchSize'])
	}

	@Override
	protected void onLoadConfig(Map configSection) {
		super.onLoadConfig(configSection)

		if (this.getClass().name == 'getl.salesforce.SalesForceConnection') {
			methodParams.validation('Super', params)
		}
	}

	/** SalesForce login */
	@Override
	String getLogin() { params.login }
	/** SalesForce login */
	@Override
    void setLogin(String value) { params.login = value }

	/** SalesForce password and token */
	@Override
	String getPassword() { params.password }
	/** SalesForce password and token */
	@Override
    void setPassword(String value) { params.password = value }

	@Override
	Map<String, String> getStoredLogins() { params.storedLogins as Map<String, String> }
	@Override
	void setStoredLogins(Map<String, String> value) {
		storedLogins.clear()
		if (value != null) storedLogins.putAll(value)
	}

	/**
	 * SalesForce SOAP Auth Endpoint
	 * Example: https://login.salesforce.com/services/Soap/u/40.0
	 */
	String getConnectURL() { params.connectURL }
	/**
	 * SalesForce SOAP Auth Endpoint
	 * <br>Example: https://login.salesforce.com/services/Soap/u/40.0
	 */
    void setConnectURL(String value) { params.connectURL = value }

	/**
	 * Batch Size for SalesForce connection
     * <br>This param do nothing for readAsBulk.
	 */
	int getBatchSize() { (params.batchSize as int)?:200 }
	/**
	 * Batch Size for SalesForce connection
	 * <br>This param do nothing for readAsBulk.
	 */
    void setBatchSize(int value) { params.batchSize = value }

	@Override
	void useLogin(String user) {
		if (!storedLogins.containsKey(user))
			throw new ExceptionGETL("User \"$user\" not found in in configuration!")

		def pwd = storedLogins.get(user)

		if (login != user && connected) connected = false
		login = user
		password = pwd
	}
}