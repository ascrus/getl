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

package getl.netsuite

import getl.jdbc.JDBCConnection
import groovy.transform.InheritConstructors

/**
 * Netsuite connection class
 * @author Dmitry Shaldin
 *
 */
@InheritConstructors
class NetsuiteConnection extends JDBCConnection {
	NetsuiteConnection() {
		super(driver: NetsuiteDriver)
	}

	NetsuiteConnection(Map params) {
		super(new HashMap([driver: NetsuiteDriver]) + params)
	}

	@Override
	protected void registerParameters () {
		super.registerParameters()
		methodParams.register('Super', ['serverDataSource', 'ciphersuites', 'accountId'])
	}
	
	@Override
	protected void onLoadConfig (Map configSection) {
		super.onLoadConfig(configSection)

		if (this.getClass().name == 'getl.netsuite.NetsuiteConnection') methodParams.validation('Super', params)
	}
	
	@Override
	protected void doInitConnection () {
		super.doInitConnection()
		driverName = 'com.netsuite.jdbc.openaccess.OpenAccessDriver'
	}

	/**
	 * Server Data Source
	 */
	public String getServerDataSource () { params.serverDataSource }
	public void setServerDataSource (String value) { params.serverDataSource = value }

	/**
	 * Ciphersuites
	 */
	public String getCiphersuites () { params.ciphersuites }
	public void setCiphersuites (String value) { params.ciphersuites = value }

	/**
	 * Account ID
	 */
	public Integer getAccountId () { params.accountId }
	public void setAccountId (Integer value) { params.accountId = value }
}