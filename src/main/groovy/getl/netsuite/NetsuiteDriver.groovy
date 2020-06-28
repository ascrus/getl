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

package getl.netsuite

import getl.driver.Driver
import getl.exception.ExceptionGETL
import getl.jdbc.JDBCDriver
import groovy.transform.InheritConstructors

/**
 * Netsuite driver class
 * @author Dmitry Shalind
 *
 */
class NetsuiteDriver extends JDBCDriver {
	NetsuiteDriver() {
		super()
		
		methodParams.unregister('eachRow', ['onlyFields', 'excludeFields', 'where', 'order', 'offset',
											'queryParams', 'sqlParams', 'fetchSize', 'forUpdate', 'filter'])

		connectionParamBegin = ';'
		connectionParamJoin = ';'
		defaultSchemaName = 'Administrator'
		fieldPrefix = '['
		fieldEndPrefix = ']'
		tablePrefix = '['
		tableEndPrefix = ']'
	}

	@SuppressWarnings("UnnecessaryQualifiedReference")
	@Override
	List<Driver.Support> supported() {
		return super.supported() + [
				Driver.Support.TIME, Driver.Support.DATE, Driver.Support.BOOLEAN/*,
				Driver.Support.BLOB, Driver.Support.CLOB*/
		]
	}

	/*
	@Override
	public List<Driver.Operation> operations() {
		return super.operations()
	}
	*/

	@SuppressWarnings("UnnecessaryQualifiedReference")
	@Override
	Map getSqlType () {
		Map res = super.getSqlType()
		res.DOUBLE.name = 'float'
		res.BOOLEAN.name = 'bit'
		res.BLOB.name = 'varbinary'
		res.BLOB.useLength = JDBCDriver.sqlTypeUse.ALWAYS
		res.TEXT.name = 'varchar'
		res.TEXT.useLength = JDBCDriver.sqlTypeUse.ALWAYS
		res.DATETIME.name = 'datetime'

		return res
	}

	@Override
	String defaultConnectURL () {
		return 'jdbc:ns://{host};ServerDataSource={serverDataSource};' +
                'encrypted=1;Ciphersuites={ciphersuites};' +
                'CustomProperties=(AccountID={accountId};RoleID=3)'
	}

	/**
	 * Build jdbc connection url
	 * @return
	 */
	@Override
    protected String buildConnectURL () {
		def url = super.buildConnectURL()
		if (url == null) return null

		NetsuiteConnection con = connection as NetsuiteConnection

		if (url.indexOf('serverDataSource') != -1) {
			if (con.serverDataSource == null) throw new ExceptionGETL('Need set property "serverDataSource"')
			url = url.replace("{serverDataSource}", con.serverDataSource)
		}

		if (url.indexOf('ciphersuites') != -1) {
			if (con.ciphersuites == null) throw new ExceptionGETL('Need set property "ciphersuites"')
			url = url.replace("{ciphersuites}", con.ciphersuites)
		}

		if (url.indexOf('accountId') != -1) {
			if (con.accountId == null) throw new ExceptionGETL('Need set property "accountId"')
			url = url.replace("{accountId}", String.valueOf(con.accountId))
		}

		return url
	}

	@Override
	String getNowFunc() { 'GETDATE()' }
}