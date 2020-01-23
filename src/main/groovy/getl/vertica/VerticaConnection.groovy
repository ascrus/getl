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

package getl.vertica

import getl.exception.ExceptionGETL
import getl.jdbc.QueryDataset
import getl.utils.DateUtils
import getl.utils.Path
import getl.utils.StringUtils
import groovy.transform.InheritConstructors
import getl.jdbc.JDBCConnection

/**
 * Vertica connection class
 * @author Alexsey Konstantinov
 *
 */
@InheritConstructors
class VerticaConnection extends JDBCConnection {
	VerticaConnection () {
		super(driver: VerticaDriver)
	}
	
	VerticaConnection (Map params) {
		super(new HashMap([driver: VerticaDriver]) + params?:[:])
		if (this.getClass().name == 'getl.vertica.VerticaConnection') methodParams.validation("Super", params?:[:])
	}

	/** Current Vertica connection driver */
	VerticaDriver getCurrentVerticaDriver() { driver as VerticaDriver }
	
	@Override
	protected void onLoadConfig (Map configSection) {
		super.onLoadConfig(configSection)
		if (this.getClass().name == 'getl.vertica.VerticaConnection') methodParams.validation("Super", params)
	}
	
	@Override
	protected void doInitConnection () {
		super.doInitConnection()
		driverName = 'com.vertica.jdbc.Driver'
	}

	/**
	 * Current session parameters
	 */
	Map<String, Object> getCurrentSession() {
		def query = new QueryDataset(connection: this, query: 'SELECT * FROM current_session')
		return query.rows()[0]
	}

	/**
	 * Drop specified interval partitions in table
	 * @param fullTableName schema and table name
	 * @param startPartition start of the partition interval
	 * @param finishPartition  enf of the partitions inrerval
	 * @param isSplit force split ros containers
	 * @param useDatePartitions partition are the dates
	 * @return function result
	 */
	Map dropPartitions(String fullTableName, def startPartition, def finishPartition,
					   boolean isSplit = false, boolean useDatePartitions = false) {
		if (fullTableName == null)
			throw new ExceptionGETL('Required value for parameter "fullTableName"!')
		if (startPartition == null)
			throw new ExceptionGETL('Required value for parameter "startPartition"!')
		if (finishPartition == null)
			throw new ExceptionGETL('Required value for parameter "finishPartition"!')

		if (startPartition instanceof String || startPartition instanceof GString)
			startPartition = '\'' + startPartition + '\''
		else if (startPartition instanceof Date) {
			if (useDatePartitions)
				startPartition = '\'' + DateUtils.FormatDate('yyyy-MM-dd', startPartition as Date) + '\'::timestamp'
			else
				startPartition = '\'' + DateUtils.FormatDate('yyyy-MM-dd HH:mm:ss', startPartition as Date) + '\'::date'
		}

		if (finishPartition instanceof String || finishPartition instanceof GString)
			finishPartition = '\'' + finishPartition + '\''
		else if (finishPartition instanceof Date) {
			if (useDatePartitions)
				finishPartition = '\'' + DateUtils.FormatDate('yyyy-MM-dd', finishPartition as Date) + '\'::timestamp'
			else
				finishPartition = '\'' + DateUtils.FormatDate('yyyy-MM-dd HH:mm:ss', finishPartition as Date) + '\'::date'
		}

		def qry = new QueryDataset()
		qry.with {
			useConnection this
			query = "SELECT DROP_PARTITIONS('{table}', {start}, {finish}, {split})"
			queryParams.table = fullTableName
			queryParams.start = startPartition
			queryParams.finish = finishPartition
			queryParams.split = isSplit
		}

		return qry.rows()[0] as Map
	}

	final def attachedVertica = [] as List<String>

	static protected String DatabaseFromConnection(VerticaConnection con) {
		def database
		if (con.connectURL != null) //noinspection DuplicatedCode
		{
			def p = new Path(mask: 'jdbc:vertica://{host}/{database}')
			def m = p.analize(con.connectURL)

			database = m.database as String
			if (database == null)
				throw new ExceptionGETL("Invalid connect URL, database unreachable!")
			def i = database.indexOf('?')
			if (i != -1) {
				database = database.substring(0, i - 1)
			}
		}
		else {
			database = con.connectDatabase
			if (database == null)
				throw new ExceptionGETL("No database is specified for the connection!")
		}

		return database
	}

	/**
	 * Create connections to another Vertica cluster for the current session
	 * @param anotherConnection connecting to another Vertica
	 */
	void attachExternalVertica(VerticaConnection anotherConnection) {
		if (anotherConnection.login == null)
			throw new ExceptionGETL("No login is specified for the connection!")
		if (anotherConnection.password == null)
			throw new ExceptionGETL("No password is specified for the connection!")

		def database, host, port = 5433
		if (anotherConnection.connectURL != null) {
			def p = new Path(mask: 'jdbc:vertica://{host}/{database}')
			def m = p.analize(anotherConnection.connectURL)
			host = m.host as String
			if (host == null)
				throw new ExceptionGETL("Invalid connect URL, host unreachable!")
			def i = host.indexOf(':')
			if (i != -1) {
				port = Integer.valueOf(host.substring(i + 1))
				host = host.substring(0, i)
			}

			database = m.database as String
			if (database == null)
				throw new ExceptionGETL("Invalid connect URL, database unreachable!")
			i = database.indexOf('?')
			if (i != -1) {
				database = database.substring(0, i - 1)
			}
		}
		else {
			host = anotherConnection.connectHost
			if (host == null)
				throw new ExceptionGETL("No host is specified for the connection!")
			port = anotherConnection.connectPortNumber?:5433
			database = anotherConnection.connectDatabase
			if (database == null)
				throw new ExceptionGETL("No database is specified for the connection!")
		}

		if (database.toLowerCase() in attachedVertica)
			throw new ExceptionGETL("The Vertica cluster with database \"$database\" is already attached to the current connection!")

		executeCommand("CONNECT TO VERTICA {database} USER {login} PASSWORD '{password}' ON '{host}',{port}",
				[host: host, port: port, database: database,
				 login: anotherConnection.login, password: anotherConnection.password])

		attachedVertica << database.toLowerCase()
	}

	void detachExternalVertica(VerticaConnection anotherConnection) {
		def database = DatabaseFromConnection(anotherConnection)
		if (!(database.toLowerCase() in attachedVertica))
			throw new ExceptionGETL("The Vertica cluster with database \"$database\" is already attached to the current connection!")

		executeCommand("DISCONNECT $database")
	}
}