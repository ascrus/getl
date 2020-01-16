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
				startPartition = '\'' + DateUtils.FormatDate('yyyy-MM-dd', startPartition) + '\'::timestamp'
			else
				startPartition = '\'' + DateUtils.FormatDateTime('yyyy-MM-dd HH:mm:ss', startPartition) + '\'::date'
		}

		if (finishPartition instanceof String || finishPartition instanceof GString)
			finishPartition = '\'' + finishPartition + '\''
		else if (finishPartition instanceof Date) {
			if (useDatePartitions)
				finishPartition = '\'' + DateUtils.FormatDate('yyyy-MM-dd', finishPartition) + '\'::timestamp'
			else
				finishPartition = '\'' + DateUtils.FormatDateTime('yyyy-MM-dd HH:mm:ss', finishPartition) + '\'::date'
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
}