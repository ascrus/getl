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
import getl.data.Dataset
import getl.exception.ExceptionGETL
import getl.tfs.TFSDataset
import groovy.transform.InheritConstructors

/**
 * SalesForce Dataset class
 * @author Dmitry Shaldin
 */
class SalesForceDataset extends Dataset {
	SalesForceDataset() {
		super()

		methodParams.register('bulkUnload', ['limit', 'where', 'orderBy', 'chunkSize'])
		methodParams.register('rows', ['limit', 'where', 'readAsBulk', 'orderBy', 'chunkSize'])
		methodParams.register('eachRow', ['limit', 'where', 'readAsBulk', 'orderBy', 'chunkSize'])
	}

	@Override
    void setConnection(Connection value) {
		if (value != null && !(value instanceof SalesForceConnection))
			throw new ExceptionGETL('Сonnection to SalesForceConnection class is allowed!')

		super.setConnection(value)
	}

	/** Use specified connection */
	SalesForceConnection useConnection(SalesForceConnection value) {
		setConnection(value)
		return value
	}

	/** Current SalesForce connection*/
	SalesForceConnection getCurrentSalesForceConnection() { connection as SalesForceConnection }

	/** SalesForce object name */
	String getSfObjectName () { params.sfObjectName }
	/** SalesForce object name */
	void setSfObjectName (final String value) { params.sfObjectName = value }

	@Override
	String getObjectName() {
		return sfObjectName
	}

	@Override
	String getObjectFullName() {
		return sfObjectName
	}

	/**
	 * Get data from SalesForce via bulk api
	 * <p><b>Parameters:</b><p>
	 * <ul>
	 * <li>Integer limit        - Limit of loaded rows
	 * <li>String where         - Where condition for query
	 * </ul>
	 * @param params	- dynamic parameters
	 */
	List<TFSDataset> bulkUnload(Map params) {
		return (connection.driver as SalesForceDriver).bulkUnload(this, params)
	}
}