package getl.salesforce

import getl.data.Connection
import getl.data.Dataset
import getl.exception.ExceptionGETL
import groovy.transform.InheritConstructors

/**
 * SalesForce Dataset class
 * @author Dmitry Shaldin
 */
@InheritConstructors
class SalesForceDataset extends Dataset {
	SalesForceDataset() {
		super()

		methodParams.register('bulkUnload', ['pathToFile', 'limit', 'where'])
	}

	@Override
	void setConnection(Connection value) {
		assert value == null || value instanceof SalesForceConnection
		super.setConnection(value)
	}

	/**
	 * SalesForce object name
	 * @return
	 */
	String getSfObjectName () { params.sfObjectName }
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
	 * <li>String fileName		- Path to file, where data should be placed
	 * <li>Integer limit        - Limit of loaded rows
	 * <li>String where         - Where condition for query
	 * </ul>
	 * @param params	- dynamic parameters
	 */
	void bulkUnload(Map params) {
		if (!(params.fileName)) throw new ExceptionGETL('Parameter \'fileName\' is missing.')
		(connection.driver as SalesForceDriver).bulkUnload(this, params)
	}
}
