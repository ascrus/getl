package getl.salesforce

import com.fasterxml.jackson.annotation.JsonIgnore
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
			throw new ExceptionGETL('Only class SalesForceConnection connections are permitted!')

		super.setConnection(value)
	}

	/** Use specified SalesForce connection */
	SalesForceConnection useConnection(SalesForceConnection value) {
		setConnection(value)
		return value
	}

	/** Current SalesForce connection*/
	@JsonIgnore
	SalesForceConnection getCurrentSalesForceConnection() { connection as SalesForceConnection }

	/** SalesForce object name */
	String getSfObjectName () { params.sfObjectName }
	/** SalesForce object name */
	void setSfObjectName (final String value) { params.sfObjectName = value }

	@Override
	@JsonIgnore
	String getObjectName() {
		return sfObjectName
	}

	@Override
	@JsonIgnore
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