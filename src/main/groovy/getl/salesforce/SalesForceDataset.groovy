package getl.salesforce

import getl.data.Connection
import getl.data.Dataset
import groovy.transform.InheritConstructors

/**
 * SalesForce Dataset class
 * @author Dmitry Shaldin
 */
@InheritConstructors
class SalesForceDataset extends Dataset {
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
}
