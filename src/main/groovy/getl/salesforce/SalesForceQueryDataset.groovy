package getl.salesforce

import groovy.transform.InheritConstructors

/**
 * SalesForce query dataset class
 * @author Dmitry Shaldin
 */
@InheritConstructors
class SalesForceQueryDataset extends SalesForceDataset {
	/** SOQL query text */
	String getQuery () { params.query }
	/** SOQL query text */
	void setQuery (String value) { params.query = value }
}