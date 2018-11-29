package getl.salesforce

import groovy.transform.InheritConstructors

@InheritConstructors
class SalesForceQueryDataset extends SalesForceDataset {
	/**
	 * SOQL query text
	 * @return
	 */
	String getQuery () { params.query }
	void setQuery (String value) { params.query = value }
}
