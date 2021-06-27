package getl.transform

import getl.data.Connection
import getl.data.VirtualDataset
import groovy.transform.InheritConstructors

/**
 * Sorted dataset class
 * @author Alexsey Konstantinov
 *
 */
@InheritConstructors
class SorterDataset extends VirtualDataset {
	@Override
	protected void initParams() {
		super.initParams()

		connection = new Connection([driver: SorterDatasetDriver])
		params.fieldOrderBy = [] as List<String>
	}

	/** List of sort column */
    List<String> getFieldOrderBy () { params.fieldOrderBy as List<String> }
    void setFieldOrderBy (List<String> value) { params.fieldOrderBy = value }
}
