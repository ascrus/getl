package getl.transform

import getl.data.Connection
import getl.data.VirtualDataset

/**
 * Sorted dataset class
 * @author Alexsey Konstantinov
 *
 */
class SorterDataset extends VirtualDataset {
	SorterDataset () {
		super()
		connection = new Connection([driver: SorterDatasetDriver])
	}

	@Override
	protected void initParams() {
		super.initParams()
		params.fieldOrderBy = [] as List<String>
	}

	/** List of sort column */
    List<String> getFieldOrderBy () { params.fieldOrderBy as List<String> }
    void setFieldOrderBy (List<String> value) { params.fieldOrderBy = value }
}
