package getl.transform

import com.fasterxml.jackson.annotation.JsonIgnore
import getl.data.Connection
import getl.data.Dataset
import groovy.transform.InheritConstructors

/**
 * Multiple dataset writer class
 * @author Alexsey Konstantinov
 *
 */
@InheritConstructors
class MultipleDataset extends Dataset {
	@Override
	protected void initParams() {
		super.initParams()

		connection = new Connection([driver: MultipleDatasetDriver])
		params.dest = new HashMap<String, Dataset>()
		params.condition = new HashMap<String, Closure>()
	}
	
	/** Destination datasets (alias:dataset) */
	Map<String, Dataset> getDest () { params.dest as Map<String, Dataset> }
	/** Destination datasets (alias:dataset) */
    void setDest (Map<String, Dataset> value) { params.dest = value }
	
	/** Conditions for filter rows to datasets (alias:condition) */
	Map<String, Closure> getCondition () { params.condition as Map<String, Closure> }
	/** Conditions for filter rows to datasets (alias:condition) */
    void setCondition (Map<String, Closure> value) { params.condition = value }
	
	@Override
    List<String> excludeSaveParams () {
		super.excludeSaveParams() + ["dest", "condition"]
	}
	
	@Override
	@JsonIgnore
    String getObjectName() { (dest != null)?dest.keySet().toList().join(', '):null }
}