package getl.transform

import com.fasterxml.jackson.annotation.JsonIgnore
import getl.data.Connection
import getl.data.Dataset

/**
 * Multiple dataset writer class
 * @author Alexsey Konstantinov
 *
 */
class MultipleDataset extends Dataset {
	MultipleDataset () {
		super()
		connection = new Connection([driver: MultipleDatasetDriver])
	}

	@Override
	protected void initParams() {
		super.initParams()
		params.dest = [:] as Map<String, Dataset>
		params.condition = [:] as Map<String, Closure>
	}
	
	/**
	 * Destination datasets (alias:dataset)
	 * @return
	 */
	Map<String, Dataset> getDest () { params.dest as Map<String, Dataset> }

    void setDest (Map<String, Dataset> value) { params.dest = value }
	
	/**
	 * Conditions for filter rows to datasets (alias:condition)
	 * @return
	 */
	Map<String, Closure> getCondition () { params.condition as Map<String, Closure> }

    void setCondition (Map<String, Closure> value) { params.condition = value }
	
	@Override
    List<String> excludeSaveParams () {
		super.excludeSaveParams() + ["dest", "condition"]
	}
	
	@Override
	@JsonIgnore
    String getObjectName() { (dest != null)?dest.keySet().toList().join(', '):null }
}