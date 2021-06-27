package getl.transform

import getl.data.Connection
import getl.data.VirtualDataset
import getl.exception.ExceptionGETL
import groovy.transform.InheritConstructors

/**
 * Aggregation dataset class
 * @author Alexsey Konstantinov
 *
 */
@InheritConstructors
class AggregatorDataset extends VirtualDataset {
	@Override
	protected void initParams() {
		super.initParams()

		connection = new Connection([driver: AggregatorDatasetDriver])
		params.fieldByGroup = [] as List<String>
		params.fieldCalc = [:] as Map<String, Map>
		params.algorithm = "HASH"
	}

    List<String> getFieldByGroup () { params.fieldByGroup as List<String> }

    void setFieldByGroup (List<String> value) { params.fieldByGroup = value }

    Map<String, Map> getFieldCalc () { params.fieldCalc as Map<String, Map> }

    void setFieldCalc (Map<String, Map> value) { params.fieldCalc = value }

    String getAlgorithm () { params.algorithm }

    void setAlgorithm (String value) {
		value = value.toUpperCase()
		if (!(value in ["HASH", "TREE"])) throw new ExceptionGETL("Unknown algorithm \"${value}\"") 
		params.algorithm = value 
	}
}
