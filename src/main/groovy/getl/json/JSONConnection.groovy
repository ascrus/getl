package getl.json

import com.fasterxml.jackson.annotation.JsonIgnore
import getl.data.Dataset
import getl.data.FileConnection
import groovy.transform.InheritConstructors

/**
 * JSON connection class
 * @author Alexsey Konstantinov
 *
 */
class JSONConnection extends FileConnection {
	JSONConnection () {
		super([driver: JSONDriver])
	}
	
	JSONConnection (Map params) {
		super(new HashMap([driver: JSONDriver]) + params?:[:])
		
		if (this.getClass().name == 'getl.json.JSONConnection') methodParams.validation("Super", params?:[:])
	}

	/** Current JSON connection driver */
	@JsonIgnore
	JSONDriver getCurrentJSONDriver() { driver as JSONDriver }

	@Override
	protected Class<Dataset> getDatasetClass() { JSONDataset }
}