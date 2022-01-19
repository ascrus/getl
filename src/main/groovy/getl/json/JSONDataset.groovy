package getl.json

import com.fasterxml.jackson.annotation.JsonIgnore
import getl.exception.ExceptionGETL
import getl.json.opts.JSONReadSpec
import groovy.transform.InheritConstructors
import groovy.transform.stc.ClosureParams
import groovy.transform.stc.SimpleType
import getl.data.Connection
import getl.data.StructureFileDataset

/**
 * JSON dataset class
 * @author Alexsey Konstantinov
 *
 */
@InheritConstructors
class JSONDataset extends StructureFileDataset {
	@Override
	protected void initParams() {
		super.initParams()

		_driver_params = new HashMap<String, Object>()
	}

	@Override
	void setConnection(Connection value) {
		if (value != null && !(value instanceof JSONConnection))
			throw new ExceptionGETL('Only class JSONConnection connections are permitted!')

		super.setConnection(value)
	}

	/** Use specified connection */
	JSONConnection useConnection(JSONConnection value) {
		setConnection(value)
		return value
	}

	/** Current JSON connection */
	@JsonIgnore
	JSONConnection getCurrentJSONConnection() { connection as JSONConnection }
	
	/** Read JSON dataset attributes */
	void readAttrs (Map params) {
		currentJSONConnection.currentJSONDriver.readAttrs(this, params)
	}

	/** Read file options */
	JSONReadSpec getReadOpts() { new JSONReadSpec(this, true, readDirective) }

	/** Read file options */
	JSONReadSpec readOpts(@DelegatesTo(JSONReadSpec)
						  @ClosureParams(value = SimpleType, options = ['getl.json.opts.JSONReadSpec'])
								  Closure cl = null) {
		def parent = readOpts
		parent.runClosure(cl)

		return parent
	}
}