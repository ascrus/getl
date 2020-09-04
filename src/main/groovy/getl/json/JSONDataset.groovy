package getl.json

import com.fasterxml.jackson.annotation.JsonIgnore
import getl.exception.ExceptionGETL
import getl.json.opts.JSONReadSpec
import groovy.transform.stc.ClosureParams
import groovy.transform.stc.SimpleType
import getl.data.Connection
import getl.data.StructureFileDataset

/**
 * JSON dataset class
 * @author Alexsey Konstantinov
 *
 */
class JSONDataset extends StructureFileDataset {
	JSONDataset () {
		super()
		_driver_params = [:] as Map<String, Object>
	}

	@Override
	protected void initParams() {
		super.initParams()
		params.convertToList = false
	}

	/** Added root {...} for JSON text */
	Boolean getConvertToList () { params.convertToList }
	void setConvertToList (Boolean value) { params.convertToList = value }

	@Override
	void setConnection(Connection value) {
		if (value != null && !(value instanceof JSONConnection))
			throw new ExceptionGETL('Сonnection to JSONConnection class is allowed!')

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

	/**
	 * Read file options
	 */
	JSONReadSpec readOpts(@DelegatesTo(JSONReadSpec)
						  @ClosureParams(value = SimpleType, options = ['getl.json.opts.JSONReadSpec'])
								  Closure cl = null) {
		def parent = new JSONReadSpec(this, true, readDirective)
		parent.runClosure(cl)

		return parent
	}
}