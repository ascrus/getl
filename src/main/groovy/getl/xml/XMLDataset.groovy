package getl.xml

import com.fasterxml.jackson.annotation.JsonIgnore
import getl.exception.ExceptionGETL
import getl.xml.opts.XMLReadSpec
import getl.data.*
import groovy.transform.stc.ClosureParams
import groovy.transform.stc.SimpleType

/**
 * XML dataset class
 * @author Alexsey Konstantinov
 *
 */
class XMLDataset extends StructureFileDataset {
	XMLDataset () {
		super()
		_driver_params = [:] as Map<String, Object>
	}

	@Override
	protected void initParams() {
		super.initParams()
		params.features = [:] as Map<String, Boolean>
	}
	
	/** Feature parsing options */
	Map<String, Boolean> getFeatures () { params."features" as Map<String, Boolean> }
	/** Feature parsing options */
	void setFeatures(Map<String, Boolean> values) {
		(params.features as Map).clear()
		if (values != null) (params.features as Map).putAll(values)
	}

	@Override
	void setConnection(Connection value) {
		if (value != null && !(value instanceof XMLConnection))
			throw new ExceptionGETL('Сonnection to XMLConnection class is allowed!')

		super.setConnection(value)
	}

	/** Use specified connection */
	XMLConnection useConnection(XMLConnection value) {
		setConnection(value)
		return value
	}

	/** Current XML connection */
	@JsonIgnore
	XMLConnection getCurrentXMLConnection() { connection as XMLConnection }

	/** Use default the attribute access method (default) */
	static public final Integer DEFAULT_ATTRIBUTE_ACCESS = 0
	/** Use default the node access method */
	static public final Integer DEFAULT_NODE_ACCESS = 1

	/** How read field if not specified the alias property
	 * <br>default: DEFAULT_ATTRIBUTE_ACCESS
	 */
	Integer getDefaultAccessMethod() {
		(params.defaultAccessMethod as Integer)?:(connection as XMLConnection).defaultAccessMethod
	}
	/** How read field if not specified the alias property
	 * <br>default: DEFAULT_ATTRIBUTE_ACCESS
	 */
	void setDefaultAccessMethod(Integer value) {
		if (!(value in [DEFAULT_NODE_ACCESS, DEFAULT_ATTRIBUTE_ACCESS]))
			throw new ExceptionGETL('Invalid default access method property!')
		params.defaultAccessMethod = value
	}

	/**
	 * Read XML dataset attributes
	 */
	void readAttrs (Map params) {
		validConnection(false)

		currentXMLConnection.currentXMLDriver.readAttrs(this, params)
	}

	/** Read file options */
	XMLReadSpec getReadOpts() { new XMLReadSpec(this, true, readDirective) }

	/** Read file options */
	XMLReadSpec readOpts(@DelegatesTo(XMLReadSpec)
						 @ClosureParams(value = SimpleType, options = ['getl.xml.opts.XMLReadSpec'])
								 Closure cl = null) {
		def parent = readOpts
		parent.runClosure(cl)

		return parent
	}
}