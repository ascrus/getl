package getl.xml

import com.fasterxml.jackson.annotation.JsonIgnore
import getl.data.FileConnection
import getl.exception.ExceptionGETL
import groovy.transform.InheritConstructors

/**
 * XML connection class 
 * @author Alexsey Konstantinov
 *
 */
class XMLConnection extends FileConnection {
	XMLConnection () {
		super(driver: XMLDriver)
	}
	
	XMLConnection (Map params) {
		super(new HashMap([driver: XMLDriver]) + params?:[:])

		methodParams.register('Super', ['defaultAccessMethod'])
		
		if (this.getClass().name == 'getl.xml.XMLConnection') methodParams.validation('Super', params?:[:])
	}

	/** Current XML connection driver */
	@JsonIgnore
	XMLDriver getCurrentXMLDriver() { driver as XMLDriver }

	/** Use default the attribute access method (default) */
	static public final Integer DEFAULT_ATTRIBUTE_ACCESS = 0
	/** Use default the node access method */
	static public final Integer DEFAULT_NODE_ACCESS = 1

	/** How read field if not specified the alias property
	 * <br>default: DEFAULT_ATTRIBUTE_ACCESS
	 */
	Integer getDefaultAccessMethod() { (params.defaultAccessMethod as Integer)?:DEFAULT_ATTRIBUTE_ACCESS }
	/** How read field if not specified the alias property
	 * <br>default: DEFAULT_ATTRIBUTE_ACCESS
	 */
	void setDefaultAccessMethod(Integer value) {
		if (value != null && !(value in [DEFAULT_NODE_ACCESS, DEFAULT_ATTRIBUTE_ACCESS]))
			throw new ExceptionGETL('Invalid default access method property!')
		params.defaultAccessMethod = value
	}
}
