package getl.jdbc

import com.fasterxml.jackson.annotation.JsonIgnore
import getl.exception.ExceptionGETL
import getl.utils.FileUtils
import groovy.transform.InheritConstructors
import getl.utils.StringUtils

/**
 * Query dataset class
 * @author Alexsey Konstantinov
 *
 */
@InheritConstructors
class QueryDataset extends JDBCDataset {
	QueryDataset() {
		super()
		sysParams.isQuery = true
	}

	/** Use specified connection */
	JDBCConnection useConnection(JDBCConnection value) {
		setConnection(value)
		return value
	}

	@Override
	@JsonIgnore
	Type getType() { super.getType()?:queryType }

	/** SQL query text */
	String getQuery () { params.query as String }
	/** SQL query text */
	void setQuery (String value) { params.query = value }

	@Override
	@JsonIgnore
	String getObjectName() { (description != null)?description:'sql query' }

	/**
	 * Load script from file
	 * @param fileName file name sql batch file
	 * @param codePage file use specified encoding page (default utf-8)
	 */
	void loadFile (String fileName, String codePage = 'utf-8') {
		setQuery(new File(FileUtils.ResourceFileName(fileName)).getText(codePage))
	}

	/**
	 * Load script from file in class path or resource directory
	 * @param fileName file name in resource catalog
	 * @param otherPath the string value or list of string values as search paths if file is not found in the resource directory
	 * @param codePage file use specified encoding page (default utf-8)
	 */
	void loadResource(String fileName, def otherPath = null, String codePage = 'utf-8') {
		def file = FileUtils.FileFromResources(fileName, otherPath)
		if (file == null)
			throw new ExceptionGETL("Resource file \"$fileName\" not found!")
		setQuery(file.getText(codePage?:'utf-8'))
	}
}