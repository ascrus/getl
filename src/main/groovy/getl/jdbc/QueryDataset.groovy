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
	@Override
	protected void initParams() {
		super.initParams()

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

	/** The path to script file */
	String getScriptFilePath() { params.scriptFilePath as String }
	/** The path to script file */
	void setScriptFilePath(String value) { params.scriptFilePath = value }

	/** Script file text encoding */
	String getScriptFileCodePage() { params.scriptFileCodePage as String }
	/** Script file text encoding */
	void setScriptFileCodePage(String value) { params.scriptFileCodePage = value }

	@Override
	@JsonIgnore
	String getObjectName() { (dslNameObject != null)?"Jdbc query [$dslNameObject]":'Jdbc query' }

	/**
	 * Load script from file
	 * @param filePath file name sql batch file
	 * @param codePage file use specified encoding page (default utf-8)
	 */
	void loadFile(String filePath, String codePage = null) {
		setQuery(readFile(filePath, codePage))
	}

	/**
	 * Read script from file
	 * @param filePath file name sql batch file
	 * @param codePage file use specified encoding page (default utf-8)
	 * @return script text
	 */
	String readFile(String filePath, String codePage = null) {
		def file = new File(FileUtils.TransformFilePath(filePath, dslCreator))
		if (!file.exists())
			throw new ExceptionGETL("Script file \"$filePath\" not found!")
		return file.getText(codePage?:scriptFileCodePage?:'utf-8')
	}

	/**
	 * Load script from file in class path or resource directory
	 * @param filePath file name in resource catalog
	 * @param otherPath the string value or list of string values as search paths if file is not found in the resource directory
	 * @param codePage file use specified encoding page (default utf-8)
	 */
	void loadResource(String filePath, def otherPath = null, String codePage = null) {
		setQuery(readFile('resource:/' + filePath, codePage))
	}
}