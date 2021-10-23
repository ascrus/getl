package getl.tfs

import getl.csv.*
import getl.exception.ExceptionGETL
import getl.utils.FileUtils
import groovy.transform.InheritConstructors

/**
 * Temporary File Storage manager class
 * @author Alexsey Konstantinov
 *
 */
@InheritConstructors
class TFS extends CSVConnection {
	@Override
	protected void registerParameters() {
		super.registerParameters()

		methodParams.register("Super", ["deleteOnExit"])
	}

	static private String _systemPath

	/** Used temporary directory */
	static String getSystemPath() {
        if (_systemPath == null) {
			_systemPath = "${FileUtils.SystemTempDir()}/getl/${FileUtils.UniqueFileName()}"
			FileUtils.ValidPath(_systemPath)
			new File(_systemPath).deleteOnExit()
		}
        return _systemPath
    }
	
	/** Global temporary file connection object */
	static public final TFS storage = new TFS([:])

	@Override
	protected void validParams() {
		super.validParams()
		if (params.fieldDelimiter == null)
			fieldDelimiter = "|"
		if (params.rowDelimiter == null)
			rowDelimiter = "\n"
		if (params.autoSchema == null)
			autoSchema = true
		if (params.extenstion == null)
			extension = 'csv'
		if (params.escaped == null)
			escaped = false
		if (params.header == null)
			header = false

		if (params.deleteOnExit == null)
			params.deleteOnExit = true
		if (params.path == null)
			params.path = "$systemPath/tfs-files.getl"
	}

	@Override
	protected void doInitConnection () {
		super.doInitConnection()
		setPath(path)
	}

	/** Delete temporary directories and files after exit is program */
	Boolean getDeleteOnExit () { params.deleteOnExit }
	/** Delete temporary directories and files after exit is program */
	void setDeleteOnExit (Boolean value) { params.deleteOnExit = value }
	
	@Override
	void setPath (String value) {
		super.setPath(value)
		if (value != null) {
			def f = new File(value)
			f.mkdirs()
			if (deleteOnExit) {
				f.deleteOnExit()
			}
		}
	}
	
	/**
	 * Create new temporary named dataset object
	 * @param name - name of object
	 * @param validExists - object required is exists 
	 * @return
	 */
	static TFSDataset dataset(String name, Boolean validExists = false) {
		dataset(storage, name, validExists)
	}
	
	/**
	 * Create new temporary unnamed dataset object	
	 * @return
	 */
	static TFSDataset dataset() {
		dataset(FileUtils.UniqueFileName(), false)
	}
	
	/**
	 * Create new temporary named dataset object
	 * @param connection - TFS connection
	 * @param name - name of object
	 * @param validExists - object required is exists 
	 * @return
	 */
	static TFSDataset dataset(TFS connection, String name, Boolean validExists = false) {
		TFSDataset ds = new TFSDataset(connection: connection, fileName: name)
		if (validExists && !ds.existsFile()) throw new ExceptionGETL("Temporary file \"${name}\" not exists")
		
		return ds
	}

	/**
	 * Create new temporary unnamed dataset object
	 * @param connection - TFS connection
	 * @return
	 */
	static TFSDataset dataset(TFS connection) {
		dataset(connection, FileUtils.UniqueFileName(), false)
	}

	@Override
	void setParams(Map<String, Object> value) {
		super.setParams(value)
		validParams()
	}
}