package getl.files.sub

import getl.utils.SynchronizeObject
import groovy.transform.Synchronized

/**
 * Processing files by files.Manager.buildList method
 * @author Alexsey Konstantinov
 *
 */
abstract class ManagerListProcessing {
	/** Parameters */
	private final Map<String, Object> params = Collections.synchronizedMap(new HashMap<String, Object>())
	/** Parameters */
	Map<String, Object> getParams() { params }
	/** Parameters */
	void setParams(Map<String, Object> value) {
		params.clear()
		if (value != null) params.putAll(value)
	}
	
	/** Clone class for use in thread */
	@Synchronized
	ManagerListProcessing newProcessing() {
		ManagerListProcessing res = getClass().getDeclaredConstructor().newInstance() as ManagerListProcessing
		res.params.putAll(params)

		return res
	}

	/** Counter directories */
	public SynchronizeObject counterDirectories = new SynchronizeObject()
	
	/**
	 * Init class for build thread
	 */
	@Synchronized
	void init() { }
	
	/**
	 * Prepare file and return allow use
	 * @param file
	 * @return
	 */
	abstract Boolean prepare(Map file)
	
	/**
	 * Done class after build thread
	 */
	@Synchronized
	void done() { }
}