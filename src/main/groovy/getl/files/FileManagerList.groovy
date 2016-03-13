package getl.files

abstract class FileManagerList {
	/**
	 * Size of list
	 * @return
	 */
	abstract public Integer size ()

	/**
	 * List item by index	
	 * @param index
	 * @return
	 */
	abstract public Map item (int index)
	
	/**
	 * Clear list
	 */
	abstract public void clear () 
}
