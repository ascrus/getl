package getl.files.sub

/**
 * List files by file manager
 * @author Alexsey Konstantinov
 *
 */
abstract class FileManagerList {
	/**
	 * Size of list
	 * @return
	 */
	abstract Integer size ()

	/**
	 * List item by index	
	 * @param index
	 * @return
	 */
	abstract Map item (Integer index)
	
	/**
	 * Clear list
	 */
	abstract void clear ()
}
