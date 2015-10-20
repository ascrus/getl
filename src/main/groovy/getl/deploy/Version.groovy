package getl.deploy

/**
 * Version manager
 * @author Aleksey Konstantinov
 */
class Version {
	/**
	 * GETL version
	 */
	public static version = "1.1.39"
	
	/**
	 * GETL version as numeric
	 */
	public static versionNum = 1.0139

	/**
	 * Compatibility GETL version
	 */
	public static versionNumCompatibility = 1.0121
	
	/**
	 * Valid compatibility version
	 * @param ver
	 * @return
	 */
	public static boolean IsCompatibility (def ver) { 
		ver >= versionNumCompatibility && ver <= versionNum 
	}
	
	/**
	 * Years development
	 */
	public static years = "2014-2015"
}
