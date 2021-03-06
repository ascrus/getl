package getl.deploy

import getl.config.ConfigSlurper
import getl.exception.ExceptionGETL
import getl.utils.FileUtils
import getl.utils.Logs
import getl.utils.StringUtils

/**
 * Version manager
 * @author Aleksey Konstantinov
 */
class Version {
	// Init class
	static private Boolean init = {
		readConfig()
		return true
	}.call()

	// Read resource file config
	@SuppressWarnings("GroovyAssignabilityCheck")
	static private void readConfig() {
		def conf = ConfigSlurper.LoadConfigFile(FileUtils.FileFromResources('/getl.conf'))
		def getlSection = (conf.getl as Map)
		if (getlSection == null)
			throw new ExceptionGETL('Invalid resource file "getl.conf"!')
		def jarSection = ((conf.getl as Map).jar as Map)
		if (jarSection == null || jarSection.version == null || jarSection.year == null)
			throw new ExceptionGETL('Invalid resource file "getl.conf"!')

		version = jarSection.version as String
		years = jarSection.year as String

		def m = version =~ /(\d+)[.](\d+)[.](.+)/
		def v1 = m[0][1] as String
		def v2 = m[0][2] as String
		def v3 = m[0][3] as String
		def i = v3.indexOf('-')
		if (i > -1) v3 = v3.substring(0, i)
		def s = v1 + '.' + StringUtils.AddLedZeroStr(v2, 2) + StringUtils.AddLedZeroStr(v3, 2)
		versionNum = new BigDecimal(s)
	}

	/** GETL version */
	static public String version
	
	/** GETL version as numeric */
	static public BigDecimal versionNum

	/** Compatibility GETL version */
	static public BigDecimal versionNumCompatibility = 4.0300
	
	/**
	 * Valid compatibility version
	 * @param ver - required version
	 * @return result of valid
	 */
	static Boolean IsCompatibility (def ver) {
		ver >= versionNumCompatibility && ver <= versionNum 
	}
	
	/** Years development */
	static public String years

	static private Boolean sayInfo = false

	@SuppressWarnings("UnnecessaryQualifiedReference")
	static void SayInfo(def isJob = true) {
		if (sayInfo) return
		sayInfo = true
		Logs.Init()
		def str = "Getl framework, version ${getl.deploy.Version.version} created by ${getl.deploy.Version.years}, All rights to the product belong to company EasyData Ltd Russia under license \"GNU General Public License 3.0\""
		if (isJob) {
			Logs.Fine('### ' + str)
			Logs.Info('### Job start')
		}
		else {
			println str
		}
	}
}