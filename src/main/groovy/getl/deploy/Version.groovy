package getl.deploy

import getl.config.ConfigSlurper
import getl.exception.ExceptionGETL
import getl.lang.Getl
import getl.utils.FileUtils
import getl.utils.Logs
import getl.utils.StringUtils

/**
 * Version manager
 * @author Aleksey Konstantinov
 */
class Version implements VersionInfo {
	Version() {
		readConfig()
	}

	/** Instance version manager */
	public static final Version instance = new Version()

	/** GETL version */
	private String version
	String getVersion() { version }

	/** GETL version as numeric */
	private BigDecimal versionNum
	BigDecimal getVersionNum() { versionNum }

	/** Compatibility GETL version */
	private BigDecimal versionNumCompatibility
	BigDecimal getVersionNumCompatibility() { versionNumCompatibility }

	/** Years development */
	private String years
	String getYears() { years }

	// Read resource file config
	@SuppressWarnings("GroovyAssignabilityCheck")
	private void readConfig() {
		def conf = ConfigSlurper.LoadConfigFile(file: FileUtils.FileFromResources('/getl.conf'))
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

	/**
	 * Valid compatibility version
	 * @param ver - required version
	 * @return result of valid
	 */
	Boolean IsCompatibility (def ver) {
		ver >= versionNumCompatibility && ver <= versionNum 
	}

	private Boolean sayInfo = false

	@SuppressWarnings("UnnecessaryQualifiedReference")
	void sayInfo(Boolean isJob = true, Getl getl = null) {
		if (sayInfo)
			return

		sayInfo = true
		def str = "Getl framework, version ${getl.deploy.Version.instance.version} created by ${getl.deploy.Version.instance.years}, All rights to the product belong to company EasyData Ltd Russia under license \"GNU General Public License 3.0\""
		if (getl != null) {
			getl.logFine('### ' + str)
			getl.logInfo('### Job start')
		}
		else if (isJob) {
			Logs.Fine('### ' + str)
			Logs.Info('### Job start')
		}
		else {
			println str
		}
	}
}