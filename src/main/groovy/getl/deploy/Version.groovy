/*
 GETL - based package in Groovy, which automates the work of loading and transforming data. His name is an acronym for "Groovy ETL".

 GETL is a set of libraries of pre-built classes and objects that can be used to solve problems unpacking,
 transform and load data into programs written in Groovy, or Java, as well as from any software that supports
 the work with Java classes.
 
 Copyright (C) EasyData Company LTD

 This program is free software: you can redistribute it and/or modify
 it under the terms of the GNU Lesser General Public License as published by
 the Free Software Foundation, either version 3 of the License, or
 (at your option) any later version.

 This program is distributed in the hope that it will be useful,
 but WITHOUT ANY WARRANTY; without even the implied warranty of
 MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 GNU General Public License for more details.

 You should have received a copy of the GNU General Public License and
 GNU Lesser General Public License along with this program.
 If not, see <http://www.gnu.org/licenses/>.
*/

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
	static private boolean init = {
		readConfig()
		return true
	}.call()

	// Read resource file config
	static private void readConfig() {
		def conf = ConfigSlurper.LoadConfigFile(FileUtils.FileFromResources('/getl.conf'))
		def getlSection = (conf.getl as Map)
		if (getlSection == null)
			throw new ExceptionGETL('Invalid resource file "getl.conf"!')
		def jarSection = (conf.getl.jar as Map)
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
	public static String version
	
	/** GETL version as numeric */
	public static BigDecimal versionNum

	/** Compatibility GETL version */
	public static versionNumCompatibility = 4.0300
	
	/**
	 * Valid compatibility version
	 * @param ver - required version
	 * @return result of valid
	 */
	static boolean IsCompatibility (def ver) {
		ver >= versionNumCompatibility && ver <= versionNum 
	}
	
	/** Years development */
	public static String years

	private static boolean sayInfo = false

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