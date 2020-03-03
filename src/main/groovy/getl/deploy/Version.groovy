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

import getl.utils.Logs

/**
 * Version manager
 * @author Aleksey Konstantinov
 */
class Version {
	/**
	 * GETL version
	 */
	public static version = "4.3.0"
	
	/**
	 * GETL version as numeric
	 */
	public static versionNum = 4.0300

	/**
	 * Compatibility GETL version
	 */
	public static versionNumCompatibility = 4.0300
	
	/**
	 * Valid compatibility version
	 * @param ver
	 * @return
	 */
	static boolean IsCompatibility (def ver) {
		ver >= versionNumCompatibility && ver <= versionNum 
	}
	
	/**
	 * Years development
	 */
	public static years = "2014-2020"

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