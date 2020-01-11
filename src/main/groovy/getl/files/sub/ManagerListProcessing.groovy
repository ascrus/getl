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

package getl.files.sub


import groovy.transform.Synchronized

/**
 * Processing files by files.Manager.buildList method
 * @author Alexsey Konstantinov
 *
 */
abstract class ManagerListProcessing {
	/** Parameters */
	final Map<String, Object> params = Collections.synchronizedMap(new HashMap<String, Object>())
	/** Parameters */
	Map<String, Object> getParams() { params }
	/** Parameters */
	void setParams(Map<String, Object> value) {
		params.clear()
		if (value != null) params.putAll(value)
	}
	
	/** Clone class for use in thread */
	@Synchronized
	ManagerListProcessing newProcessing () {
		ManagerListProcessing res = getClass().newInstance() as ManagerListProcessing
		res.params.putAll(params)

		return res
	}
	
	/**
	 * Init class for build thread
	 */
	@Synchronized
	void init () { }
	
	/**
	 * Prepare file and return allow use
	 * @param file
	 * @return
	 */
	abstract boolean prepare (Map file)
	
	/**
	 * Done class after build thread
	 */
	@Synchronized
	void done () { }
}