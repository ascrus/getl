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

package getl.jdbc

import getl.exception.ExceptionGETL
import getl.utils.FileUtils
import groovy.transform.InheritConstructors
import getl.utils.StringUtils

/**
 * Query dataset class
 * @author Alexsey Konstantinov
 *
 */
@InheritConstructors
class QueryDataset extends JDBCDataset {
	QueryDataset() {
		super()
		sysParams.isQuery = true
	}

	/** Use specified connection */
	JDBCConnection useConnection(JDBCConnection value) {
		setConnection(value)
		return value
	}

	@Override
	Type getType() { super.getType()?:queryType }

	/** SQL query text */
	String getQuery () { params.query as String }
	/** SQL query text */
	void setQuery (String value) { params.query = value }

	@Override
	String getObjectName() { (description != null)?description:'sql query' }

	/**
	 * Load script from file
	 * @param fileName file name sql batch file
	 * @param codePage file use specified encoding page (default utf-8)
	 */
	void loadFile (String fileName, String codePage = 'utf-8') {
		setQuery(new File(FileUtils.ResourceFileName(fileName)).getText(codePage))
	}

	/**
	 * Load script from file in class path or resource directory
	 * @param fileName file name in resource catalog
	 * @param otherPath the string value or list of string values as search paths if file is not found in the resource directory
	 * @param codePage file use specified encoding page (default utf-8)
	 */
	void loadResource(String fileName, def otherPath = null, String codePage = 'utf-8') {
		def file = FileUtils.FileFromResources(fileName, otherPath)
		if (file == null)
			throw new ExceptionGETL("Resource file \"$fileName\" not found!")
		setQuery(file.getText(codePage?:'utf-8'))
	}
}