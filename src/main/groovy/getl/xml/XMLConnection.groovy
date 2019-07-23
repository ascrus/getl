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

package getl.xml

import getl.data.FileConnection
import getl.exception.ExceptionGETL

/**
 * XML connection class 
 * @author Alexsey Konstantinov
 *
 */
@groovy.transform.InheritConstructors
class XMLConnection extends FileConnection {
	XMLConnection () {
		super([driver: XMLDriver])
	}
	
	XMLConnection (Map params) {
		super(new HashMap([driver: XMLDriver]) + params)
		
		if (this.getClass().name == 'getl.xml.XMLConnection') methodParams.validation("Super", params)
	}

	/** Use default the attribute access method (default) */
	static final DEFAULT_ATTRIBUTE_ACCESS = 0
	/** Use default the node access method */
	static final DEFAULT_NODE_ACCESS = 1

	/** How read field if not specified the alias property
	 * <br>default: DEFAULT_ATTRIBUTE_ACCESS
	 */
	Integer getDefaultAccessMethod() { (params.defaultAccessMethod as Integer)?:DEFAULT_ATTRIBUTE_ACCESS }
	/** How read field if not specified the alias property
	 * <br>default: DEFAULT_ATTRIBUTE_ACCESS
	 */
	void setDefaultAccessMethod(Integer value) {
		if (!(value in [DEFAULT_NODE_ACCESS, DEFAULT_ATTRIBUTE_ACCESS]))
			throw new ExceptionGETL('Invalid default access method property!')
		params.defaultAccessMethod = value
	}
}
