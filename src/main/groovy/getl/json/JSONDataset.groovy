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

package getl.json

import getl.exception.ExceptionGETL
import getl.json.opts.JSONReadSpec
import getl.lang.opts.BaseSpec
import groovy.transform.stc.ClosureParams
import groovy.transform.stc.SimpleType

import java.util.Map

import getl.data.Connection
import getl.data.StructureFileDataset
import groovy.transform.InheritConstructors

/**
 * JSON dataset class
 * @author Alexsey Konstantinov
 *
 */
@InheritConstructors
class JSONDataset extends StructureFileDataset {
	JSONDataset () {
		super()
		params.convertToList = false
		driver_params = [:]
	}

	/** Added root {...} for JSON text */
	boolean getConvertToList () { params.convertToList }
	void setConvertToList (boolean value) { params.convertToList = value }

	@Override
	void setConnection(Connection value) {
		if (value != null && !(value instanceof JSONConnection))
			throw new ExceptionGETL('Сonnection to JSONConnection class is allowed!')

		super.setConnection(value)
	}

	/** Use specified connection */
	JSONConnection useConnection(JSONConnection value) {
		setConnection(value)
		return value
	}

	/** Current JSON connection */
	JSONConnection getCurrentJSONConnection() { connection as JSONConnection }
	
	/** Read JSON dataset attributes */
	void readAttrs (Map params) {
		((JSONDriver)(connection.driver)).readAttrs(this, params)
	}

	/**
	 * Read file options
	 */
	JSONReadSpec readOpts(@DelegatesTo(JSONReadSpec)
						  @ClosureParams(value = SimpleType, options = ['getl.json.opts.JSONReadSpec'])
								  Closure cl = null) {
		def thisObject = dslThisObject?:BaseSpec.DetectClosureDelegate(cl)
		def parent = new JSONReadSpec(this, thisObject, true, readDirective)
		parent.runClosure(cl)

		return parent
	}
}