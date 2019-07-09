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

import getl.json.opts.JSONReadSpec

import java.util.Map;

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
	}
	
	/** Added root {...} for JSON text */
	boolean getConvertToList () { params.convertToList }
	void setConvertToList (boolean value) { params.convertToList = value }
	
	@Override
	void setConnection(Connection value) {
		assert value == null || value instanceof JSONConnection
		super.setConnection(value)
	}
	
	/** Read JSON dataset attributes */
	void readAttrs (Map params) {
		((JSONDriver)(connection.driver)).readAttrs(this, params)
	}

	/**
	 * Read file options
	 */
	JSONReadSpec readOpts(@DelegatesTo(JSONReadSpec) Closure cl = null) {
		def parent = new JSONReadSpec(true, readDirective)
		parent.thisObject = parent.DetectClosureDelegate(cl)
		if (cl != null) {
			def code = cl.rehydrate(parent.DetectClosureDelegate(cl), parent, parent.DetectClosureDelegate(cl))
			code.resolveStrategy = Closure.OWNER_FIRST
			code.call()
			parent.prepareParams()
		}

		return parent
	}
}
