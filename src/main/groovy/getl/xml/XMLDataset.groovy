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

import getl.exception.ExceptionGETL
import getl.lang.opts.BaseSpec
import getl.xml.opts.XMLReadSpec
import groovy.transform.InheritConstructors
import getl.data.*
import groovy.transform.stc.ClosureParams
import groovy.transform.stc.SimpleType

/**
 * XML dataset class
 * @author Alexsey Konstantinov
 *
 */
@InheritConstructors
class XMLDataset extends StructureFileDataset {
	XMLDataset () {
		super()
		
		Map<String, Boolean> m = [:]
		params.features = m
	}
	
	/** Feature parsing options */
	Map<String, Boolean> getFeatures () { params."features" as Map<String, Boolean> }
	/** Feature parsing options */
	void setFeatures(Map<String, Boolean> values) {
		(params.features as Map).clear()
		if (values != null) (params.features as Map).putAll(values)
	}

	@Override
	void setConnection(Connection value) {
		if (value != null && !(value instanceof XMLConnection))
			throw new ExceptionGETL('Сonnection to XMLConnection class is allowed!')

		super.setConnection(value)
	}

	/** Use specified connection */
	XMLConnection useConnection(XMLConnection value) {
		setConnection(value)
		return value
	}

	/** Use default the attribute access method (default) */
	static final DEFAULT_ATTRIBUTE_ACCESS = 0
	/** Use default the node access method */
	static final DEFAULT_NODE_ACCESS = 1

	/** How read field if not specified the alias property
	 * <br>default: DEFAULT_ATTRIBUTE_ACCESS
	 */
	Integer getDefaultAccessMethod() {
		(params.defaultAccessMethod) as Integer?:(connection as XMLConnection).defaultAccessMethod
	}
	/** How read field if not specified the alias property
	 * <br>default: DEFAULT_ATTRIBUTE_ACCESS
	 */
	void setDefaultAccessMethod(Integer value) {
		if (!(value in [DEFAULT_NODE_ACCESS, DEFAULT_ATTRIBUTE_ACCESS]))
			throw new ExceptionGETL('Invalid default access method property!')
		params.defaultAccessMethod = value
	}

	/**
	 * Read XML dataset attributes
	 */
	void readAttrs (Map params) {
		((XMLDriver)(connection.driver)).readAttrs(this, params)
	}

	/**
	 * Read file options
	 */
	XMLReadSpec readOpts(@DelegatesTo(XMLReadSpec)
						 @ClosureParams(value = SimpleType, options = ['getl.xml.opts.XMLReadSpec'])
								 Closure cl = null) {
		def ownerObject = sysParams.dslOwnerObject?:this
		def thisObject = sysParams.dslThisObject?:BaseSpec.DetectClosureDelegate(cl)
		def parent = new XMLReadSpec(ownerObject, thisObject, true, readDirective)
		parent.runClosure(cl)

		return parent
	}

	/**
	 * Perform operations on a xml dataset
	 * @param cl closure code
	 * @return source xml dataset
	 */
	XMLDataset dois(@DelegatesTo(XMLDataset) Closure cl) {
		this.with(cl)
		return this
	}
}