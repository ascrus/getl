/*
 GETL - based package in Groovy, which automates the work of loading and transforming data. His name is an acronym for "Groovy ETL".

 GETL is a set of libraries of pre-built classes and objects that can be used to solve problems unpacking,
 transform and load data into programs written in Groovy, or Java, as well as from any software that supports
 the work with Java classes.
 
 Copyright (C) 2013-2015  Alexsey Konstantonov (ASCRUS)

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

package getl.data

import getl.utils.*
import getl.exception.ExceptionGETL
import groovy.transform.InheritConstructors

/**
 * Base structure dataset class
 * @author Alexsey Konstantinov
 *
 */
@InheritConstructors
class StructureFileDataset extends FileDataset {
	StructureFileDataset () {
		super()
		
		List<Field> l = []
		params.attributeField = l
		
		Map<String, Object> m = [:]
		params.attributeValue = m
	}
	
	@Override
	protected void onLoadConfig (Map configSection) {
		super.onLoadConfig(MapUtils.CleanMap(configSection, ["attributes"]))
		if (configSection.containsKey("attributes")) {
			def l = configSection.attributes as List<Map>
			List<Field> fl = []
			l.each { Map it ->
				String name = it.name
				if (it.name == null) throw new ExceptionGETL("Required field name: ${it}")
				Field.Type type = (it.type as Field.Type)?:Field.Type.STRING
				boolean isNull = it.isNull?:true
				Integer length = it.length as Integer
				Integer precision = it.precision as Integer
				boolean isKey = it.isKey?:false
				boolean isAutoincrement = it.isAutoincrement?:false
				boolean isReadOnly = it.isReadOnly?:false
//				String defaultValie = it.defaultValue
				String compute = it.compute
				String format = it.format
				String alias = it.alias
				boolean trim = it.trim?:false
				String decimalSeparator = it.decimalSeparator
				String description = it.description
				Map extended = it.extended as Map
				
				Field f = new Field(
						name: name, type: type, isNull: isNull, length: length, precision: precision,
						isKey: isKey, isAutoincrement: isAutoincrement, isReadOnly: isReadOnly,
						compute: compute, description: description, format: format, alias: alias,
						trim: trim, decimalSeparator: decimalSeparator, extended: extended)

				fl << f
			}
			params.attributeField = fl
		}
	}
	
	/**
	 * List of attributes for structured file
	 * @return
	 */
	List<Field> getAttributeField () { params.attributeField as List<Field> }

	void setAttributeField (List<Field> value) {
		 List<Field> l = []
		 value.each { Field f ->
			 l << f.copy()
		 }
		 params.attributeField = l
	}
	
	/**
	 * Attribute value
	 * @return
	 */
	Map<String, Object> getAttributeValue () { params.attributeValue as Map<String, Object> }

	void setAttributeValue (Map<String, Object> value) {
		Map<String, Object> m = [:]
		m.putAll(value)
		params.attributeValue = m
	}
	
	/**
	 * Name of root node
	 * @return
	 */
	String getRootNode () { params.rootNode }

	void setRootNode (String value) { params.rootNode = value }
}
