package getl.data

import com.fasterxml.jackson.annotation.JsonIgnore
import getl.utils.*
import getl.exception.ExceptionGETL
import groovy.transform.stc.ClosureParams
import groovy.transform.stc.SimpleType

/**
 * Base structure dataset class
 * @author Alexsey Konstantinov
 *
 */
class StructureFileDataset extends FileDataset {
	@Override
	protected void initParams() {
		super.initParams()
		params.attributeField = [] as List<Field>
		params.attributeValue = [:] as Map<String, Object>
	}
	
	@Override
	protected void onLoadConfig (Map configSection) {
		super.onLoadConfig(MapUtils.CleanMap(configSection, ["attributes"]))
		if (configSection.containsKey("attributes")) {
			def l = configSection.attributes as List<Map>
			List<Field> fl = []
			l.each { Map it ->
				def name = it.name as String
				if (it.name == null)
					throw new ExceptionGETL("Required field name: ${it}")
				def type = (it.type as Field.Type)?:Field.Type.STRING
				def isNull = BoolUtils.IsValue(it.isNull, true)
				def length = it.length as Integer
				def precision = it.precision as Integer
				def isKey = BoolUtils.IsValue(it.isKey)
				def isAutoincrement = BoolUtils.IsValue(it.isAutoincrement)
				def isReadOnly = BoolUtils.IsValue(it.isReadOnly)
				def compute = it.compute as String
				def format = it.format as String
				def alias = it.alias as String
				def trim = BoolUtils.IsValue(it.trim)
				def decimalSeparator = it.decimalSeparator as String
				def description = it.description as String
				def extended = it.extended as Map
				
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
	
	/** List of attributes field  */
	List<Field> getAttributeField () { params.attributeField as List<Field> }
	/** List of attributes field */
	void setAttributeField (List<Field> value) {
		 List<Field> l = []
		 value.each { Field f ->
			 l << f.copy()
		 }
		 params.attributeField = l
	}

	/** Find attribute field by name */
	Field attributeByName(String name) {
		if (name == null) throw new ExceptionGETL('The value of parameter "name" must be specified!')
		name = name.toUpperCase()
		return attributeField.find { it.name.toUpperCase() == name }
	}

	/** Dataset attribute field */
	Field attributeField(String name,
						 @DelegatesTo(Field)
						 @ClosureParams(value = SimpleType, options = ['getl.data.Field']) Closure cl) {
		Field parent = attributeByName(name)
		if (parent == null) {
			parent = new Field(name: name)
			attributeField << parent
		}
		if (cl != null) parent.with(cl)

		return parent
	}
	
	/** Attribute value */
	@JsonIgnore
	Map<String, Object> getAttributeValue () { params.attributeValue as Map<String, Object> }
	/** Attribute value */
	void setAttributeValue (Map<String, Object> value) {
		Map<String, Object> m = [:]
		m.putAll(value)
		params.attributeValue = m
	}
	
	/** Name of root node */
	String getRootNode () { params.rootNode as String }
	/** Name of root node */
	void setRootNode (String value) { params.rootNode = value }
}