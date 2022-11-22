package getl.data

import com.fasterxml.jackson.annotation.JsonIgnore
import getl.exception.DatasetError
import getl.exception.RequiredParameterError
import getl.utils.*
import groovy.transform.InheritConstructors
import groovy.transform.stc.ClosureParams
import groovy.transform.stc.SimpleType

/**
 * Base structure dataset class
 * @author Alexsey Konstantinov
 *
 */
@InheritConstructors
class StructureFileDataset extends WebServiceDataset {
	@Override
	protected void initParams() {
		super.initParams()
		params.attributeField = [] as List<Field>
		params.attributeValue = new HashMap<String, Object>()
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
					throw new DatasetError(this, '#struct_files.invalid_config_attrs', [attr: it])
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
	List<Field> getAttributeField() { params.attributeField as List<Field> }
	/** List of attributes field */
	void setAttributeField(List<Field> value) {
		 List<Field> l = []
		 value.each { Field f ->
			 l << f.copy()
		 }
		 params.attributeField = l
	}
	/**
	 * Assign attributes from a list of map elements
	 * @param list attributes description list
	 */
	void assignAttributeField(List<Map> list) {
		def res = [] as List<Field>
		list.each {m ->
			res << Field.ParseMap(m)
		}
		setAttributeField(res)
	}

	/** Find attribute field by name */
	Field attributeByName(String name) {
		if (name == null)
			throw new RequiredParameterError('name', 'attributeByName')
		name = name.toUpperCase()
		return attributeField.find { it.name.toUpperCase() == name }
	}

	/** Dataset attribute field */
	Field attributeField(String name,
						 @DelegatesTo(Field)
						 @ClosureParams(value = SimpleType, options = ['getl.data.Field']) Closure cl = null) {
		Field parent = attributeByName(name)
		if (parent == null) {
			parent = new Field(name: name)
			attributeField << parent
		}
		if (cl != null)
			parent.tap(cl)

		return parent
	}
	
	/** Attribute value */
	@JsonIgnore
	Map<String, Object> getAttributeValue () { params.attributeValue as Map<String, Object> }
	/** Attribute value */
	void setAttributeValue (Map<String, Object> value) {
		Map<String, Object> m = new HashMap<String, Object>()
		m.putAll(value)
		params.attributeValue = m
	}
	
	/** Name of root list node */
	String getRootNode () { params.rootNode as String }
	/** Name of root list node */
	void setRootNode (String value) { params.rootNode = value }

	/** Name of data map node */
	String getDataNode () { params.dataNode as String }
	/** Name of data map node */
	void setDataNode (String value) { params.dataNode = value }

	/** Return the order of arrays of the root node */
	List<String> rootNodePath() {
		if (rootNode == null)
			return [] as List<String>

		return rootNode.split('[|]').toList()
	}

	/** Check and initialize dataset attributes */
	@JsonIgnore
	Closure<Boolean> getOnInitAttributes() { params.onInitAttributes as Closure<Boolean> }
	/** Check and initialize dataset attributes */
	void setOnInitAttributes(Closure<Boolean> value) { params.onInitAttributes = value }
	/** Check and initialize dataset attributes */
	void initAttributes(@ClosureParams(value = SimpleType, options = ['getl.data.StructureFileDataset']) Closure<Boolean> cl) {
		setOnInitAttributes(cl)
	}
}