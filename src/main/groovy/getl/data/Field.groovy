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

import getl.exception.ExceptionGETL
import getl.utils.*

/**
 * Base field class
 * @author Alexsey Konstantinov
 *
 */
class Field implements Serializable {
	public enum Type {
		STRING, INTEGER, BIGINT, NUMERIC, DOUBLE, BOOLEAN, DATE, TIME, DATETIME, BLOB, TEXT, OBJECT, ROWID
	}

	/**
	 * Field name	
	 * @return
	 */
	private String name = null
	public String getName () { name }
	public void setName (String value) { name = value }
	
	/**
	 * Field type
	 * @return
	 */
	private Type type = Type.STRING
	public Type getType () { type }
	public void setType (Type value) { type = value }
	
	/**
	 * Database field type
	 */
	public Object dbType
	
	/**
	 * Database field name
	 */
	public String typeName
	
	/**
	 * Value can not be null
	 * @return
	 */
	private boolean isNull = true
	public boolean getIsNull () { isNull }
	public void setIsNull (boolean value) { isNull = value }
	
	/**
	 * Length of value
	 * @return
	 */
	private Integer length
	public Integer getLength () { length }
	public void setLength (Integer value) { length = value }
	
	/**
	 * Precision number of numeric value
	 * @return
	 */
	private Integer precision
	public Integer getPrecision () { precision }
	public void setPrecision (Integer value) { precision = value }
	
	/**
	 * Field is primary key
	 * @return
	 */
	private boolean isKey = false
	public boolean getIsKey () { isKey }
	public void setIsKey (boolean value) { 
		isKey = value
		if (isKey) isNull = false else ordKey = null 
	}
	
	/**
	 * Number order from primary key
	 * @return
	 */
	private Integer ordKey
	public Integer getOrdKey () { ordKey }
	public void setOrdKey (Integer value) { ordKey = value }
	
	/**
	 * Field is auto increment
	 * @return
	 */
	private boolean isAutoincrement = false
	public boolean getIsAutoincrement () { isAutoincrement }
	public void setIsAutoincrement (boolean value) { isAutoincrement = value }
	
	/**
	 * Field can not write
	 * @return
	 */
	private boolean isReadOnly = false
	public boolean getIsReadOnly () { isReadOnly }
	public void setIsReadOnly (boolean value) { isReadOnly = value }
	
	/**
	 * Default value from field (used only creating dataset)
	 * @return
	 */
	private String defaultValue = null
	public String getDefaultValue () { defaultValue }
	public void setDefaultValue (String value) { defaultValue = value }
	
	/**
	 * Compute columns
	 * @return
	 */
	private String compute
	public String getCompute () { compute }
	public void setCompute (String value) { compute = value }
	
	/**
	 * Minimum value (for validation and generation)
	 */
	private def minValue = null
	public def getMinValue () { minValue }
	public void setMinValue (def value) { minValue = value }
	
	/**
	 * Minimum value (for validation and generation)
	 */
	private def maxValue = null
	public def getMaxValue () { maxValue }
	public void setMaxValue (def value) { maxValue = value }
	
	/**
	 * Format pattern on numeric and datetime fields
	 * @return
	 */
	private String format
	public String getFormat () { format }
	public void setFormat (String value) { format = value }
	
	/**
	 * Name of the field in the data source (if different from Field name)
	 * @return
	 */
	private String alias
	public String getAlias () { alias }
	public void setAlias (String value) { alias = value }
	
	/**
	 * Trim space (used for reading datasource)
	 * @return
	 */
	private boolean trim = false
	public boolean getTrim () { trim }
	public void setTrim (boolean value) { trim = value }
	
	/**
	 * Decimal separator
	 * @return
	 */
	private String decimalSeparator
	public String getDecimalSeparator () { decimalSeparator }
	public void setDecimalSeparator (String value) { decimalSeparator = value }
	
	/**
	 * Field description (comments)
	 * @return
	 */
	private String description
	public String getDescription () { description }
	public void setDescription (String value) { description = value }
	
	/**
	 * Extended attributes
	 * @return
	 */
	private Map extended = [:]
	public Map getExtended () { extended }
	public void setExtended (Map value) {
		extended.clear() 
		if (value != null) extended.putAll(value) 
	}
	
	/**
	 * Get value method
	 */
	public String getMethod
	
	/**
	 * Allow length for field
	 */
	public static boolean AllowLength(Field f) {
		(f.type in [Field.Type.STRING, Field.Type.NUMERIC, Field.Type.BLOB, Field.Type.TEXT, Field.Type.ROWID])
	}
	
	/**
	 * Allow precision for field
	 * @param f
	 * @return
	 */
	public static boolean AllowPrecision(Field f) {
		(f.type in [Field.Type.NUMERIC])
	}
	
	/**
	 * Allow create field in table
	 * @param f
	 * @return
	 */
	public static boolean AllowCreatable(Field f) {
		!(f.type in [Field.Type.ROWID])
	}
	
	/**
	 * Build map from field
	 * @return
	 */
	public Map toMap() {
		def n = [:]
		n.name = name
		n.type = type.toString()
		if (typeName != null) n."typeName" = typeName
		
		if (AllowLength(this) && length != null) n.length = length
		if (AllowPrecision(this) && precision != null) n.precision = precision
		if (!isNull) n.isNull = isNull
		if (isKey) n.isKey = isKey
		if (ordKey != null) n.ordKey = ordKey
		if (isAutoincrement) n.isAutoincrement = isAutoincrement
		if (isReadOnly) n.isReadOnly = isReadOnly
		if (defaultValue != null) n.defaultValue = defaultValue
		if (compute != null) n.compute = compute
		if (format != null) n.format = format
		if (alias != null) n.alias = alias
		if (trim) n.trim = trim
		if (decimalSeparator != null) n.decimalSeparator = decimalSeparator
		if (description != null) n.description = description
		
		n
	}
	
	/**
	 * Parse map to field
	 * @param strField
	 * @return
	 */
	public static Field ParseMap(Map strField) {
		if (strField == null) throw new ExceptionGETL("Can not parse null Map to fields")
		String name = strField.name
		if (strField.name == null) throw new ExceptionGETL("Required field name: ${strField}")
		Field.Type type = strField.type?:Field.Type.STRING
		String typeName = strField.typeName
		boolean isNull = (strField.isNull != null)?strField.isNull:true
		Integer length = strField.length
		Integer precision = strField.precision
		boolean isKey = (strField.isKey != null)?strField.isKey:false
		Integer ordKey = strField.ordKey
		boolean isAutoincrement = (strField.isAutoincrement != null)?strField.isAutoincrement:false
		boolean isReadOnly = (strField.isReadOnly != null)?strField.isReadOnly:false
		String defaultValue = strField.defaultValue
		String compute = strField.compute
		def minValue = strField.minValue
		def maxValue = strField.maxValue
		String format = strField.format
		String alias = strField.alias
		boolean trim = (strField.trim != null)?strField.trim:false
		String decimalSeparator = strField.decimalSeparator
		String description = strField.description
		Map extended = strField.extended
		
		new Field(name: name, type: type, typeName: typeName, isNull: isNull, length: length, precision: precision,
					isKey: isKey, ordKey: ordKey, isAutoincrement: isAutoincrement, isReadOnly: isReadOnly,
					defaultValue: defaultValue, compute: compute, minValue: minValue, maxValue: maxValue,
					format: format, alias: alias, trim: trim,
					decimalSeparator: decimalSeparator, description: description, extended: extended)
	}
	
	/**
	 * Attribute to string
	 */
	public String toString() {
		def s = [:]
		s.name = name
		s.type = type
		s.typeName = typeName
		s.isNull = isNull
		s.length = length
		s.precision = precision
		s.isKey = isKey
		s.ordKey = ordKey
		s.isAutoincrement = isAutoincrement
		s.isReadOnly = isReadOnly
		s.defaultValue = defaultValue
		s.compute = compute
		s.minValue = minValue
		s.maxValue = maxValue
		s.format = format
		s.alias = alias
		s.trim = trim
		s.decimalSeparator = decimalSeparator
		s.description = description
		s.extended = extended
		s.getMethod = getMethod
		
		MapUtils.ToJson(s)
	}
	
	/**
	 * Assign from field
	 * @param f
	 */
	public void assign (Field f) {
		type = (f.type != Type.OBJECT)?f.type:type
		typeName = f.typeName
		dbType = f.dbType
		isNull = (!f.isNull)?false:isNull
		isKey = (f.isKey)?true:isKey
		ordKey = (f.ordKey != null)?f.ordKey:ordKey
		length = (f.length > 0)?f.length:length
		precision = (f.precision > 0)?f.precision:precision
		isAutoincrement = (f.isAutoincrement)?true:isAutoincrement
		isReadOnly = (f.isReadOnly)?true:isReadOnly
		defaultValue = (f.defaultValue != null)?f.defaultValue:defaultValue
		compute = (f.compute != null)?f.compute:compute
		minValue = (f.minValue != null)?f.minValue:minValue
		maxValue = (f.maxValue != null)?f.maxValue:maxValue
		format = (f.format != null)?f.format:format
		alias = (f.alias != null)?f.alias:alias
		trim = BoolUtils.IsValue(f.trim, trim)
		decimalSeparator = (f.decimalSeparator != null)?f.decimalSeparator:decimalSeparator
		description = (f.description != null)?f.description:description
		extended.putAll(f.extended)
	}
	
	/**
	 * Clone field
	 * @return
	 */
	public Field copy() {
		return new Field(
				name: this.name, type: this.type, typeName: this.typeName, dbType: this.dbType, isNull: this.isNull, length: this.length, precision: this.precision, isKey: this.isKey, ordKey: this.ordKey,
				isAutoincrement: this.isAutoincrement, isReadOnly: this.isReadOnly, defaultValue: this.defaultValue, compute: this.compute, 
				minValue: this.minValue, maxValue: this.maxValue, format: this.format, alias: this.alias, trim: this.trim, 
				decimalSeparator: this.decimalSeparator, description: this.description, extended: CloneUtils.CloneMap(extended))
	}
	
	public static boolean IsConvertibleType(Field.Type source, Field.Type dest) {
		if (source == dest) return true
		
		boolean res = false
		switch (dest) {
			case Field.Type.STRING: case Field.Type.TEXT:
				res = true
				break
				
			case Field.Type.BIGINT:
				res = (source in [Field.Type.INTEGER, Field.Type.NUMERIC])
				break
				
			case Field.Type.DATETIME:
				res = (source in [Field.Type.DATE, Field.Type.TIME])
				break
				
			case Field.Type.DOUBLE:
				res = (source in [Field.Type.NUMERIC, Field.Type.INTEGER, Field.Type.BIGINT])
				break
				
			case Field.Type.NUMERIC:
				res = (source in [Field.Type.DOUBLE, Field.Type.INTEGER, Field.Type.BIGINT])
				break
				
			case Field.Type.BLOB:
				res = (source in [Field.Type.STRING, Field.Type.TEXT])
				break
				
		}
		
		res
	}
}