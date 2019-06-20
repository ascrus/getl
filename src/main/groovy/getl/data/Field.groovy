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
	/**
	 * Data type
	 */
	static enum Type {
		STRING, INTEGER, BIGINT, NUMERIC, DOUBLE, BOOLEAN, DATE, TIME, DATETIME, BLOB, TEXT, OBJECT, ROWID, UUID
	}

	private String name = null
	/**
	 * Field name
	 */
	String getName() { return this.name }
	/**
	 * Field name
	 */
	void setName(String value) { this.name = value }
	
	private Type type = Type.STRING
	/**
	 * Data type
	 */
	Type getType() { return this.type }
	/**
	 * Data type
	 */
	void setType(Type value) { this.type = value }
	
	/**
	 * Database field type
	 */
	public Object dbType
	
	/**
	 * Database field name
	 */
	public String typeName
	
	private Boolean isNull = true
	/**
	 * Value can not be null
	 */
	Boolean getIsNull() { return this.isNull }
	/**
	 * Value can not be null
	 */
	void setIsNull(Boolean value) { this.isNull = value }
	
	private Integer length
	/**
	 * Length of value
	 */
	Integer getLength() { return this.length }
	/**
	 * Length of value
	 */
	void setLength(Integer value) { this.length = value }
	
	private Integer precision
	/**
	 * Precision number of numeric value
	 */
	Integer getPrecision() { return this.precision }
	/**
	 * Precision number of numeric value
	 */
	void setPrecision(Integer value) { this.precision = value }
	
	private Boolean isKey = false
	/**
	 * Field is primary key
	 */
	Boolean getIsKey() { return this.isKey }
	/**
	 * Field is primary key
	 */
	void setIsKey(Boolean value) {
		this.isKey = value
		if (this.isKey) this.isNull = false else this.ordKey = null
	}
	
	private Integer ordKey
	/**
	 * Number order from primary key
	 */
	Integer getOrdKey() { return this.ordKey }
	/**
	 * Number order from primary key
	 */
	void setOrdKey(Integer value) { ordKey = value }

	private boolean isPartition = false
	/**
	 * Use field in partition key
	 */
	Boolean getInPartition() { return this.isPartition}
	/**
	 * Use field in partition key
	 */
	void setIsPartition(Boolean value) { this.isPartition = value }

	private Integer ordPartition
	/**
	 * Number order if field use in partition key
	 */
	Integer getOrdPartition() { return this.ordPartition }
	/**
	 * Number order if field use in partition key
	 */
	void setOrdPartition(Integer value) { this.ordPartition = value }
	
	private boolean isAutoincrement = false
	/**
	 * Field is auto increment
	 */
	boolean getIsAutoincrement() { return this.isAutoincrement }
	/**
	 * Field is auto increment
	 */
	void setIsAutoincrement(boolean value) { this.isAutoincrement = value }
	
	private boolean isReadOnly = false
	/**
	 * Field can not write
	 */
	boolean getIsReadOnly() { return this.isReadOnly }
	/**
	 * Field can not write
	 */
	void setIsReadOnly(boolean value) { this.isReadOnly = value }
	
	private String defaultValue = null
	/**
	 * Default value from field (used only creating dataset)
	 */
	String getDefaultValue() { return this.defaultValue }
	/**
	 * Default value from field (used only creating dataset)
	 */
	void setDefaultValue(String value) { this.defaultValue = value }
	
	private String compute
	/**
	 * Compute columns
	 */
	String getCompute() { return this.compute }
	/**
	 * Compute columns
	 */
	void setCompute(String value) { this.compute = value }
	
	private def minValue = null
	/**
	 * Minimum value (for validation and generation)
	 */
	def getMinValue() { return this.minValue }
	/**
	 * Minimum value (for validation and generation)
	 */
	void setMinValue(def value) { this.minValue = value }
	
	private def maxValue = null
	/**
	 * Minimum value (for validation and generation)
	 */
	def getMaxValue() { return this.maxValue }
	/**
	 * Minimum value (for validation and generation)
	 */
	void setMaxValue(def value) { this.maxValue = value }
	
	private String format
	/**
	 * Format pattern on numeric and datetime fields
	 */
	String getFormat () { return this.format }
	/**
	 * Format pattern on numeric and datetime fields
	 */
	void setFormat(String value) { this.format = value }
	
	private String alias
	/**
	 * Name of the field in the data source (if different from Field name)
	 */
	String getAlias() { return this.alias }
	/**
	 * Name of the field in the data source (if different from Field name)
	 */
	void setAlias(String value) { this.alias = value }
	
	private boolean trim = false
	/**
	 * Trim space (used for reading datasource)
	 */
	boolean getTrim() { return this.trim }
	/**
	 * Trim space (used for reading datasource)
	 */
	void setTrim(boolean value) { this.trim = value }
	
	private String decimalSeparator
	/**
	 * Decimal separator
	 */
	String getDecimalSeparator() { return this.decimalSeparator }
	/**
	 * Decimal separator
	 */
	void setDecimalSeparator (String value) { this.decimalSeparator = value }
	
	private String description
	/**
	 * Field description (comments)
	 */
	String getDescription() { return this.description }
	/**
	 * Field description (comments)
	 */
	void setDescription(String value) { this.description = value }
	
	private final Map extended = [:] as Map<String, Object>
	/**
	 * Extended attributes
	 */
	Map getExtended() { return this.extended }
	/**
	 * Extended attributes
	 */
	void setExtended (Map value) {
		this.extended.clear()
		if (value != null) this.extended.putAll(value)
	}
	
	/**
	 * Get value method
	 */
	public String getMethod

	static Field.Type getIntegerFieldType() { Field.Type.INTEGER}
	static Field.Type getBigintFieldType() { Field.Type.BIGINT}
	static Field.Type getNumericFieldType() { Field.Type.NUMERIC}
	static Field.Type getDoubleFieldType() { Field.Type.DOUBLE}
	static Field.Type getStringFieldType() { Field.Type.STRING}
	static Field.Type getTextFieldType() { Field.Type.TEXT}
	static Field.Type getDateFieldType() { Field.Type.DATE}
	static Field.Type getTimeFieldType() { Field.Type.TIME}
	static Field.Type getDatetimeFieldType() { Field.Type.DATETIME}
	static Field.Type getBooleanFieldType() { Field.Type.BOOLEAN}
	static Field.Type getBlobFieldType() { Field.Type.BLOB}
	static Field.Type getUuidFieldType() { Field.Type.UUID}
	static Field.Type getRowidFieldType() { Field.Type.ROWID}
	static Field.Type getObjectFieldType() { Field.Type.OBJECT}
	
	/**
	 * Allow length for field
	 */
	static boolean AllowLength(Field f) {
		return (f.type in [Field.Type.STRING, Field.Type.NUMERIC, Field.Type.BLOB, Field.Type.TEXT, Field.Type.ROWID])
	}
	
	/**
	 * Allow precision for field
	 */
	static boolean AllowPrecision(Field f) {
		return (f.type in [Field.Type.NUMERIC])
	}
	
	/**
	 * Allow create field in table
	 */
	static boolean AllowCreatable(Field f) {
		return !(f.type in [Field.Type.ROWID])
	}
	
	/**
	 * Build map from field
	 */
	Map toMap() {
		def n = [:]
		n.name = name
		n.type = type.toString()
		if (typeName != null) n."typeName" = typeName
		
		if (AllowLength(this) && length != null) n.length = length
		if (AllowPrecision(this) && precision != null) n.precision = precision
		if (!isNull) n.isNull = isNull
		if (isKey) n.isKey = isKey
		if (ordKey != null) n.ordKey = ordKey
		if (isPartition) n.isPartition = isPartition
		if (ordPartition != null) n.ordPartition = ordPartition
		if (isAutoincrement) n.isAutoincrement = isAutoincrement
		if (isReadOnly) n.isReadOnly = isReadOnly
		if (defaultValue != null) n.defaultValue = defaultValue
		if (compute != null) n.compute = compute
		if (format != null) n.format = format
		if (alias != null) n.alias = alias
		if (trim) n.trim = trim
		if (decimalSeparator != null) n.decimalSeparator = decimalSeparator
		if (description != null) n.description = description
		
		return n
	}
	
	/**
	 * Parse map to field
	 * @param strField
	 * @return
	 */
	static Field ParseMap(Map strField) {
		if (strField == null) throw new ExceptionGETL("Can not parse null Map to fields")
		String name = strField.name
		if (strField.name == null) throw new ExceptionGETL("Required field name: ${strField}")
		Field.Type type = strField.type?:Field.Type.STRING
		String typeName = strField.typeName
		boolean isNull = BoolUtils.IsValue(strField.isNull,true)
		Integer length = strField.length
		Integer precision = strField.precision
		boolean isKey = BoolUtils.IsValue(strField.isKey, false)
		Integer ordKey = strField.ordKey
		boolean isPartition = BoolUtils.IsValue(strField.isPartition, false)
		Integer ordPartition = strField.ordPartition
		boolean isAutoincrement = BoolUtils.IsValue(strField.isAutoincrement, false)
		boolean isReadOnly = BoolUtils.IsValue(strField.isReadOnly, false)
		String defaultValue = strField.defaultValue
		String compute = strField.compute
		def minValue = strField.minValue
		def maxValue = strField.maxValue
		String format = strField.format
		String alias = strField.alias
		boolean trim = BoolUtils.IsValue(strField.trim,false)
		String decimalSeparator = strField.decimalSeparator
		String description = strField.description
		Map extended = strField.extended
		
		return new Field(
					name: name, type: type, typeName: typeName, isNull: isNull, length: length, precision: precision,
					isKey: isKey, ordKey: ordKey, isPartition: isPartition, ordPartition: ordPartition,
					isAutoincrement: isAutoincrement, isReadOnly: isReadOnly,
					defaultValue: defaultValue, compute: compute, minValue: minValue, maxValue: maxValue,
					format: format, alias: alias, trim: trim,
					decimalSeparator: decimalSeparator, description: description, extended: extended
		)
	}
	
	/**
	 * Attribute to string
	 */
	String toString() {
		def s = [:]
		s.name = name
		s.type = type
		s.typeName = typeName
		s.isNull = isNull
		s.length = length
		s.precision = precision
		s.isKey = isKey
		s.ordKey = ordKey
		s.isPartition = isPartition
		s.ordPartition = ordPartition
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
		
		return MapUtils.ToJson(s)
	}
	
	/**
	 * Assign from field
	 */
	void assign(Field f) {
		type = (f.type != Type.OBJECT)?f.type:type
		typeName = f.typeName
		dbType = f.dbType
		isNull = (!f.isNull)?false:isNull
		isKey = (f.isKey)?true:isKey
		ordKey = (f.ordKey != null)?f.ordKey:ordKey
		isPartition = (f.isPartition)?true:isPartition
		ordPartition = (f.ordPartition != null)?f.ordPartition:ordPartition
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
	 */
	Field copy() {
		return new Field(
				name: this.name, type: this.type, typeName: this.typeName, dbType: this.dbType, isNull: this.isNull,
				length: this.length, precision: this.precision, isKey: this.isKey, ordKey: this.ordKey,
				isPartition: this.isPartition, ordPartition: this.ordPartition, isAutoincrement: this.isAutoincrement,
				isReadOnly: this.isReadOnly, defaultValue: this.defaultValue, compute: this.compute,
				minValue: this.minValue, maxValue: this.maxValue, format: this.format, alias: this.alias,
				trim: this.trim, decimalSeparator: this.decimalSeparator, description: this.description,
				extended: CloneUtils.CloneMap(extended)
		)
	}

	/**
	 * Valid convert one type to another
	 */
	static boolean IsConvertibleType(Field.Type source, Field.Type dest) {
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
		
		return res
	}

    /**
     * Valid equal current object by object
     */
    boolean canEqual(java.lang.Object other) {
        return other instanceof Field
    }

	boolean equalsAll(java.lang.Object other) {
		if (other == null) return false
		if (this.is(other)) return true
		if (!(other instanceof Field)) return false
		if (!other.canEqual(this)) return false

        def o = other as Field

        if (this.name?.toUpperCase() != o.name?.toUpperCase()) return false
        if (this.type != o.type) return false
        if (this.isNull != o.isNull) return false
        if (BoolUtils.IsValue(this.isKey) != BoolUtils.IsValue(o.isKey)) return false
        if (this.ordKey != o.ordKey) return false
		if (BoolUtils.IsValue(this.isPartition) != BoolUtils.IsValue(o.isPartition)) return false
		if (this.ordPartition != o.ordPartition) return false
        if (this.length != o.length) return false
        if (this.precision != o.precision) return false
        if (this.dbType != o.dbType) return false
        if (this.typeName?.toUpperCase() != o.typeName?.toUpperCase()) return false
        if (this.decimalSeparator != o.decimalSeparator) return false
        if (this.alias?.toUpperCase() != o.alias?.toUpperCase()) return false
        if (this.compute != o.compute) return false
        if (this.defaultValue != o.defaultValue) return false
        if (this.minValue != o.minValue) return false
        if (this.maxValue != o.maxValue) return false
        if (this.format != o.format) return false
        if (BoolUtils.IsValue(this.isAutoincrement) != BoolUtils.IsValue(o.isAutoincrement)) return false
        if (BoolUtils.IsValue(this.isReadOnly) != BoolUtils.IsValue(o.isReadOnly)) return false
        if (BoolUtils.IsValue(this.trim) != BoolUtils.IsValue(o.trim)) return false
        if (this.extended != o.extended) return false
        if (this.description != o.description) return false
        if (this.getMethod != o.getMethod) return false

		return true
	}

    @Override
	boolean equals(java.lang.Object other) {
		if (other == null) return false
		if (this.is(other)) return true
		if (!(other instanceof Field)) return false
		if (!other.canEqual(this)) return false

		def o = other as Field

		if (this.name?.toUpperCase() != o.name?.toUpperCase()) return false
		if (this.type != o.type && !(this.type in [Field.Type.STRING, Field.Type.TEXT] && o.type in [Field.Type.STRING, Field.Type.TEXT])) return false
		if (this.isNull != o.isNull) return false
		if (this.isKey != o.isKey) return false
		if (this.isPartition != o.isPartition) return false
		if (AllowLength(this) && this.length != o.length) return false
		if (AllowPrecision(this) && this.precision != o.precision) return false
		if (this.isAutoincrement != o.isAutoincrement) return false

		return true
	}
}