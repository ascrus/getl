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
	public static enum Type {
		STRING, INTEGER, BIGINT, NUMERIC, DOUBLE, BOOLEAN, DATE, TIME, DATETIME, BLOB, TEXT, OBJECT, ROWID, UUID
	}

	/**
	 * Field name	
	 * @return
	 */
	private String name = null
	public String getName() { return this.name }
	public void setName(String value) { this.name = value }
	
	/**
	 * Field type
	 * @return
	 */
	private Type type = Type.STRING
	public Type getType() { return this.type }
	public void setType(Type value) { this.type = value }
	
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
	 */
	private boolean isNull = true
	public boolean getIsNull() { return this.isNull }
	public void setIsNull(boolean value) { this.isNull = value }
	
	/**
	 * Length of value
	 */
	private Integer length
	public Integer getLength() { return this.length }
	public void setLength(Integer value) { this.length = value }
	
	/**
	 * Precision number of numeric value
	 */
	private Integer precision
	public Integer getPrecision() { return this.precision }
	public void setPrecision(Integer value) { this.precision = value }
	
	/**
	 * Field is primary key
	 */
	private boolean isKey = false
	public boolean getIsKey() { return this.isKey }
	public void setIsKey(boolean value) {
		this.isKey = value
		if (this.isKey) this.isNull = false else this.ordKey = null
	}
	
	/**
	 * Number order from primary key
	 */
	private Integer ordKey
	public Integer getOrdKey() { return this.ordKey }
	public void setOrdKey(Integer value) { ordKey = value }

	/**
	 * Use field in partition key
	 */
	private boolean isPartition = false
	public Boolean getInPartition() { return this.isPartition}
	public void setIsPartition(Boolean value) { this.isPartition = value }

	/**
	 * Number order if field use in partition key
	 */
	private Integer ordPartition
	public Integer getOrdPartition() { return this.ordPartition }
	public void setOrdPartition(Integer value) { this.ordPartition = value }
	
	/**
	 * Field is auto increment
	 */
	private boolean isAutoincrement = false
	public boolean getIsAutoincrement() { return this.isAutoincrement }
	public void setIsAutoincrement(boolean value) { this.isAutoincrement = value }
	
	/**
	 * Field can not write
	 */
	private boolean isReadOnly = false
	public boolean getIsReadOnly() { return this.isReadOnly }
	public void setIsReadOnly(boolean value) { this.isReadOnly = value }
	
	/**
	 * Default value from field (used only creating dataset)
	 */
	private String defaultValue = null
	public String getDefaultValue() { return this.defaultValue }
	public void setDefaultValue(String value) { this.defaultValue = value }
	
	/**
	 * Compute columns
	 */
	private String compute
	public String getCompute() { return this.compute }
	public void setCompute(String value) { this.compute = value }
	
	/**
	 * Minimum value (for validation and generation)
	 */
	private def minValue = null
	public def getMinValue() { return this.minValue }
	public void setMinValue(def value) { this.minValue = value }
	
	/**
	 * Minimum value (for validation and generation)
	 */
	private def maxValue = null
	public def getMaxValue() { return this.maxValue }
	public void setMaxValue(def value) { this.maxValue = value }
	
	/**
	 * Format pattern on numeric and datetime fields
	 */
	private String format
	public String getFormat () { return this.format }
	public void setFormat(String value) { this.format = value }
	
	/**
	 * Name of the field in the data source (if different from Field name)
	 */
	private String alias
	public String getAlias() { return this.alias }
	public void setAlias(String value) { this.alias = value }
	
	/**
	 * Trim space (used for reading datasource)
	 */
	private boolean trim = false
	public boolean getTrim() { return this.trim }
	public void setTrim(boolean value) { this.trim = value }
	
	/**
	 * Decimal separator
	 */
	private String decimalSeparator
	public String getDecimalSeparator() { return this.decimalSeparator }
	public void setDecimalSeparator (String value) { this.decimalSeparator = value }
	
	/**
	 * Field description (comments)
	 */
	private String description
	public String getDescription() { return this.description }
	public void setDescription(String value) { this.description = value }
	
	/**
	 * Extended attributes
	 */
	private final Map extended = [:] as Map<String, Object>
	public Map getExtended() { return this.extended }
	public void setExtended (Map value) {
		this.extended.clear()
		if (value != null) this.extended.putAll(value)
	}
	
	/**
	 * Get value method
	 */
	public String getMethod
	
	/**
	 * Allow length for field
	 */
	public static boolean AllowLength(Field f) {
		return (f.type in [Field.Type.STRING, Field.Type.NUMERIC, Field.Type.BLOB, Field.Type.TEXT, Field.Type.ROWID])
	}
	
	/**
	 * Allow precision for field
	 * @param f
	 * @return
	 */
	public static boolean AllowPrecision(Field f) {
		return (f.type in [Field.Type.NUMERIC])
	}
	
	/**
	 * Allow create field in table
	 * @param f
	 * @return
	 */
	public static boolean AllowCreatable(Field f) {
		return !(f.type in [Field.Type.ROWID])
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
	public static Field ParseMap(Map strField) {
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
	 * @param f
	 */
	public void assign(Field f) {
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
	 * @return
	 */
	public Field copy() {
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
		
		return res
	}

    /**
     * Valid equal current object by object
     * @param other
     * @return
     */
    public boolean canEqual(java.lang.Object other) {
        return other instanceof Field
    }

	public boolean equalsAll(java.lang.Object other) {
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
	public boolean equals(java.lang.Object other) {
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