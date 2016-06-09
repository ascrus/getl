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

package getl.utils

import getl.data.*
import getl.data.Field.Type
import getl.exception.ExceptionGETL
import getl.jdbc.*
import groovy.json.JsonSlurper

/**
 * Generation code library functions class 
 * @author Alexsey Konstantinov
 *
 */
class GenerationUtils {
	public static final Long EMPTY_BIGINT
	public static final def EMPTY_BLOB
	public static final def EMPTY_CLOB
	public static final Boolean EMPTY_BOOLEAN
	public static final Date EMPTY_DATE
	public static final java.sql.Timestamp EMPTY_DATETIME
	public static final Double EMPTY_DOUBLE
	public static final Integer EMPTY_INTEGER
	public static final BigDecimal EMPTY_NUMERIC
	public static final def EMPTY_OBJECT
	public static final String EMPTY_STRING
	public static final def EMPTY_TEXT
	public static final java.sql.Time EMPTY_TIME
	
	/**
	 * Convert string alias as a modifier to access the value of field
	 * @param value
	 * @return
	 */
	public static String ProcessAlias(String value, boolean quote) {
		List<String> elementsPath = value.split("[.]") .toList()
		for (int i = 0; i < elementsPath.size(); i++) {
			elementsPath[i] = ((quote)?'"':"") + elementsPath[i] + ((quote)?'"':"") + ((i < elementsPath.size() - 1)?"?":"")
		}
		elementsPath.join(".")
	}
	
	/** 
	 * Convert alias of field as a modifier to access the value of field
	 * @param field
	 * @param quote
	 * @return
	 */
	public static String Field2Alias(Field field, boolean quote) {
		String a = (field.alias != null)?field.alias:field.name
		ProcessAlias(a, quote)
	}
	
	/**
	 * Convert alias of field as a modifier to access the value of field
	 * @param field
	 * @return
	 */
	public static String Field2Alias(Field field) {
		Field2Alias(field, true)
	}
	
	
	/**
	 * Generation code create empty value as field type into variable
	 * @param t
	 * @param v
	 * @return
	 */
	public static String GenerateEmptyValue(getl.data.Field.Type type, String variableName) {
		def r = ""
		switch (type) {
			case getl.data.Field.Type.STRING:
				r = "String ${variableName}"
				break
			case getl.data.Field.Type.BOOLEAN:
				r =  "Boolean ${variableName}"
				break
			case getl.data.Field.Type.INTEGER:
				r =  "Integer ${variableName}"
				break
			case getl.data.Field.Type.BIGINT:
				r =  "Long ${variableName}"
				break
			case getl.data.Field.Type.NUMERIC:
				r =  "BigDecimal ${variableName}"
				break
			case getl.data.Field.Type.DOUBLE:
				r =  "Double ${variableName}"
				break
			case getl.data.Field.Type.DATE:
				r =  "java.sql.Date ${variableName}"
				break
			case getl.data.Field.Type.DATETIME:
				r =  "java.sql.Timestamp ${variableName}"
				break
			case getl.data.Field.Type.TIME:
				r = "java.sql.Time ${variableName}"
				break
			case getl.data.Field.Type.OBJECT: case getl.data.Field.Type.BLOB: case getl.data.Field.Type.TEXT:
				r =  "def ${variableName}"
				break
			default:
				throw new ExceptionGETL("Type ${type} not supported")
		}
		r
	}
	
	public static String DateFormat(getl.data.Field.Type type) {
		String df
		
		if (type == getl.data.Field.Type.DATE)
			df = "yyyy-MM-dd"
		else if (type == getl.data.Field.Type.TIME)
			df = "HH:mm:ss"
		else if (type == getl.data.Field.Type.DATETIME)
			df = "yyyy-MM-dd HH:mm:ss"
		else
			throw new ExceptionGETL("Can not return date format from \"${type}\" type")

		df
	}
	
	/**
	 * Generate convert code from source field to destination field
	 * @param dest
	 * @param source
	 * @param dataformat
	 * @param sourceValue
	 * @param nullValue
	 * @return
	 */
	public static String GenerateConvertValue(Field dest, Field source, String dataformat, String sourceValue, String nullValue) {
		if (dest.type == source.type) {
			return "(${sourceValue} != null)?${sourceValue}:${nullValue}"
		}
		
		def r = ""
		
		switch (dest.type) {
			case getl.data.Field.Type.STRING:
				if (source.type == getl.data.Field.Type.DATE || source.type == getl.data.Field.Type.TIME || source.type == getl.data.Field.Type.DATETIME) {
					dataformat = (dataformat != null)?dataformat:GenerationUtils.DateFormat(source.type)
					r =  "${sourceValue}.format(\"${dataformat}\")"
				}
				else {
					r = "(${sourceValue} != null)?String.valueOf(${sourceValue}):${nullValue}"
				}
				
				break
			case getl.data.Field.Type.BOOLEAN:
				if (source.type == getl.data.Field.Type.INTEGER || source.type == getl.data.Field.Type.BIGINT)
					r = "(${sourceValue} != null)?Boolean.valueOf(${sourceValue} == 1):${nullValue}"
				else if (source.type == getl.data.Field.Type.STRING) {
					def bf = ["true", "false"]
					if (dataformat != null) {
						bf = dataformat.toLowerCase().split("[|]")
					}
					r =  "(${sourceValue} != null)?(${sourceValue}.toLowerCase() == \"${bf[0]}\"):${nullValue}"
				}
				else
					throw new ExceptionGETL("Unknown how convert type ${source.type} to ${dest.type} (${source.name}->${dest.name})")
				
				break
			case getl.data.Field.Type.INTEGER:
				if (source.type == getl.data.Field.Type.STRING)
					r =  "(${sourceValue} != null)?new Integer(${sourceValue}):${nullValue}"
				else if (source.type == getl.data.Field.Type.DOUBLE || source.type == getl.data.Field.Type.NUMERIC || source.type == getl.data.Field.Type.BIGINT)
					r = "(${sourceValue} != null)?Integer.valueOf(${sourceValue}.intValue()):${nullValue}"
				else if (source.type == getl.data.Field.Type.BOOLEAN)
					r = "(${sourceValue} != null)?Integer.valueOf((${sourceValue})?1:0):${nullValue}"
				else
					throw new ExceptionGETL("Unknown how convert type ${source.type} to ${dest.type} (${source.name}->${dest.name})")
				
				break
				
			case getl.data.Field.Type.BIGINT:
				if (source.type == getl.data.Field.Type.STRING)
					r =  "(${sourceValue} != null)?new Long(${sourceValue}):${nullValue}"
				else if (source.type == getl.data.Field.Type.INTEGER)
					r = "(${sourceValue} != null)?Long.valueOf(${sourceValue}):${nullValue}"
				else if (source.type == getl.data.Field.Type.DOUBLE || source.type == getl.data.Field.Type.NUMERIC)
					r = "(${sourceValue} != null)?Long.valueOf(${sourceValue}.longValue()):${nullValue}"
				else if (source.type == getl.data.Field.Type.BOOLEAN)
					r = "(${sourceValue} != null)?Long.valueOf((${sourceValue})?1:0):${nullValue}"
				else
					throw new ExceptionGETL("Unknown how convert type ${source.type} to ${dest.type} (${source.name}->${dest.name})")
				
				break
				
			case getl.data.Field.Type.NUMERIC:
				if (source.type == getl.data.Field.Type.STRING)
					r = "(${sourceValue} != null)?new BigDecimal(${sourceValue}):${nullValue}"
				else if (source.type == getl.data.Field.Type.BOOLEAN)
					r = "(${sourceValue} != null)?BigDecimal.valueOf((${sourceValue})?1:0):${nullValue}"
				else if (source.type == getl.data.Field.Type.INTEGER || source.type == getl.data.Field.Type.DOUBLE || source.type == getl.data.Field.Type.BIGINT)
					r =  "(${sourceValue} != null)?BigDecimal.valueOf(${sourceValue}):${nullValue}"
				else
					throw new ExceptionGETL("Unknown how convert type ${source.type} to ${dest.type} (${source.name}->${dest.name})")

				break
			case getl.data.Field.Type.DOUBLE:
				if (source.type == getl.data.Field.Type.STRING)
					r =  "(${sourceValue} != null)?new Double(${sourceValue}):${nullValue}"
				if (source.type == getl.data.Field.Type.BOOLEAN)
					r = "(${sourceValue} != null)?Double.valueOf((${sourceValue})?1:0):${nullValue}"
				else if (source.type == getl.data.Field.Type.INTEGER || source.type == getl.data.Field.Type.NUMERIC || source.type == getl.data.Field.Type.BIGINT)
					r =  "(${sourceValue} != null)?Double.valueOf(${sourceValue}):${nullValue}"
				else
					throw new ExceptionGETL("Unknown how convert type ${source.type} to ${dest.type} (${source.name}->${dest.name})")
				
				break
			case getl.data.Field.Type.DATE:
				if (source.type != getl.data.Field.Type.STRING && source.type != getl.data.Field.Type.DATETIME) throw new ExceptionGETL("Unknown how convert type ${source.type} to ${dest.type} (${source.name}->${dest.name})")
				if (source.type == getl.data.Field.Type.STRING) {
					dataformat = (dataformat != null)?dataformat:GenerationUtils.DateFormat(dest.type)
					r =  "(${sourceValue} != null)?getl.utils.DateUtils.ParseDate(\"${dataformat}\", ${sourceValue}):${nullValue}"
				}
				else {
					r = "(${sourceValue} != null)?org.codehaus.groovy.runtime.DateGroovyMethods.clearTime(${sourceValue}):${nullValue}"
				}
				
				break
				
			case getl.data.Field.Type.DATETIME:
				if (source.type != getl.data.Field.Type.STRING && source.type != getl.data.Field.Type.DATE) throw new ExceptionGETL("Unknown how convert type ${source.type} to ${dest.type} (${source.name}->${dest.name})")
				
				if (source.type == getl.data.Field.Type.STRING) {
					dataformat = (dataformat != null)?dataformat:GenerationUtils.DateFormat(dest.type)
					r =  "(${sourceValue} != null)?getl.utils.DateUtils.ParseDate(\"${dataformat}\", ${sourceValue}):${nullValue}"
				}
				else {
					r = "${sourceValue}"
				}
				
				break
				
			case getl.data.Field.Type.TIME:
				if (source.type == getl.data.Field.Type.INTEGER || source.type == getl.data.Field.Type.BIGINT) {
					r = "(${sourceValue} != null)?new Time(${sourceValue}):${nullValue}"
				}
				else if (source.type == getl.data.Field.Type.STRING) {
					r = "(${sourceValue} != null)?Time.valueOf(${sourceValue}):${nullValue}"
				}
				if (r == "") throw new ExceptionGETL("Unknown how convert type ${source.type} to ${dest.type} (${source.name}->${dest.name})")
				break
				
			case getl.data.Field.Type.OBJECT: case getl.data.Field.Type.BLOB: case getl.data.Field.Type.TEXT:
				throw new ExceptionGETL("Unknown how convert type ${source.type} to ${dest.type} (${source.name}->${dest.name})")
				
				break
			default:
				throw new ExceptionGETL("Type ${dest.type} not supported (${dest.name})")
		}
		r
	}
	
	public static String GenerateConvertValue(Field dest, Field source, String dataformat, String sourceValue) {
		if (dest.type == source.type) {
			return "${sourceValue}"
		}
		
		def r = ""
		
		switch (dest.type) {
			case getl.data.Field.Type.STRING:
				if (source.type == getl.data.Field.Type.DATE || source.type == getl.data.Field.Type.TIME || source.type == getl.data.Field.Type.DATETIME) {
					dataformat = (dataformat != null)?dataformat:GenerationUtils.DateFormat(source.type)
					r =  "getl.utils.DateUtils.FormatDate(\"${dataformat}\", (Date)${sourceValue})"
				}
				else {
					r = "getl.utils.ConvertUtils.Object2String(${sourceValue})"
				}
				
				break
			case getl.data.Field.Type.BOOLEAN:
				if (source.type == getl.data.Field.Type.INTEGER || source.type == getl.data.Field.Type.BIGINT)
					r = "getl.utils.ConvertUtils.Int2Boolean((Integer){sourceValue}?.intValue())"
				else if (source.type == getl.data.Field.Type.STRING) {
					def bf = ["true", "false"]
					if (dataformat != null) {
						bf = dataformat.toLowerCase().split("[|]")
					}
					r =  "getl.utils.ConvertUtils.String2Boolean((String)${sourceValue}, \"${bf[0]}\")"
				}
				else
					throw new ExceptionGETL("Unknown how convert type ${source.type} to ${dest.type} (${source.name}->${dest.name})")
				
				break
			case getl.data.Field.Type.INTEGER:
				if (source.type == getl.data.Field.Type.STRING)
					r =  "getl.utils.ConvertUtils.Object2Int(${sourceValue})"
				else if (source.type == getl.data.Field.Type.DOUBLE || source.type == getl.data.Field.Type.NUMERIC || source.type == getl.data.Field.Type.BIGINT)
					r = "${sourceValue}?.intValue()"
				else if (source.type == getl.data.Field.Type.BOOLEAN)
					r = "getl.utils.ConvertUtils.Boolean2Int((Boolean)${sourceValue})"
				else
					throw new ExceptionGETL("Unknown how convert type ${source.type} to ${dest.type} (${source.name}->${dest.name})")
				
				break
				
			case getl.data.Field.Type.BIGINT:
				if (source.type == getl.data.Field.Type.STRING)
					r =  "getl.utils.ConvertUtils.Object2Long(${sourceValue})"
				else if (source.type == getl.data.Field.Type.INTEGER || source.type == getl.data.Field.Type.DOUBLE || source.type == getl.data.Field.Type.NUMERIC)
					r = "${sourceValue}?.longValue()"
				else if (source.type == getl.data.Field.Type.BOOLEAN)
					r = "getl.utils.ConvertUtils.Boolean2Int((Boolean)${sourceValue})?.longValue()"
				else
					throw new ExceptionGETL("Unknown how convert type ${source.type} to ${dest.type} (${source.name}->${dest.name})")
				
				break
				
			case getl.data.Field.Type.NUMERIC:
				if (source.type == getl.data.Field.Type.STRING)
					r =  "getl.utils.ConvertUtils.Object2BigDecimal(${sourceValue})" 
				else if (source.type == getl.data.Field.Type.INTEGER || source.type == getl.data.Field.Type.DOUBLE || source.type == getl.data.Field.Type.BIGINT)
					r = "getl.utils.ConvertUtils.Object2BigDecimal(${sourceValue})"
				else if (source.type == getl.data.Field.Type.BOOLEAN)
					r = "getl.utils.ConvertUtils.Boolean2BigDecimal((Boolean)${sourceValue})"
				else
					throw new ExceptionGETL("Unknown how convert type ${source.type} to ${dest.type} (${source.name}->${dest.name})")

				break
			case getl.data.Field.Type.DOUBLE:
				if (source.type == getl.data.Field.Type.STRING || source.type == getl.data.Field.Type.INTEGER || source.type == getl.data.Field.Type.NUMERIC || source.type == getl.data.Field.Type.BIGINT)
						r =  "getl.utils.ConvertUtils.Object2Double(${sourceValue})"
				else if (source.type == getl.data.Field.Type.BOOLEAN)
					r = "getl.utils.ConvertUtils.Boolean2Double((Boolean)${sourceValue})"
				else
					throw new ExceptionGETL("Unknown how convert type ${source.type} to ${dest.type} (${source.name}->${dest.name})")
				
				break
			case getl.data.Field.Type.DATE:
				if (source.type != getl.data.Field.Type.STRING && source.type != getl.data.Field.Type.DATETIME) throw new ExceptionGETL("Unknown how convert type ${source.type} to ${dest.type} (${source.name}->${dest.name})")
				if (source.type == getl.data.Field.Type.STRING) {
					dataformat = (dataformat != null)?dataformat:GenerationUtils.DateFormat(dest.type)
					r =  "getl.utils.DateUtils.ParseDate(\"${dataformat}\", (String)${sourceValue})"
				}
				else {
					r = "getl.utils.DateUtils.ClearTime((Date)${sourceValue})"
				}
				
				break
				
			case getl.data.Field.Type.DATETIME:
				if (source.type != getl.data.Field.Type.STRING && source.type != getl.data.Field.Type.DATE) throw new ExceptionGETL("Unknown how convert type ${source.type} to ${dest.type} (${source.name}->${dest.name})")
				
				if (source.type == getl.data.Field.Type.STRING) {
					dataformat = (dataformat != null)?dataformat:GenerationUtils.DateFormat(dest.type)
					r =  "getl.utils.DateUtils.ParseDate(\"${dataformat}\", (String)${sourceValue})"
				}
				else {
					r = "${sourceValue}"
				}
				
				break
				
			case getl.data.Field.Type.TIME:
				if (source.type == getl.data.Field.Type.BIGINT)
					r = "getl.utils.ConvertUtils.Long2Time((Long)${sourceValue})"
				else if (source.type == getl.data.Field.Type.STRING)
					r = "getl.utils.ConvertUtils.String2Time((String)${sourceValue})"
				else
					throw new ExceptionGETL("Unknown how convert type ${source.type} to ${dest.type} (${source.name}->${dest.name})")
				
				break
				
			case getl.data.Field.Type.TEXT:
				if (source.type == getl.data.Field.Type.STRING)
					r = "new javax.sql.rowset.serial.SerialClob(((String)${sourceValue}).chars)"
				else
					r = "${sourceValue}"
				
				break
				
			case getl.data.Field.Type.OBJECT: case getl.data.Field.Type.BLOB: 
				r = "${sourceValue}"
				
				break
			default:
				throw new ExceptionGETL("Type ${dest.type} not supported (${dest.name})")
		}
		
		r
	}
	
	public static final Random random = new Random()
	
	public static int GenerateInt () {
		random.nextInt()
	}
	
	public static int GenerateInt (int minValue, int maxValue) {
		def res = minValue - 1
		while (res < minValue) res = random.nextInt(maxValue + 1)
		res
	}
	
	public static String GenerateString (int length) {
		String result = ""
		while (result.length() < length) result += ((result.length() > 0)?" ":"") + StringUtils.RandomStr().replace('-', ' ')
		
		def l2 = (int)(length / 2)
		def l = GenerateInt(l2, length)
		
		StringUtils.LeftStr(result + "a", l)
	}
	
	public static long GenerateLong () {
		random.nextLong()
	}
	
	public static BigDecimal GenerateNumeric () {
		BigDecimal.valueOf(random.nextDouble()) + random.nextInt()
	}
	
	public static BigDecimal GenerateNumeric (int precision) {
		NumericUtils.Round(BigDecimal.valueOf(random.nextDouble()) + random.nextInt(), precision)
	}
	
	public static BigDecimal GenerateNumeric (int length, int precision) {
		BigDecimal res
		def intSize = length - precision
		if (intSize == 0) {
			res = NumericUtils.Round(BigDecimal.valueOf(random.nextDouble()), precision)
		}
		else if (intSize < 15) {
			int lSize = Math.pow(10, intSize) - 1
			res = NumericUtils.Round(BigDecimal.valueOf(random.nextDouble()) + random.nextInt(lSize), precision)
		}
		else {
			res = NumericUtils.Round(BigDecimal.valueOf(random.nextDouble()) + random.nextLong(), precision)
		}
	}
	
	public static double GenerateDouble () {
		random.nextDouble() + random.nextLong()
	}
	
	public static boolean GenerateBoolean () {
		random.nextBoolean()
	}
	
	public static Date GenerateDate() {
		DateUtils.AddDate("dd", -GenerateInt(0, 365), DateUtils.CurrentDate())
	}
	
	public static Date GenerateDate(int days) {
		DateUtils.AddDate("dd", -GenerateInt(0, days), DateUtils.CurrentDate())
	}
	
	public static Date GenerateDateTime() {
		DateUtils.AddDate("ss", -GenerateInt(0, 525600), DateUtils.Now())
	}
	
	public static Date GenerateDateTime(int seconds) {
		DateUtils.AddDate("ss", -GenerateInt(0, seconds), DateUtils.Now())
	}
	
	
	public static def GenerateValue (Field f) {
		GenerateValue(f, null)
	}
	
	/**
	 * Generate random value from fields
	 * @param f
	 * @param rowID
	 * @return
	 */
	public static def GenerateValue (Field f, def rowID) {
		def result
		def l = f.length?:1
		
		if (f.isNull && GenerateBoolean()) return null
		
		switch (f.type) {
			case getl.data.Field.Type.STRING:
				result = GenerateString(l)
				break
			case getl.data.Field.Type.BOOLEAN:
				result = GenerateBoolean()
				break
			case getl.data.Field.Type.INTEGER: case getl.data.Field.Type.BIGINT:
				if (f.isKey && rowID != null) {
					result = rowID
				}
				else {
					if (f.minValue == null && f.maxValue == null) result = GenerateInt() else result = GenerateInt(f.minValue?:0, f.maxValue?:1000000)
				}
				if (f.type == getl.data.Field.Type.BIGINT) result = result.longValue()
				
				break 
			case getl.data.Field.Type.NUMERIC:
				if (f.isKey && rowID != null) {
					result = rowID
				}
				else {
					result = GenerateNumeric(f.length, f.precision)
				}
				break
			case getl.data.Field.Type.DOUBLE:
				result = GenerateDouble()
				break
			case getl.data.Field.Type.DATE:
				result = GenerateDate()
				break
			case getl.data.Field.Type.TIME:
				result = DateUtils.CurrentTime()
				break
			case getl.data.Field.Type.DATETIME:
				result = GenerateDateTime()
				break
			default:
				result = GenerateString(l)
		}
		
		result
	}
	
	public static Map GenerateRowValues (List<Field> field) {
		GenerateRowValues(field, null)
	}
	
	public static Map GenerateRowValues (List<Field> field, def rowID) {
		Map row = [:]
		field.each { Field f ->
			def fieldName = f.name.toLowerCase()
			def value = GenerateValue(f, rowID)
			row."$fieldName" = value
		}
		
		row
	}
	
	/**
	 * Return string value for generators 
	 * @param value
	 * @return
	 */
	public static String GenerateStringValue (String value) {
		if (value == null) return "null"
		return '"' + value.replace('"', '\\"') + '"'
	}
	
	public static String GenerateCommand(String command, int numTab, boolean condition) {
		if (!condition) return ""
		StringUtils.Replicate("\t", numTab) + command
	}
	
	/**
	 * Generation groovy closure create fields from dataset fields
	 * @param fields
	 * @return
	 */
	public static String GenerateScriptAddFields (List<Field> fields) {
		StringBuilder sb = new StringBuilder()
		sb << "{\nList<Field> res = []\n\n"
		fields.each { Field f ->
			sb << """// ${f.name}
res << new Field(
		name: ${GenerateStringValue(f.name)}, 
		type: ${GenerateStringValue(f.type.toString())},
"""
		def cmd = []
		def c

		c = GenerateCommand("length: ${f.length}", 2, Field.AllowLength(f) && f.length != null)
		if (c != "") cmd << c
		  
		c = GenerateCommand("precision: ${f.precision}", 2, Field.AllowPrecision(f) && f.precision != null)
		if (c != "") cmd << c
		
		c = GenerateCommand("isNull: ${f.isNull}", 2, !f.isNull)
		if (c != "") cmd << c
		
		c = GenerateCommand("isKey: ${f.isKey}", 2, f.isKey)
		if (c != "") cmd << c
		
		c = GenerateCommand("isAutoincrement: ${f.isAutoincrement}", 2, f.isAutoincrement)
		if (c != "") cmd << c
		
		c = GenerateCommand("isReadOnly: ${f.isReadOnly}", 2, f.isReadOnly)
		if (c != "") cmd << c
		
		c = GenerateCommand("defaultValue: ${GenerateStringValue(f.defaultValue)}", 2, f.defaultValue != null)
		if (c != "") cmd << c
		
		c = GenerateCommand("compute: ${GenerateStringValue(f.compute)}", 2, f.compute != null)
		if (c != "") cmd << c
		
		c = GenerateCommand("minValue: ${f.minValue}", 2, f.minValue != null)
		if (c != "") cmd << c
		
		c = GenerateCommand("maxValue: ${f.maxValue}", 2, f.maxValue != null)
		if (c != "") cmd << c
		
		c = GenerateCommand("format: ${GenerateStringValue(f.format)}", 2, f.format != null)
		if (c != "") cmd << c
		
		c = GenerateCommand("alias: ${GenerateStringValue(f.alias)}", 2, f.alias != null)
		if (c != "") cmd << c
		
		c = GenerateCommand("trim: ${f.trim}", 2, f.trim)
		if (c != "") cmd << c
		
		c = GenerateCommand("decimalSeparator: ${GenerateStringValue(f.decimalSeparator)}", 2, f.decimalSeparator != null)
		if (c != "") cmd << c
		
		c = GenerateCommand("description: ${GenerateStringValue(f.description)}", 2, f.description != null)
		if (c != "") cmd << c
		
		sb << cmd.join(",\n")
		
sb << """
	)

"""
		}
		sb << "res\n\n}"
		
		sb.toString()
	}
	
	/**
	 * Convert list of field to Map structure
	 * @param fields
	 * @return
	 */
	public static Map Fields2Map (List<Field> fields) {
		if (fields == null) return null
		
		def res = [:]
		res.fields = []
		def l = res.fields
		
		fields.each { Field f ->
			l << f.toMap()
		}
		
		res
	}
	
	/**
	 * Convert list of field to name of list structure
	 * @param fields
	 * @return
	 */
	public static List<String> Fields2List (JDBCDataset dataset, List<String> excludeFields) {
		if (dataset == null) return null
		
		def res = []
		
		dataset.field.each { Field f ->
			if (excludeFields != null && excludeFields.find { it.toLowerCase() == f.name.toLowerCase() } != null) return
			res << dataset.sqlObjectName(f.name)
		}
		
		res
	}
	
	/**
	 * Convert list of field to JSON string
	 * @param fields
	 * @return
	 */
	public static String GenerateJsonFields (List<Field> fields) {
		MapUtils.ToJson(Fields2Map(fields))
	}
	
	/**
	 * Convert map to list of field
	 * @param value
	 * @return
	 */
	public static List<Field> Map2Fields (Map value) {
		List<Field> res = []
		
		value.fields?.each {
			res << Field.ParseMap(it)
		}
		
		res
	}
	
	/**
	 * Parse JSON to list of field
	 * @param value
	 * @return
	 */
	public static List<Field> ParseJsonFields (String value) {
		if (value == null) return null
		
		def b = new JsonSlurper()
		def l = b.parseText(value)
		
		Map2Fields(l)
	}
	
	/**
	 * Remove fields in list of field by field name and return removed fields
	 * @param fields
	 * @param names
	 */
	public static List<Field> RemoveFields (List<Field> fields, List<String> names) {
		List<Field> res = []
		names.each { name ->
			name = name.toLowerCase()
			def o = fields.find { Field f -> f.name.toLowerCase() == name }
			if (o != null) {
				res << o
				fields.remove(o)
			}
		}
		res
	}

	/**
	 * Disable field attribute and return new list field	
	 * @param fields
	 * @param disableNotNull
	 * @param disableKey
	 * @param disableAutoincrement
	 * @param disableExtended
	 * @return
	 */
	public static List<Field> DisableAttributeField (List<Field> fields, boolean disableNotNull, boolean disableKey, boolean disableAutoincrement, 
														boolean disableExtended, boolean excludeReadOnly) {
		List<Field> res = []
		fields.each { Field f -> 
			def nf = f.copy()
			
			if (!excludeReadOnly || !nf.isReadOnly ) {
				if (disableNotNull && !nf.isNull) nf.isNull = true
				if (disableKey && nf.isKey) nf.isKey = false
				if (disableAutoincrement && nf.isAutoincrement) nf.isAutoincrement = false
				if (disableExtended && nf.extended != null) nf.extended = null
			
				res << nf
			} 
		}
		
		res
	}

	/**
	 * Run groovy script
	 * @param value
	 * @return
	 */												
	public static def EvalGroovyScript(String value) {
		EvalGroovyScript(value, [:])
	}

	/**
	 * Run groovy script
	 * @param value
	 * @param vars
	 * @return
	 */
	@groovy.transform.CompileStatic
	public static def EvalGroovyScript(String value, Map<String, Object> vars) {
		if (value == null) return null
		
		Binding bind = new Binding()
		vars?.each { String key, Object val ->
			bind.setVariable(key, val)
		}
		
		def sh = new GroovyShell(bind)
		
		def res
		try {
			res = sh.evaluate(value)
		}
		catch (Exception e) {
			Logs.Severe("Error parse [${StringUtils.CutStr(value, 1000)}]")
			StringBuilder sb = new StringBuilder("script:\n$value\nvars:")
			vars.each { varName, varValue -> sb.append("\n	$varName: ${StringUtils.LeftStr(varValue.toString(), 256)}") }
			Logs.Dump(e, 'GenerationUtils', 'EvalGroovyScript', sb.toString())
			throw e
		}
		
		res
	}
	
	/**
	 * Evaluate ${variable} in text
	 * @param value
	 * @param vars
	 * @return
	 */
	@groovy.transform.CompileStatic
	public static String EvalText(String value, Map<String, Object> vars) {
		if (value == null) return null
		vars.each { String key, Object val ->
			key = '${' + key + '}'
			value = value.replace(key, val.toString())
		}
		
		value
	}
	
	/**
	 * Convert field type to string type
	 * @param field
	 * @return
	 */
	public static void FieldConvertToString (getl.data.Field field) {
		def len
		def type = getl.data.Field.Type.STRING
		switch (field.type) {
			case getl.data.Field.Type.STRING: 
				break
			case getl.data.Field.Type.ROWID:
				field.length = 50
				break
			case getl.data.Field.Type.TEXT: case getl.data.Field.Type.BLOB:
				type = getl.data.Field.Type.STRING
				if (field.length == null) len = 65535
				break
			case getl.data.Field.Type.BIGINT:
				len = 38
				break
			case getl.data.Field.Type.INTEGER:
				len = 13
				break
			case getl.data.Field.Type.DATE: case getl.data.Field.Type.DATETIME: case getl.data.Field.Type.TIME:
				len = 30
				break
			case getl.data.Field.Type.BOOLEAN:
				len = 5
				break
			case getl.data.Field.Type.DOUBLE: 
				len = 50
				break
			case getl.data.Field.Type.NUMERIC:
				len = (field.length?:50) + 1
				break
			default:
				throw new ExceptionGETL("Not support convert field type \"${field.type}\" to \"STRING\" from field \"${field.name}\"")
		}
		field.type = type
		if (len != null) field.length = len
		field.precision = null
		field.typeName = null
	}
	
	/**
	 * Convert all dataset fields to string
	 * @param dataset
	 */
	public static void ConvertToStringFields (Dataset dataset) {
		dataset.field.each { FieldConvertToString(it) }
	}
	
	/**
	 * Return object name with SQL syntax
	 * @param name
	 * @return
	 */
	public static String SqlObjectName (JDBCConnection connection, String name) {
		JDBCDriver drv = connection.driver

		drv.prepareObjectNameForSQL(name)
	}
	
	/**
	 * Return list object name with SQL syntax 
	 * @param connection
	 * @param listNames
	 * @return
	 */
	public static List<String> SqlListObjectName (JDBCConnection connection, List<String> listNames) {
		List<String> res = []
		JDBCDriver drv = connection.driver

		listNames.each { name ->
			res << drv.prepareObjectNameForSQL(name)
		}
		
		res
	}
	
	public static String Field2ParamName(String fieldName) {
		if (fieldName == null) return null
		
		fieldName.replaceAll("(?i)[^a-z0-9_]", "_").toLowerCase()
	}
	
	/**
	 * Return key fields name by sql syntax with expression and exclude fields list
	 * @param expr - string expression with {field} and {orig} macros
	 * @return
	 */
	public static List<String> SqlKeyFields (JDBCConnection connection, List<Field> fields, String expr, List<String> excludeFields) {
		excludeFields = (excludeFields != null)?excludeFields*.toLowerCase():[]
		def kf = []
		fields.each { Field f ->
			if ((!(f.name.toLowerCase() in excludeFields)) && f.isKey) kf << f
		}
		kf.sort(true) { Field a, Field b -> (a.ordKey?:999999) <=> (b.ordKey?:999999) }
		
		List<String> res = []
		kf.each { Field f ->
			if (expr == null) {
				res << SqlObjectName(connection, f.name) 
			} 
			else {
				res << expr.replace("{orig}", f.name.toLowerCase()).replace("{field}", SqlObjectName(connection, f.name)).replace("{param}", "${Field2ParamName(f.name)}")
			}
		}
		
		res
	}
	
	/**
	 * Return fields name by sql syntax with expression and exclude fields list
	 * @param expr - string expression with {field} macros
	 * @return
	 */
	public static List<String> SqlFields (JDBCConnection connection, List<Field> fields, String expr, List<String> excludeFields) {
		excludeFields = (excludeFields != null)?excludeFields*.toLowerCase():[]
		
		List<String> res = []
		fields.each { Field f ->
			if (!(f.name.toLowerCase() in excludeFields)) {
				if (expr == null) {
					res << SqlObjectName(connection, f.name)
				} 
				else {
					res << expr.replace("{orig}", f.name.toLowerCase()).replace("{field}", SqlObjectName(connection, f.name)).replace("{param}", "${Field2ParamName(f.name)}")
				}
			}
		}
		
		res
	}
	
	/**
	 * Return values only key fields from row
	 * @param fields
	 * @param row
	 * @return
	 */
	public static Map RowKeyMapValues(List<Field> fields, Map row, List<String> excludeFields) {
		Map res = [:]
		if (excludeFields != null) excludeFields = excludeFields*.toLowerCase() else excludeFields = []
		fields.each { Field f ->
			if (f.isKey) {
				if (!(f.name.toLowerCase() in excludeFields)) res."${f.name.toLowerCase()}" = row."${f.name.toLowerCase()}"
			}
		}
		
		res
	}

	/**
	 * Return list of fields row values	
	 * @param fields
	 * @param row
	 * @return
	 */
	public static List RowListValues (List<String> fields, Map row) {
		def res = new ArrayList()
		fields.each { String n ->
			res << row."${n.toLowerCase()}"
		}
		
		res
	}
	
	/**
	 * Return map of fields row values
	 * @param fields
	 * @param row
	 * @param toLower
	 * @return
	 */
	public static Map RowMapValues (List<String> fields, Map row, boolean toLower) {
		Map res = [:]
		if (toLower) {
			fields.each { String n ->
				n = n.toLowerCase()
				res."$n" = row."$n"
			}
		}
		else {
			fields.each { String n ->
				n = n.toLowerCase()
				res."${n.toUpperCase()}" = row."$n"
			}
		}
		
		res
	}

	/**
	 * Return map of fields row values	
	 * @param fields
	 * @param row
	 * @return
	 */
	public static Map RowMapValues (List<String> fields, Map row) {
		RowMapValues(fields, row, true)
	}
	
	/**
	 * Generation row copy closure
	 * @param fields
	 * @return
	 */
	public static Map GenerateRowCopy(List<Field> fields) {
		StringBuilder sb = new StringBuilder()
		sb << "{ inRow, Map outRow, java.sql.Connection connection ->\n"
		def i = 0
		fields.each { Field f ->
			String methodGetValue = ((f.getMethod?:"{field}").replace("{field}", "inRow.'${f.name}'"))
			
			if (f.type == getl.data.Field.Type.DATE) {
				sb << "outRow.'${f.name.toLowerCase()}' = getl.utils.DateUtils.ClearTime($methodGetValue)\n"
			}
			else if (f.type == getl.data.Field.Type.DATETIME) {
				i++
				sb << """def _getl_temp_var_$i = $methodGetValue
if (_getl_temp_var_$i == null) outRow.'${f.name.toLowerCase()}' = null else outRow.'${f.name.toLowerCase()}' = new Date(_getl_temp_var_${i}.time)  
"""
			}
			else {
				sb << "outRow.'${f.name.toLowerCase()}' = $methodGetValue\n"
			}
		}
		sb << "}"
		def statement = sb.toString()
//		println statement
		Closure code = EvalGroovyScript(statement)
		
		[statement: statement, code: code]
	}
	
	/**
	 * Generation field copy by fields
	 * @param fields
	 * @return
	 */
	public static Closure GenerateFieldCopy(List<Field> fields) {
		StringBuilder sb = new StringBuilder()
		sb << "{ Map inRow, outRow ->\n"
		fields.each { Field f ->
			sb << "outRow.'${f.name}' = inRow.'${f.name.toLowerCase()}'\n"
		}
		sb << "}"
		Closure result = GenerationUtils.EvalGroovyScript(sb.toString())
		result
	}
	
	public static String GenerateSetParam(int paramNum, int fieldType, String value) {
		String res
		switch (fieldType) {
			case java.sql.Types.BIGINT:
				res = "if ($value != null) stat.setLong($paramNum, $value) else stat.setNull($paramNum, java.sql.Types.BIGINT)"
				break
				 
			case java.sql.Types.INTEGER: case java.sql.Types.SMALLINT: case java.sql.Types.TINYINT:
				res = "if ($value != null) stat.setInt($paramNum, $value) else stat.setNull($paramNum, java.sql.Types.INTEGER)"
				break
			
			case java.sql.Types.CHAR: case java.sql.Types.NCHAR:
			case java.sql.Types.LONGVARCHAR: case java.sql.Types.LONGNVARCHAR:
			case java.sql.Types.VARCHAR: case java.sql.Types.NVARCHAR:
				res = "if ($value != null) stat.setString($paramNum, $value) else stat.setNull($paramNum, java.sql.Types.VARCHAR)"
				break
			
			case java.sql.Types.BOOLEAN: case groovy.sql.Sql.BIT:
				res = "if ($value != null) stat.setBoolean($paramNum, $value) else stat.setNull($paramNum, java.sql.Types.BOOLEAN)"
				break
				
			case java.sql.Types.DOUBLE: case java.sql.Types.FLOAT: case java.sql.Types.REAL:
				res = "if ($value != null) stat.setDouble($paramNum, $value) else stat.setNull($paramNum, java.sql.Types.DOUBLE)"
				break
				
			case java.sql.Types.DECIMAL: case java.sql.Types.NUMERIC:
				res = "if ($value != null) stat.setBigDecimal($paramNum, $value) else stat.setNull($paramNum, java.sql.Types.DECIMAL)"
				break
				
			case java.sql.Types.BLOB: case java.sql.Types.LONGVARBINARY: case java.sql.Types.VARBINARY:
				res = "if ($value != null) stat.setBlob($paramNum, new javax.sql.rowset.serial.SerialBlob($value)) else stat.setNull($paramNum, java.sql.Types.BLOB)"
				break
				
			case java.sql.Types.CLOB: case java.sql.Types.NCLOB:
				res = "if ($value != null) stat.setClob($paramNum, $value) else stat.setNull($paramNum, java.sql.Types.CLOB)"
				break
				
			case java.sql.Types.DATE:
				res = "if ($value != null) stat.setDate($paramNum, new java.sql.Date(${value}.getTime())) else stat.setNull($paramNum, java.sql.Types.DATE)"
				break
				
			case java.sql.Types.TIME:
				res = "if ($value != null) stat.setTime($paramNum, new java.sql.Time(${value}.getTime())) else stat.setNull($paramNum, java.sql.Types.TIME)"
				break
				
			case java.sql.Types.TIMESTAMP:
				res = "if ($value != null) stat.setTimestamp($paramNum, new java.sql.Timestamp(${value}.getTime())) else stat.setNull($paramNum, java.sql.Types.TIMESTAMP)"
				break
				
			default:
				res = "if ($value != null) stat.setObject($paramNum, $value) else stat.setNull($paramNum, java.sql.Types.OBJECT)"
		}
		
		res
	}
}
