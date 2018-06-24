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

import getl.data.Field
import groovy.json.JsonBuilder
import groovy.json.JsonSlurper
import getl.exception.ExceptionGETL
import groovy.json.internal.LazyMap

/**
 * Map library functions class
 * @author Alexsey Konstantinov
 *
 */
@groovy.transform.CompileStatic
class MapUtils {
	/**
	 * Copy all map items as new objects with Json builder
	 * @param map
	 * @return
	 */
	public static Map DeepCopy (Map map) {
		if (map == null) return null
		
		Map res
		
		def str = ToJson(map)
		
		def json = new JsonSlurper()
		res = (Map)(json.parseText(str))

		return res
	}

	/**
	 * Set keys of map to lower case
	 * @param m
	 * @return
	 */
	public static Map MapToLower(Map<String, Object> m) {
		if (m == null) return null
		
		def r = [:]
		m.each { String k, v ->
			r.put(k.toLowerCase(), v)
		}

		return r
	}
	
	/**
	 * Clean maps with list of key
	 * @param m
	 * @param keys
	 * @return
	 */
	public static Map CleanMap (Map m, List<String> keys) {
		if (m == null) return null
		
		Map result = [:]
		result.putAll(m)
		keys.each {
			result.remove(it)
		}

		return result
	}
	
	/**
	 * Remove keys from map
	 * @param m
	 * @param keys
	 */
	public static void RemoveKeys (Map m, List<String> keys) {
		if (m == null) return
		
		keys.each {
			m.remove(it)
		}
	}
	
	/**
	 * Get only level naming map values 
	 * @param map
	 * @param level
	 * @return
	 */
	public static Map GetLevel (Map map, String level) {
		if (map == null) return null
		
		Map result = [:]
		int lengthLevel = level.length()
		map.each { k, v ->
			def s = k.toString()
			if (level.length() < s.length() && level == s.toLowerCase().substring(0, lengthLevel)) {
				result.put(s.substring(lengthLevel), v)
			}
			
		}
		return result
	}
	
	/**
	 * Clone map
	 * @param map
	 * @return
	 */
	public static Map Copy(Map map) {
		if (map == null) return null
		
		Map m = [:]
		m.putAll(map)
		
		return m
	}
	
	/**
	 * Clone map exclude specified keys
	 * @param map
	 * @param excludeKeys
	 * @return
	 */
	public static Map Copy(Map map, List<String> excludeKeys) {
		if (map == null) return null
		
		Map m = [:]
		m.putAll(map)
		if (!excludeKeys.isEmpty()) m = CleanMap(m, excludeKeys)
		
		return m
	}
	
	/**
	 * Clone map include specified keys
	 * @param map
	 * @param includeKeys
	 * @return
	 */
	public static Map CopyOnly(Map <String, Object> map, List<String> includeKeys) {
		if (map == null) return null
		
		Map res = [:]
		def keys = ListUtils.ToLowerCase(includeKeys)
		
		map.each { String k, v ->
			if (keys.find { it == k.toLowerCase()} ) res.put(k, v)
		}
		
		return res
	}
	
	/**
	 * Analize and return unknown keys in map
	 * @param map
	 * @param definedKey
	 * @param ignoreComments
	 * @return
	 */
	public static List<String> Unknown(Map<String, Object> map, List<String> definedKey, boolean ignoreComments) {
		if (map == null) map = [:]
		
		List<String> res = []
		map.each { String k, v ->
			if (ignoreComments && k.substring(0, 1) == "_") return
			def o = definedKey.find { d ->
				if (d.matches(".*[*]")) {
					def s = d.substring(0, d.length() - 1)
					return k.length() > s.length() && k.substring(0, s.length()) == s
				} 
				k == d
			}
			if (o == null) res << k
		}

		return res
	}
	
	/**
	 * Analize and return unknown keys in map
	 * @param map
	 * @param definedKey
	 * @return
	 */
	public static List<String> Unknown(Map map, List<String> definedKey) {
		 return Unknown(map, definedKey, false)
	}
	
	/**
	 * Find value of name section from Map
	 * @param content
	 * @param section
	 * @return
	 */
	public static Map FindSection (Map content, String section) {
		if (content == null) return null
		
		String[] sections = section.split("[.]")
		Map cur = content
		sections.each {
			if (cur != null) {
				if (!cur.containsKey(it)) {
					cur = null
				}
				else {
					cur = (Map)(cur.get(it))
				}
			}
		}

		return (cur != content)?cur:null
	}
	
	/**
	 * Validate existing name section from Map
	 * @param content
	 * @param section
	 * @return
	 */
	public static boolean ContainsSection (Map content, String section) {
		return (FindSection(content, section) != null)
	}
	
	/**
	 * Set value to name section from Map
	 * @param content
	 * @param name
	 * @param value
	 */
	public static void SetValue(Map<String, Object> content, String name, value) {
		if (content == null) return
		if (name == null) return
		
		String[] sections = name.split("[.]")
		Map <String, Object> cur = content
		for (int i = 0; i < sections.length - 1; i++) {
			String s = sections[i]
			if (cur.get(s) == null) {
				def m = [:]
				cur.put(s, m)
				cur = m
			}
			else {
				cur = (Map<String, Object>)(cur.get(s))
			}
		}
		cur.put(sections[sections.length - 1], value)
	}
	
	/**
	 * Merge map
	 * @param source
	 * @param added
     * @param mergeList
	 */
	public static void MergeMap (Map<String, Object> source, Map<String, Object> added, boolean existUpdate = true, boolean mergeList = false) {
		if (source == null || added == null) return
		added.each { String key, value ->
			MergeMapChildren(source, value, key, existUpdate, mergeList)
		}
	}

    /**
     * Merge children map node
     * @param source
     * @param added
     * @param key
     * @param mergeList
     */
	private static void MergeMapChildren (Map source, def added, String key, boolean existUpdate, boolean mergeList) {
		if (!(added instanceof Map)) {
			if (mergeList && added instanceof List) {
                List origList = source.get(key)
                if (origList != null) {
                    List newList = origList + (added as List)
                    source.put(key, newList.unique())
                }
                else {
                    source.put(key, added)
                }
            }
            else {
                if (!existUpdate && source.containsKey(key)) return
                source.put(key, added)
            }

			return
		}
		def c = added as Map<String, Object>
        def subsource = source.get(key) as Map<String, Object>
        if (subsource == null) {
            subsource = [:] as Map<String, Object>
            source.put(key, subsource)
        }
		c.each { String subkey, value ->
			MergeMapChildren(subsource, value, subkey, existUpdate, mergeList)
		}
	}

	/**
	 * Process arguments to Map
	 * @param args
	 * @return
	 */
	public static Map<String, Object> ProcessArguments (def args) {
		List<String> la
		if (args instanceof List) {
			la = (List<String>)args
		}
		else if (args instanceof String[]) {
			la = (args as String[]).collect() as List<String>
		}
		else if (args instanceof String) {
			la = [args as String]
		}
		else {
			throw new ExceptionGETL("Invalid arguments for processing")
		}
		Map<String, Object> res = [:]
		if (args != null) {
			for (int i = 0; i < la.size(); i++) {
				def str = la[i]
				def le = str.indexOf('=')
				String name
				String value
				if (le != -1) {
					name = str.substring(0, le)
					def c = name.indexOf('.')
					if (c == -1) {
						 name = name.toLowerCase()
					}
					else {
						name = name.substring(0, c + 1).toLowerCase() + name.substring(c + 1) 
					}
					value = str.substring(le + 1)
				}
				else {
					name = la[i]
					if (i < la.size() - 1) {
						if (la[i + 1].substring(0, 1) != '-') {
							i++
							value = la[i]
						}
					} 
				}

				if (name.substring(0, 1) == '-') name = name.substring(1)
				if (value != null) {
					value = value.trim()
					def p = value =~ /"(.+)"/
					if (p.size() == 1) {
						List l = (List)p[0]
						value = l[1]
					}
				}
				
				SetValue(res, name, value)
			}
		}

		return res
	}
	
	/**
	 * Convert map to JSon text
	 * @param value
	 * @return
	 */
	public static String ToJson (Map value) {
		if (value == null) return null
		
		JsonBuilder b = new JsonBuilder()
		def res
		b.call(value)
		res = b.toPrettyString()

		return res
	}
	
	/**
	 * Evaluate string map value from variables
	 * @param value
	 * @param vars
	 * @return
	 */
	public static Map EvalMacroValues(Map value, Map vars) {
		if (value == null) return null
		
		def res = [:]
		value.each { k, v ->
			if (v instanceof String || v instanceof GString) {
				def val = v.toString().replace("\\", "\\\\").replace('"""', '\\"\\"\\"').replace('${', '\u0001{').replace('$', '\\$').replace('\u0001{', '${')

				if (val.trim() != '"') res.put(k, GenerationUtils.EvalGroovyScript('"""' + val + '"""', vars)) else res.put(k, val)
			}
			else if (v instanceof Map) {
				res.put(k, EvalMacroValues(v as Map, vars))
			}
			else if (v instanceof List) {
				res.put(k, ListUtils.EvalMacroValues(v as List, vars))
			}
			else {
				res.put(k, v)
			}
		}

		return res
	}

    /**
     * Convert lazy map to hash map
     * @param value
     * @return
     */
	public static HashMap Lazy2HashMap(Map data) {
        def res = [:] as HashMap
        data.each { key, value ->
            if (value instanceof LazyMap) value = Lazy2HashMap((Map)value)
            res.put(key, value)
        }

        return res
	}

	/**
	 * Convert map structure to url parameters
	 * @param m
	 * @return
	 */
	public static String MapToUrlParams(Map<String, Object> m) {
        List l = []
        m.each { String k, v ->
            l << "$k=${v.toString()}"
        }

        return l.join('&')
    }

    @groovy.transform.CompileDynamic
    public static Map<String, Object> Xml2Map(def node) {
        def rootName = node.name().localPart as String
        def rootMap = Xml2MapAttrs(node)
        def res = [:] as Map<String, Object>
        res.put("$rootName".toString(), rootMap)
        return res
    }

    /**
     * Convert XML node attributes to Map object
     * @param node
     * @return
     */
	@groovy.transform.CompileDynamic
	private static Map<String, Object> Xml2MapAttrs(def node) {
		def res = [:] as Map<String, Object>

		if (node.attributes().size() > 0)
			try {
				res.putAll(node.attributes())
			}
			catch (Exception e) {
				Logs.Severe("Error parse attrubutes for xml node: $node")
				throw e
			}

		if (node.children().size() > 0)
			res.putAll(XmlChildren2Map(node.children()))

		return res
	}

    /**
     * Convert XML node children to Map object
     * @param childrens
     * @return
     */
	@groovy.transform.CompileDynamic
	private static Map<String, Object> XmlChildren2Map(def childrens) {
		def res = [:] as Map<String, Object>

		childrens.each { def node ->
			try {
				def name = node.name().localPart

				if (node.children().size() == 1 && node.children().get(0) instanceof String) {
					res.put(name, node.children().get(0))
					return
				}

				if (node.attributes().name == null &&
						(node.children().size() == 0 || node.children().get(0).attributes().name == null)) {
					List<Map> values
					if (!res.containsKey(name)) {
						values = [] as List<Map>
						res."$name" = values
					} else {
						values = res."$name" as List<Map>
					}

					def nodeMap = Xml2MapAttrs(node)
					values << nodeMap
				}
				else {
					Map values
					if (!res.containsKey(name)) {
						values = [:] as Map
						res."$name" = values
					} else {
						values = res."$name" as Map
					}

					def nodeMap = Xml2MapAttrs(node)
					def nodeName = nodeMap.name
					if (nodeName != null) {
						MapUtils.RemoveKeys(nodeMap, ['name'])
						values.put(nodeName, nodeMap)
					}
					else {
						values.putAll(nodeMap)
					}
				}
			}
			catch (Exception e) {
				Logs.Severe("Error parse xml node: $node")
				throw e
			}
		}

		return res
	}

    /**
     * Process XSD content with reader and convert to Map
     * @param xsdName
     * @param read
     * @return
     */
    @groovy.transform.CompileDynamic
    public static Map<String, Object> Xsd2Map(String xsdName, List<String> exclude, Closure reader) {
        def data = reader.call(xsdName)
        def map = Xml2Map(data)
        if (exclude == null) exclude = [] as List<String>
        exclude << xsdName
        map.schema.include?.each { Map includeXsd ->
            String schemaLocation = includeXsd.schemaLocation
            if (schemaLocation == null)
                throw new ExceptionGETL("Invalid XSD include section: $includeXsd!")

            if (schemaLocation in exclude) return

            def includeMap = Xsd2Map(schemaLocation, exclude, reader)
            MapUtils.MergeMap(map, includeMap, false, true)

            exclude << schemaLocation
        }

        return map
    }

    /**
     * Process XSD file with resource and convert to Map
     * @param path - path from resource
     * @param fileName - xsd file name
     * @return
     */
    @groovy.transform.CompileDynamic
    public static Map<String, Object> XsdFromResource(String path, String fileName) {
        def xml = new XmlParser()

        def reader = { xsdFileName ->
            def xsdPath = "$path/$xsdFileName".toString()
            Logs.Config("Read resource xsd file \"$xsdPath\"")
            def input = ClassLoader.getResourceAsStream(xsdPath)
            if (input == null)
                throw new ExceptionGETL("Xsd resource \"$xsdPath\" not found!")

            return xml.parse(input)
        }

        return Xsd2Map(fileName, null, reader)
    }

	@groovy.transform.CompileDynamic
	public static List<Field> XsdMap2Fields(Map<String, Object> map, String objectTypeName) {
		if (map == null) 
            throw new ExceptionGETL('Parameter "map" required!')

        if (map.isEmpty()) 
            throw new ExceptionGETL('Parameter "map" is empty!')

        def tableFields = map.schema.complexType?."$objectTypeName"?.all?.element as Map<String, Object>
        if (tableFields == null || tableFields.isEmpty())
            throw new ExceptionGETL("Invalid or not found the complex type \"$objectTypeName\"!")

        def fieldList = [] as List<Field>

        tableFields.each { String fieldName, Map<String, Object> fieldParams ->
//            println fieldName + ': ' + fieldParams
            def field = new Field(name: fieldName)

            def typeName = fieldParams.type as String
            if (typeName == null)
                throw new ExceptionGETL("Required type for field \"$fieldName\"")

            XsdType2FieldType(map, typeName, field)
            if (fieldParams.minOccurs == 0 && fieldParams.maxOccurs == 1) field.isNull = true

            fieldList << field
        }

        return fieldList
	}

    @groovy.transform.CompileDynamic
    public static void XsdType2FieldType(Map<String, Object> map, String typeName, Field field) {
        def res = XsdTypeProcess(map, typeName)

        if (res.typeName == null) throw new ExceptionGETL("Required base name for \"$typeName\" type")

        field.typeName = res.typeName
        switch (res.typeName) {
            case 'string': case 'token': case 'XMLLiteral': case 'NMTOKEN':
                field.type = Field.Type.STRING
                field.length = res.length?:255
                break
            case 'anyURI':
                field.type = Field.Type.STRING
                field.length = res.length?:500
                break
            case 'decimal':
                field.type = Field.Type.NUMERIC
                field.precision = res.precision?:4
                field.length = res.length?:(res.precision + 18)
                break
            case 'float': case 'double':
                field.type = Field.Type.DOUBLE
                break
            case 'int': case 'byte': case 'integer': case 'negativeInteger': case 'nonNegativeInteger':
            case 'nonPositiveInteger': case 'positiveInteger': case 'short': case 'unsignedInt':
            case 'unsignedShort': case 'unsignedByte':
                field.type = Field.Type.INTEGER
                break
            case 'long': case 'unsignedLong':
                field.type = Field.Type.BIGINT
                break
            case 'date':
                field.type = Field.Type.DATE
                break
            case 'time':
                field.type = Field.Type.TIME
                break
            case 'dateTime':
                field.type = Field.Type.DATETIME
                break
            case 'boolean':
                field.type = Field.Type.BOOLEAN
                break
            case 'complex':
                field.type = Field.Type.OBJECT
                field.extended.putAll(res.extended)
                break
            default:
                throw new ExceptionGETL("Unknown primitive XSD type \"${res.typeName}\"")
        }
    }

    @groovy.transform.CompileDynamic
    private static Map<String, Object> XsdTypeProcess(Map<String, Object> map, String typeName) {
        if (typeName == null)
            throw new ExceptionGETL('Required \"typeName\" parameter!')

        def res = [:] as Map<String, Object>
        if (typeName.matches('xs.*[:].+')) {
            res.typeName = typeName.substring(typeName.indexOf(':') + 1)
        }
        else {
            def typeContent = map.schema.simpleType?."$typeName" as Map
            if (typeContent != null) {
                if (typeContent.containsKey("union")) {
                    def typeParams = typeContent.union[0] as Map
                    def includeTypes = typeParams.memberTypes as String
                    if (includeTypes == null)
                        throw new ExceptionGETL("Invalid union operator for \"$typeName\" type!")

                    def listTypes = includeTypes.split(' ')
                    listTypes.each { String type ->
                        res.putAll(XsdTypeProcess(map, type))
                    }
                }
                else {
                    def typeParams = typeContent.restriction?.get(0) as Map
                    if (typeParams == null) throw new ExceptionGETL("Invalid simple type \"$typeName\"")
                    def baseType = typeParams.base as String
                    if (baseType != null)
                        res.putAll(XsdTypeProcess(map, baseType))

                    if (typeContent.annotation?.get(0)?.documentation != null)
                        res.description = typeContent.annotation.get(0).documentation as String

                    if (typeParams.maxLength?.get(0)?.value != null)
                        res.length = typeParams.maxLength.get(0).value as Integer

                    else if (typeParams.length?.get(0)?.value != null)
                        res.length = typeParams.length.get(0)?.value as Integer

                    else if (typeParams.enumeration != null) {
                        def maxLength = 0
                        typeParams.enumeration.each { Map<String, Object> enumParams ->
                            if ((enumParams.value as String)?.length() > maxLength)
                                maxLength = (enumParams.value as String)?.length()
                        }

                        if (maxLength > 0)
                            res.length = maxLength
                    }

                    if (typeParams.totalDigits?.get(0)?.value != null)
                        res.length = typeParams.totalDigits.get(0).value as Integer

                    if (typeParams.fractionDigits?.get(0)?.value != null)
                        res.precision = typeParams.fractionDigits.get(0).value as Integer
                }
            } else {
                typeContent = map.schema.complexType?."$typeName" as Map

                if (typeContent == null)
                    throw new ExceptionGETL("Type \"$typeName\" not found!")

                res.typeName = 'complex'
                res.extended = typeContent
            }
        }

        return res
    }
}