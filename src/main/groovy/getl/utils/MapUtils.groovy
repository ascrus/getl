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

import groovy.json.JsonBuilder
import groovy.json.JsonSlurper
import getl.exception.ExceptionGETL

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
	 */
	public static void MergeMap (Map<String, Object> source, Map<String, Object> added) {
		if (source == null || added == null) return
		added.each { String key, value ->
			MergeMapChildren(source, value, key)
		}
	}

	private static void MergeMapChildren (Map source, def added, String key) {
		if (!(added instanceof Map)) {
            source.put(key, added)
			return
		}
		def c = added as Map<String, Object>
        def subsource = source.get(key) as Map<String, Object>
        if (subsource == null) {
            subsource = [:] as Map<String, Object>
            source.put(key, subsource)
        }
		c.each { String subkey, value ->
			MergeMapChildren(subsource, value, subkey)
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
}