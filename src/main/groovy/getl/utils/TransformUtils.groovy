package getl.utils

import groovy.transform.CompileStatic

/**
 * Data transformation library functions class 
 * @author Alexsey Konstantinov
 *
 */
@CompileStatic
class TransformUtils {
	/**
	 * Convert a set of fields with values in the map
	 * @param text
	 * @param fieldDelimited
	 * @param valueDelimited
	 * @return
	 */
    static Map DenormalizeColumn(String text, String fieldDelimited, String valueDelimited) {
		if (text == null) return null
		def fields = text.split(fieldDelimited)
		def values = [:]
		fields.each { String v ->
			def i = v.indexOf(valueDelimited)
			def name = (i >= 0)?v.substring(0, i).trim():v.trim()
			def value = (i >= 0)?v.substring(i + 1).trim():null
			values.put(name.toLowerCase(), value)
		}
		
		values
	}

	/**
	 * Convert text with delimiter to rows 
	 * @param text
	 * @param fieldDelimited
	 * @return
	 */
	static List ListFromColumn(String text, String fieldDelimited) {
		if (text == null) return null
		text += fieldDelimited + '\u0001'
		List res = []
		List v = text.split(fieldDelimited).toList()
		for (Integer i = 0; i < v.size() - 1; i++) {
			if (v[i] == '') res << null else res << v[i]
		}
		
		res
	}
}
