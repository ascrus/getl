package getl.utils

import getl.exception.ExceptionGETL
import groovy.transform.CompileDynamic
import groovy.transform.CompileStatic

/**
 * List library functions class
 * @author Alexsey Konstantinov
 *
 */
@CompileStatic
class ListUtils {
	/**
	 * Return to list items that fit the specified condition 
	 * @param list
	 * @param from
	 * @return
	 */
	static List CopyWhere(List list, Closure from) {
		if (list == null) return null
		
		def result = []
		list.each {  
			//noinspection GroovyAssignabilityCheck
            if (from(it)) result << it
		}

		return result
	}
	
	/**
	 * Enclose each item in the list in quotes
	 * @param list
	 * @param quote
	 * @return
	 */
	static List<String> QuoteList(List list, String quote) {
		if (list == null) return null
		
		def res = []
		list.each { if (it != null && it != '') res << "${quote}${it}${quote}" }

		return res
	}
	
	/**
	 * Return the sorted list for a given condition
	 * @param list
	 * @param closure
	 * @return
	 */
	static List SortListTo(List list, Closure closure) {
		if (list == null) return null
		
		return list.sort(false, closure)
	}
	
	/**
	 * Sort the list by a specified condition
	 * @param list
	 * @param closure
	 */
	static void SortList(List list, Closure closure) {
		if (list == null) return
		
		list.sort(true, closure)
	}
	
	/**
	 * Convert all element of list to lower case
	 * @param list
	 * @return
	 */
	static List<String> ToLowerCase(List<String> list) {
		if (list == null) return null
		
		def res = []
		list.each {
			res << it.toLowerCase()
		}
		return res
	}
	
	/**
	 * Convert all element of list to upper case
	 * @param list
	 * @return
	 */
	static List<String> ToUpperCase(List<String> list) {
		if (list == null) return null
		
		def res = []
		list.each {
			res << it.toUpperCase()
		}
		return res
	}
	
	/**
	 * Return first not null value from list
	 * @param listValues
	 * @return
	 */
	static def NotNullValue(List listValues) {
		return listValues?.find { it != null }
	}

	/**
	 * Return first not null value from list
	 * @param values
	 * @return
	 */
	static def NotNullValue(Object... values) {
		return values?.find { it != null }
	}
	
	/**
	 * Evaluate macros for value in list
	 * @param value
	 * @param vars
	 * @return
	 */
	static List EvalMacroValues(List value, Map vars) {
		def res = []
		
		value.each { v ->
			if (v instanceof String || v instanceof GString) {
				def val = v.toString().replace("\\", "\\\\").replace('"""', '\\"\\"\\"').replace('${', '\u0001{').replace('$', '\\$').replace('\u0001{', '${')
				
				if (val.trim() != '"') res << GenerationUtils.EvalGroovyScript('"""' + val + '"""', vars, true) else res << val
			}
			else if (v instanceof List) {
				List r = v as List
				res << EvalMacroValues(r, vars)
			}
			else if (v instanceof Map) {
				Map r = v as Map
				res << MapUtils.EvalMacroValues(r, vars)
			}
			else {
				res << v
			}
		}
		
		return res
	}

	/**
	 * Split string and return list	
	 * @param value
	 * @param expr
	 * @return
	 */
	static List Split(String value, String expr) {
		if (value == null) return null
		
		String[] res = value.split(expr)
		return (List)res.collect()
	}

    /**
     * Build Json text by list
     * @param list
     * @return
     */
	static String ToJson(List list) {
		if (list == null) return null
		
		StringBuilder sb = new StringBuilder()
		sb << "[\n"
		list.each { sb << "	$it,\n" }
		sb << "]"
		
		return sb.toString()
	}

    /**
     * Convert an array of elements into a text list with a comma separator
     * @param list
     * @return
     */
    @CompileDynamic
	static String List2StrArray(List list) {
        if (list == null) return null
        if (list.size() == 0) return ''
        if (list.size() == 1) return "${list[0]}".toString()
		list = list.sort(false)

		def array = [] as List<List>

        String firstElement = list[0]
        String lastElement = list[0]
        for (Integer i = 1; i < list.size(); i++) {
            String elem = list[i]

            if (lastElement.next() == elem) {
                lastElement = elem
            }
            else {
                array << [firstElement, lastElement]
                firstElement = elem
                lastElement = elem
            }
        }
        array << [firstElement, lastElement]

        def strList = [] as List<String>
        array.each { List elem ->
            if (elem[0] == elem[1])
                strList << elem[0].toString()
            else
                strList << "${elem[0]}-${elem[1]}".toString()
        }

        return strList.join(',')
	}

    /**
     * Convert a comma-delimited text list into an array of elements with the specified type
     * @param strList
     * @param elemClass
     * @return
     */
    @CompileDynamic
	static List StrArray2List(String strList, Class elemClass = String, String delimRegex = ',') {
        if (strList == null)
			return null

        def list = strList.split(delimRegex)
        def result = []
        list.each { String elem ->
            if (elem == '')
				return

            def i = elem.indexOf('-')
            if (i == -1)
                result << elemClass.newInstance(elem)
            else {
                def firstElem = elemClass.newInstance(elem.substring(0, i))
                def lastElem = elemClass.newInstance(elem.substring(i + 1))
                (firstElem..lastElem).each { subElem -> result << subElem}
            }
        }

        return result
    }

	/**
	 * Divide the list of items into sub-lists by the specified number of divisor
	 * @param list original list of items
	 * @param divisor divisor value
	 * @param limit split no more n-elements
	 * @return list of lists of separated items
	 */
	static List<List> SplitList(List list, Integer divisor, Integer limit = null) {
		if (divisor <= 0)
			throw new ExceptionGETL('The divisor must be greater than zero!')
		if (limit != null && limit <= 0)
			throw new ExceptionGETL('The limit must be greater than zero!')

		def res = ([] as List<List>)
		if (list.isEmpty()) return res

		def cur = 0
		def size = 0
		for (Long i = 0; i < list.size(); i++) {
			if (limit != null && i == limit) break

			if (size == cur) {
				res << ([] as List)
				size++
			}

			res[cur] << list[i]
			cur++
			if (cur == divisor) cur = 0
		}

		return res
	}

	/** Convert object to list */
	static List ToList(Object value, String delimRegex = ',') {
		if (value == null)
			return null

		if (value instanceof List)
			return value

		def res = []
		def elements = value.toString().split(delimRegex)
		elements.each { el ->
			el = el.trim()
			if (el.length() > 0)
				res << el
		}

		return res
	}
}