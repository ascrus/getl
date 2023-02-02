package getl.utils

import getl.exception.ExceptionGETL
import groovy.transform.CompileStatic

/**
 * Boolean functions
 * @author Alexsey Konstantonov
 *
 */
@CompileStatic
class BoolUtils {
	/**
	 * Return boolean by value
	 * @param value - any value, compatible with boolean type
	 * @param defaultValue - return if value has null
	 * @return
	 */
	@SuppressWarnings('GroovyUnusedAssignment')
	static Boolean IsValue(Object value, Boolean defaultValue = false) {
		if (defaultValue == null)
			defaultValue = false

		if (value == null)
			return defaultValue
		if (value instanceof Boolean)
			return value

        Boolean result
		if (value instanceof List) {
			for (v in (value as List)) {
				if (v != null) {
					if (v instanceof Boolean)
                        result = v
                    else
					{
                        def s = v.toString().toLowerCase()
						if (s.length() != 0) {
							if (s in ['true', '1', 'on']) {
								result = true
							} else {
								if (s in ['false', '0', 'off']) {
									result = false
								} else {
									throw new ExceptionGETL("Invalid boolean value \"$s\"")
								}
							}
						}
                    }
				}
				if (result != null)
					break
			}
			if (result == null)
				return defaultValue
			else
				return result
		}

        def v = value.toString().toLowerCase()
		if (v.length() ==0) return defaultValue
		if (v in ['true', '1', 'on']) {
            result = true
        }
        else if (v in ['false', '0', 'off']) {
            result = false
        }
        else {
            throw new ExceptionGETL("Invalid boolean value \"$v\"")
        }

        return result
	}

	/**
	 * Validation instanceof use class for super class
	 * @param useClass
	 * @param superClass
	 * @return
	 */
	static Boolean ClassInstanceOf(Class useClass, Class superClass) {
		def c = useClass.getSuperclass()
		def i = 0
		while ( c.name != 'java.lang.Object' && i < 1000) {
			if (c == superClass) return true
			
			c = c.getSuperclass()
			i++
		}
		
		return false
	}
}