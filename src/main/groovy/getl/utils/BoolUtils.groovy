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
    @SuppressWarnings("DuplicatedCode")
    static Boolean IsValue(def value, Boolean defaultValue = false) {
		if (value == null) return defaultValue
		if (value instanceof Boolean) return value

        Boolean result
		if (value instanceof List) {
			value.each { v ->
				if (result == null && v != null) {
					if (v instanceof Boolean) {
                        result = v
                    }
                    else //noinspection DuplicatedCode
					{
                        def s = v.toString().toLowerCase()
                        if (s in ['true', '1', 'on']) {
                            result = true
                        }
                        else {
                            if (s in ['false', '0', 'off']) {
                                result = false
                            }
                            else {
                                throw new ExceptionGETL("Invalid boolean value \"$s\"")
                            }
                        }
                    }
				} 
			}
			if (result == null) return defaultValue else return result
		}

        def v = value.toString().toLowerCase()
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

	/*static public Boolean IsValue(def value) {
		IsValue(value, false)
	}*/
	
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
		
		false
	}
}
