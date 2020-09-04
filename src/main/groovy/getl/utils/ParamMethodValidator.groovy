package getl.utils

import getl.exception.ExceptionGETL

/**
 * Paramaters method manager class 
 * @author Alexsey Konstantinov
 *
 */
class ParamMethodValidator {
	private final Map<String, Object> methodParams = [:] as Map<String, Object>

	/**
	 * Register list of parameters by method
	 * @param methodName name method
	 * @param parameterList list of parameter by method
	 */
	void register(String methodName, List<String> parameterList) {
		if (methodParams.get(methodName) == null) {
			methodParams.put(methodName, parameterList)
		}
		else {
			(methodParams.get(methodName) as List<String>).addAll(parameterList)
		}
	}
	
	/**
	 * Unregister list of parameters by method 
	 * @param methodName name method
	 * @param parameterList list of parameter by method
	 */
	void unregister(String methodName, List<String> parameterList) {
		def params = methodParams.get(methodName) as List<String>
		if (params == null || params.isEmpty())
			throw new ExceptionGETL("Unknown method \"$methodName\"")
		methodParams.put(methodName, params - parameterList)
	}
	
	/**
	 * Detect allowed methods
	 * @return
	 */
	List<String> methods () {
		return methodParams.keySet().toList()
	}
	
	/**
	 * Detect allowed parameters for specofied method
	 * @param methodName name method
	 * @return list of register parameter by method
	 */
	List<String> params(String methodName) {
		def res = methodParams.get(methodName) as List<String>
		if (res == null) throw new ExceptionGETL("Unknown method $methodName")
		
		return res
	} 
	
	/**
	 * Check passed method parameters
	 * @param methodName name method
	 * @param runParams list of parameters
	 */
	void validation(String methodName, Map runParams) {
		def list = params(methodName)
		
		def unknown = MapUtils.Unknown(runParams as Map<String, Object>, list, true)
		if (unknown.isEmpty()) return
		def slist = unknown.join(", ")
		
		throw new ExceptionGETL("Unknown parameters [${slist}] for method \"${methodName}\", avaible parameters: ${list}")
	}
	
	/**
	 * Check passed method parameters
	 * @param methodName name method
	 * @param runParams list of parameters
	 * @param otherValidator list of over registered parameter
	 */
	void validation(String methodName, Map runParams, List<List<String>> others) {
		def list = params(methodName)
		
		def vlist = [] as List<String>
		vlist.addAll(list)
		others.each { vlist.addAll(it) }
		
		def unknown = MapUtils.Unknown(runParams as Map<String, Object>, vlist, true)
		if (unknown.isEmpty()) return
		def slist = unknown.join(", ")
		
		throw new ExceptionGETL("Unknown parameters [${slist}] for method \"${methodName}\", avaible parameters: ${vlist}")
	}
	
	/**
	 * Valid map content names 
	 * @param content data in map object
	 * @param excludeSections list of ignored map group names
	 */
	void validation(Map content, String contentName, List excludeSections = []) {
		if (content == null) return
		validationSub(content, contentName, contentName, excludeSections?:[])
	}
	
	private void validationSub (Map<String, Object> content, String contentName, String path, List excludeSections) {
		if (contentName in excludeSections) return
		
		def listMethods = methodParams.get(contentName) as List<String>
		if (listMethods == null) throw new ExceptionGETL("Content name \"$path\" not found, avaible names ${methodParams.keySet().toList()}")
		content?.each { String key, value ->
			if (key.substring(0, 1) == "_") return
			if (listMethods.indexOf(key) == -1)
				throw new ExceptionGETL("Invalid parameter \"${path}.${key}\", allow ${listMethods}")
			if (value instanceof Map) {
				def subContentName = "${contentName}.${key}"
				if (methodParams."$subContentName" != null) {
					validationSub(value, subContentName, "${path}.${key}", excludeSections)
				}
				else {
					subContentName = "${contentName}.${key}.*"
					def subContent = methodParams."$subContentName"
					if (subContent != null) {
						value.each { subKey, subValue ->
							if (!(subValue instanceof Map))
								throw new ExceptionGETL("Invalid parameter \"${contentName}.${key}.${subKey}\", map expected")
							validationSub(subValue, subContentName, "${contentName}.${key}.${subKey}", excludeSections)
						}
					}
				}
			}
			else if (value instanceof List) {
				def subContentName = "${contentName}.${key}.*"
				def subContent = methodParams."$subContentName"
				if (subContent != null) {
					value.each { subValue ->
						if (!(subValue instanceof Map))
							throw new ExceptionGETL("Invalid parameter \"${contentName}.${key}.*\", map expected")
						validationSub(subValue, subContentName, "${contentName}.${key}[]", excludeSections)
					}
				}
			}
		}
	}

	String toString() {
		MapUtils.ToJson(methodParams)
	}
}
