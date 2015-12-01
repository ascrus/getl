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

import getl.exception.ExceptionGETL

/**
 * Paramaters method manager class 
 * @author Alexsey Konstantinov
 *
 */
class ParamMethodValidator {
	private final Map methodParams = [:]

	/**
	 * Register list of parameters by method
	 * @param methodName
	 * @param parameterList
	 */
	public void register (String methodName, List<String> parameterList) {
		if (methodParams."${methodName}" == null) {
			methodParams."${methodName}" = parameterList
		}
		else {
			methodParams."${methodName}".addAll(parameterList)
		}
	}
	
	/**
	 * Unregister list of parameters by method 
	 * @param methodName
	 * @param parameterList
	 */
	public void unregister (String methodName, List<String> parameterList) {
		List<String> params = methodParams."${methodName}"
		if (params == null || params.isEmpty()) throw new ExceptionGETL("Unknown method \"$methodName\"")
		methodParams."${methodName}" = params - parameterList
	}
	
	/**
	 * Allowed methods
	 * @return
	 */
	public List<String> methods () {
		List<String> res = []
		methodParams.each { key -> res << key }
		
		res
	}
	
	/**
	 * Allowed parameters for method
	 * @param methodName
	 * @return
	 */
	public List<String> params(String methodName) {
		def res = methodParams."${methodName}"
		if (res == null) throw new ExceptionGETL("Unknown method ${methodName}")
		
		res
	} 
	
	/**
	 * Validation running parameters list for method
	 * @param methodName
	 * @param runParams
	 */
	public void validation(String methodName, Map runParams) {
		def list = params(methodName)
		
		def unknown = MapUtils.Unknown(runParams, list, true)
		if (unknown.isEmpty()) return
		def slist = unknown.join(", ")
		
		throw new ExceptionGETL("Unknown parameters [${slist}] for method \"${methodName}\", avaible parameters: ${list}")
	}
	
	/**
	 * Validation running parameters list for method including other parameters
	 * @param methodName
	 * @param runParams
	 * @param otherValidator
	 */
	public void validation(String methodName, Map runParams, List<List<String>> others) {
		def list = params(methodName)
		
		def vlist = []
		vlist.addAll(list)
		others.each { vlist.addAll(it) }
		
		def unknown = MapUtils.Unknown(runParams, vlist, true)
		if (unknown.isEmpty()) return
		def slist = unknown.join(", ")
		
		throw new ExceptionGETL("Unknown parameters [${slist}] for method \"${methodName}\", avaible parameters: ${vlist}")
	}
	
	/**
	 * Valid map content names 
	 * @param content
	 */
	public void validation(Map content, String contentName, List excludeSections) {
		if (content == null) return
		validationSub(content, contentName, contentName, excludeSections?:[])
	}
	
	private void validationSub (Map content, String contentName, String path, List excludeSections) {
		if (contentName in excludeSections) return
		
		List listMethods = methodParams."${contentName}"
		if (listMethods == null) throw new ExceptionGETL("Content name \"$path\" not found, avaible names ${methodParams.keySet().toList()}")
		content?.each { String key, value ->
			if (key.substring(0, 1) == "_") return
			if (listMethods.indexOf(key) == -1) throw new ExceptionGETL("Invalid parameter \"${path}.${key}\", allow ${listMethods}")
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
							if (!(subValue instanceof Map)) throw new ExceptionGETL("Invalid parameter \"${contentName}.${key}.${subKey}\", map expected")
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
						if (!(subValue instanceof Map)) throw new ExceptionGETL("Invalid parameter \"${contentName}.${key}.${subKey}\", map expected")
						validationSub(subValue, subContentName, "${contentName}.${key}[]", excludeSections)
					}
				}
			}
		}
	}

	
	public String toString() {
		MapUtils.ToJson(methodParams)
	}
}
