/*
 GETL - based package in Groovy, which automates the work of loading and transforming data. His name is an acronym for "Groovy ETL".

 GETL is a set of libraries of pre-built classes and objects that can be used to solve problems unpacking,
 transform and load data into programs written in Groovy, or Java, as well as from any software that supports
 the work with Java classes.
 
 Copyright (C) EasyData Company LTD

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

import getl.lang.opts.BaseSpec
import groovy.transform.AutoClone
import groovy.transform.InheritConstructors
import groovy.transform.CompileStatic
import getl.utils.opts.PathVarsSpec
import java.util.regex.Matcher
import java.util.regex.Pattern
import getl.data.Field
import getl.exception.ExceptionGETL

/**
 * Analize and processing path value class
 * @author Alexsey Konstantinov
 */
@AutoClone
class Path {
	protected ParamMethodValidator methodParams = new ParamMethodValidator()
	
	Path () {
		registerMethod()
	}
	
	Path (Map params) {
		registerMethod()
		compile(params)
	}
	
	private void registerMethod () {
		methodParams.register("compile", ["mask", "sysVar", "patterns", "vars"])
	} 
	
	/**
	 * Change from process path Windows file separator to Unix separator  
	 */
	public boolean changeSeparator = (File.separatorChar != '/'.charAt(0))
	
	/**
	 * Ignoring converting error and set null value
	 */
	public boolean ignoreConvertError = false

	/** Mask path
	 * <br>(use {var} for definition variables) */
	String mask
	/** Mask path
	 * <br>(use {var} for definition variables) */
	String getMask() { mask }
	/** Mask path
	 * <br>(use {var} for definition variables) */
	void setMask(String value) {
		isCompile = false
		mask = value
	}
	
	/** Original string mask */
	private String maskStr
	/** Original string mask */
	String getMaskStr () { maskStr }
	
	/** Path elements */
	private List<Map> elements = []
	/** Path elements */
	List<Map> getElements () { elements }
	
	/** SQL like elements */
	private List<Map> likeElements = []
	/** SQL like elements */
	List<Map> getLikeElements () { likeElements }
	
	/** Count level for mask */
	int getCountLevel () { elements.size() }
	
	/** Root path with mask */
	private String rootPath
	/** Root path with mask */
	String getRootPath () { this.rootPath }
	
	/** Count elements in path */
	private int numLocalPath
	/** Count elements in path */
	int getNumLocalPath () { this.numLocalPath }
	
	/** Expression file path with mask */
	private String maskPath
	/** Expression file path with mask */
	String getMaskPath () { this.maskPath }

	/** Expression folder path with mask */
	private String maskFolder
	/** Expression folder path with mask */
	String getMaskFolder () { this.maskFolder }
	
	/** Expression mask file */
	private String maskFile
	/** Expression mask file */
	String getMaskFile () { this.maskFile }
	
	/** Expression folder path with mask for SQL like */
	private String likeFolder
	/** Expression folder path with mask for SQL like */
	String getLikeFolder () { this.likeFolder }
	
	/** Expression mask file for SQL like */
	private String likeFile
	/** Expression mask file for SQL like */
	String getLikeFile () { this.likeFile }
	
	Map<String, Map> vars = [:]

	/**
	 * Used variables in mask<br><br>
	 * <b>Field for var:</b>
	 * <ul>
	 * <li>Field.Type type	- type var
	 * <li>String format	- parse format
	 * <li>int len			- length value of variable
	 * <li>int lenMin		- minimum length of variable
	 * <li>int lenMax		- maximum length of variable
	 * </ul>
	 */
	Map<String, Map> getVars () { this.vars }

	/** Mask variables */
	Map<String, Map<String, Object>> maskVariables = [:]
	/** Mask variables */
	Map<String, Map<String, Object>> getMaskVariables() { maskVariables }

	/** System parameters */
	Map<String, Object> sysParams = [:] as Map<String, Object>
	/** System parameters */
	Map<String, Object> getSysParams() { sysParams }

	/** This object with Getl Dsl repository */
	Object getDslThisObject() { sysParams.dslThisObject }

	/** Owner object with Getl Dsl repository */
	Object getDslOwnerObject() { sysParams.dslOwnerObject }

	/** Define variable options */
	Map variable(String name, @DelegatesTo(PathVarsSpec) Closure cl = null) {
		if (name == null || name == '') throw new ExceptionGETL('Name required for variable!')

		def var = maskVariables.get(name) as Map
		if (var == null) {
			var = [:]
			maskVariables.put(name, (var as Map<String, Object>))
			isCompile = false
		}

		def thisObject = dslThisObject?: BaseSpec.DetectClosureDelegate(cl)
		def parent = new PathVarsSpec(this, thisObject, true, var)
		parent.runClosure(cl)
		if (cl != null) isCompile = false

		return parent.params
	}

	/** Mask path pattern */
	Pattern maskPathPattern
	/** Path already compiled */
	Boolean isCompile = false
	Boolean getIsCompile() { isCompile }

	/**
	 * Compile path mask<br><br>
	 * <b>Parameters</b>
	 * <ul>
	 * <li>String mask - mask of filename<br>
	 * <li>boolean sysVar - include system variables #file_date and #file_size
	 * <li>Map patterns - pattern of variable for use in generation regular expressions
	 * <li>Map<String, Map> vars - attributes of variable
	 * </ul>
	 */
	void compile(Map params = [:]) {
		if (params == null) params = [:]
		methodParams.validation("compile", params)
		maskStr = (params.mask as String)?:mask
		if (maskStr == null) throw new ExceptionGETL("Required parameter \"mask\"")
		boolean sysVar = BoolUtils.IsValue(params.sysVar)
		Map patterns = (params.patterns as Map)?:[:]
		def varAttr = ([:] as Map<String, Map<String, Object>>)
		varAttr.putAll(maskVariables)
		if (params.vars != null) MapUtils.MergeMap(varAttr, params.vars as Map<String, Map<String, Object>>)

		elements.clear()
		likeElements.clear()
		vars.clear()
		rootPath = null
		maskPath = null
		maskPathPattern = null
		maskFolder = null
		maskFile = null
		likeFolder = null
		likeFile = null
		numLocalPath = -1
	
		def rmask = FileUtils.FileMaskToMathExpression(maskStr)

		String[] d = rmask.split("/")
		StringBuilder rb = new StringBuilder()

		def listFoundVars = [] as List<String>
		for (int i = 0; i < d.length; i++) {
			Map p = [:]
			p.vars = []
			StringBuilder b = new StringBuilder()
			
			int f = 0
			int s = d[i].indexOf('{')
			while (s >= 0) {
				int e = d[i].indexOf('}', s)
				if (e >= 0) {
					b.append(d[i].substring(f, s))
					
					def vn = d[i].substring(s + 1, e).toLowerCase()
					def pm = patterns.get(vn) 
					if (pm == null) {
						def vt = varAttr.get(vn)
						def type = vt?.type as Field.Type
						if (type != null && type instanceof String) type = Field.Type."$type"
						if (type in [Field.Type.DATE, Field.Type.DATETIME, Field.Type.TIME]) {
							String df
							if (vt.format != null) {
								df = vt.format
							}
							else {
								if (type == Field.Type.DATE) {
									df = "yyyy-MM-dd"
								}
								else if (type == Field.Type.TIME) {
									df = "HH:mm:ss"
								}
								else {
									df = "yyyy-MM-dd HH:mm:ss"
								}
							}
							
							def vm = df.toLowerCase()
							vm = vm.replace("d", "\\d").replace("y", "\\d").replace("m", "\\d").replace("h", "\\d").replace("s", "\\d")
							b.append("($vm)")
						}
						else if (type in [Field.Type.BIGINT, Field.Type.INTEGER]) {
							if (vt?.len != null) {
								b.append("(\\d{${vt.len}})")
							}
							else if (vt?.lenMin != null && vt?.lenMax != null) {
								b.append("(\\d{${vt.lenMin},${vt.lenMax}})")
							}
							else {
								b.append("(\\d+)")
							}
						}
						else {
							if (vt?.format != null) {
								b.append("(${vt.format})")	
							}
							else {
								b.append("(.+)")
							}
						}
					}
					else {
						b.append("(${pm})")
					}
					
					if (vn in listFoundVars)
						throw new ExceptionGETL("Duplicate variable \"${vn}\" in path mask \"$mask\"")

					(p.vars as List).add(vn)
					listFoundVars << vn
					
					f = e + 1
				}
				s = d[i].indexOf('{', f)
			}
			if (f > 0 && f < d[i].length() - 1) {
				b.append(d[i].substring(f))
			}
			
			p.mask = ((p.vars as List).size() == 0)?d[i]:b.toString()
			p.like = ((p.vars as List).size() == 0)?d[i]:'%'
			elements.add(p)
			
			if ((p.vars as List).size() == 0) {
				if (numLocalPath == -1 && i < d.length - 1)
					rb.append("/" + d[i])
			}
			else
				if (numLocalPath == -1) numLocalPath = i
		}
		rootPath = (rb.length() == 0)?".":rb.toString().substring(1)

		for (int i = 0; i < elements.size(); i++) {
			List<String> v = elements[i].vars as List<String>
			for (int x = 0; x < v.size(); x++) {
				vars.put(v[x] as String, [:])
			}
		}
		patterns.each { k, v ->
			def p = vars.get(k) as Map
			if (p != null) {
				p.pattern = v
			}
		}
		generateMaskPattern()
		
		if (sysVar) {
			vars.put("#file_date", [:])
			vars.put("#file_size", [:])
		}
		
		varAttr.each { name, values ->
			def fn = name.toLowerCase()
			Map<String, Object> attrList = vars.get(fn)
			if (attrList != null) {
				values.each { attr, value ->
					def a = attr.toLowerCase()
					if (!(a in ["type", "format", "len", "lenmin", "lenmax"])) throw new ExceptionGETL("Unknown variable attribute \"${attr}\"") 
					attrList.put(a, value)
				}
			}
		}
		
		maskFile = maskStr.replace('$', '\\$')
		likeFile = maskStr
		vars.each { key, value ->
			def vo = "(?i)(\\{${key}\\})"
			maskFile = maskFile.replaceAll(vo, "\\\$\$1")
			
			likeFile = likeFile.replace("{$key}", "%")
		}
		maskFile = maskFile.replace('\\', '\\\\')
		likeFile = likeFile.replace('*', '%').replace('\\', '\\\\')
				.replace('.', '\\.').replace('$', '\\$')

		isCompile = true
	}
	
	/** Generation mask path pattern on elements */
	void generateMaskPattern () {
		StringBuilder b = new StringBuilder()
		b.append("(?i)")
		for (int i = 0; i < elements.size(); i++) {
			b.append(elements[i].mask)
			if (i < elements.size() - 1) b.append("/")
		}
		maskPath = b.toString()
		maskPathPattern = Pattern.compile(maskPath)
		maskFolder = null
		likeFolder = null
		
		if (!elements.isEmpty() /*&& elements[elements.size() - 2].vars.size() > 0*/) {
			StringBuilder mp = new StringBuilder()
			StringBuilder lp = new StringBuilder()
			mp.append("(?i)")
			for (int i = 0; i < elements.size() - 1; i++) {
				mp.append((elements[i].mask as String) + "/")
				lp.append((elements[i].like as String).replace('.', '\\.') + "/")
			}
			maskFolder = mp.toString() + "(.+)"
			likeFolder = lp.toString()
			if (elements.size() > 1) likeFolder = likeFolder.substring(0, lp.length() - 1)
		}
	}
	
	/** Generation mask path pattern on elements */
	Pattern generateMaskPattern (int countElements) {
		if (countElements > elements.size()) throw new ExceptionGETL("Can not generate pattern on ${countElements} level for ${elements.size()} elements")
		StringBuilder b = new StringBuilder()
		b.append("(?i)")
		for (int i = 0; i < countElements; i++) {
			b.append(elements[i].mask)
			if (i < countElements - 1) b.append("/")
		}
		def mp = b.toString()
		Pattern.compile(mp)
	}
	
	/** Get all files in specific path */
	List<Map> getFiles(String path) {
		File dir = new File(path)
		File[] f = dir.listFiles()
		
		List<Map> l = []
		
		if (f == null) return l
		
		for (int i = 0; i < f.length; i++) {
			if (f[i].isDirectory()) {
				List<Map> n = getFiles(path + "/" + f[i].getName())
				for (int x = 0; x < n.size(); x++) {
					def m = [:]
					m.fileName = n[x].fileName
					l << m
				}
			}
			else
				if (f[i].isFile()) {
					def m = [:]
					m.fileName = path +"/" + f[i].getName()
					l << m
				}
		}
		l
	}
	
	/** Get all files with root path */
	List<Map> getFiles() {
		getFiles(rootPath)
	}
	
	/** Analize file with mask and return value of variables */
	Map analizeFile(String fileName) {
		analize(fileName, false)
	}
	
	/** Analize dir with mask and return value of variables */
	Map analizeDir(String dirName) {
		analize(dirName, true)
	}

	/** Checking the value by compiled mask */
	Boolean match(String value) {
		return value.matches(maskPathPattern)
	}
	
	/** Analize object name */
	Map analize(String objName, boolean isHierarchy = false) {
		if (!isCompile) compile()

		def fn = objName
		if (fn == null) return null
		
		if (changeSeparator) fn = fn.replace('\\', '/')
		
		Integer countDirs
		String pattern
		
		if (isHierarchy) {
			countDirs = objName.split("/").length
			pattern = generateMaskPattern(countDirs)
		}
		else {
			pattern = maskPathPattern
		}
		
		if (!fn.matches(pattern)) return null
		
		def res = [:]
		
		
		Matcher mat
		mat = fn =~ pattern
		if (mat.groupCount() >= 1 && ((List)mat[0]).size() > 1) {
			int i = 0
            def isError = false
			vars.each { key, value ->
                if (isError) return

                //noinspection GroovyAssignabilityCheck
                def v = (mat[0][i + 1]) as String

				if (v?.length() == 0) v = null
				
				if (v != null && value.len != null && v.length() != value.len) {
					if (!ignoreConvertError) throw new ExceptionGETL("Invalid [${key}]: length value \"${v}\" <> ${value.len}")
					v = null
				}

                //noinspection GroovyAssignabilityCheck
                if (v != null && value.lenMin != null && v.length() < Integer.valueOf(value.lenMin)) {
					if (!ignoreConvertError) throw new ExceptionGETL("Invalid [${key}]: length value \"${v}\" < ${value.lenMin}")
					v = null
				}
                //noinspection GroovyAssignabilityCheck
                if (v != null && value.lenMax != null && v.length() > Integer.valueOf(value.lenMax)) {
					if (!ignoreConvertError) throw new ExceptionGETL("Invalid [${key}]: length value \"${v}\" > ${value.lenMax}")
					v = null
				}
				
				if (value.type != null && v != null) {
					def type = value.type
					if (type instanceof String) type = Field.Type."$type"
					switch (type) {
						case Field.Type.DATE:
							def format = (value.format != null)?(value.format as String):'yyyy-MM-dd'
							try {
								v = DateUtils.ParseDate(format, v, ignoreConvertError)
							}
							catch (Exception e) {
								throw new ExceptionGETL("Invalid [${key}]: can not parse value \"${v}\" to date: ${e.message}")
							}
                            isError = (v == null)
							break
						case Field.Type.DATETIME:
							def format = (value.format != null)?(value.format as String):'yyyyMMddHHmmss'
							try {
								v = DateUtils.ParseDate(format, v, ignoreConvertError)
							}
							catch (Exception e) {
								throw new ExceptionGETL("Invalid [${key}]: can not parse value \"${v}\" to datetime: ${e.message}")
							}
                            isError = (v == null)
							break
						case Field.Type.INTEGER:
							try {
								v = Integer.valueOf(v)
							}
							catch (Exception e) {
								if (!ignoreConvertError) throw new ExceptionGETL("Invalid [${key}]: can not parse value \"${v}\" to integer: ${e.message}")
								v = null
							}
                            isError = (v == null)
							break
						case Field.Type.BIGINT:
							try {
								v = Long.valueOf(v)
							}
							catch (Exception e) {
								if (!ignoreConvertError) throw new ExceptionGETL("Invalid [${key}]: can not parse value \"${v}\" to bigint: ${e.message}")
								v = null
							}
                            isError = (v == null)
							break
						case Field.Type.STRING:
							break
						default:
							throw new ExceptionGETL("Unknown type ${value.type}")
					}
				}
                if (isError) return
				
				res.put(key, v)
				i++
			}
            if (isError) return null
		}
		
		return res
	}
	
	/** Filter files with mask */
	List<Map> filterFiles(List<Map> files) {
		List<Map> res = []
		files.each { file ->
			def m = analizeFile(file.filename as String)
			if (m != null) res << file
		}

		return res
	}
	
	/** Filter files from root path with mask */
	List<Map> filterFiles() {
		filterFiles(getFiles())
	}
	
	/** Generation file name with variables */
	@CompileStatic
	String generateFileName(Map varValues, boolean formatValue) {
		if (!isCompile) compile()

		def v = [:] as Map<String, Object>
		if (formatValue) {
			vars.each { key, value ->
				def val = formatVariable(key, varValues.get(key))
				v.put(key, val)
			}
		}
		else {
			vars.each { key, value ->
				v.put(key, varValues.get(key))
			}
		}
		def res = GenerationUtils.EvalText(maskFile, v)
		
		return res
	}

	/** Generation file name with variables */
	@CompileStatic
	String generateFileName(Map varValues) {
		generateFileName(varValues, true)
	}

	/**
	 * Format variable with type of value
	 */
	String formatVariable (String varName, def value) {
		if (!vars.containsKey(varName)) throw new ExceptionGETL("Variable ${varName} not found")
		def v = vars.get(varName)
		def type = v."type"
		if (type == null) {
            type = Field.Type.STRING
        }
        else if (type instanceof String) {
            type = Field.Type."$type"
        }
		
		switch (type) {
			case Field.Type.DATE:
				def format = (v.format as String)?:'yyyy-MM-dd'
				value = DateUtils.FormatDate(format, value as Date)
				break
				
			case Field.Type.TIME:
				def format = (v.format as String)?:'HH:mm:ss'
				value = DateUtils.FormatDate(format, value as Date)
				break
				
			case Field.Type.DATETIME:
				def format = (v.format as String)?:'yyyy-MM-dd HH:mm:ss'
				value = DateUtils.FormatDate(format, value as Date)
				break
		}
		
		return value
	}

	String toString () {
		StringBuilder b = new StringBuilder()
		b.append("""original mask: $maskStr
root path: $rootPath
count level in path: $countLevel
count root levels in path: $numLocalPath
mask path: $maskPath
mask folder: $maskFolder
mask file: $maskFile
like folder: $likeFolder
like file: $likeFile
variables: $vars
elements:
""")
		for (int i = 0; i < elements.size(); i++) {
			def pe = elements[i]
			b.append("[${i+1}]:\t")
			b.append(pe.mask)
            List vr = pe.vars as List
			if (vr.size() > 0) b.append(" [")
			for (int v = 0; v < vr.size(); v++) {
				b.append(vr.get(v))
				if (v < vr.size() - 1)
					b.append(", ")
			}
			if (vr.size() > 0) b.append("]")
			b.append("\n")
		}

		return b.toString()
	}
}