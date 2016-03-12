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

package getl.proc

import getl.data.*
import getl.data.Dataset.Status
import getl.driver.Driver
import getl.exception.ExceptionGETL
import getl.utils.*
import getl.tfs.*

/**
 * Data flow manager class 
 * @author Alexsey Konstantinov
 *
 */
class Flow {
	protected ParamMethodValidator methodParams = new ParamMethodValidator()
	
	Flow () {
		methodParams.register("copy", ["source", "tempSource", "dest", "tempDest", "inheritFields", "tempFields", "map",
			"source_*", "dest_*", "autoMap", "autoConvert", "autoTran", "clear", "saveErrors", "excludeFields", "mirrorCSV",
			"bulkLoad", "bulkAsGZIP", "onInit", "onWrite", "onDone", "debug"])
		
		methodParams.register("writeTo", ["dest", "dest_*", "autoTran", "tempDest", "tempFields", "bulkLoad", "bulkAsGZIP", "clear"])
		methodParams.register("writeAllTo", ["dest", "dest_*", "autoTran", "bulkLoad", "bulkAsGZIP"])
		
		methodParams.register("process", ["source", "source_*", "tempSource", "saveErrors"])
	}
	
	/**
	 * Name in config from section "datasets"
	 */
	private String config
	public String getConfig () { config }
	public void setConfig (String value) {
		config = value
		if (config != null) {
			if (Config.ContainsSection("flows.${this.config}")) {
				doInitConfig()
			}
			else {
				Config.RegisterOnInit(doInitConfig)
			}
		}
	}
	
	/**
	 * Call init configuraion
	 */
	private final Closure doInitConfig = {
		if (config == null) return
		Map cp = Config.FindSection("flows.${config}")
		if (cp.isEmpty()) throw new ExceptionGETL("Config section \"flows.${config}\" not found")
		onLoadConfig(cp)
		Logs.Config("Load config \"flows\".\"${config}\" for flow")
	}
	
	protected void onLoadConfig (Map configSection) {
		MapUtils.MergeMap(params, configSection)
	}
	
	/**
	 * Dataset public parameters
	 */
	public final Map params = [:]
	
	protected Map convertFieldMap(Map map) {
		def result = [:]
		map.each { k, v ->
			def m = [:]
			if (v != null) {
				String[] l = v.split(";")
				m.name = l[0].toLowerCase()
				for (int i = 1; i < l.length; i++) {
					def p = l[i].indexOf("=")
					if (p == -1) {
						m.put(l[i].toLowerCase(), null)
					}
					else {
						def pname = l[i].substring(0, p).toLowerCase()
						def pvalue = l[i].substring(p + 1)
						m.put(pname, pvalue)
					}
				}
			}
			result.put(k.toLowerCase(), m)
		}
		result
	}
	
	protected String findFieldInFieldMap (Map map, String field) {
		String result = null
		field = field.toLowerCase()
		map.each { String k, Map v ->
			if (v.name?.toLowerCase() == field) {
				result = k
				return
			}
		}
		result
	}
	
	/**
	 * Transformation fields script
	 */
	public String scriptMap
	
	protected void generateMap(Dataset source, Dataset dest, Map fieldMap, Boolean autoConvert, List<String> excludeFields, Map result) {
		def countMethod = new BigDecimal(dest.field.size() / 100).intValue() + 1
		def curMethod = 0

		StringBuilder sb = new StringBuilder()
		sb << "{ Map inRow, Map outRow ->\n"
		
		if (countMethod > 1) {
			(1..countMethod).each { sb << "	method_${it}(inRow, outRow)\n" }
			sb << "}\n"
		}
		else {
			sb << "\n"
		}
		
		def map = convertFieldMap(fieldMap)
		List<String> destFields = []
		List<String> sourceFields = []
		
		int c = 0
		dest.field.each { Field d ->
			c++
			
			if (countMethod > 1) {
				def fieldMethod = new BigDecimal(c / 100).intValue() + 1
				if (fieldMethod != curMethod) {
					if (curMethod > 0) sb << "}\n"
					curMethod = fieldMethod
					sb << "\nvoid method_${curMethod} (Map inRow, Map outRow) {\n"
					sb << "\n"
				}
			}
			
			if (excludeFields.find { d.name.toLowerCase() == it.toLowerCase() } != null) {
				sb << "// Exclude field ${d.name}\n\n"
				return
			}
			
			// Dest field name			
			def dn = d.name.toLowerCase()
			// Map field name
			def mn = dn
			
			def convert = (autoConvert == null || autoConvert) 
			String mapFormat
			// Has contains field in mapping
			if (map.containsKey(dn)) {
				Map mapName = map.get(dn)
				// No source map field
				if (mapName.isEmpty()) {
					// Nothing mapping
					mn = null
				}
				else {
					// Set source map field
					Field sf = source.fieldByName(mapName.name)
					if (sf == null) throw new ExceptionGETL("Not found field \"${mapName.name}\" in source dataset")
					mn = sf.name.toLowerCase()
					if (mapName.convert != null) convert = (mapName.convert.trim().toLowerCase() == "true")
					if (mapName.format != null) mapFormat = mapName.format
				}
			}
			else {
				// Has contains field as source field from map
				if (findFieldInFieldMap(map, dn) != null) { 
					// Nothing mapping
					mn = null
				}
			}
			
			Field s 
			// Use field is mapping
			if (mn != null) s = source.fieldByName(mn)
			// Not use
			if (s == null) {
				if (!d.isAutoincrement && !d.isReadOnly) {
					sb << "outRow.'${dn}' = getl.utils.GenerationUtils.EMPTY_${d.type.toString().toUpperCase()}"
					destFields << dn
				}
				else {
					sb << "// $dn: NOT VALUE REQUIRED"
				}
			}
			else {
				// Assign value
				String sn = s.name.toLowerCase()
//				println "${d.type} == ${s.type} || !${convert}"
				if (d.type == s.type || !convert) {
					sb << "outRow.'${dn}' = inRow.'${sn}'"
				}
				else {
					sb << "outRow.'${dn}' = "
					sb << GenerationUtils.GenerateConvertValue(d, s, mapFormat, "inRow.'${sn}'")
				}
				destFields << dn
				sourceFields << sn
			}
			
			sb << "\n\n"
		}
		
		sb << "\n}"
		scriptMap = sb.toString()
		result.code = GenerationUtils.EvalGroovyScript(scriptMap)
		result.sourceFields = sourceFields
		result.destFields = destFields
	}
	
	protected void assignFieldToTemp (Dataset source, Dataset dest, Map map) {
		map = convertFieldMap(map)
		dest.field = source.field
		map.each { k, v ->
			Field f = dest.fieldByName(v.name)
			if (f != null) {
				if (k != null && k != "") f.name = k else dest.removeField(f)
			}
		}
	}
	
	/**
	 * Error rows for "copy" process 
	 */
	public TFSDataset errorsDataset
	
	/**
	 * Copy rows from dataset to other dataset
	 * 
	 * <p><b>Dynamic parameters:</b></p>
	 * <ul>
	 * <li>Dataset source				- source dataset
	 * <li>String tempSource			- name temporary dataset for source use 
	 * <li>Dataset dest					- destination dataset
	 * <li>boolean inheritFields		- destinition fields inherit from source fields 
	 * <li>String tempDest				- name temporary dataset for dest use
	 * <li>List<Field> tempFields		- list of field from destination dataset
	 * <li>Map map						- map card columns with syntax: [<destination field>:"<source field>:<convert format>"]
	 * <li>Map source_*					- parameters for source read process
	 * <li>Map dest_*					- parameters for destination write process
	 * <li>boolean autoMap				- auto mapping value from source fields to destination fields 
	 * <li>boolean autoConvert			- auto converting type value from source fields to destination fields
	 * <li>boolean autoTran				- auto starting and finishing transaction for copy process
	 * <li>boolean clear				- clearing destination dataset before copy
	 * <li>boolean saveErrors			- save assert errors to temporary dataset "errorsDataset"
	 * <li>List<String> excludeFields	- list of fields that do not need to use
	 * <li>String mirrorCSV				- filename  of mirror CSV dataset
	 * <li>boolean bulkLoad				- load to destinition as bulk load (if supported)
	 * <li>boolean bulkAsGZIP			- generate bulk CSV file in GZIP format (you need set parameter for destination bulk gzip format) 
	 * <li>Closure onInit()				- initialization code on start process copying
	 * <li>Closure onWrite(inRow, outRow)	- code executed before writing to destination dataset
	 * <li>Closure onDone()				- code to complete process copying
	 * </ul>   
	 * 
	 * @param params	- Dynamic parameters (required)
	 * @return - Count of process rows
	 */

	public long copy (Map params) {
		copy(params, null)
	}
	
	/**
	 * Copy rows from dataset to other dataset
	 * 
	 * <p><b>Dynamic parameters:</b></p>
	 * <ul>
	 * <li>Dataset source				- source dataset
	 * <li>String tempSource			- name temporary dataset for source use 
	 * <li>Dataset dest					- destination dataset
	 * <li>boolean inheritFields		- destination fields inherit from source fields 
	 * <li>String tempDest				- name temporary dataset for dest use
	 * <li>List<Field> tempFields		- list of field from destination dataset
	 * <li>Map map						- map card columns with syntax: [<destination field>:"<source field>:<convert format>"]
	 * <li>Map source_*					- parameters for source read process
	 * <li>Map dest_*					- parameters for destination write process
	 * <li>boolean autoMap				- auto mapping value from source fields to destination fields 
	 * <li>boolean autoConvert			- auto converting type value from source fields to destination fields
	 * <li>boolean autoTran				- auto starting and finishing transaction for copy process
	 * <li>boolean clear				- clearing destination dataset before copy
	 * <li>boolean saveErrors			- save assert errors to temporary dataset "errorsDataset"
	 * <li>List<String> excludeFields	- list of fields that do not need to use
	 * <li>String mirrorCSV				- filename  of mirror CSV dataset
	 * <li>boolean bulkLoad				- load to destination as bulk load (only is supported)
	 * <li>boolean bulkAsGZIP			- generate bulk CSV file in GZIP format 
	 * <li>Closure onInit()				- initialization code on start process copying
	 * <li>Closure onWrite(inRow, outRow)	- code executed before writing to destination dataset
	 * <li>Closure onDone()				- code to complete process copying
	 * <li>boolean debug				- save transformation code to dumn (default false)
	 * </ul>   
	 * 
	 * @param params	- Dynamic parameters (required)
	 * @param map_code	- Processing code  (not required)
	 * @return - Count of process rows
	 */
	public long copy (Map params, Closure map_code) {
		if (!this.params.isEmpty()) {
			params = this.params + params
		}
		
		methodParams.validation("copy", params)
		
		Dataset source = params.source
		String sourceDescription
		if (source == null && params.tempSource != null) {
			source = TFS.dataset(params.tempSource, true)
			sourceDescription = "temp.${params.tempSource}"
		}
		if (source == null) new ExceptionGETL("Required parameter \"source\"")
		if (sourceDescription == null) sourceDescription = source.objectName
		
		Dataset dest = params.dest
		String destDescription
		boolean isDestTemp = false
		if (dest == null && params.tempDest != null) {
			dest = (Dataset)TFS.dataset(params.tempDest)
			destDescription = "temp.${params.tempDest}"
			if (params.tempFields!= null) dest.field = params.tempFields else isDestTemp = true
		}
		if (dest == null) throw new ExceptionGETL("Required parameter \"dest\"")
		if (destDescription == null) destDescription = dest.objectName
		if (dest.sysParams.isTFSFile != null && dest.sysParams.isTFSFile && dest.field.isEmpty() && !isDestTemp) isDestTemp = true
		def isDestVirtual = (dest.sysParams.isVirtual != null && dest.sysParams.isVirtual) 
		
		boolean inheritFields = BoolUtils.IsValue([dest.sysParams.inheriteFields, params.inheritFields], false)
		
		boolean isBulkLoad = (params.bulkLoad != null)?params.bulkLoad:false
		if (isBulkLoad && (isDestTemp || isDestVirtual)) throw new ExceptionGETL("Is not possible to start the process BulkLoad for a given destination dataset")
		if (isBulkLoad && !dest.connection.driver.isOperation(Driver.Operation.BULKLOAD)) throw new ExceptionGETL("Destinitaion dataset not support bulk load") 
		
		boolean bulkAsGZIP = (params.bulkAsGZIP != null)?params.bulkAsGZIP:false
		
		Map map = (params.map != null)?params.map:[:]
		Map sourceParams = MapUtils.GetLevel(params, "source_")
		Closure prepareSource = sourceParams.prepare 
		
		boolean autoMap = (params.autoMap != null)?params.autoMap:true
		boolean autoConvert = (params.autoConvert != null)?params.autoConvert:true
		boolean autoTran = (params.autoTran != null)?params.autoTran:true
		autoTran = autoTran && 
					(dest.connection.driver.isSupport(Driver.Support.TRANSACTIONAL) && 
						(dest.connection.params.autoCommit == null || !dest.connection.params.autoCommit))
		boolean clear = (params.clear != null)?params.clear:false
		boolean isSaveErrors = (params.saveErrors != null)?params.saveErrors:false
		
		List<String> excludeFields = (params.excludeFields != null)?params.excludeFields*.toLowerCase():[]
		
		Closure writeCode = (params.onWrite != null)?params.onWrite:null
		Closure initCode = (params.onInit != null)?params.onInit:null
		Closure doneCode = (params.onDone != null)?params.onDone:null
		
		boolean debug = (params.debug != null)?params.debug:false
		
		if (isSaveErrors) errorsDataset = TFS.dataset() else errorsDataset = null
		
		Dataset writer
		TFSDataset bulkDS
		
		Map destParams = MapUtils.GetLevel(params, "dest_")
		Map bulkParams
		if (isBulkLoad) {
			bulkParams = destParams
			if (bulkAsGZIP) bulkParams.compressed = "GZIP"
			if (autoTran) bulkParams.autoCommit = false
			destParams = [:]
			
			bulkDS = TFS.dataset()
			if (bulkAsGZIP) {
				bulkDS.isGzFile = true
			}
			if (dest.field.isEmpty()) dest.retrieveFields()
			bulkDS.field = dest.field
			writer = bulkDS
		}
		else {
			writer = dest
		}
		
		Closure auto_map_code
		Map generateResult = [:]
		
		def initDest = {
			List<String> result = []
			if (autoMap) {
				generateMap(source, writer, map, autoConvert, excludeFields, generateResult)
				auto_map_code = generateResult.code
				result = generateResult.destFields
			}

			result
		}
		
		def initSource = {
			if (prepareSource != null) prepareSource()
			
			if (inheritFields) {
				assignFieldToTemp(source, writer, map)
			}
			
			if (clear) dest.truncate()
			destParams.prepare = initDest
			writer.openWrite(destParams)
			
			if (isSaveErrors) {
				errorsDataset.field = writer.field
				errorsDataset.resetFieldToDefault()
				if (autoMap) {
					errorsDataset.removeFields { f ->
						generateResult.destFields.find { it.toLowerCase() == f.name.toLowerCase() } == null
					}
				}
				errorsDataset.field << new Field(name: "error")
				errorsDataset.openWrite()
			}
			
			List<String> result = []
			if (autoMap) {
				result = generateResult.sourceFields
			}
			
			if (initCode != null) initCode()

			result
		}
		
		sourceParams.prepare = initSource
		
		autoTran = (autoTran && !dest.connection.isTran())
		if (autoTran) {
			dest.connection.startTran()
		}
		
		long count = 0
		try {
			try {
				source.eachRow(sourceParams) { inRow ->
					boolean isError = false
					def outRow = [:]
					if (auto_map_code != null) auto_map_code(inRow, outRow)
					if (map_code != null) {
						try {
							map_code(inRow, outRow)
						}
						catch (AssertionError e) {
							if (!isSaveErrors) {
								writer.isWriteError = true
								throw e
							}
							isError = true
							Map errorRow = [:]
							errorRow.putAll(outRow)
							errorRow.error = StringUtils.ProcessAssertionError(e)
							errorsDataset.write(errorRow)
						}
						catch (Exception e) {
							Logs.Severe("FLOW ERROR IN ROW [${sourceDescription}]:\n${inRow}")
							Logs.Severe("FLOW ERROR OUT ROW [${destDescription}]:\n${outRow}")
							throw e
						}
					} 
					if (!isError) {
						writer.write(outRow)
						if (writeCode != null) writeCode(inRow, outRow)
					}
					count++
				}
				
				writer.doneWrite()
				if (isSaveErrors) errorsDataset.doneWrite()
			}
			catch (Exception e) {
				writer.isWriteError = true
				throw e
			}
			finally {
				writer.closeWrite()
				if (isSaveErrors) errorsDataset.closeWrite()
				
			}
			
			if (isBulkLoad) {
				bulkParams.source = bulkDS 
				bulkParams.autoCommit = false
				try {
					dest.bulkLoadFile(bulkParams)
				}
				catch (Throwable e) {
					if (debug && Logs.fileNameHandler != null) {
						def dn = "${Logs.DumpFolder()}/${dest.objectName}__${DateUtils.FormatDate('yyyy_MM_dd_HH_mm_ss', DateUtils.Now())}.csv"
						if (bulkAsGZIP) dn += ".gz"
						FileUtils.CopyToFile(bulkDS.fullFileName(), dn, true)
					}
					throw e
				}
			}
			
			if (doneCode != null) doneCode()
		}
		catch (Throwable e) {
			Logs.Exception(e, getClass().name + ".copy", "${sourceDescription}->${destDescription}")
			Logs.Dump(e, getClass().name + ".copy", "${sourceDescription}->${destDescription}", scriptMap)
			
			if (autoTran) {
				dest.connection.rollbackTran()
			}
			
			throw e
		}
		finally {
			if (isBulkLoad) bulkDS.drop()
		}
		
		if (autoTran) {
			dest.connection.commitTran()
		}
		
		count
	}
	
	/**
	 * Write user data to dataset
	 * <p><b>Dynamic parameters:</b></p>
	 * <ul>
	 * <li>Dataset dest					- destination dataset
	 * <li>boolean autoTran				- auto starting and finishing transaction for copy process
	 * <li>String tempDest				- name temporary dataset for dest use
	 * <li>List<Field> tempFields		- list of field from destination dataset
	 * <li>boolean bulkLoad				- load to destination as bulk load (only is supported)
	 * <li>boolean bulkAsGZIP			- generate bulk CSV file in GZIP format
	 * </ul>
	 *
	 * @param params	- parameters
	 * @param code		- user code generation rows
	 */
	public long writeTo(Map params, Closure code) {
		if (!this.params.isEmpty()) {
			params = this.params + params
		}
		
		methodParams.validation("writeTo", params)
		
		Dataset dest = params.dest
		String destDescription
		if (dest == null && params.tempDest != null) {
			if (params.tempFields == null) throw new ExceptionGETL("Required parameter \"tempFields\" from temp storage \"${params.tempDest}\"")
			dest = TFS.dataset(params.tempDest)
			destDescription = "temp.${params.tempDest}"
			dest.field = params.tempFields
		}
		if (dest == null) throw new ExceptionGETL("Required parameter \"dest\"")
		if (destDescription == null) destDescription = dest.objectName
		
		boolean autoTran = (params.autoTran != null)?params.autoTran:true
		autoTran = autoTran && (dest.connection.driver.isSupport(Driver.Support.TRANSACTIONAL) && 
								(dest.connection.params.autoCommit == null || !dest.connection.params.autoCommit))
		
		boolean isBulkLoad = (params.bulkLoad != null)?params.bulkLoad:false
		if (isBulkLoad && !dest.connection.driver.isOperation(Driver.Operation.BULKLOAD)) throw new ExceptionGETL("Destinitaion dataset not support bulk load")
		
		boolean bulkAsGZIP = (params.bulkAsGZIP != null)?params.bulkAsGZIP:false
		
		boolean clear = (params.clear != null)?params.clear:false
		
		Map destParams = MapUtils.GetLevel(params, "dest_")
		Map bulkParams
		
		TFSDataset bulkDS
		Dataset writer
		
		if (isBulkLoad) {
			bulkDS = TFS.dataset()
			dest.retrieveFields()
			bulkDS.field = dest.field
			
			bulkParams = destParams
			if (bulkAsGZIP) bulkParams.compressed = "GZIP"
			if (autoTran) bulkParams.autoCommit = false
			if (bulkAsGZIP) bulkDS.isGzFile = true
			bulkParams.source = bulkDS
			bulkParams.abortOnError = true
			
			destParams = [:]
			writer = bulkDS
		}
		else {
			writer = dest
		}

//		SynchronizeObject counter = new SynchronizeObject()
		long counter = 0		
		def updateCode = { Map row ->
			writer.write(row)
			counter++
		}
		
		if (autoTran && !isBulkLoad) {
			dest.connection.startTran()
		}
		
		if (clear) dest.truncate()
		
		def isError = false
		try {
			writer.openWrite(destParams)
			code(updateCode)
			writer.doneWrite()
		}
		catch (Throwable e) {
			isError = true
			writer.isWriteError = true
			Logs.Exception(e, getClass().name + ".writeTo", writer.objectName)
			if (autoTran && !isBulkLoad) Executor.RunIgnoreErrors { 
				dest.connection.rollbackTran() 
				}
			throw e
		}
		finally {
			if (writer.status == Dataset.Status.WRITE) Executor.RunIgnoreErrors { writer.closeWrite() }
			if (isBulkLoad && isError) Executor.RunIgnoreErrors { bulkDS.drop() }
		}
		
		if (autoTran && !isBulkLoad) {
			dest.connection.commitTran()
		}

		if (isBulkLoad) {
			if (autoTran) {
				dest.connection.startTran()
			}
			try {
				dest.bulkLoadFile(bulkParams)
			}
			catch (Throwable e) {
				Logs.Exception(e, getClass().name + ".writeTo", "${destDescription}")
				
				if (autoTran) Executor.RunIgnoreErrors { 
					dest.connection.rollbackTran() 
				}
				
				throw e
			}
			finally {
				bulkDS.drop()
			}
			if (autoTran) {
				dest.connection.commitTran()
			}
			counter = dest.updateRows
		}
		
		counter
	}
	
	/**
	 * Write user data to list of dataset
	 * <p><b>Dynamic parameters:</b></p>
	 * <ul>
	 * <li>Map<Name, Dataset> dest		- list of destination datasets
	 * <li>boolean autoTran				- auto starting and finishing transaction for copy process
	 * <li>List bulkLoad				- list of destination dataset must load as bulk load (only is supported)
	 * <li>List bulkAsGZIP				- list of destination dataset must generate bulk CSV file in GZIP format
	 * </ul>
	 *
	 * @param params	- parameters
	 * @param code		- user code generation rows
	 */
	public void writeAllTo(Map params, Closure code) {
		if (!this.params.isEmpty()) {
			params = this.params + params
		}
		
		methodParams.validation("writeAllTo", params)
		
		Map<String, Dataset> dest = params.dest
		if (dest == null) throw new ExceptionGETL("Required parameter \"dest\"")
		
		boolean autoTran = (params.autoTran != null)?params.autoTran:true
		
		List bulkLoad = (params.bulkLoad != null)?params.bulkLoad:null
		List bulkAsGZIP = (params.bulkAsGZIP != null)?params.bulkAsGZIP:null
		
		def destAutoTran = [:]
		def destParams = [:]
		def bulkParams = [:]
		def bulkLoadDS = [:]
		def writer = [:]
		dest.each { String n, Dataset d ->
			// Define is required bulk load use
			def isBulk = bulkLoad?.find { it == n} != null
			def isBulkGZip = bulkAsGZIP?.find { it == n} != null
			
			// Get destination params
			Map p = MapUtils.GetLevel(params, "dest_${n}_")
			destParams."${n}" = p
			
			// Valid auto transaction
			def isAutoTran = autoTran &&
									(d.connection.driver.isSupport(Driver.Support.TRANSACTIONAL) && !d.connection.isTran() &&
									(d.connection.params.autoCommit == null || !d.connection.params.autoCommit))
						
			// Define auto transaction condition
			if (isAutoTran) {
				def at = destAutoTran.get(d.connection)
				if (at == null || at != "ALL") {
					def tr = at
					if (at == null && !isBulk) {
						tr = "COPY"
					}
					else if (at == null && isBulk) {
						tr = "BULK"
					}
					else if (at != null && ((at == "COPY" && isBulk) || (at == "BULK" && !isBulk))) {
						tr = "ALL"
					}
					if (at != tr) destAutoTran.put(d.connection, tr)
				}
			} 
			
			// Valid support bulk load
			if (isBulk) {
				if (!d.connection.driver.isOperation(Driver.Operation.BULKLOAD)) throw new ExceptionGETL("Destination dataset \"${n}\" not support bulk load")
				d.retrieveFields()
				Dataset bulkDS = TFS.dataset()
				bulkLoadDS."${n}" = bulkDS
				
				def bp = destParams."${n}"
				if (isBulkGZip) bp.compressed = "GZIP"
				if (isAutoTran) bp.autoCommit = false
				if (bulkAsGZIP) bulkDS.isGzFile = true
				bp.source = bulkDS
				bp.abortOnError = true
				
				bulkParams."${n}" = bp
				destParams."${n}" = [:]
				
				if (bp.prepare != null) {
					List<String> useFields = bp.prepare()
					bp.prepare = null
					if (useFields.isEmpty()) {
						bulkDS.field = d.field
					}
					else {
						List<Field> lf = []
						useFields.each { String fn ->
							def f = d.fieldByName(fn)
							if (f == null) throw new ExceptionGETL("Can not find field \"${fn}\" in \"${d.objectName}\" for list result of prepare code")
							lf << f
						}
						bulkDS.field = ls
					}
				}
				else {
					bulkDS.field = d.field
				}
				
				writer."${n}" = bulkDS
			}
			else {
				writer."${n}" = d
			}
			
			
		}
		
//		SynchronizeObject curUpdater = new SynchronizeObject()
		String curUpdater
		
		def updateCode = { String name, Map row ->
			curUpdater = name
			Dataset d = writer."${name}"
			d.write(row)
		}
		
		def closeDests = { boolean isError ->
			writer.each { n, Dataset d ->
				if (d.status == Dataset.Status.WRITE) {
					if (!isError) d.closeWrite() else Executor.RunIgnoreErrors { d.closeWrite() }
				}
				if (isError && bulkLoadDS."${n}" != null) Executor.RunIgnoreErrors { bulkLoadDS."${n}".drop() }
			}
		}
		
		// Start transactions
		def startTrans = { List useMode ->
			destAutoTran.each { Connection c, String mode ->
				if (mode in useMode) {
					c.startTran()
				}
			}
		}
		
		def rollbackTrans = { List useMode ->
			destAutoTran.each { Connection c, String mode ->
				if (mode in useMode) {
					Executor.RunIgnoreErrors { c.rollbackTran() }
				}
			}
		}
		
		def commitTrans = { List useMode ->
			destAutoTran.each { Connection c, String mode ->
				if (mode in useMode) {
					c.commitTran()
				}
			}
		}
		
		startTrans(["ALL", "COPY"])
		
		writer.each { String n, Dataset d ->
			try {
				d.openWrite(destParams."${n}")
			}
			catch (Throwable e) {
				Logs.Exception(e, getClass().name + ".writeAllTo.openWrite", d.objectName)
				closeDests(true)
				rollbackTrans(["ALL", "COPY"])
				throw e
			}
		}
		
		try {
			code(updateCode)
		}
		catch (Throwable e) {
			writer.each { String n, Dataset d ->
				d.isWriteError = true
			}
			Logs.Exception(e, getClass().name + ".writeAllTo.code", curUpdater)
			closeDests(true)
			rollbackTrans(["ALL", "COPY"])
			throw e
		}
		
		try {
			writer.each { String n, Dataset d ->
				d.doneWrite()
			}
		}
		catch (Throwable e) {
			def destDescription = writer.keySet().toList().join(",")
			Logs.Exception(e, getClass().name + ".writeAllTo.doneWrite", destDescription)
			closeDests(true)
			rollbackTrans(["ALL", "COPY"])
			throw e
		}
		
		try {
			closeDests(false)
		}
		catch (Throwable e) {
			def destDescription = writer.keySet().toList().join(",")
			Logs.Exception(e, getClass().name + ".writeAllTo.closeWrite", destDescription)
			closeDests(true)
			rollbackTrans(["ALL", "COPY"])
			throw e
		}
		
		startTrans(["BULK"])
		bulkLoadDS.each { String n, Dataset d ->
			Dataset ds = dest."${n}"
			try {
				Map bp = bulkParams."${n}"
				ds.bulkLoadFile(bp)
			}
			catch (Throwable e) {
				Logs.Exception(e, getClass().name + ".writeToAll.bulkLoadFile", "${ds.objectName}")
				rollbackTrans(["ALL", "COPY", "BULK"])
				throw e
			}
		}
		
		commitTrans(["ALL", "COPY", "BULK"])
		
		bulkLoadDS.each { String n, Dataset d ->
			Executor.RunIgnoreErrors { d.drop() }
		}
	}
	
	/**
	 * Read and proccessed data from dataset
	 * <p><b>Dynamic parameters:</b></p>
	 * <ul>
	 * <li>Dataset source				- source dataset
	 * <li>String tempSource			- name temporary dataset for source use
	 * <li>boolean saveErrors			- save assert errors to temporary dataset "errorsDataset" 
	 * </ul>
	 * 
	 * @param params		- Flow parameters
	 * @param fields		- List of result fields
	 * @param code			- User code with proccess row
	 * @return 				- Count proccessed rows 
	 */
	@groovy.transform.CompileStatic
	public long process(Map params, Closure code) {
		if (!this.params.isEmpty()) {
			params = this.params + params
		}
		
		methodParams.validation("process", params)
		
		Dataset source = (Dataset)(params.source)
		if (source == null && params.tempSource != null) {
			source = TFS.dataset((String)(params.tempSource), true)
		}
		if (source == null) new ExceptionGETL("Required parameter \"source\"")
		
		Map sourceParams = MapUtils.GetLevel(params, "source_")
		
		def onInitSource = {
			errorsDataset.field = source.field
			errorsDataset.resetFieldToDefault()
			errorsDataset.field << new Field(name: "error")
			errorsDataset.openWrite()
		}
		
		boolean isSaveErrors = (params.saveErrors != null)?params.saveErrors:false
		if (isSaveErrors) {
			errorsDataset = TFS.dataset()
			sourceParams.prepare = onInitSource
		}
		else {
			errorsDataset = null
		}

		long count = 0
		try {
			source.eachRow (sourceParams) { Map row ->
				try {
					code(row)
					count++
				}
				catch (AssertionError e) {
					if (!isSaveErrors) {
						throw e
					}
					Map errorRow = [:]
					errorRow.putAll(row)
					errorRow.error = StringUtils.ProcessAssertionError(e)
					errorsDataset.write(errorRow)
				}
			}
		}
		finally {
			if (isSaveErrors) {
				if (errorsDataset.status == Dataset.Status.WRITE) {
					errorsDataset.doneWrite()
					errorsDataset.closeWrite()
				}
			}
		}

		count
	}
}
