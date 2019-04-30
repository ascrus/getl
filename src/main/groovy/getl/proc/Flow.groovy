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
		methodParams.register("copy", ["source", "tempSource", "dest", "tempDest", "inheritFields",
			"createDest", "tempFields", "map", "source_*", "dest_*", "autoMap", "autoConvert", "autoTran", "clear",
			"saveErrors", "excludeFields", "mirrorCSV", "notConverted", "bulkLoad", "bulkAsGZIP", "bulkEscaped",
			"onInit", "onWrite", "onDone", "debug", "writeSynch"])
		
		methodParams.register("writeTo", ["dest", "dest_*", "autoTran", "tempDest", "tempFields",
			"bulkLoad", "bulkAsGZIP", "bulkEscaped", "clear", "writeSynch", "onInit", "onDone"])
		methodParams.register("writeAllTo", ["dest", "dest_*", "autoTran", "bulkLoad", "bulkAsGZIP",
			"bulkEscaped", "writeSynch", "onInit", "onDone"])
		
		methodParams.register("process", ["source", "source_*", "tempSource", "saveErrors",
			"onInit", "onDone"])
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
	public final Map<String, Object> params = [:]

	protected static Map convertFieldMap(Map<String, String> map) {
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
		return result
	}
	
	protected static String findFieldInFieldMap (Map<String, Map> map, String field) {
		String result = null
		field = field.toLowerCase()

//		map.each { String k, Map v ->
//			if (v.name?.toLowerCase() == field) {
//				result = k
//				return
//			}
//		}

        def fr = map.find { String key, Map value -> value.name?.toLowerCase() == field }
        if (fr != null) result = fr.key

		return result
	}
	
	/**
	 * Transformation fields script
	 */
	public String scriptMap
	
	protected void generateMap(Dataset source, Dataset dest, Map fieldMap, Boolean autoConvert, List<String> excludeFields, List<String> notConverted, Map result) {
		def countMethod = new BigDecimal(dest.field.size() / 100).intValue() + 1
		def curMethod = 0

		StringBuilder sb = new StringBuilder()
		sb << "{ Map inRow, Map outRow ->\n"
		
		(1..countMethod).each { sb << "	method_${it}(inRow, outRow)\n" }
		sb << "}\n"

		def map = convertFieldMap(fieldMap)
		List<String> destFields = []
		List<String> sourceFields = []
		
		int c = 0
		dest.field.each { Field d ->
			c++
			
			def fieldMethod = new BigDecimal(c / 100).intValue() + 1
			if (fieldMethod != curMethod) {
				if (curMethod > 0) sb << "}\n"
				curMethod = fieldMethod
				sb << '\n@groovy.transform.CompileStatic'
				sb << "\nvoid method_${curMethod} (Map inRow, Map outRow) {\n"
			}

			// Dest field name
			def dn = d.name.toLowerCase()

			if (dn in excludeFields) {
				sb << "// Exclude field ${d.name}\n\n"
				return
			}
			
			// Map field name
			def mn = dn
			
			def convert = (!(d.name.toLowerCase() in notConverted)) && (autoConvert == null || autoConvert)
			 
			String mapFormat = null
			// Has contains field in mapping
			if (map.containsKey(dn)) {
				Map mapName = map.get(dn) as Map
				// No source map field
				if (mapName.isEmpty()) {
					// Nothing mapping
					mn = null
				}
				else {
					// Set source map field
					Field sf = source.fieldByName(mapName.name as String)
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
					dn = dn.replace("'", "\\'")
					sb << "outRow.put('${dn}', getl.utils.GenerationUtils.EMPTY_${d.type.toString().toUpperCase()})"
					destFields << d.name
				}
				else {
					sb << "// $dn: NOT VALUE REQUIRED"
				}
			}
			else {
				// Assign value
				String sn = s.name.toLowerCase().replace("'", "\\'")
				dn = dn.replace("'", "\\'")
				if (d.type == s.type || !convert) {
					sb << "outRow.put('${dn}', inRow.get('${sn}'))"
				}
				else {
					sb << "outRow.put('${dn}', "
					sb << GenerationUtils.GenerateConvertValue(d, s, mapFormat, "inRow.get('${sn}')")
					sb << ')'
				}
				destFields << d.name
				sourceFields << s.name
			}
			
			sb << "\n"
		}
		
		sb << "\n}"
		scriptMap = sb.toString()

//		println scriptMap

		result.code = GenerationUtils.EvalGroovyScript(scriptMap)
		result.sourceFields = sourceFields
		result.destFields = destFields
	}
	
	protected static void assignFieldToTemp (Dataset source, Dataset dest, Map map, List<String> excludeFields) {
		map = convertFieldMap(map)
		dest.field = source.field
		if (!excludeFields.isEmpty()) dest.field.removeAll { it.name.toLowerCase() in excludeFields }
        dest.field.each { Field f -> f.isReadOnly = false }
		map.each { k, v ->
			Field f = dest.fieldByName(v.name as String)
			if (f != null) {
				if (k != null && k != '') f.name = k else dest.removeField(f)
			}
		}
	}
	
	/**
	 * Copy rows from dataset to other dataset
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
	 * @param params	- Dynamic parameters (required)
	 * @param map_code	- Processing code  (not required)
	 * @return - Count of process rows
	 */
	public long copy (Map params, Closure map_code) {
		if (!this.params.isEmpty()) {
			params = this.params + params
		}

		methodParams.validation("copy", params)

		def p = new FlowCopySpec(params)
		p.process = map_code

		copy(p)
	}

	/**
	 * Copy rows from dataset to other dataset
	 *
	 * @param params	- Dynamic parameters (required)
	 * @return - Count of process rows
	 */
	public long copy (FlowCopySpec params) {
		params.errorsDataset = null
		params.countRow = 0
		Closure map_code = params.process

		Dataset source = params.source
		if (source == null) throw new ExceptionGETL("Required parameter \"source\"")

		String sourceDescription
		if (source == null && params.tempSourceName != null) {
			source = TFS.dataset(params.tempSourceName as String, true)
			sourceDescription = "temp.${params.tempSourceName}"
		}
		if (source == null) new ExceptionGETL("Required parameter \"source\"")
		if (sourceDescription == null) sourceDescription = source.objectName
		
		Dataset dest = params.dest
		if (dest == null) throw new ExceptionGETL("Required parameter \"dest\"")
		String destDescription
		boolean isDestTemp = false
		if (dest == null && params.tempDestName != null) {
			dest = (Dataset)TFS.dataset(params.tempDestName as String)
			destDescription = "temp.${params.tempDestName}"
			if (params.tempFields!= null) dest.setField((List<Field>)params.tempFields) else isDestTemp = true
		}
		if (dest == null) throw new ExceptionGETL("Required parameter \"dest\"")
		if (destDescription == null) destDescription = dest.objectName
		if (dest.sysParams.isTFSFile != null && dest.sysParams.isTFSFile && dest.field.isEmpty() && !isDestTemp) isDestTemp = true
		def isDestVirtual = (dest.sysParams.isVirtual != null && dest.sysParams.isVirtual) 
		
		boolean inheritFields = BoolUtils.IsValue([params.inheritFields, dest.sysParams.inheriteFields], false)
        boolean createDest = BoolUtils.IsValue([params.createDest, dest.sysParams.createDest], false)
		boolean writeSynch = BoolUtils.IsValue(params.writeSynch, false)
		
		boolean isBulkLoad = (params.bulkLoad != null)?params.bulkLoad:false
		if (isBulkLoad && (isDestTemp || isDestVirtual)) throw new ExceptionGETL("Is not possible to start the process BulkLoad for a given destination dataset")
		if (isBulkLoad && !dest.connection.driver.isOperation(Driver.Operation.BULKLOAD)) throw new ExceptionGETL("Destinitaion dataset not support bulk load")
		boolean bulkEscaped = (params.bulkEscaped != null)?params.bulkEscaped:false 
		
		boolean bulkAsGZIP = (params.bulkAsGZIP != null)?params.bulkAsGZIP:false
		
		Map map = (params.map != null)?(params.map as Map):[:]
		Map sourceParams = params.sourceParams?:[:]
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
		List<String> notConverted = (params.notConverted != null)?params.notConverted*.toLowerCase():[]
		
		Closure writeCode = params.onWrite
		Closure initCode = params.onInit
		Closure doneCode = params.onDone
		
		boolean debug = (params.debug != null)?params.debug:false

		TFSDataset errorsDataset
		if (isSaveErrors) {
			errorsDataset = TFS.dataset()
			params.errorsDataset = errorsDataset
		}
		
		Dataset writer
		TFSDataset bulkDS = null
		
		Map destParams = params.destParams?:[:]
		Map bulkParams = null
		if (isBulkLoad) {
			bulkParams = destParams
			if (bulkAsGZIP) bulkParams.compressed = "GZIP"
			if (autoTran) bulkParams.autoCommit = false
			destParams = [:]
			
			bulkDS = TFS.dataset()
			bulkDS.escaped = bulkEscaped
			if (bulkAsGZIP) {
				bulkDS.isGzFile = true
			}
			if (dest.field.isEmpty()) dest.retrieveFields()
			bulkDS.field = dest.field
			writer = bulkDS
			writeSynch = false
		}
		else {
			writer = dest
		}
		
		Closure auto_map_code
		Map generateResult = [:]
		
		def initDest = {
			List<String> result = []
			if (autoMap) {
				generateMap(source, writer, map, autoConvert, excludeFields, notConverted, generateResult)
				auto_map_code = generateResult.code
				result = generateResult.destFields
			}

			result
		}
		
		def initSource = {
			if (prepareSource != null) prepareSource()
			
			if (inheritFields) {
				assignFieldToTemp(source, writer, map, excludeFields)
			}

			if (initCode != null) initCode(source, writer)
            if (createDest) dest.create()

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
			
			result
		}
		
		sourceParams.prepare = initSource
		
		autoTran = (autoTran && !dest.connection.isTran())
		if (autoTran) {
			dest.connection.startTran()
		}
		
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
						if (!writeSynch) writer.write(outRow) else writer.writeSynch(outRow)
						if (writeCode != null) writeCode(inRow, outRow)
					}
					params.countRow++
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

		return params.countRow
	}

	/**
	 * Write user data to dataset
	 *
	 * @param params	- parameters
	 * @param code		- user code generation rows
	 */

	public long writeTo(Map params, Closure code) {
		if (!this.params.isEmpty()) {
			params = this.params + params
		}

		methodParams.validation("writeTo", params)

		def p = new FlowWriteSpec(params)
		p.process = code

		writeTo(p)
	}
	
	/**
	 * Write user data to dataset
	 *
	 * @param params - parameters
	 */
	public long writeTo(FlowWriteSpec params) {
		params.countRow = 0

		Closure code = params.process
		if (code == null) throw new ExceptionGETL("Required process code for write to destination dataset")
		
		Dataset dest = params.dest
		if (dest == null) throw new ExceptionGETL("Required parameter \"dest\"")

		String destDescription
		if (dest == null && params.tempDestName != null) {
			if (params.tempFields == null) throw new ExceptionGETL("Required parameter \"tempFields\" from temp storage \"${params.tempDestName}\"")
			dest = TFS.dataset(params.tempDestName as String)
			destDescription = "temp.${params.tempDestName}"
			dest.setField((List<Field>)params.tempFields)
		}
		if (dest == null) throw new ExceptionGETL("Required parameter \"dest\"")
		if (destDescription == null) destDescription = dest.objectName
		
		boolean autoTran = (params.autoTran != null)?params.autoTran:true
		autoTran = autoTran && (dest.connection.driver.isSupport(Driver.Support.TRANSACTIONAL) && 
								(dest.connection.params.autoCommit == null || !dest.connection.params.autoCommit))
		
		boolean isBulkLoad = (params.bulkLoad != null)?params.bulkLoad:false
		if (isBulkLoad && !dest.connection.driver.isOperation(Driver.Operation.BULKLOAD)) throw new ExceptionGETL("Destinitaion dataset not support bulk load")
		boolean bulkAsGZIP = (params.bulkAsGZIP != null)?params.bulkAsGZIP:false
		boolean bulkEscaped = (params.bulkEscaped != null)?params.bulkEscaped:false
		
		boolean clear = (params.clear != null)?params.clear:false
		
		boolean writeSynch = BoolUtils.IsValue(params."writeSynch", false)

		Closure initCode = params.onInit
		Closure doneCode = params.onDone
		
		Map destParams = params.destParams?:[:]
		Map bulkParams = null
		
		TFSDataset bulkDS = null
		Dataset writer

		if (initCode != null) initCode()
		
		if (isBulkLoad) {
			bulkDS = TFS.dataset()
			bulkDS.escaped = bulkEscaped
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
			writeSynch = false
		}
		else {
			writer = dest
		}

		def updateCode = { Map row ->
			if (!writeSynch) writer.write(row) else writer.writeSynch(row)
			params.countRow++
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

		if (doneCode != null) doneCode()

		return params.countRow
	}

	/**
	 * Write user data to list of dataset
	 *
	 * @param params	- parameters
	 * @param code		- user code generation rows
	 */
	public void writeAllTo(Map params, Closure code) {
		if (!this.params.isEmpty()) {
			params = this.params + params
		}

		methodParams.validation("writeAllTo", params)

		def p = new FlowWriteManySpec(params)
		p.process = code

		writeAllTo(p)
	}
	
	/**
	 * Write user data to list of dataset
	 *
	 * @param params	- parameters
	 */
	public void writeAllTo(FlowWriteManySpec params) {
		Closure code = params.process

		Map<String, Dataset> dest = params.dest
		if (dest == null || dest.isEmpty()) throw new ExceptionGETL("Required parameter \"dest\"")
		
		boolean autoTran = (params.autoTran != null)?params.autoTran:true
		boolean writeSynch = BoolUtils.IsValue(params."writeSynch", false)

		boolean isBulkLoad = (params.bulkLoad != null)?params.bulkLoad:false
		boolean bulkAsGZIP = (params.bulkAsGZIP != null)?params.bulkAsGZIP:false
		boolean bulkEscaped = (params.bulkEscaped != null)?params.bulkEscaped:false

		Closure initCode = params.onInit
		Closure doneCode = params.onDone
		
		Map<Connection, String> destAutoTran = [:]
		def destParams = [:]
		def bulkParams = [:]
		Map<String, Dataset> bulkLoadDS = [:]
		Map<String, Dataset> writer = [:]
		dest.each { String n, Dataset d ->
			// Get destination params
			Map p = params.destParams?.get(n)?:[:]
			destParams.put(n, p)
			
			// Valid auto transaction
			def isAutoTran = autoTran &&
									(d.connection.driver.isSupport(Driver.Support.TRANSACTIONAL) && !d.connection.isTran() &&
									(d.connection.params.autoCommit == null || !d.connection.params.autoCommit))
						
			// Define auto transaction condition
			if (isAutoTran) {
				def at = destAutoTran.get(d.connection)
				if (at == null || at != "ALL") {
					def tr = at
					if (at == null && !isBulkLoad) {
						tr = "COPY"
					}
					else if (at == null && isBulkLoad) {
						tr = "BULK"
					}
					else if (at != null && ((at == "COPY" && isBulkLoad) || (at == "BULK" && !isBulkLoad))) {
						tr = "ALL"
					}
					if (at != tr) destAutoTran.put(d.connection, tr)
				}
			} 
			
			// Valid support bulk load
			if (isBulkLoad) {
				if (!d.connection.driver.isOperation(Driver.Operation.BULKLOAD)) throw new ExceptionGETL("Destination dataset \"${n}\" not support bulk load")
				d.retrieveFields()
				TFSDataset bulkDS = TFS.dataset()
				bulkDS.escaped = bulkEscaped
				bulkLoadDS."${n}" = bulkDS
				
				def bp = destParams."${n}"
				if (bulkAsGZIP) bp.compressed = "GZIP"
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
						bulkDS.setField(lf)
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

		if (initCode != null) initCode()

		String curUpdater = null
		
		def updateCode = { String name, Map row ->
			curUpdater = name
			Dataset d = writer."${name}"
			if (!writeSynch) d.write(row) else d.writeSynch(row)
		}
		
		def closeDests = { boolean isError ->
			writer.each { String n, Dataset d ->
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
				d.openWrite(destParams."${n}" as Map)
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

		if (doneCode != null) doneCode()
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
	public long process(Map params, Closure code) {
		if (!this.params.isEmpty()) {
			params = this.params + params
		}

		methodParams.validation("process", params)

		def p = new FlowProcessSpec(params)
		p.process = code

		process(p)
	}
	
	/**
	 * Read and proccessed data from dataset
	 *
	 * @param params		- Flow parameters
	 * @return 				- Count proccessed rows
	 */
	@groovy.transform.CompileStatic
	public long process(FlowProcessSpec params) {
		params.errorsDataset = null
		params.countRow = 0

		Closure code = params.process
		if (code == null) throw new ExceptionGETL('Required \"process\" code closure')
		
		Dataset source = params.source
		if (source == null) throw new ExceptionGETL("Required parameter \"source\"")

		if (source == null && params.tempSourceName != null) {
			source = TFS.dataset((String)(params.tempSourceName), true)
		}
		if (source == null) new ExceptionGETL("Required parameter \"source\"")
		
		Map sourceParams = params.sourceParams?:[:]

		Closure initCode = params.onInit
		Closure doneCode = params.onDone
		
		boolean isSaveErrors = (params.saveErrors != null)?params.saveErrors:false
		TFSDataset errorsDataset
		if (isSaveErrors) {
			errorsDataset = TFS.dataset()
			params.errorsDataset = errorsDataset
		}

		def onInitSource = {
			if (initCode != null) initCode(source)

			if (isSaveErrors) {
				errorsDataset.field = source.field
				errorsDataset.resetFieldToDefault()
				errorsDataset.field << new Field(name: "error")
				errorsDataset.openWrite([:])
			}

			List<String> result = []
			return result
		}
		sourceParams.prepare = onInitSource

		try {
			source.eachRow (sourceParams) { Map row ->
				try {
					code(row)
					params.countRow++
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
			if (doneCode != null) doneCode()
		}
		finally {
			if (isSaveErrors) {
				if (errorsDataset.status == Dataset.Status.WRITE) {
					errorsDataset.doneWrite()
					errorsDataset.closeWrite()
				}
			}
		}

		params.countRow
	}
}