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
import getl.transform.*
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
		methodParams.register('copy',
				['source', 'tempSource', 'dest', 'destChild', 'tempDest', 'inheritFields', 'createDest',
				 'tempFields', 'map', 'source_*', 'sourceParams', 'dest_*', 'destParams', 'destChildParams',
				 'autoMap', 'autoConvert', 'autoTran', 'clear', 'saveErrors', 'excludeFields', 'mirrorCSV',
				 'notConverted', 'bulkLoad', 'bulkAsGZIP', 'bulkEscaped', 'onInit', 'onWrite', 'onDone',
				 'process', 'processChild', 'debug', 'writeSynch'])

		methodParams.register('writeTo', ['dest', 'dest_*', 'destParams', 'autoTran', 'tempDest',
										  'tempFields', 'bulkLoad', 'bulkAsGZIP', 'bulkEscaped', 'clear', 'writeSynch',
										  'onInit', 'onDone', 'process'])
		methodParams.register('writeAllTo', ['dest', 'dest_*', 'destParams', 'autoTran', 'bulkLoad',
											 'bulkAsGZIP', 'bulkEscaped', 'writeSynch', 'onInit', 'onDone', 'process'])

		methodParams.register('process', ['source', 'source_*', 'sourceParams', 'tempSource', 'saveErrors',
										  'onInit', 'onDone', 'process'])
	}
	
	/**
	 * Name in config from section "datasets"
	 */
	private String config

	String getConfig () { config }

	void setConfig (String value) {
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

	TFSDataset errorsDataset
	/**
	 * Array of rows with errors
	 */
	TFSDataset getErrorsDataset() { errorsDataset }

	Long countRow = 0
	/**
	 * Last number of rows processed
	 */
	Long getCountRow() { countRow }

	protected static Map<String, Map> ConvertFieldMap(Map<String, String> map) {
		def result = [:] as Map<String, Map>
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

        def fr = map.find { String key, Map value -> (value.name as String)?.toLowerCase() == field }
        if (fr != null) result = fr.key

		return result
	}
	
	/**
	 * Transformation fields script
	 */
	public String scriptMap

	protected static String GenerateMap(Dataset source, Dataset dest, Map fieldMap, Boolean autoConvert, List<String> excludeFields, List<String> notConverted, Map result) {
		def countMethod = (dest.field.size() / 100).intValue() + 1
		def curMethod = 0

		StringBuilder sb = new StringBuilder()
		sb << "{ Map inRow, Map outRow ->\n"
		
		(1..countMethod).each { sb << "	method_${it}(inRow, outRow)\n" }
		sb << "}\n"

		Map<String, Map> map = ConvertFieldMap(fieldMap) as Map<String, Map>
		List<String> destFields = []
		List<String> sourceFields = []
		
		int c = 0
		dest.field.each { Field d ->
			c++
			
			def fieldMethod = (c / 100).intValue() + 1
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
			String mn = dn
			
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
					if (mapName.convert != null) convert = ((mapName.convert as String).trim().toLowerCase() == "true")
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
		def scriptMap = sb.toString()

//		println scriptMap

		result.code = GenerationUtils.EvalGroovyScript(scriptMap)
		result.sourceFields = sourceFields
		result.destFields = destFields

		return scriptMap
	}

	protected static void assignFieldToTemp (Dataset source, Dataset dest, Map<String, String> map, List<String> excludeFields) {
		def fieldMap = ConvertFieldMap(map)
		dest.field = source.field
		if (!excludeFields.isEmpty()) dest.field.removeAll { it.name.toLowerCase() in excludeFields }
        dest.field.each { Field f -> f.isReadOnly = false }
		fieldMap.each { String k, Map v ->
			Field f = dest.fieldByName(v.name as String)
			if (f != null) {
				if (k != null && k != '') f.name = k else dest.removeField(f)
			}
		}
	}
	
	/**
	 * Copy rows from dataset to other dataset
	 */
	long copy (Map params, Closure map_code = null) {
		methodParams.validation("copy", params)

		errorsDataset = null
		countRow = 0

		if (map_code == null && params.process != null) map_code = params.process as Closure

		Dataset source = params.source as Dataset
		if (source == null) throw new ExceptionGETL("Required parameter \"source\"")

		String sourceDescription
		if (source == null && params.tempSource != null) {
			source = TFS.dataset(params.tempSource as String, true)
			sourceDescription = "temp.${params.tempSource}"
		}
		if (source == null) new ExceptionGETL("Required parameter \"source\"")
		if (source.connection == null) throw new ExceptionGETL("Required specify a connection for the source!")
		if (sourceDescription == null) sourceDescription = source.objectName
		
		Dataset dest = params.dest as Dataset
		if (dest == null) throw new ExceptionGETL("Required parameter \"dest\"")
		if (dest.connection == null) throw new ExceptionGETL("Required specify a connection for the destination!")
		String destDescription
		boolean isDestTemp = false
		if (dest == null && params.tempDest != null) {
			dest = (Dataset)TFS.dataset(params.tempDest as String)
			destDescription = "temp.${params.tempDest}"
			if (params.tempFields!= null) dest.setField((List<Field>)params.tempFields) else isDestTemp = true
		}
		if (dest == null) throw new ExceptionGETL("Required parameter \"dest\"")
		if (destDescription == null) destDescription = dest.objectName
		Map destSysParams = dest.sysParams as Map
		if (dest instanceof TFSDataset && dest.field.isEmpty() && !isDestTemp) isDestTemp = true
		def isDestVirtual = (dest instanceof VirtualDataset || dest instanceof MultipleDataset)

		Map<String, Dataset> destChild = params.destChild as Map<String, Dataset>?:[:]
		destChild.each { String name, Dataset dataset ->
			if (dataset.connection == null)
				throw new ExceptionGETL("No set connection property for the children dataset \"$name\"!")
			if (!dataset.connection.driver.isSupport(Driver.Support.WRITE))
				throw new ExceptionGETL("The children dataset \"$name\" does not support writing data!")
		}
		Map<String, Map<String, Object>> destChildParams = params.destChildParams as Map<String, Map<String, Object>>?:[:]
		destChildParams.each { String name, Map childParams ->
			if (!destChild.containsKey(name))
				throw new ExceptionGETL("Parameters for not described dataset \"$name\" are specified!")
		}
		Map<String, Closure> destChildProcess = params.destChildProcess as Map<String, Closure>?:[:]
		destChildProcess.each { String name, Closure childProcess ->
			if (!destChild.containsKey(name))
				throw new ExceptionGETL("Process for not described dataset \"$name\" are specified!")
		}
		
		boolean inheritFields = BoolUtils.IsValue(params.inheritFields)
		if ((dest instanceof  TFSDataset || dest instanceof AggregatorDataset) && dest.field.isEmpty())
			inheritFields = true
        boolean createDest = BoolUtils.IsValue([params.createDest, destSysParams.createDest], false)
		boolean writeSynch = BoolUtils.IsValue(params.writeSynch, false)
		
		boolean isBulkLoad = BoolUtils.IsValue(params.bulkLoad, false)
		boolean bulkEscaped = BoolUtils.IsValue(params.bulkEscaped, false)
		boolean bulkAsGZIP = BoolUtils.IsValue(params.bulkAsGZIP, false)
		if (isBulkLoad) {
			if (isDestTemp || isDestVirtual) throw new ExceptionGETL("Is not possible to start the process BulkLoad for a given destination dataset!")
			if (!dest.connection.driver.isOperation(Driver.Operation.BULKLOAD)) throw new ExceptionGETL("Destination dataset not support bulk load!")
			destChild.each { String name, Dataset dataset ->
				if (!dataset.connection.driver.isOperation(Driver.Operation.BULKLOAD)) throw new ExceptionGETL("Destination children dataset \"$name\" not support bulk load!")
			}
		}

		Map<String, String> map = (params.map != null)?(params.map as Map<String, String>):[:]

		Map<String, Object> sourceParams
		if (params.sourceParams != null && !(params.sourceParams as Map).isEmpty()) {
			sourceParams = params.sourceParams as Map<String, Object>
		}
		else {
			sourceParams = ((MapUtils.GetLevel(params, "source_") as Map<String, Object>)?:[:]) as Map<String, Object>
		}

		Closure prepareSource = sourceParams.prepare as Closure
		
		boolean autoMap = BoolUtils.IsValue(params.autoMap, true)
		boolean autoConvert = BoolUtils.IsValue(params.autoConvert, true)
		boolean autoTranParams = BoolUtils.IsValue(params.autoTran, true)
		def autoTran = autoTranParams &&
					dest.connection.driver.isSupport(Driver.Support.TRANSACTIONAL) &&
							BoolUtils.IsValue(dest.connection.params.autoCommit)
		Map<String, Boolean> autoTranChild = [:]
		destChild.each { String name, Dataset dataset ->
			autoTranChild.put(name, autoTranParams &&
					dataset.connection.driver.isSupport(Driver.Support.TRANSACTIONAL) &&
					!dataset.connection.isTran() &&
					BoolUtils.IsValue([destChildParams.get(name)?.get('autoCommit'),
									   dataset.connection.params.autoCommit]))
		}

		boolean clear = BoolUtils.IsValue(params.clear, false)
		boolean isSaveErrors = BoolUtils.IsValue(params.saveErrors, false)
		
		List<String> excludeFields = (params.excludeFields != null)?(params.excludeFields as List<String>)*.toLowerCase():[]
		List<String> notConverted = (params.notConverted != null)?(params.notConverted as List<String>)*.toLowerCase():[]
		
		Closure writeCode = params.onWrite as Closure
		Closure initCode = params.onInit as Closure
		Closure doneCode = params.onDone as Closure
		
		boolean debug = BoolUtils.IsValue(params.debug, false)

		if (isSaveErrors) errorsDataset = TFS.dataset()

		Dataset writer
		def writerChild = [:] as Map<String, Dataset>
		TFSDataset bulkDS
		def bulkChildDS = [:] as Map<String, TFSDataset>

		Map<String, Object> destParams
		if (params.destParams != null && !(params.destParams as Map).isEmpty()) {
			destParams = params.destParams as Map<String, Object>
		}
		else {
			destParams = ((MapUtils.GetLevel(params, "dest_") as Map<String, Object>)?:[:]) as Map<String, Object>
		}

		Map<String, Object> bulkParams = [:]
		Map<String, Map<String, Object>> bulkChildParams = [:]
		if (isBulkLoad) {
			bulkParams.putAll(destParams?:[:])
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

			destChild.each { String name, Dataset dataset ->
				bulkChildParams.put(name, [:])
				Map<String, Object> childParams = bulkChildParams.get(name)
				childParams.putAll(destChildParams.get(name)?:[:])
				if (bulkAsGZIP) childParams.compressed = "GZIP"
				if (autoTran) childParams.autoCommit = false
				destChildParams.get(name).clear()

				def childDS = TFS.dataset()
				childDS.escaped = bulkEscaped
				if (bulkAsGZIP) {
					childDS.isGzFile = true
				}
				if (dataset.field.isEmpty()) dataset.retrieveFields()
				childDS.field = dataset.field

				bulkChildDS.put(name, childDS)
				writerChild.put(name, childDS)
			}
		}
		else {
			writer = dest
			destChild.each { String name, Dataset dataset ->
				writerChild.put(name, dataset)
			}
		}
		
		Closure auto_map_code
		Map generateResult = [:]
		
		def initDest = {
			List<String> result = []
			if (autoMap) {
				scriptMap = GenerateMap(source, writer, map, autoConvert, excludeFields, notConverted, generateResult)
				auto_map_code = generateResult.code as Closure
				result = generateResult.destFields as List<String>
			}

			result
		}

		Closure<List<String>> initSource = {
			if (prepareSource != null) prepareSource.call()
			
			if (inheritFields) {
				assignFieldToTemp(source, writer, map, excludeFields)
			}

			if (initCode != null) initCode.call(source, writer)
            if (createDest) dest.create()

			if (clear) dest.truncate()
			destParams.prepare = initDest
			writer.openWrite(destParams)

			writerChild.each { String name, Dataset dataset ->
				Map<String, Object> childParams = destChildParams.get(name)
				dataset.openWrite(childParams)
			}
			
			if (isSaveErrors) {
				errorsDataset.field = writer.field
				errorsDataset.resetFieldToDefault()
				if (autoMap) {
					errorsDataset.removeFields { Field f ->
						generateResult.destFields.find { String n -> n.toLowerCase() == f.name.toLowerCase() } == null
					}
				}
				errorsDataset.field << new Field(name: "error")
				errorsDataset.openWrite()
			}
			
			List<String> result = []
			if (autoMap) {
				result = generateResult.sourceFields as List<String>
			}
			
			result
		}
		
		sourceParams.prepare = initSource
		
		autoTran = (autoTran && !dest.connection.isTran())
		if (autoTran) {
			dest.connection.startTran()
		}
		autoTranChild.each { String name, Boolean useTran ->
			if (BoolUtils.IsValue(useTran)) {
				Dataset dataset = destChild.get(name)
				dataset.connection.startTran()
			}
		}

		try {
			try {
				source.eachRow(sourceParams) { inRow ->
					boolean isError = false
					def outRow = [:]
					if (auto_map_code != null) auto_map_code(inRow, outRow)
					if (map_code != null) {
						try {
							map_code.call(inRow, outRow)
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
						if (writeCode != null) writeCode.call(inRow, outRow)

						writerChild.each { String name, Dataset dataset ->
							def procChild = destChildProcess.get(name)
							if (procChild != null) {
								def updateCode = { Map row ->
									if (!writeSynch) dataset.write(row) else dataset.writeSynch(row)
									countRow++
								}
								procChild.call(updateCode, inRow)
							}
						}
					}
					countRow++
				}
				
				writer.doneWrite()
				writerChild.each { String name, Dataset dataset ->
					dataset.doneWrite()
				}
				if (isSaveErrors) errorsDataset.doneWrite()
			}
			catch (Exception e) {
				writer.isWriteError = true
				throw e
			}
			finally {
				writer.closeWrite()
				writerChild.each { String name, Dataset dataset ->
					dataset.closeWrite()
				}
				if (isSaveErrors) errorsDataset.closeWrite()
				
			}
			
			if (isBulkLoad) {
				bulkParams.source = bulkDS 
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

				writerChild.each { String name, Dataset dataset ->
					def childParams = bulkChildParams.get(name)
					childParams.source = dataset
					def childDataset = destChild.get(name)
					try {
						childDataset.bulkLoadFile(childParams)
					}
					catch (Throwable e) {
						if (debug && Logs.fileNameHandler != null) {
							def dn = "${Logs.DumpFolder()}/${childDataset.objectName}__${DateUtils.FormatDate('yyyy_MM_dd_HH_mm_ss', DateUtils.Now())}.csv"
							if (bulkAsGZIP) dn += ".gz"
							FileUtils.CopyToFile(dataset.fullFileName(), dn, true)
						}
						throw e
					}
				}
			}
			
			if (doneCode != null) doneCode.call()
		}
		catch (Throwable e) {
			Logs.Exception(e, getClass().name + ".copy", "${sourceDescription}->${destDescription}")

			if (autoTran) dest.connection.rollbackTran()
			autoTranChild.each { String name, Boolean useTran ->
				if (BoolUtils.IsValue(useTran)) {
					Dataset dataset = destChild.get(name)
					dataset.connection.rollbackTran()
				}
			}
			
			throw e
		}
		finally {
			if (isBulkLoad) {
				bulkDS.drop()
				bulkChildDS.each { String name, Dataset dataset ->
					dataset.drop()
				}
			}
		}
		
		if (autoTran) dest.connection.commitTran()
		autoTranChild.each { String name, Boolean useTran ->
			if (BoolUtils.IsValue(useTran)) {
				Dataset dataset = destChild.get(name)
				dataset.connection.commitTran()
			}
		}

		return countRow
	}

	/**
	 * Write user data to dataset
	 *
	 * @param params - parameters
	 */
	long writeTo(Map params, Closure code = null) {
		methodParams.validation("writeTo", params)

		countRow = 0

		if (code == null) code = params.process as Closure
		if (code == null) throw new ExceptionGETL("Required process code for write to destination dataset")
		
		Dataset dest = params.dest as Dataset
		if (dest == null) throw new ExceptionGETL("Required parameter \"dest\"")
		if (dest.connection == null) throw new ExceptionGETL("Required specify a connection for the destination!")

		String destDescription
		if (dest == null && params.tempDest != null) {
			if (params.tempFields == null) throw new ExceptionGETL("Required parameter \"tempFields\" from temp storage \"${params.tempDest}\"")
			dest = TFS.dataset(params.tempDest as String)
			destDescription = "temp.${params.tempDest}"
			dest.setField((List<Field>)params.tempFields)
		}
		if (dest == null) throw new ExceptionGETL("Required parameter \"dest\"")
		if (destDescription == null) destDescription = dest.objectName
		
		boolean autoTran = (params.autoTran != null)?params.autoTran:true
		autoTran = autoTran && (dest.connection.driver.isSupport(Driver.Support.TRANSACTIONAL) && 
								(dest.connection.params.autoCommit == null || !dest.connection.params.autoCommit))
		
		boolean isBulkLoad = (params.bulkLoad != null)?params.bulkLoad:false
		if (isBulkLoad && !dest.connection.driver.isOperation(Driver.Operation.BULKLOAD)) throw new ExceptionGETL("Destinataion dataset not support bulk load")
		boolean bulkAsGZIP = (params.bulkAsGZIP != null)?params.bulkAsGZIP:false
		boolean bulkEscaped = (params.bulkEscaped != null)?params.bulkEscaped:false
		
		boolean clear = (params.clear != null)?params.clear:false
		
		boolean writeSynch = BoolUtils.IsValue(params."writeSynch", false)

		Closure initCode = params.onInit as Closure
		Closure doneCode = params.onDone as Closure
		
		Map<String, Object> destParams
		if (params.destParams != null && !(params.destParams as Map).isEmpty()) {
			destParams = params.destParams as Map<String, Object>
		}
		else {
			destParams = ((MapUtils.GetLevel(params, "dest_") as Map<String, Object>)?:[:]) as Map<String, Object>
		}

		Map<String, Object> bulkParams = [:]
		
		TFSDataset bulkDS = null
		Dataset writer

		if (initCode != null) initCode.call()
		
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
			countRow++
		}
		
		if (autoTran && !isBulkLoad) {
			dest.connection.startTran()
		}
		
		if (clear) dest.truncate()
		
		def isError = false
		try {
			writer.openWrite(destParams)
			code.call(updateCode)
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
			countRow = dest.updateRows
		}

		if (doneCode != null) doneCode.call()

		return countRow
	}

	/**
	 * Write user data to list of dataset
	 */
	void writeAllTo(Map params, Closure code = null) {
		methodParams.validation("writeAllTo", params)

		if (code == null) code = params.process as Closure
		if (code == null) throw new ExceptionGETL("Required process code for write to destination datasets")

		Map<String, Dataset> dest = params.dest as Map<String, Dataset>
		if (dest == null || dest.isEmpty()) throw new ExceptionGETL("Required parameter \"dest\"")
		
		boolean autoTran = (params.autoTran != null)?params.autoTran:true
		boolean writeSynch = BoolUtils.IsValue(params."writeSynch", false)

		boolean isBulkLoad = (params.bulkLoad != null)?params.bulkLoad:false
		boolean bulkAsGZIP = (params.bulkAsGZIP != null)?params.bulkAsGZIP:false
		boolean bulkEscaped = (params.bulkEscaped != null)?params.bulkEscaped:false

		Closure initCode = params.onInit as Closure
		Closure doneCode = params.onDone as Closure
		
		Map<Connection, String> destAutoTran = [:]
		def destParams = [:]
		def bulkParams = [:]
		Map<String, Dataset> bulkLoadDS = [:]
		Map<String, Dataset> writer = [:]
		dest.each { String n, Dataset d ->
			// Get destination params
			if (params.destParams != null && (params.destParams as Map).get(n) != null) {
				destParams.put(n, (params.destParams as Map).get(n) as Map<String, Object>)
			}
			else {
				Map<String, Object> p = (MapUtils.GetLevel(params, "dest_${n}_") as Map<String, Object>) ?: [:]
				destParams.put(n, p)
			}

			if (d.connection == null) throw new ExceptionGETL("Required specify a connection for the \"$n\" destination!")
			
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
				bulkLoadDS.put(n, bulkDS)
				
				def bp = destParams.get(n) as Map
				if (bulkAsGZIP) bp.compressed = "GZIP"
				if (isAutoTran) bp.autoCommit = false
				if (bulkAsGZIP) bulkDS.isGzFile = true
				bp.source = bulkDS
				bp.abortOnError = true
				
				bulkParams.put(n, bp)
				destParams.put(n, [:])
				
				if (bp.prepare != null) {
					List<String> useFields = (bp.prepare as Closure).call() as List<String>
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
				
				writer.put(n, bulkDS)
			}
			else {
				writer.put(n, d)
			}
			
			
		}

		if (initCode != null) initCode.call()

		String curUpdater = null
		
		def updateCode = { String name, Map row ->
			curUpdater = name
			Dataset d = writer.get(name)
			if (!writeSynch) d.write(row) else d.writeSynch(row)
		}
		
		def closeDests = { boolean isError ->
			writer.each { String n, Dataset d ->
				if (d.status == Dataset.Status.WRITE) {
					if (!isError) d.closeWrite() else Executor.RunIgnoreErrors { d.closeWrite() }
				}
				if (isError && bulkLoadDS.get(n) != null) Executor.RunIgnoreErrors { (bulkLoadDS.get(n) as Dataset).drop() }
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
				d.openWrite(destParams.get(n) as Map)
			}
			catch (Throwable e) {
				Logs.Exception(e, getClass().name + ".writeAllTo.openWrite", d.objectName)
				closeDests(true)
				rollbackTrans(["ALL", "COPY"])
				throw e
			}
		}
		
		try {
			code.call(updateCode)
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
			Dataset ds = dest.get(n) as Dataset
			try {
				Map bp = bulkParams.get(n) as Map
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

		if (doneCode != null) doneCode.call()
	}

	/**
	 * Read and proccessed data from dataset
	 */
	long process(Map params, Closure code = null) {
		methodParams.validation("process", params)

		errorsDataset = null
		countRow = 0

		if (code == null) code = params.process as Closure
		if (code == null) throw new ExceptionGETL('Required \"process\" code closure')
		
		Dataset source = params.source as Dataset
		if (source == null) throw new ExceptionGETL("Required parameter \"source\"")
		if (source.connection == null) throw new ExceptionGETL("Required specify a connection for the source!")

		if (source == null && params.tempSourceName != null) {
			source = TFS.dataset((String)(params.tempSourceName), true)
		}
		if (source == null) new ExceptionGETL("Required parameter \"source\"")
		
		Map<String, Object> sourceParams
		if (params.sourceParams != null && !(params.sourceParams as Map).isEmpty()) {
			sourceParams = params.sourceParams as Map<String, Object>
		}
		else {
			sourceParams = ((MapUtils.GetLevel(params, "source_") as Map<String, Object>)?:[:]) as Map<String, Object>
		}

		Closure initCode = params.onInit as Closure
		Closure doneCode = params.onDone as Closure
		
		boolean isSaveErrors = (params.saveErrors != null)?params.saveErrors:false
		if (isSaveErrors) errorsDataset = TFS.dataset()

		def onInitSource = {
			if (initCode != null) initCode.call(source)

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
					code.call(row)
					countRow++
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
			if (doneCode != null) doneCode.call()
		}
		finally {
			if (isSaveErrors) {
				if (errorsDataset.status == Dataset.Status.WRITE) {
					errorsDataset.doneWrite()
					errorsDataset.closeWrite()
				}
			}
		}

		return countRow
	}
}