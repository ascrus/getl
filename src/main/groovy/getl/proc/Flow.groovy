package getl.proc

import com.fasterxml.jackson.annotation.JsonIgnore
import getl.csv.CSVDataset
import getl.data.*
import getl.driver.Driver
import getl.exception.ExceptionGETL
import getl.lang.Getl
import getl.proc.sub.FieldStatistic
import getl.proc.sub.FlowCopyChild
import getl.transform.*
import getl.utils.*
import getl.tfs.*
import groovy.transform.CompileStatic
import groovy.transform.stc.ClosureParams
import groovy.transform.stc.SimpleType
import java.util.concurrent.ConcurrentHashMap

/**
 * Data flow manager class 
 * @author Alexsey Konstantinov
 */
@CompileStatic
class Flow {
	Flow() {
		registerParameters()
		initParams()
	}

	Flow(Getl owner) {
		dslCreator = owner
		registerParameters()
		initParams()
	}

	/** Getl instance owner */
	protected Getl dslCreator

	/** Current logger */
	@JsonIgnore
	Logs getLogger() { (dslCreator?.logging?.manager != null)?dslCreator.logging.manager:Logs.global }

	/**
	 * Initialization parameters
	 */
	protected void initParams() { }

	/**
	 * Register parameters
	 */
	protected void registerParameters() {
		methodParams.register('copy',
				['source', 'tempSource', 'dest', 'destChild', 'tempDest', 'inheritFields', 'createDest',
				 'tempFields', 'map', 'source_*', 'sourceParams', 'dest_*', 'destParams',
				 'autoMap', 'autoConvert', 'autoTran', 'clear', 'saveErrors', 'excludeFields', 'mirrorCSV',
				 'notConverted', 'bulkLoad', 'bulkAsGZIP', 'bulkEscaped', 'onInit', 'onDone', 'onFilter', 'onBulkLoad',
				 'process', 'debug', 'writeSynch', 'cacheName', 'convertEmptyToNull', 'copyOnlyWithValue',
				 'formatDate', 'formatTime', 'formatDateTime', 'formatTimestampWithTz', 'uniFormatDateTime',
				 'formatBoolean', 'formatNumeric', 'copyOnlyMatching', 'statistics'])
		methodParams.register('copy.destChild',
				['dataset', 'datasetParams', 'process', 'init', 'done'])

		methodParams.register('writeTo', ['dest', 'dest_*', 'destParams', 'autoTran', 'tempDest',
										  'tempFields', 'bulkLoad', 'bulkAsGZIP', 'bulkEscaped', 'clear', 'writeSynch',
										  'onInit', 'onDone', 'process'])
		methodParams.register('writeAllTo', ['dest', 'dest_*', 'destParams', 'autoTran', 'bulkLoad',
											 'bulkAsGZIP', 'bulkEscaped', 'writeSynch', 'onInit', 'onDone', 'process'])

		methodParams.register('process', ['source', 'source_*', 'sourceParams', 'tempSource', 'saveErrors',
										  'onInit', 'onDone', 'process', 'statistics', 'onFilter'])
	}

	/** Parameters validator */
	protected ParamMethodValidator methodParams = new ParamMethodValidator()
	
	private String config
	/** Name in config from section "flows" */
	String getConfig () { config }
	/** Name in config from section "flows" */
	void setConfig (String value) {
		config = value
		if (config != null) {
			if (Config.ContainsSection("flows.${this.config}")) {
				doInitConfig.call()
			}
			else {
				Config.RegisterOnInit(doInitConfig)
			}
		}
	}
	
	/**
	 * Call init configuration
	 */
	private final Closure doInitConfig = {
		if (config == null)
			return
		Map cp = Config.FindSection("flows.${config}")

		if (cp.isEmpty())
			throw new ExceptionGETL("Config section \"flows.${config}\" not found")

		onLoadConfig(cp)
		logger.config("Load config \"flows\".\"${config}\" for flow")
	}
	
	protected void onLoadConfig (Map configSection) {
		MapUtils.MergeMap(params, configSection)
	}
	
	/** Flow parameters */
	private final Map<String, Object> params = new HashMap<String, Object>()

	/** Flow parameters */
	Map<String, Object> getParams() { params }
	/** Flow parameters */
	void setParams(Map<String, Object> value) {
		params.clear()
		initParams()
		if (value != null) params.putAll(value)
	}

	/** Dataset of error rows */
	private TFSDataset errorsDataset
	/** Dataset of error rows */
	TFSDataset getErrorsDataset() { errorsDataset }

	private Long countRow = 0
	/** Last number of rows processed */
	Long getCountRow() { countRow }

	private final Map<String, FieldStatistic> statistics = new HashMap<String, FieldStatistic>()
	/** Processed fields statistics */
	Map<String, FieldStatistic> getStatistics() { statistics }

	/** Formalize column mapping */
	protected static Map<String, Map> ConvertFieldMap(Map<String, String> map) {
		def result = new HashMap<String, Map>()
		map.each { k, v ->
			def m = new HashMap()
			if (v != null) {
				String[] l = v.split(";")
				m.name = l[0].toLowerCase()
				for (Integer i = 1; i < l.length; i++) {
					def p = l[i].indexOf("=")
					if (p == -1) {
						m.put(l[i].toLowerCase(), null)
					}
					else {
						def pName = l[i].substring(0, p).toLowerCase()
						def pValue = l[i].substring(p + 1)
						m.put(pName, pValue)
					}
				}
			}
			result.put(k.toLowerCase(), m)
		}
		return result
	}

	@SuppressWarnings('unused')
	protected static String findFieldInFieldMap (Map<String, Map> map, String field) {
		String result = null
		field = field.toLowerCase()

        def fr = map.find { String key, Map value -> (value.name as String)?.toLowerCase() == field }
        if (fr != null) result = fr.key

		return result
	}
	
	/** Transformation fields script */
	private String scriptMap
	/** Transformation fields script */
	String getScriptMap() { scriptMap }

	/** Cache code repository */
	static private final ConcurrentHashMap<String, Map<String, Object>> cacheCode = new ConcurrentHashMap<String, Map<String, Object>>()

	/**
	 * Generate code for mapping records from source to destination
	 * @param source
	 * @param dest
	 * @param fieldMap
	 * @param formats
	 * @param convertEmptyToNull
	 * @param saveOnlyWithValue
	 * @param autoConvert
	 * @param excludeFields
	 * @param notConverted
	 * @param cacheName
	 * @param result
	 * @return
	 */
	private String generateMap(Dataset source, Dataset dest, Map fieldMap, Map formats, Boolean convertEmptyToNull,
							   Boolean saveOnlyWithValue, Boolean autoConvert,
							   List<String> notConverted, String cacheName, Map result) {
		def countMethod = (dest.field.size() / 100).intValue() + 1
		def curMethod = 0

		StringBuilder sb = new StringBuilder()
		sb << "{ Map inRow, Map outRow ->\n"
		
		(1..countMethod).each { sb << "	method_${it}(inRow, outRow)\n" }
		sb << "}\n"

		Map<String, Map> map = ConvertFieldMap(fieldMap) as Map<String, Map>
		List<String> destFields = []
		List<String> sourceFields = []
		
		def c = 0
		def cf = 0
		dest.field.each { Field df ->
			// Dest field name
			def dn = df.name.toLowerCase()

			if (!map.containsKey(dn))
				return

			c++
			cf++
			
			def fieldMethod = (c / 100).intValue() + 1
			if (fieldMethod != curMethod) {
				if (curMethod > 0) sb << "}\n"
				curMethod = fieldMethod
				sb << '\n@groovy.transform.CompileStatic'
				sb << "\nvoid method_${curMethod} (Map inRow, Map outRow) {\n"
			}

			// Map field name
			String mn
			
			def convert = (!(df.name.toLowerCase() in notConverted)) && (autoConvert == null || autoConvert)
			 
			String mapFormat = null
			Map mapName = map.get(dn) as Map
			// No source map field
			if (mapName.isEmpty()) {
				// Nothing mapping
				mn = null
			}
			else {
				// Set source map field
				Field sf = source.fieldByName(mapName.name as String)
				if (sf == null)
					throw new ExceptionGETL("Not found field \"${mapName.name}\" in source dataset")

				mn = sf.name.toLowerCase()
				if (mapName.convert != null)
					convert = ((mapName.convert as String).trim().toLowerCase() == "true")
				if (mapName.format != null)
					mapFormat = mapName.format
				else
					mapFormat = GenerationUtils.FieldFormat(formats, df)
			}

			Field s
			// Use field is mapping
			if (mn != null)
				s = source.fieldByName(mn)

			// Not use
			if (s == null) {
				if (!df.isAutoincrement && !df.isReadOnly && df.compute == null) {
					dn = dn.replace("'", "\\'")
					sb << "outRow.put('${dn}', getl.utils.GenerationUtils.EMPTY_${df.type.toString().toUpperCase()})"
					destFields << df.name
				}
				else {
					sb << "// $dn: NOT VALUE REQUIRED"
				}
			}
			else {
				// Assign value
				String sn = s.name.toLowerCase().replace("'", "\\'")
				dn = dn.replace("'", "\\'")
				if ((df.type == s.type || !convert) && !saveOnlyWithValue && !convertEmptyToNull) {
					sb << "outRow.put('${dn}', inRow.get('${sn}'))"
				}
				else {
					sb << GenerationUtils.GenerateConvertValue(dest: df, source: s, format: mapFormat,
							convertEmptyToNull: convertEmptyToNull, saveOnlyWithValue: saveOnlyWithValue,
							sourceMap: 'inRow', sourceValue: '"' + sn + '"', destMap: 'outRow')
				}
				destFields << df.name
				sourceFields << s.name
			}
			
			sb << "\n"
		}
		
		sb << "\n}"
		def scriptMap = sb.toString()

//		println scriptMap

		if (cf == 0)
			throw new ExceptionGETL("No fields were found for copying data from source \"$source\" to destination \"$dest\"!")

		synchronized (cacheCode) {
			if (cacheName != null) {
				def cache = cacheCode.get(cacheName)
				if (cache != null && (cache.hash as Integer) == scriptMap.hashCode())
					result.code = cache.code as Closure
			}

			if (result.code == null) {
				result.code = GenerationUtils.EvalGroovyClosure(value: scriptMap, owner: dslCreator)
				if (cacheName != null) {
					def cache = cacheCode.get(cacheName)
					if (cache == null) {
						cache = new ConcurrentHashMap<String, Object>()
						cacheCode.put(cacheName, cache)
					}
					cache.putAll([hash: scriptMap.hashCode(), code: result.code])
				}
			}
		}
		result.sourceFields = sourceFields
		result.destFields = destFields

		return scriptMap
	}

	protected static void assignFieldToTemp(Dataset source, Dataset dest, Map<String, String> map) {
		dest.removeFields()
		source.field.each { sf ->
			def fn = sf.name.toLowerCase()
			if (map.containsKey(fn)) {
				def df = sf.clone() as Field
				df.tap {
					typeName = null
					columnClassName = null
					isReadOnly = false
					defaultValue = null
					isAutoincrement = null
					compute = null
				}
				dest.field.add(df)
			}
		}
	}
	
	/**
	 * Copy rows from dataset to other dataset
	 */
	Long copy(Map params,
			   @ClosureParams(value = SimpleType, options = ['java.util.HashMap', 'java.util.HashMap'])
					   Closure map_code = null) {
		methodParams.validation("copy", params)

		errorsDataset = null
		countRow = 0
		statistics.clear()

		if (map_code == null && params.process != null)
			map_code = params.process as Closure

		String cacheName = params.cacheName

		Dataset source = params.source as Dataset
		if (source == null)
			throw new ExceptionGETL("Required parameter \"source\"")

		String sourceDescription
		if (source == null && params.tempSource != null) {
			source = TFS.dataset(params.tempSource as String, true)
			sourceDescription = "temp.${params.tempSource}"
		}
		if (source == null)
			new ExceptionGETL("Required parameter \"source\"")
		if (source.connection == null)
			throw new ExceptionGETL("Required specify a connection for the source!")
		if (sourceDescription == null)
			sourceDescription = source.objectName
		
		Dataset dest = params.dest as Dataset
		if (dest == null)
			throw new ExceptionGETL("Required parameter \"dest\"")
		if (dest.connection == null)
			throw new ExceptionGETL("Required specify a connection for the destination!")
		String destDescription
		def isDestTemp = false
		if (dest == null && params.tempDest != null) {
			dest = (Dataset)TFS.dataset(params.tempDest as String)
			destDescription = "temp.${params.tempDest}"
			if (params.tempFields!= null) dest.setField((List<Field>)params.tempFields) else isDestTemp = true
		}
		if (dest == null)
			throw new ExceptionGETL("Required parameter \"dest\"")
		if (destDescription == null) destDescription = dest.objectName
		Map destSysParams = dest.sysParams as Map
		if (dest instanceof TFSDataset && dest.field.isEmpty() && !isDestTemp) isDestTemp = true
		def isDestVirtual = (dest instanceof VirtualDataset || dest instanceof MultipleDataset)

		def inheritFields = BoolUtils.IsValue(params.inheritFields)
		if ((dest instanceof  TFSDataset || dest instanceof AggregatorDataset) && dest.field.isEmpty())
			inheritFields = true
		def createDest = BoolUtils.IsValue([params.createDest, destSysParams.createDest], false)
		def writeSynch = BoolUtils.IsValue(params.writeSynch, false)

		def autoMap = BoolUtils.IsValue(params.autoMap, true)
		def copyOnlyMatching = BoolUtils.IsValue(params.copyOnlyMatching, false)
		def autoConvert = BoolUtils.IsValue(params.autoConvert, true)
		def autoTranParams = BoolUtils.IsValue(params.autoTran, true)
		def autoTran = autoTranParams &&
				dest.connection.isSupportTran &&
				!dest.connection.isTran() &&
				!BoolUtils.IsValue(dest.connection.params.autoCommit, false)

		def destChild = params.destChild as Map<String, Map<String, Object>>?:(new HashMap<String, Map<String, Object>>())
		def childs = new HashMap<String, FlowCopyChild>()
		destChild.each { String name, Map<String, Object> childParams ->
			def dataset = childParams.dataset as Dataset
			if (dataset == null)
				throw new ExceptionGETL("No set dataset property for the children dataset \"$name\"!")
			if (dataset.connection == null)
				throw new ExceptionGETL("No set connection property for the children dataset \"$name\"!")
			if (!dataset.connection.driver.isSupport(Driver.Support.WRITE))
				throw new ExceptionGETL("The children dataset \"$name\" does not support writing data!")

			def process = childParams.process as Closure
			if (childParams.process == null)
				throw new ExceptionGETL("No set process closure for the children dataset \"$name\"!")

			def datasetParams = childParams.datasetParams as Map
			def autoTranChild = (autoTranParams &&
					dataset.connection.isSupportTran &&
					!dataset.connection.isTran() &&
					!BoolUtils.IsValue([datasetParams?.get('autoCommit'),
										dataset.connection.params.autoCommit], false))

			def childInit = childParams.onInit as Closure
			def childDone = childParams.onDone as Closure

			def child = new FlowCopyChild(dataset: dataset, process: process, writeSynch: writeSynch,
					datasetParams: datasetParams, autoTran: autoTranChild, onInit: childInit, onDone: childDone)
			childs.put(name, child)
		}
		def isChilds = (!childs.isEmpty())

		def isBulkLoad = BoolUtils.IsValue(params.bulkLoad, false)
		Boolean bulkEscaped = params.bulkEscaped
		Boolean bulkAsGZIP = params.bulkAsGZIP
		if (isBulkLoad) {
			if (isDestTemp || isDestVirtual)
				throw new ExceptionGETL("Is not possible to start the process BulkLoad for a given destination dataset!")
			if (!dest.connection.driver.isOperation(Driver.Operation.BULKLOAD))
				throw new ExceptionGETL("Destination dataset not support bulk load!")

			childs.each { String name, FlowCopyChild child ->
				if (!(child.dataset).connection.driver.isOperation(Driver.Operation.BULKLOAD))
					throw new ExceptionGETL("Destination children dataset \"$name\" not support bulk load!")
			}
		}

		List<String> excludeFields = (params.excludeFields != null)?(params.excludeFields as List<String>)*.toLowerCase():[]
		List<String> notConverted = (params.notConverted != null)?(params.notConverted as List<String>)*.toLowerCase():[]

		Map<String, String> map = (params.map != null)?(params.map as Map<String, String>):(new HashMap<String, String>())
		List<String> requiredStatistics = (params.statistics != null)?(params.statistics as List<String>)*.toLowerCase():([] as List<String>)

		Map<String, Object> sourceParams
		if (params.sourceParams != null && !(params.sourceParams as Map).isEmpty()) {
			sourceParams = params.sourceParams as Map<String, Object>
		}
		else {
			sourceParams = ((MapUtils.GetLevel(params, "source_") as Map<String, Object>)?:new HashMap<String, Object>())
		}

		Closure prepareSource = sourceParams.prepare as Closure

		def clear = BoolUtils.IsValue(params.clear, false)
		def isSaveErrors = BoolUtils.IsValue(params.saveErrors, false)
		
		Closure initCode = params.onInit as Closure
		Closure doneCode = params.onDone as Closure
		Closure filterCode = params.onFilter as Closure
		Closure bulkLoadCode = params.onBulkLoad as Closure

		def debug = BoolUtils.IsValue(params.debug, false)

		def formats = new HashMap<String, String>()
		formats.formatDate = params.formatDate as String
		formats.formatTime = params.formatTime as String
		formats.formatDateTime = params.formatDateTime as String
		formats.formatTimestampWithTz = params.formatTimestampWithTz as String
		formats.uniFormatDateTime = params.uniFormatDateTime as String
		formats.formatBoolean = params.formatBoolean as String
		formats.formatNumeric = params.formatNumeric as String

		def convertEmptyToNull = BoolUtils.IsValue(params.convertEmptyToNull)
		def copyOnlyWithValue = BoolUtils.IsValue(params.copyOnlyWithValue)

		if (isSaveErrors)
			errorsDataset = TFS.dataset()

		Dataset writer

		Map<String, Object> destParams
		if (params.destParams != null && !(params.destParams as Map).isEmpty()) {
			destParams = params.destParams as Map<String, Object>
		}
		else {
			destParams = ((MapUtils.GetLevel(params, "dest_") as Map<String, Object>)?:new HashMap<String, Object>()) as Map<String, Object>
		}

		Map<String, Object> bulkParams = new HashMap<String, Object>()
		TFSDataset bulkDS = null
		if (isBulkLoad) {
			bulkParams.putAll(destParams?:new HashMap<String, Object>())
			if (bulkAsGZIP)
				bulkParams.compressed = "GZIP"
			if (autoTran)
				bulkParams.autoCommit = false
			destParams = new HashMap<String, Object>()

			if (dest.field.isEmpty())
				dest.retrieveFields()
			bulkDS = PrepareBulkDSParams(dest, bulkEscaped, bulkAsGZIP)
			writer = bulkDS
			bulkParams.source = bulkDS
			writeSynch = false

			childs.each { String name, FlowCopyChild child ->
				def dataset = child.dataset

				def bulkChildParams = new HashMap<String, Object>()
				child.bulkParams = bulkChildParams
				if (child.datasetParams != null)
					bulkChildParams.putAll(child.datasetParams)

				if (bulkAsGZIP)
					bulkChildParams.compressed = "GZIP"
				if (child.autoTran)
					bulkChildParams.autoCommit = false
				child.datasetParams?.clear()

				if (dataset.field.isEmpty())
					dataset.retrieveFields()

				def childDS = TFS.dataset()
				childDS.field = dataset.field
				dataset.prepareCsvTempFile(childDS)
				if (bulkEscaped != null)
					childDS.escaped = bulkEscaped
				if (bulkAsGZIP != null)
					childDS.isGzFile = bulkAsGZIP

				bulkChildParams.source = childDS
				child.writer = childDS
			}
		}
		else {
			writer = dest
			childs.each { String name, FlowCopyChild child ->
				child.writer = child.dataset
			}
		}
		
		Closure auto_map_code
		Map generateResult = new HashMap()
		Map<String, String> mapRules = null

		Closure<List<String>> initDest = {
			List<String> result = []
			if (autoMap) {
				scriptMap = generateMap(source, writer, mapRules, formats, convertEmptyToNull, copyOnlyWithValue,
										autoConvert, notConverted, cacheName, generateResult)
				auto_map_code = generateResult.code as Closure
				result = generateResult.destFields as List<String>
			}

			return result
		}

		Closure<List<String>> initSource = {
			List<String> result = []

			if (prepareSource != null)
				result = prepareSource.call() as List<String>

			mapRules = PrepareMap(source, (inheritFields)?source:dest, map, copyOnlyMatching, autoMap, excludeFields)
			
			if (inheritFields)
				assignFieldToTemp(source, writer, mapRules)
			else
				if (isBulkLoad)
					PrepareBulkDsFields(dest, bulkDS, mapRules)

			if (initCode != null)
				initCode.call()

			childs.each { String name, FlowCopyChild child ->
				if (child.onInit != null)
					child.onInit.call()
			}

            if (createDest)
				dest.create()

			if (clear)
				dest.truncate(truncate: false)

			destParams.prepare = initDest
			if (!writeSynch)
				writer.openWrite(destParams)
			else
				writer.openWriteSynch(destParams)

			childs.each { String name, FlowCopyChild child ->
				def childWriter = child.writer
				def datasetParams = child.datasetParams
				if (!writeSynch)  childWriter.openWrite(datasetParams) else childWriter.openWriteSynch(datasetParams)
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
			

			return result
		}
		
		sourceParams.prepare = initSource
		
		autoTran = (autoTran && !dest.connection.isTran())
		if (autoTran) {
			dest.connection.startTran()
		}
		childs.each { String name, FlowCopyChild child ->
			def dataset = child.dataset as Dataset
			def autoTranChild = BoolUtils.IsValue(child.autoTran)
			if (autoTranChild) dataset.connection.startTran()
		}

		try {
			try {
				source.eachRow(sourceParams) { inRow ->
					if (filterCode != null) {
						if (!filterCode.call(inRow))
							return
					}

					def isError = false
					def outRow = new HashMap()

					if (auto_map_code != null) {
						try {
							auto_map_code.call(inRow, outRow)
						}
						catch (Exception e) {
							logger.severe("Column auto mapping error: ${e.message}")
							logger.dump(e, 'flow', cacheName?:'none', 'Column mapping:\n' + scriptMap)
							throw e
						}
					}

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
							Map errorRow = new HashMap()
							errorRow.putAll(outRow)
							errorRow.error = StringUtils.ProcessAssertionError(e)
							errorsDataset.write(errorRow)
						}
						catch (Exception e) {
							logger.severe("Flow error in row [${sourceDescription}]:\n${inRow}")
							logger.severe("Flow error out row [${destDescription}]:\n${outRow}")
							throw e
						}
					} 
					if (!isError) {
						if (!writeSynch)
							writer.write(outRow)
						else
							writer.writeSynch(outRow)

						if (isChilds) {
							childs.each { String name, FlowCopyChild child ->
								child.processRow(inRow.clone() as Map)
							}
						}

						requiredStatistics.each { fieldName ->
							def val = inRow.get(fieldName) as Comparable
							if (val != null) {
								def statVal = statistics.get(fieldName)
								if (statVal == null)
									statistics.put(fieldName, new FieldStatistic(fieldName, val))
								else {
									if (statVal.minimumValue > val)
										statVal.minimumValue = val
									if (statVal.maximumValue < val)
										statVal.maximumValue = val
								}
							}
						}

						countRow++
					}
				}
				
				writer.doneWrite()
				childs.each { String name, FlowCopyChild child ->
					def childWriter = child.writer
					childWriter.doneWrite()
				}
				if (isSaveErrors) errorsDataset.doneWrite()
			}
			catch (Exception e) {
				writer.isWriteError = true
				throw e
			}
			finally {
				if (!writeSynch) writer.closeWrite() else writer.closeWriteSynch()
				childs.each { String name, FlowCopyChild child ->
					def childWriter = child.writer
					if (!writeSynch) childWriter.closeWrite() else childWriter.closeWriteSynch()
				}
				if (isSaveErrors) errorsDataset.closeWrite()
				
			}
			
			if (isBulkLoad) {
				if (bulkLoadCode != null)
					bulkLoadCode.call(bulkParams)
				try {
					dest.bulkLoadFile(bulkParams)
				}
				catch (Exception e) {
					if (debug && logger.fileNameHandler != null) {
						def dn = "${logger.dumpFolder()}/${dest.objectName}__${DateUtils.FormatDate('yyyy_MM_dd_HH_mm_ss', DateUtils.Now())}.csv"
						if (bulkAsGZIP) dn += ".gz"
						FileUtils.CopyToFile((bulkParams.source as CSVDataset).fullFileName(), dn, true)
					}
					throw e
				}

				childs.each { String name, FlowCopyChild child ->
					def dataset = child.dataset
					def bulkChildParams = child.bulkParams

					try {
						dataset.bulkLoadFile(bulkChildParams)
					}
					catch (Exception e) {
						if (debug && logger.fileNameHandler != null) {
							def dn = "${logger.dumpFolder()}/${(bulkChildParams.source as Dataset).objectName}__${DateUtils.FormatDate('yyyy_MM_dd_HH_mm_ss', DateUtils.Now())}.csv"
							if (bulkAsGZIP) dn += ".gz"
							FileUtils.CopyToFile((bulkChildParams.source as CSVDataset).fullFileName(), dn, true)
						}
						throw e
					}
				}
			}
			
			if (doneCode != null)
				doneCode.call()
			childs.each { String name, FlowCopyChild child ->
				if (child.onDone != null) {
					child.onDone.call()
				}
			}
		}
		catch (Exception e) {
			logger.severe("Error copying rows from \"${sourceDescription}\" to \"${destDescription}\": ${e.message}")

			if (autoTran && dest.connection.isTran())
				Executor.RunIgnoreErrors(dslCreator) {
					dest.connection.rollbackTran()
				}
			childs.each { String name, FlowCopyChild child ->
				def dataset = child.dataset
				def autoTranChild = BoolUtils.IsValue(child.autoTran)
				if (autoTranChild && dataset.connection.isTran())
					Executor.RunIgnoreErrors(dslCreator) {
						dataset.connection.rollbackTran()
					}
			}
			throw e
		}
		finally {
			if (isBulkLoad) {
				(bulkParams.source as CSVDataset).drop()
				childs.each { String name, FlowCopyChild child ->
					def bulkChildParams = child.bulkParams
					(bulkChildParams.source as Dataset).drop()
				}
			}
		}
		
		if (autoTran)
			dest.connection.commitTran()

		childs.each { String name, FlowCopyChild child ->
			def dataset = child.dataset as Dataset
			def autoTranChild = BoolUtils.IsValue(child.autoTran)
			if (autoTranChild)
				dataset.connection.commitTran()
		}

		return countRow
	}

	/**
	 * Prepare mapping between source and destination datasets
	 * @param source source dataset
	 * @param dest destination dataset
	 * @param map custom mapping rules
	 * @param copyOnlyMatching write to the destination dataset only the fields present in the source dataset
	 * @param autoMap auto mapping by field names
	 * @param excludeFields list of fields to exclude from the source
	 * @return mapping result
	 */
	static protected Map<String, String> PrepareMap(Dataset source, Dataset dest, Map<String, String> map,
										  Boolean copyOnlyMatching, Boolean autoMap, List<String> excludeFields) {
		def res = new HashMap<String, String>()

		if (autoMap) {
			source.field.each { f ->
				def fn = f.name.toLowerCase()
				if (!(fn in excludeFields) && dest.fieldByName(fn) != null)
					res.put(fn, fn)
			}
		}
		map.each {k , v -> res.put(k.toLowerCase(), v) }

		if (!copyOnlyMatching)
			dest.field.each { f ->
				def fn = f.name.toLowerCase()
				if (!res.containsKey(fn) && !(fn in excludeFields))
					res.put(fn, null)
			}

		return res
	}

	/**
	 * Prepare bulk load dataset options
	 * @param dest destination dataset
	 * @param bulkEscaped use escape sequences in CSV
	 * @param bulkAsGZIP write CSV file with GZ compression
	 * @return temporary file dataset
	 */
	static protected TFSDataset PrepareBulkDSParams(Dataset dest, Boolean bulkEscaped, Boolean bulkAsGZIP) {
		TFSDataset bulkDS = TFS.dataset()
		dest.prepareCsvTempFile(bulkDS)
		if (bulkEscaped != null)
			bulkDS.escaped = bulkEscaped
		if (bulkAsGZIP != null)
			bulkDS.isGzFile = bulkAsGZIP

		return bulkDS
	}

	/**
	 * Prepare bulk load dataset fields
	 * @param dataset destination dataset
	 * @param bulkDS bulk load dataset
	 * @param map field mapping
	 */
	static protected void PrepareBulkDsFields(Dataset dataset, TFSDataset bulkDS, Map<String, String> map) {
		dataset.field.each {f ->
			if (map.containsKey(f.name.toLowerCase()))
				bulkDS.addField Field.New(f.name) { type = f.type; length = f.length; precision = f.precision }
		}
	}

	/**
	 * Write user data to dataset
	 *
	 * @param params - parameters
	 */
	@SuppressWarnings("DuplicatedCode")
	Long writeTo(Map params,
				 @ClosureParams(value = SimpleType, options = ['groovy.lang.Closure'])
						 Closure code = null) {
		methodParams.validation("writeTo", params)

		countRow = 0

		if (code == null) code = params.process as Closure
		if (code == null)
			throw new ExceptionGETL("Required process code for write to destination dataset")
		
		Dataset dest = params.dest as Dataset
		if (dest == null)
			throw new ExceptionGETL("Required parameter \"dest\"")
		if (dest.connection == null)
			throw new ExceptionGETL("Required specify a connection for the destination!")

		if (dest == null && params.tempDest != null) {
			if (params.tempFields == null)
				throw new ExceptionGETL("Required parameter \"tempFields\" from temp storage \"${params.tempDest}\"")
			dest = TFS.dataset(params.tempDest as String)
			dest.setField((List<Field>)params.tempFields)
		}
		if (dest == null)
			throw new ExceptionGETL("Required parameter \"dest\"")

		def autoTran = BoolUtils.IsValue(params.autoTran, true)
		autoTran = autoTran &&
				dest.connection.isSupportTran &&
				!dest.connection.isTran() &&
				!BoolUtils.IsValue(dest.connection.params.autoCommit, false)
		
		def isBulkLoad = BoolUtils.IsValue(params.bulkLoad)
		if (isBulkLoad && !dest.connection.driver.isOperation(Driver.Operation.BULKLOAD))
			throw new ExceptionGETL("Destination dataset not support bulk load")
		def bulkAsGZIP = BoolUtils.IsValue(params.bulkAsGZIP)
		def bulkEscaped = BoolUtils.IsValue(params.bulkEscaped)
		
		def clear = BoolUtils.IsValue(params.clear)
		
		def writeSynch = BoolUtils.IsValue(params."writeSynch", false)

		Closure initCode = params.onInit as Closure
		Closure doneCode = params.onDone as Closure
		
		Map<String, Object> destParams
		if (params.destParams != null && !(params.destParams as Map).isEmpty()) {
			destParams = params.destParams as Map<String, Object>
		}
		else {
			destParams = ((MapUtils.GetLevel(params, "dest_") as Map<String, Object>)?:new HashMap<String, Object>())
		}

		Map<String, Object> bulkParams = new HashMap<String, Object>()
		
		TFSDataset bulkDS = null
		Dataset writer

		if (initCode != null) initCode.call()

		if (isBulkLoad) {
			bulkDS = TFS.dataset()
			bulkDS.escaped = bulkEscaped
			if (dest.field.isEmpty()) dest.retrieveFields()
			bulkDS.field = dest.field
			
			bulkParams = destParams
			if (bulkAsGZIP) bulkParams.compressed = "GZIP"
			if (autoTran) bulkParams.autoCommit = false
			if (bulkAsGZIP) bulkDS.isGzFile = true
			bulkParams.source = bulkDS
			if (bulkParams.abortOnError == null) bulkParams.abortOnError = true
			
			destParams = new HashMap<String, Object>()
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
		
		if (clear) dest.truncate(truncate: false)
		
		def isError = false
		try {
			if (!writeSynch) writer.openWrite(destParams) else writer.openWriteSynch(destParams)
			code.call(updateCode)
			writer.doneWrite()
		}
		catch (Exception e) {
			isError = true
			writer.isWriteError = true
			logger.severe("Error writing rows to \"${writer.objectName}\": ${e.message}")
			if (autoTran && !isBulkLoad && dest.connection.isTran())
				Executor.RunIgnoreErrors(dslCreator) {
					dest.connection.rollbackTran()
				}
			throw e
		}
		finally {
			if (writer.status == Dataset.Status.WRITE)
				Executor.RunIgnoreErrors(dslCreator) {
					if (!writeSynch)
						writer.closeWrite()
					else
						writer.closeWriteSynch()
				}
			if (isBulkLoad && isError)
				Executor.RunIgnoreErrors(dslCreator) { bulkDS.drop() }
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
			catch (Exception e) {
				logger.severe("Error loading CSV file \"${bulkDS.fullFileName()}\" to \"${writer.objectName}\": ${e.message}")
				
				if (autoTran && dest.connection.isTran())
					Executor.RunIgnoreErrors(dslCreator) {
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
	void writeAllTo(Map params, @ClosureParams(value = SimpleType, options = ['groovy.lang.Closure'])
			Closure code = null) {
		methodParams.validation("writeAllTo", params)

		if (code == null) code = params.process as Closure
		if (code == null)
			throw new ExceptionGETL("Required process code for write to destination datasets")

		Map<String, Dataset> dest = params.dest as Map<String, Dataset>
		if (dest == null || dest.isEmpty())
			throw new ExceptionGETL("Required parameter \"dest\"")
		
		def autoTran = BoolUtils.IsValue(params.autoTran, true)
		def writeSynch = BoolUtils.IsValue(params."writeSynch", false)

		def isBulkLoad = BoolUtils.IsValue(params.bulkLoad)
		def bulkAsGZIP = BoolUtils.IsValue(params.bulkAsGZIP)
		def bulkEscaped = (params.bulkEscaped)

		Closure initCode = params.onInit as Closure
		Closure doneCode = params.onDone as Closure

		if (initCode != null) initCode.call()
		
		Map<Connection, String> destAutoTran = new HashMap<Connection, String>()
		def destParams = new HashMap()
		def bulkParams = new HashMap()
		Map<String, Dataset> bulkLoadDS = new HashMap<String, Dataset>()
		Map<String, Dataset> writer = new HashMap<String, Dataset>()
		dest.each { String n, Dataset d ->
			// Get destination params
			if (params.destParams != null && (params.destParams as Map).get(n) != null) {
				destParams.put(n, (params.destParams as Map).get(n) as Map<String, Object>)
			}
			else {
				Map<String, Object> p = (MapUtils.GetLevel(params, "dest_${n}_") as Map<String, Object>)
						?:new HashMap<String, Object>()
				destParams.put(n, p)
			}

			if (d.connection == null)
				throw new ExceptionGETL("Required specify a connection for the \"$n\" destination!")
			
			// Valid auto transaction
			def isAutoTran = autoTran &&
                    d.connection.isSupportTran &&
                    !d.connection.isTran() &&
					!BoolUtils.IsValue(d.connection.params.autoCommit, false)
						
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
				if (!d.connection.driver.isOperation(Driver.Operation.BULKLOAD))
					throw new ExceptionGETL("Destination dataset \"${n}\" not support bulk load")
				if (d.field.isEmpty()) d.retrieveFields()
				TFSDataset bulkDS = TFS.dataset()
				bulkDS.escaped = bulkEscaped
				bulkLoadDS.put(n, bulkDS)
				
				def bp = destParams.get(n) as Map
				if (bulkAsGZIP) bp.compressed = "GZIP"
				if (isAutoTran) bp.autoCommit = false
				if (bulkAsGZIP) bulkDS.isGzFile = true
				bp.source = bulkDS
				if (bp.abortOnError == null) bp.abortOnError = true
				
				bulkParams.put(n, bp)
				destParams.put(n, new HashMap())
				
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
							if (f == null)
								throw new ExceptionGETL("Can not find field \"${fn}\" in \"${d.objectName}\" for list result of prepare code")
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

		String curUpdater = null
		
		def updateCode = { String name, Map row ->
			curUpdater = name
			Dataset d = writer.get(name)
			if (!writeSynch) d.write(row) else d.writeSynch(row)
		}
		
		def closeDestinations = { Boolean isError ->
			writer.each { String n, Dataset d ->
				if (d.status == Dataset.Status.WRITE) {
					if (!isError)
						if (!writeSynch) d.closeWrite()  else d.closeWriteSynch()
					else
						Executor.RunIgnoreErrors(dslCreator) {
							if (!writeSynch) d.closeWrite()  else d.closeWriteSynch()
						}
				}
				if (isError && bulkLoadDS.get(n) != null)
					Executor.RunIgnoreErrors(dslCreator) { (bulkLoadDS.get(n) as Dataset).drop() }
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
				if (mode in useMode && c.isTran()) {
					Executor.RunIgnoreErrors(dslCreator) { c.rollbackTran() }
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
				if (!writeSynch)
					d.openWrite(destParams.get(n) as Map) else d.openWriteSynch(destParams.get(n) as Map)
			}
			catch (Exception e) {
				logger.severe("Error writing rows to \"${d.objectName}\": ${e.message}")
				closeDestinations(true)
				rollbackTrans(["ALL", "COPY"])
				throw e
			}
		}
		
		try {
			code.call(updateCode)
		}
		catch (Exception e) {
			writer.each { String n, Dataset d ->
				d.isWriteError = true
			}
			logger.severe("Error writing rows to \"${writer.collect { name, ds -> '"' + ds.objectName + '"' }}\": ${e.message}")
			closeDestinations(true)
			rollbackTrans(["ALL", "COPY"])
			throw e
		}
		
		try {
			writer.each { String n, Dataset d ->
				d.doneWrite()
			}
		}
		catch (Exception e) {
			logger.severe("Error saving buffer to \"${writer.collect { name, ds -> '"' + ds.objectName + '"' }}\": ${e.message}")
			closeDestinations(true)
			rollbackTrans(["ALL", "COPY"])
			throw e
		}
		
		try {
			closeDestinations(false)
		}
		catch (Exception e) {
			logger.severe("Close error \"${writer.collect { name, ds -> '"' + ds.objectName + '"' }}\": ${e.message}")
			closeDestinations(true)
			rollbackTrans(["ALL", "COPY"])
			throw e
		}
		
		startTrans(["BULK"])
		bulkLoadDS.each { String n, Dataset d ->
			Dataset ds = dest.get(n) as Dataset
			Map bp = bulkParams.get(n) as Map
			try {
				ds.bulkLoadFile(bp)
			}
			catch (Exception e) {
				logger.severe("Error loading CSV file \"${bp.source}\" to \"${ds.objectName}\": ${e.message}")
				rollbackTrans(["ALL", "COPY", "BULK"])
				throw e
			}
		}
		
		commitTrans(["ALL", "COPY", "BULK"])
		
		bulkLoadDS.each { String n, Dataset d ->
			Executor.RunIgnoreErrors(dslCreator) { d.drop() }
		}

		if (doneCode != null) doneCode.call()
	}

	/**
	 * Read and processed data from dataset
	 */
	Long process(Map params,
				 @ClosureParams(value = SimpleType, options = ['java.util.HashMap'])
						 Closure code = null) {
		methodParams.validation("process", params)

		errorsDataset = null
		countRow = 0

		if (code == null) code = params.process as Closure
		if (code == null)
			throw new ExceptionGETL('Required \"process\" code closure')
		
		Dataset source = params.source as Dataset
		if (source == null)
			throw new ExceptionGETL("Required parameter \"source\"")
		if (source.connection == null)
			throw new ExceptionGETL("Required specify a connection for the source!")

		if (source == null && params.tempSourceName != null) {
			source = TFS.dataset((String)(params.tempSourceName), true)
		}
		if (source == null) new ExceptionGETL("Required parameter \"source\"")
		
		Map<String, Object> sourceParams
		if (params.sourceParams != null && !(params.sourceParams as Map).isEmpty()) {
			sourceParams = params.sourceParams as Map<String, Object>
		}
		else {
			sourceParams = ((MapUtils.GetLevel(params, "source_") as Map<String, Object>)?:new HashMap<String, Object>())
		}

		Closure initCode = params.onInit as Closure
		Closure doneCode = params.onDone as Closure
		
		def isSaveErrors = BoolUtils.IsValue(params.saveErrors)
		if (isSaveErrors) errorsDataset = TFS.dataset()

		def onInitSource = {
			if (initCode != null) initCode.call()

			if (isSaveErrors) {
				errorsDataset.field = source.field
				errorsDataset.resetFieldToDefault()
				errorsDataset.field << new Field(name: "error")
				errorsDataset.openWrite(new HashMap())
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
					Map errorRow = new HashMap()
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