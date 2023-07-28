//file:noinspection DuplicatedCode
package getl.json

import getl.exception.DatasetError
import getl.exception.IOFilesError
import getl.exception.RequiredParameterError
import groovy.json.JsonGenerator
import groovy.json.JsonSlurper
import groovy.json.StreamingJsonBuilder
import groovy.transform.CompileStatic
import getl.data.*
import getl.driver.*
import getl.utils.*
import groovy.transform.InheritConstructors
import org.apache.groovy.json.internal.LazyMap

/**
 * JSON driver class
 * @author Alexsey Konstantinov
 *
 */
@InheritConstructors
class JSONDriver extends WebServiceDriver {
	@Override
	protected void registerParameters() {
		super.registerParameters()
		methodParams.register('eachRow', ['fields', 'filter', 'initAttr'])
	}

	@SuppressWarnings("UnnecessaryQualifiedReference")
	@Override
	List<Driver.Support> supported() {
		[Driver.Support.EACHROW, Driver.Support.AUTOLOADSCHEMA, Driver.Support.WRITE]
	}

	@SuppressWarnings("UnnecessaryQualifiedReference")
	@Override
	List<Driver.Operation> operations() {
		[Driver.Operation.DROP]
	}

	/** Current JSON connection */
	@SuppressWarnings('unused')
	JSONConnection getCurrentJSONConnection() { connection as JSONConnection }

	@Override
	List<Field> fields(Dataset dataset) {
		return null
	}
	
	/**
	 * Read only attributes from dataset
	 * @param dataset source dataset
	 * @param initAttr attributes initialization code
	 * @param sb text buffer for generated script
	 */
	@CompileStatic
	@SuppressWarnings("GrMethodMayBeStatic")
	protected void generateAttrRead(JSONDataset dataset, Closure initAttr, StringBuilder sb) {
		List<Field> attrs = dataset.attributeField?:[]
		if (attrs.isEmpty()) return
		
		sb << "Map<String, Object> attrValue = new HashMap<String, Object>()\n"
		def a = 0
		attrs.each { Field d ->
			a++
			
			Field s = d.copy()
			if (s.type == Field.Type.DATETIME) s.type = Field.Type.STRING
			
			String path = GenerationUtils.Field2Alias(d, true)
			sb << GenerationUtils.GenerateConvertValue(dest: d, source: s, format: dataset.fieldFormat(d, true),
					sourceMap: 'data', sourceValue: path, destMap: 'attrValue', cloneObject: false, saveOnlyWithValue: true)
			
			sb << "\n"
		}
		sb << "dataset.attributeValue = attrValue\n"
		if (initAttr != null)
			sb << '''if (!initAttr(dataset)) { 
	directive = Closure.DONE
	return 
}
'''
		sb << "\n"
	}
	
	/**
	 * Read attributes and rows from dataset
	 * @param dataset source dataset
	 * @param listFields list of read fields
	 * @param rootNode start read node name
	 * @param limit limit read rows (0 for unlimited)
	 * @param data json data object
	 * @param initAttr attributes initialization code
	 * @param code row process code
	 */
	@CompileStatic
	protected void readRows(JSONDataset dataset, List<String> listFields, Long limit, Object data, Closure initAttr, Closure code) {
		StringBuilder sb = new StringBuilder()
		sb << "{ getl.json.JSONDataset dataset, Closure initAttr, Closure code, Object data, Long limit ->\n"
		generateAttrRead(dataset, initAttr, sb)
		sb << 'proc(dataset, code, data, limit)\n'
		sb << '}\n'
		sb << '@groovy.transform.CompileStatic\n'
		sb << 'void proc(getl.json.JSONDataset dataset, Closure code, Object data, Long limit) {\n'

		def genScript = GenerationUtils.GenerateConvertFromBuilderMap(dataset, listFields,'Map',
				'struct','row', 1,
				2 + (dataset.rootNodePath().size() - 1), true)
		sb << genScript.head

		GenerationUtils.GenerateEachRow(dataset, 'data', 'struct', 'row', genScript.body, sb)
		sb << '\n}'
		//println sb.toString()
		//assert 1 == 0

		def script = sb.toString()
		Closure cl = dataset._cacheReadClosure(sb.toString())

		try {
			cl.call(dataset, initAttr, code, data, limit)
		}
		catch (Exception e) {
			connection.logger.severe("Json file $dataset processing error", e)
			connection.logger.dump(e, 'json', dataset.toString(), "// Generation script:\n$script")
			throw e
		}
	}
	
	/**
	 * Read only attributes from dataset
	 * @param dataset source dataset
	 * @param params process parameters
	 */
	@CompileStatic
	void readAttrs (JSONDataset dataset, Map params) {
		params = params?:new HashMap()
		def data = readData(dataset, params)
		
		StringBuilder sb = new StringBuilder()
		generateAttrRead(dataset, null, sb)
		
		def vars = [dataset: dataset, data: data]
		GenerationUtils.EvalGroovyScript(value: sb.toString(), vars: vars, owner: dataset.dslCreator)
	}
	
	/**
	 * Read JSON data from file
	 * @param dataset source dataset
	 * @param params process parameters
	 */
	@CompileStatic
	protected Object readData(JSONDataset dataset, Map params) {
		def convertToList = BoolUtils.IsValue(dataset.readOpts.convertToList)
		
		def json = new JsonSlurper()
		Object data = null
		
		def reader = getFileReader(dataset, params)
		try {
			if (!convertToList) {
					data = json.parse(reader) as Object
			}
			else {
				StringBuilder sb = new StringBuilder()
				sb << "[\n"
				reader.eachLine {
					sb << it
					sb << "\n"
				}
				def lastObjPos = sb.lastIndexOf("}")
				if (sb.substring(lastObjPos + 1, sb.length()).trim() == ',') sb.delete(lastObjPos + 1, sb.length())
				sb << "\n]"
				data = json.parseText(sb.toString()) as LazyMap
			}
		}
		catch (Exception e) {
			connection.logger.severe("Error parsing json file $dataset")
			throw e
		}
		finally {
			reader.close()
		}
		
		return data
	}

	/**
	 * Read and process JSON file
	 * @param dataset processed file
	 * @param params process parameters
	 * @param prepareCode prepare field code
	 * @param code process row code
	 */
	@CompileStatic
	protected void doRead(JSONDataset dataset, Map params, Closure prepareCode, Closure code) {
		if (dataset.field.isEmpty())
			throw new DatasetError(dataset, '#dataset.non_fields')

		def data = params.localDatasetData?:dataset.localDatasetData
		def limit = ConvertUtils.Object2Long(params.limit)?:0L

		if (data == null) {
			def fn = fullFileNameDataset(dataset)
			if (fn == null)
				throw new RequiredParameterError(dataset, 'fileName', 'read')

			File f = new File(fn)
			if (!f.exists())
				throw new IOFilesError(dataset, '#io.file.not_found', [path: fn])

			data = readData(dataset, params)
		}
		
		def fields = [] as List<String>
		if (prepareCode != null) {
			prepareCode.call(fields)
		}
		else if (params.fields != null)
			fields = ListUtils.ToList(params.fields) as List<String>

		readRows(dataset, fields, limit, data, (params.initAttr as Closure<Boolean>)?:dataset.onInitAttributes, code)
	}

	@SuppressWarnings('UnnecessaryQualifiedReference')
	@CompileStatic
	@Override
	Long eachRow(Dataset dataset, Map params, Closure prepareCode, Closure code) {
		super.eachRow(dataset, params, prepareCode, code)

		def ws = dataset as JSONDataset
		if (ws.autoCaptureFromWeb() && ws.downloadFileSize == 0)
			return 0L

		Closure<Boolean> filter = params.filter as Closure<Boolean>
		
		def countRec = 0L
		doRead(ws, params, prepareCode) { Map row ->
			if (filter != null && !(filter.call(row)))
				return
			
			countRec++
			code.call(row)
			if (code.directive == Closure.DONE)
				directive = Closure.DONE
		}
		
		return countRec
	}

	@Override
	void openWrite(Dataset dataset, Map params, Closure prepareCode) {
		def ds = dataset as JSONDataset
		if (ds.field.isEmpty())
			throw new DatasetError(dataset, "#dataset.non_fields")
		if (ds.rootNode == null)
			throw new RequiredParameterError(dataset, 'rootNode', 'write')

		(dataset.connection as JSONConnection).validPath()
		def fn = fullFileNameDataset(ds)
		if (fn == null)
			throw new RequiredParameterError(dataset, 'fileName', 'write')

		def writer = getFileWriter(ds, params, null)

		def driverParams = ds._driver_params as Map<String, Object>
		driverParams.put('write_rows', [] as List<Map<String, Object>>)
		driverParams.put('write_writer', writer)
	}

	@Override
	@CompileStatic
	void write(Dataset dataset, Map row) {
		((dataset._driver_params as Map<String, Object>).get('write_rows') as List<Map>).add(row)
	}

	@Override
	@CompileStatic
	void doneWrite (Dataset dataset) {
		def ds = dataset as JSONDataset
		def driverParams = (dataset._driver_params as Map<String, Object>)

		def writer = driverParams.get('write_writer') as Writer
		def writeRows = driverParams.get('write_rows') as List<Map<String, Object>>

		def format = ds.uniFormatDateTime()?:ds.formatTimestampWithTz()
		def gen = new JsonGenerator.Options().disableUnicodeEscaping().dateFormat(format, Locale.default).timezone(TimeZone.default.getID()).build()
		if (ds.rootNode != '.') {
			def jsonRoot = new HashMap<String, Object>()
			MapUtils.SetValue(jsonRoot, ds.rootNode, writeRows)
			jsonRoot.put(ds.rootNode, writeRows)
			new StreamingJsonBuilder(writer, jsonRoot, gen)
		}
		else {
			new StreamingJsonBuilder(writer, writeRows, gen)
		}

		ds.writtenFiles[0].countRows = writeRows.size()
	}

	@Override
	@CompileStatic
	void closeWrite(Dataset dataset) {
		def driverParams = (dataset._driver_params as Map<String, Object>)
		def writer = driverParams.get('write_writer') as Writer
		try {
			writer.close()
		}
		finally {
			driverParams.remove('write_rows')
			driverParams.remove('write_filename')
		}

		super.closeWrite(dataset)
	}
}