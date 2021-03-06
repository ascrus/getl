package getl.xml

import groovy.transform.CompileStatic
import getl.data.*
import getl.driver.*
import getl.exception.ExceptionGETL
import getl.utils.*
import groovy.transform.InheritConstructors
import groovy.xml.XmlParser

/**
 * XML driver class
 * @author Alexsey Konstantinov
 *
 */
@InheritConstructors
class XMLDriver extends WebServiceDriver {
	@Override
	protected void registerParameters() {
		super.registerParameters()

		methodParams.register("eachRow", ["fields", "filter", "initAttr"])
	}

	@SuppressWarnings("UnnecessaryQualifiedReference")
	@Override
	List<Driver.Support> supported() {
		[Driver.Support.EACHROW, Driver.Support.AUTOLOADSCHEMA]
	}

	@SuppressWarnings("UnnecessaryQualifiedReference")
	@Override
	List<Driver.Operation> operations() {
		[Driver.Operation.DROP]
	}

	@Override
	List<Field> fields(Dataset dataset) {
		return null
	}
	
	/**
	 * Generate attribute read code
	 */
	@SuppressWarnings("GrMethodMayBeStatic")
	protected void generateAttrRead (XMLDataset dataset, Closure<Boolean> initAttr, StringBuilder sb) {
		List<Field> attrs = dataset.attributeField?:[]
		if (attrs.isEmpty()) return
		
		sb << "Map<String, Object> attrValue = [:]\n"
		def a = 0
		attrs.each { Field d ->
			a++
			
			Field s = d.copy()
			if (s.type in [Field.Type.DATETIME, Field.Type.DATE, Field.Type.TIME, Field.Type.TIMESTAMP_WITH_TIMEZONE])
				s.type = Field.Type.STRING
			
			String path = GenerationUtils.Field2Alias(d, true)
			sb << GenerationUtils.GenerateConvertValue(dest: d, source: s, format: dataset.fieldFormat(d),
					sourceMap: 'data', sourceValue: path, destMap: 'attrValue', cloneObject:  false, saveOnlyWithValue: false)
			
			sb << "\n"
		}
		sb << "dataset.attributeValue = attrValue\n"
		if (initAttr != null) sb << "if (!initAttr.call(dataset)) return\n"
		sb << "\n"
	}

	/** Generate the default alias for field */
	@SuppressWarnings("GrMethodMayBeStatic")
	protected String field2alias(Field field, String name, Boolean isAlias, Integer defaultAccessMethod) {
		if (isAlias) {
			if (!(name in ['@', '#']))
				return StringUtils.ProcessObjectName(name, true, false)

			if (name == '@') {
				name = field.name
				defaultAccessMethod = XMLDataset.DEFAULT_ATTRIBUTE_ACCESS
			}
			else if (name == '#') {
				name = field.name
				defaultAccessMethod = XMLDataset.DEFAULT_NODE_ACCESS
			}
		}

		if (defaultAccessMethod == null)
			defaultAccessMethod = XMLDataset.DEFAULT_ATTRIBUTE_ACCESS

		String res
		def fieldName = name
		if (defaultAccessMethod == XMLDataset.DEFAULT_ATTRIBUTE_ACCESS) {
			res = "\"@$fieldName\""
		}
		else {
			switch (field.type) {
				case Field.Type.STRING:
					res = "\"${fieldName}\"[0]?.text()"
					break
				case Field.Type.INTEGER:
				 	res = "\"${fieldName}\"[0]?.toInteger()"
					break
				case Field.Type.BIGINT:
					res = "\"${fieldName}\"[0]?.toBigInteger()"
					break
				case Field.Type.NUMERIC:
					res = "\"${fieldName}\"[0]?.toBigDecimal()"
					break
				case Field.Type.DOUBLE:
					res = "\"${fieldName}\"[0]?.toDouble()"
					break
				case Field.Type.OBJECT:
					res = "\"${fieldName}\"[0]?.value()"
					break
				default:
					throw new ExceptionGETL("Not supported type \"${field.type}\" for XML dataset!")
			}
		}

		return res
	}

	/**
	 * Read attributes and rows from dataset
	 */
	@CompileStatic
	protected void readRows(XMLDataset dataset, List<String> listFields, Integer limit, Object data, Closure<Boolean> initAttr, Closure code) {
		StringBuilder sb = new StringBuilder()
		sb << "{ getl.xml.XMLDataset dataset, Closure initAttr, Closure code, groovy.util.Node data, Integer limit ->\n"
		generateAttrRead(dataset, initAttr, sb)

		sb << 'proc(dataset, code, data, limit)\n'
		sb << '}\n'
		sb << 'void proc(getl.xml.XMLDataset dataset, Closure code, groovy.util.Node data, Integer limit) {\n'

		Closure<String> prepareField = { XMLDataset ds, Field field, String name, Boolean isAlias ->
			return field2alias(field, name, isAlias, ds.defaultAccessMethod)
		}

		def genScript = GenerationUtils.GenerateConvertFromBuilderMap(dataset, listFields, 'groovy.util.Node',
				true, dataset.dataNode, 'struct', 'row', 0, 1,
				false, prepareField)
		sb << genScript.head

		def rootNode = dataset.rootNode
		sb << "def cur = 0L\n"
		sb << 'data' + ((rootNode != ".")?(".${StringUtils.ProcessObjectName(rootNode, true, true)}"):'') + ".each { struct ->\n"
		sb << """	if (limit > 0) {
	cur++
	if (cur > limit) {
		directive = Closure.DONE
		return
	}
}
"""
		sb << '	Map<String, Object> row = [:]\n'
		sb << genScript.body
		sb << "	code.call(row)\n"
		sb << "}\n}"
//		println sb.toString()

		def script = sb.toString()
		Closure cl = dataset._cacheReadClosure(script)

		try {
			cl.call(dataset, initAttr, code, data, limit)
		}
		catch (Exception e) {
			Logs.Severe("Xml file $dataset processing error: ${e.message}")
			Logs.Dump(e, 'xml', dataset.toString(), "// Generation script:\n$script")
			throw e
		}
	}
	
	/**
	 * Read only attributes from dataset
	 */
	@CompileStatic
	void readAttrs(XMLDataset dataset, Map params) {
		params = params?:[:]
		def data = readData(dataset, params)
		
		StringBuilder sb = new StringBuilder()
		generateAttrRead(dataset, null, sb)
		
		def vars = [dataset: dataset, data: data] as Map<String, Object>
		GenerationUtils.EvalGroovyScript(sb.toString(), vars)
	}
	
	/**
	 * Read XML data from file
	 */
	@CompileStatic
	protected def readData (XMLDataset dataset, Map params) {
		def xml = new XmlParser()

		dataset.features.each { String option, Boolean value ->
			xml.setFeature(option, value)
		}
		
		def data = null
		def reader = getFileReader(dataset, params)
		try {
			data = xml.parse(reader)
		}
		finally {
			reader.close()
		}
		
		return data
	}

	/**
	 * Read dataset attribute and rows
	 */
	@CompileStatic
	protected void doRead (XMLDataset dataset, Map params, Closure prepareCode, Closure code) {
		if (dataset.field.isEmpty())
			throw new ExceptionGETL("Required fields description with xml dataset!")
		if (dataset.rootNode == null)
			throw new ExceptionGETL("Required \"rootNode\" value with xml dataset!")

		String fn = fullFileNameDataset(dataset)
		if (fn == null)
			throw new ExceptionGETL("Required \"fileName\" parameter with dataset!")
		File f = new File(fn)
		if (!f.exists())
			throw new ExceptionGETL("File \"${fn}\" not found!")
		
		Integer limit = (params.limit != null)?(params.limit as Integer):0
		
		def data = readData(dataset, params)
		
		List<String> fields = []
		if (prepareCode != null) {
			prepareCode.call(fields)
		}
		else if (params.fields != null) fields = params.fields as List<String>
		
		readRows(dataset, fields, limit, data, params.initAttr as Closure<Boolean>, code)
	}

	@CompileStatic
	@Override
	Long eachRow(Dataset dataset, Map params, Closure prepareCode, Closure code) {
		Closure<Boolean> filter = params."filter" as Closure<Boolean>
		
		def countRec = 0L
		doRead(dataset as XMLDataset, params, prepareCode) { Map row ->
			if (filter != null && !filter.call(row)) return
			
			countRec++
			code.call(row)
		}
		
		return countRec
	}

	@Override
	void openWrite(Dataset dataset, Map params, Closure prepareCode) {
		throw new ExceptionGETL('Not support this features!')
	}

	@Override
	void write(Dataset dataset, Map row) {
		throw new ExceptionGETL('Not support this features!')
	}

	@Override
	void closeWrite(Dataset dataset) {
		throw new ExceptionGETL('Not support this features!')
	}
}