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

package getl.json

import groovy.json.JsonSlurper
import groovy.transform.InheritConstructors
import getl.data.*
import getl.driver.*
import getl.exception.ExceptionGETL
import getl.utils.*

/**
 * JSON driver class
 * @author Alexsey Konstantinov
 *
 */
@InheritConstructors
class JSONDriver extends FileDriver {
	public JSONDriver () {
		methodParams.register("eachRow", ["fields", "filter"])
	}

	@Override
	public List<Driver.Support> supported() {
		[Driver.Support.EACHROW, Driver.Support.AUTOLOADSCHEMA]
	}

	@Override
	public List<Driver.Operation> operations() {
		[Driver.Operation.DROP]
	}

	@Override
    public
    List<Field> fields(Dataset dataset) {
		return null
	}
	
	/**
	 * Read only attributes from dataset
	 * @param dataset
	 * @param initAttr
	 * @param sb
	 */
	private static void generateAttrRead (Dataset dataset, Closure initAttr, StringBuilder sb) {
		List<Field> attrs = (dataset.params.attributeField != null)?dataset.params.attributeField:[]
		if (attrs.isEmpty()) return
		
		sb << "Map<String, Object> attrValue = [:]\n"
		int a = 0
		attrs.each { Field d ->
			a++
			
			Field s = d.copy()
			if (s.type == Field.Type.DATETIME) s.type = Field.Type.STRING
			
			String path = GenerationUtils.Field2Alias(d)
			sb << "attrValue.'${d.name.toLowerCase()}' = "
			sb << GenerationUtils.GenerateConvertValue(d, s, d.format, "data.${path}")
			
			sb << "\n"
		}
		sb << "dataset.params.attributeValue = attrValue\n"
		if (initAttr != null) sb << "if (!initAttr(dataset)) return\n"
		sb << "\n"
	}
	
	/**
	 * Read attributes and rows from dataset
	 * @param dataset
	 * @param listFields
	 * @param rootNode
	 * @param limit
	 * @param data
	 * @param initAttr
	 * @param code
	 */
	private void readRows (Dataset dataset, List<String> listFields, String rootNode, long limit, data, Closure initAttr, Closure code) {
		StringBuilder sb = new StringBuilder()
		generateAttrRead(dataset, initAttr, sb)
		
		if (limit > 0) sb << "long cur = 0\n"
		sb << "data" + ((rootNode != ".")?"." + rootNode:"") + ".each { struct ->\n"
		if (limit > 0) {
			sb << "cur++"
			sb << "if (cur > ${limit}) return"
		}
		sb << "	Map row = [:]\n"
		int c = 0
		dataset.field.each { Field d ->
			c++
			if (listFields.isEmpty() || listFields.find { it.toLowerCase() == d.name.toLowerCase() }) {
				Field s = d.copy()
				if (s.type == Field.Type.DATETIME) s.type = Field.Type.STRING
				
				String path = GenerationUtils.Field2Alias(d)
				sb << "	row.'${d.name.toLowerCase()}' = "
				sb << GenerationUtils.GenerateConvertValue(d, s, d.format, "struct.${path}")
				
				sb << "\n"
			}
		}
		sb << "	code(row)\n"
		sb << "}"
		
		def vars = [dataset: dataset, initAttr: initAttr, code: code, data: data]
		try {
			GenerationUtils.EvalGroovyScript(sb.toString(), vars)
		}
		catch (Exception e) {
//			Logs.Dump(e, getClass().name, dataset.toString(), "generate script:\n${sb.toString()}")
			throw e
		}
	}
	
	/**
	 * Read only attributes from dataset
	 * @param dataset
	 * @param params
	 */
	public void readAttrs (Dataset dataset, Map params) {
		params = params?:[:]
//		String rootNode = dataset.params.rootNode
		def data = readData(dataset, params)
		
		StringBuilder sb = new StringBuilder()
		generateAttrRead(dataset, null, sb)
		
		def vars = [dataset: dataset, data: data]
		try {
			GenerationUtils.EvalGroovyScript(sb.toString(), vars)
		}
		catch (Exception e) {
//			Logs.Dump(e, getClass().name, dataset.toString(), "generate script:\n${sb.toString()}")
			throw e
		}
	}
	
	/**
	 * Read JSON data from file
	 * @param dataset
	 * @param params
	 * @return
	 */
	private def readData (Dataset dataset, Map params) {
		boolean convertToList = (dataset.params.convertToList != null)?dataset.params.convertToList:false
		
		def json = new JsonSlurper()
		def data = null
		
		def reader = getFileReader(dataset, params)
		try {
			if (!convertToList) {
					data = json.parse(reader)
			}
			else {
				StringBuilder sb = new StringBuilder()
				sb << "[\n"
				reader.eachLine {
					sb << it
					sb << "\n"
				}
				int lastObjPos = sb.lastIndexOf("}")
				if (sb.substring(lastObjPos + 1, sb.length()).trim() == ',') sb.delete(lastObjPos + 1, sb.length())
				sb << "\n]"
				data = json.parseText(sb.toString())
			}
		}
		finally {
			reader.close()
		}
		
		return data
	}
	
	private void doRead(Dataset dataset, Map params, Closure prepareCode, Closure code) {
		if (dataset.field.isEmpty()) throw new ExceptionGETL("Required fields description with dataset")
		if (dataset.params.rootNode == null) throw new ExceptionGETL("Required \"rootNode\" parameter with dataset")
		String rootNode = dataset.params.rootNode
		
		String fn = fullFileNameDataset(dataset)
		if (fn == null) throw new ExceptionGETL("Required \"fileName\" parameter with dataset")
		File f = new File(fn)
		if (!f.exists()) throw new ExceptionGETL("File \"${fn}\" not found")
		
		long limit = (params.limit != null)?params.limit:0

		def data = readData(dataset, params)
		
		List<String> fields = []
		if (prepareCode != null) {
			prepareCode(fields)
		}
		else if (params.fields != null) fields = params.fields
		
		readRows(dataset, fields, rootNode, limit, data, params.initAttr as Closure, code)
	}
	
	@Override
	public long eachRow (Dataset dataset, Map params, Closure prepareCode, Closure code) {
		Closure filter = params."filter"
		
		long countRec = 0
		doRead(dataset, params, prepareCode) { row ->
			//noinspection GroovyAssignabilityCheck
			if (filter != null && !filter(row)) return
			
			countRec++
			//noinspection GroovyAssignabilityCheck
			code(row)
		}
		
		countRec
	}

	@Override
	public
	void openWrite(Dataset dataset, Map params, Closure prepareCode) {
		throw new ExceptionGETL('Not support this features!')

	}

	@Override
	public
	void write(Dataset dataset, Map row) {
		throw new ExceptionGETL('Not support this features!')

	}

	@Override
	public
	void closeWrite(Dataset dataset) {
		throw new ExceptionGETL('Not support this features!')
	}
}
