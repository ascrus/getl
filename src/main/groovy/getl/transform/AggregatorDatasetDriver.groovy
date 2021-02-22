package getl.transform

import getl.data.*
import getl.driver.VirtualDatasetDriver
import getl.exception.ExceptionGETL
import getl.utils.GenerationUtils
import groovy.transform.InheritConstructors

/**
 * Aggregation driver class
 * @author Alexsey Konstantinov
 *
 */
@InheritConstructors
class AggregatorDatasetDriver extends VirtualDatasetDriver {
	
	static private List<String> getFieldByGroup(Dataset dataset) {
		def res = dataset.params.fieldByGroup as List<String>
		if (res == null) throw new ExceptionGETL("Required parameter \"fieldByGroup\" in dataset")
		res
	}
	
	static private Map<String, Map> getFieldCalc(Dataset dataset) {
		def res = dataset.params.fieldCalc as Map<String, Map>
		if (res == null) throw new ExceptionGETL("Required parameter \"fieldCalc\" in dataset")
		res
	}
	
	@Override

	void openWrite(Dataset dataset, Map params, Closure prepareCode) {
		Dataset ds = getDestination(dataset)
		def fieldByGroup = getFieldByGroup(dataset)
		def fieldCalc = getFieldCalc(dataset)
		def algorithm = dataset.params.algorithm
		if (algorithm == null) throw new ExceptionGETL("Required parameter \"algorithm\" in dataset")
		algorithm = (algorithm as String).toUpperCase()
		if (!(algorithm in ["HASH", "TREE"])) throw new ExceptionGETL("Unknown algorithm \"${algorithm}\"")
		Closure aggregateCode = generateAggrCode(fieldByGroup, fieldCalc)
		Map filter = [:]
		fieldCalc.each { name, value ->
			if (value.filter != null) {
				filter.put(name.toLowerCase(), value.filter)
			}
		}

		if (prepareCode != null) prepareCode.call(dataset.field)
		
		ds.openWrite(params)
		
		if (algorithm == "HASH") {
			LinkedHashMap data = new LinkedHashMap()
			dataset.params.aggregator_data = data
		}
		else {
			TreeMap data = new TreeMap()
			dataset.params.aggregator_data = data
		}
		
		dataset.params.aggregator_code = aggregateCode
		dataset.params.aggregator_filter = filter
	}

	@Override

	void write(Dataset dataset, Map row) {
		def data = dataset.params.aggregator_data as Map
		def aggregateCode = dataset.params.aggregator_code as Closure
		def filter = dataset.params.aggregator_filter as Map
		aggregateCode.call(row, data, filter)
	}

	@Override

	void doneWrite(Dataset dataset) {
		Dataset ds = getDestination(dataset)
		def data = dataset.params.aggregator_data as Map<Object, Map>
		data.each { key, value ->
			ds.write(value)
		}
		ds.doneWrite()
	}

	@Override

	void closeWrite(Dataset dataset) {
		Dataset ds = getDestination(dataset)
		ds.closeWrite()
		
		dataset.params.aggregator_data = null
		dataset.params.remove("aggregator_data")
		dataset.params.remove("aggregator_code")
	}
	
	static private Closure generateAggrCode(List<String> fieldByGroup, Map<String, Map> fieldCalc) {
		StringBuilder sb = new StringBuilder()
		sb << """{ Map row, Map data, Map filter ->
	List<String> key = []
	Map keyM = [:]
"""
		def num = 0
		if (!fieldByGroup.isEmpty()) {
			fieldByGroup.each { String name ->
				num++
				name = name.toLowerCase()
				sb << """
	String k_${num} = (row.'${name}' != null)?row.'${name}'.toString().toLowerCase():"NULL"
	key << k_${num}
	keyM.'${name}' = row.'${name}'
"""
			}
		
			sb << "	String keyS = key.join(\"\\t\")"
		}
		else {
			sb << "	String keyS = \"*ALL*\""
		}
			
		sb << """
	Map value = data.get(keyS)
	if (value == null) {
		value = keyM
		data.put(keyS, value)
	}
"""
		num = 0
		fieldCalc.each { String name, Map mp ->
			name = name.toLowerCase().replace("'", "\\'")
			String method = (mp.method != null)?(mp.method as String).toUpperCase():"SUM"
			def filter = mp.filter as Closure
			def source = (mp.fieldName as String)?.replace("'", "\\'")
			if (source == null && method != "COUNT") throw new ExceptionGETL("Required fieldName in parameters by field \"${name}\"")
			
			if (filter != null) {
				sb << """
	Closure valid = filter."${name}"
	if (valid(row)) {
"""
			}
			 
			switch (method) {
				case "COUNT":
					sb << """
	if (value.'${name}' != null) value.'${name}' = value.'${name}' + 1 else value.'${name}' = 1  
"""
					break

				case "SUM":
					sb << """
	if (row.'${source}' != null) {
		if (value.'${name}' != null) 
			value.'${name}' = value.'${name}' + row.'${source}'
		else
			value.'${name}' = row.'${source}'
	}
"""
					break

				case "MIN":
					sb << """
	if (row.'${source}' != null && ((value.'${name}' == null || row.'${source}' < value.'${name}'))) value.'${name}' = row.'${source}'
"""
					break

				case "MAX":
					sb << """
	if (row.'${source}' != null && ((value.'${name}' == null || row.'${source}' > value.'${name}'))) value.'${name}' = row.'${source}'
"""
					break

				default:
					throw new ExceptionGETL("Not supported method ${method}")
			}
			
			if (filter != null) sb << "}\n"
		}
		sb << "}"
		
		Closure result = GenerationUtils.EvalGroovyClosure(sb.toString())
		
		result
	}
}
