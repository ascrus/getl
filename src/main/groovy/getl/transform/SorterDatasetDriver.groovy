package getl.transform

import getl.data.*
import getl.driver.VirtualDatasetDriver
import getl.exception.ExceptionGETL
import getl.utils.GenerationUtils
import groovy.transform.InheritConstructors

/**
 * Sorted driver class
 * @author Alexsey Konstantinov
 *
 */
@InheritConstructors
class SorterDatasetDriver extends VirtualDatasetDriver {
	static private List<String> getFieldOrderBy(Dataset dataset) {
		def res = dataset.params.fieldOrderBy as List<String>
		if (res == null) throw new ExceptionGETL("Required parameter \"fieldOrderBy\" in dataset")

		return res
	}
	
	@Override
	void openWrite(Dataset dataset, Map params, Closure prepareCode) {
		Dataset ds = getDestination(dataset)
		def fieldOrderBy = getFieldOrderBy(dataset)
		
		ds.openWrite(params)
		if (prepareCode != null) prepareCode.call(dataset.field)
		
		dataset._driver_params.sorter_code = generateSortCode(fieldOrderBy)
		dataset._driver_params.sorter_data = new LinkedList<Map>()
	}

	@Override
	void write(Dataset dataset, Map row) {
		def data = dataset._driver_params.sorter_data as List<Map>
		data << row
	}

	@Override
	void doneWrite(Dataset dataset) {
		def data = dataset._driver_params.sorter_data as List<Map>
		def code = dataset._driver_params.sorter_code as Closure
		data.sort(true, code)
		
		Dataset ds = getDestination(dataset)
		data.each { Map row ->
			ds.write(row)
		}
		ds.doneWrite()
	}

	@Override
	void closeWrite(Dataset dataset) {
		Dataset ds = getDestination(dataset)
		ds.closeWrite()
		
		(dataset._driver_params.sorter_data as List).clear()
		dataset._driver_params.remove("sorter_data")
		dataset._driver_params.remove("sorter_code")
	}
	
	static private Closure generateSortCode(List<String> fieldOrderBy) {
		StringBuilder sb = new StringBuilder()
		sb << "{ Map row1, Map row2 ->\n	def eq\n"
		fieldOrderBy.each { field ->
			field = field.toLowerCase().replace("'", "\\'")

			sb << """
	eq = (row1.'$field' <=> row2.'$field')
	if (eq == 1) return 1
	if (eq == -1) return -1
"""
		}
		sb << "	return 0\n}"
		
		Closure result = GenerationUtils.EvalGroovyClosure(sb.toString())
		
		return result
	}
}
