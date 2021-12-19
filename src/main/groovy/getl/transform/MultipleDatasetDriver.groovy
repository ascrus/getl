package getl.transform

import getl.csv.CSVDataset
import getl.data.*
import getl.driver.Driver
import getl.exception.ExceptionGETL
import groovy.transform.InheritConstructors

/**
 * Multiple writer driver class
 * @author Alexsey Konstantinov
 *
 */
@InheritConstructors
class MultipleDatasetDriver extends Driver {
	@SuppressWarnings("UnnecessaryQualifiedReference")
	@Override
	List<Support> supported() {
		return [Driver.Support.WRITE]
	}

	@Override
	List<Operation> operations() {
		return []
	}

	@Override
	Boolean isConnected() {
		return true
	}

	@Override
	void connect() { }

	@Override
	void disconnect() { }

	@Override
	List<Object> retrieveObjects(Map params, Closure<Boolean> filter) {
		throw new ExceptionGETL("Retrieve objects not supported")
	}
	
	static private Map<String, Dataset> getDestination(Dataset dataset) {
		def ds = dataset.params.dest as Map<String, Dataset>
		if (ds == null) throw new ExceptionGETL("Required MultipleDataset object class")
		if (ds.isEmpty()) throw new ExceptionGETL("Required set param \"dest\" with dataset")
		
		return ds
	}

	@Override
	List<Field> fields(Dataset dataset) {
		throw new ExceptionGETL("Retrieve fields not supported")
	}
	
	static private List<Field> destField(Dataset dataset) {
		Map<String, Dataset> ds = getDestination(dataset)
		
		def alias = ds.keySet().toArray()
		return ds.get(alias[0]).field
	}

	@Override
	void startTran(Boolean useSqlOperator = false) { }

	@Override
	void commitTran(Boolean useSqlOperator = false) { }

	@Override
	void rollbackTran(Boolean useSqlOperator = false) { }

	@Override
	void createDataset(Dataset dataset, Map params) { }

	@Override
	void dropDataset(Dataset dataset, Map params) { }

	@Override
	Long eachRow(Dataset dataset, Map params, Closure prepareCode, Closure code) { return 0 }

	@Override
	void openWrite(Dataset dataset, Map params, Closure prepareCode) {
		Map<String, Dataset> ds = getDestination(dataset)
		List<Field> fields = destField(dataset)
		ds.each { String alias, Dataset dest ->
			if (dest.field.isEmpty()) dest.field = fields
			dest.openWrite(params)
		}
		if (prepareCode != null) prepareCode.call(fields)
	}

	@Override
	void write(Dataset dataset, Map row) {
		Map<String, Dataset> ds = getDestination(dataset)
		def cond = (dataset.params.condition as Map<String, Closure>)?:[:]
		
		// Valid conditions and write only filtered rows		
		cond.each { String alias, Closure valid ->
			Dataset dest = ds.get(alias)
			if (dest == null) throw new ExceptionGETL("Unknown alias dataset \"$alias\"")
			if (valid(row)) {
				dest.write(row)
			}
		}

		// Write rows to dataset has not conditions
		ds.each { String alias, Dataset dest ->
			if (!cond.containsKey(alias)) {
				dest.write(row)
			}
		}
	}

	@Override
	void doneWrite(Dataset dataset) {
		Map<String, Dataset> ds = getDestination(dataset)
		ds.each { String alias, Dataset dest ->
			dest.doneWrite()
		}
	}

	@Override
	void closeWrite(Dataset dataset) {
		Map<String, Dataset> ds = getDestination(dataset)
		ds.each { String alias, Dataset dest ->
			dest.closeWrite()
		}
	}

	@Override
	void bulkLoadFile(CSVDataset source, Dataset dest, Map params, Closure prepareCode) { }

	@Override
	void clearDataset(Dataset dataset, Map params) { }

	@Override
	Long executeCommand(String command, Map params) {
		throw new ExceptionGETL("Execution command not supported")
	}

	@Override
	Long getSequence(String sequenceName) {
		throw new ExceptionGETL("Sequence not supported")
	}
}
