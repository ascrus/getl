package getl.driver

import getl.csv.CSVDataset
import getl.data.*
import getl.exception.ExceptionGETL
import groovy.transform.InheritConstructors

/**
 * Base virtual dataset driver 
 * @author Alexsey Konstantinov
 *
 */
@InheritConstructors
abstract class VirtualDatasetDriver extends Driver {
	@SuppressWarnings("UnnecessaryQualifiedReference")
	@Override
	List<Driver.Support> supported() {
		[Driver.Support.WRITE]
	}

	@SuppressWarnings("UnnecessaryQualifiedReference")
	@Override
	List<Driver.Operation> operations() {
		[]
	}

	@Override
	Boolean isConnected() {
		true
	}

	@Override
	void connect() { }

	@Override
	void disconnect() { }

	@Override
	List<Object> retrieveObjects(Map params, Closure<Boolean> filter) {
		throw new ExceptionGETL('Not support this features!')
	}
	
	protected static Dataset getDestination(Dataset dataset) {
		def ds = dataset.params.dest as Dataset
		if (ds == null)
			throw new ExceptionGETL("Required set param \"dest\" with dataset")
		
		return ds
	}

	@Override

	List<Field> fields(Dataset dataset) {
		throw new ExceptionGETL('Not support this features!')
	}

	@Override

	void startTran() {
		throw new ExceptionGETL('Not support this features!')
	}

	@Override

	void commitTran() {
		throw new ExceptionGETL('Not support this features!')
	}

	@Override

	void rollbackTran() {
		throw new ExceptionGETL('Not support this features!')
	}

	@Override

	void createDataset(Dataset dataset, Map params) {
		throw new ExceptionGETL('Not support this features!')
	}

	@Override

	void dropDataset(Dataset dataset, Map params) {
		throw new ExceptionGETL('Not support this features!')
	}

	@Override

	Long eachRow(Dataset dataset, Map params, Closure prepareCode, Closure code) {
		throw new ExceptionGETL('Not support this features!')
	}
	
	@Override

	void bulkLoadFile(CSVDataset source, Dataset dest, Map params, Closure prepareCode) {
		throw new ExceptionGETL('Not support this features!')
	}

	@Override

	void clearDataset(Dataset dataset, Map params) {
		throw new ExceptionGETL('Not support this features!')
	}

	@Override
	Long executeCommand(String command, Map params) {
		throw new ExceptionGETL('Not support this features!')
	}

	@Override
	Long getSequence(String sequenceName) {
		throw new ExceptionGETL('Not support this features!')
	}
}
