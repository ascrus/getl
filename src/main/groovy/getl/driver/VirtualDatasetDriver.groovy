package getl.driver

import getl.csv.CSVDataset
import getl.data.*
import getl.exception.NotSupportError
import getl.exception.RequiredParameterError
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
		throw new NotSupportError('retrieveObjects')
	}
	
	protected static Dataset getDestination(Dataset dataset) {
		def ds = dataset.params.dest as Dataset
		if (ds == null)
			throw new RequiredParameterError(dataset, 'dest', 'dataset')
		
		return ds
	}

	@Override

	List<Field> fields(Dataset dataset) {
		throw new NotSupportError(connection, 'fields')
	}

	@Override
	void startTran(Boolean useSqlOperator = false) {
		throw new NotSupportError(connection, 'startTran')
	}

	@Override

	void commitTran(Boolean useSqlOperator = false) {
		throw new NotSupportError(connection, 'commitTran')
	}

	@Override

	void rollbackTran(Boolean useSqlOperator = false) {
		throw new NotSupportError(connection, 'rollbackTran')
	}

	@Override

	void createDataset(Dataset dataset, Map params) {
		throw new NotSupportError(connection, 'createDataset')
	}

	@Override

	void dropDataset(Dataset dataset, Map params) {
		throw new NotSupportError(connection, 'dropDataset')
	}

	@Override
	Long eachRow(Dataset dataset, Map params, Closure prepareCode, Closure code) {
		throw new NotSupportError(connection, 'eachRow')
	}
	
	@Override

	void bulkLoadFile(CSVDataset source, Dataset dest, Map params, Closure prepareCode) {
		throw new NotSupportError(connection, 'bulkLoadFile')
	}

	@Override

	void clearDataset(Dataset dataset, Map params) {
		throw new NotSupportError(connection, 'clearDataset')
	}

	@Override
	Long executeCommand(String command, Map params) {
		throw new NotSupportError(connection, 'executeCommand')
	}

	@Override
	Long getSequence(String sequenceName) {
		throw new NotSupportError(connection, 'sequence')
	}
}
