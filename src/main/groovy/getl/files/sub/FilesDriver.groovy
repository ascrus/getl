package getl.files.sub

import getl.data.*
import getl.driver.*
import getl.exception.NotSupportError
import groovy.transform.InheritConstructors

/**
 * Files driver
 * @author Alexsey Konstantinov
 *
 */
@InheritConstructors
class FilesDriver extends FileDriver {

	@Override
	List<Support> supported() {
		[]
	}

	@Override
	List<Operation> operations() {
		[Operation.DROP]
	}

	@Override

	List<Field> fields(Dataset dataset) {
		throw new NotSupportError(connection, 'fields')
	}

	@Override

	Long eachRow(Dataset dataset, Map params, Closure prepareCode, Closure code) {
		throw new NotSupportError(connection, 'eachRow')
	}

	@Override

	void openWrite(Dataset dataset, Map params, Closure prepareCode) {
		throw new NotSupportError(connection, 'openWrite')
	}

	@Override

	void write(Dataset dataset, Map row) {
		throw new NotSupportError(connection, 'write')
	}

	@Override

	void closeWrite(Dataset dataset) {
		throw new NotSupportError(connection, 'closeWrite')
	}
}
