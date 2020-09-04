package getl.files.sub

import getl.data.*
import getl.driver.*
import getl.exception.ExceptionGETL
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
		throw new ExceptionGETL('Not support this features!')
	}

	@Override

	Long eachRow(Dataset dataset, Map params, Closure prepareCode, Closure code) {
		throw new ExceptionGETL('Not support this features!')
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
