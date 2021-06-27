package getl.tfs

import getl.exception.ExceptionGETL
import getl.utils.FileUtils
import groovy.transform.InheritConstructors

import getl.csv.CSVDataset
import getl.data.*

/**
 * Temporary file storage dataset class
 * @author Alexsey Konstantinov
 *
 */
@InheritConstructors
class TFSDataset extends CSVDataset {
	@Override
	protected void initParams() {
		super.initParams()

		manualSchema = true
		isTemporaryFile = true
		if (fileName == null)
			fileName = FileUtils.UniqueFileName()
	}

	@Override
	void setConnection(Connection value) {
		if (value != null && !(value instanceof TFS))
			throw new ExceptionGETL('The tfs dataset only supports tfs connections!')
		super.setConnection(value)
	}
}