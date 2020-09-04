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
class TFSDataset extends CSVDataset {
	TFSDataset () {
		super()
		manualSchema = true
		isTemporaryFile = true
		if (fileName == null)
			fileName = FileUtils.UniqueFileName()
	}

	@Override
    void openWrite (Map procParams) {
        super.openWrite(procParams)

        if ((connection as TFS).deleteOnExit) {
            new File(fullFileName()).deleteOnExit()

            if (autoSchema) {
                File s = new File(fullFileSchemaName())
                if (s.exists()) s.deleteOnExit()
            }
        }
    }
	
	@Override
	void setConnection(Connection value) {
		if (value != null && !(value instanceof TFS))
			throw new ExceptionGETL('The tfs dataset only supports tfs connections!')
		super.setConnection(value)
	}
}