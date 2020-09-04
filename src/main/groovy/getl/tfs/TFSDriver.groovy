package getl.tfs

import getl.csv.CSVDriver
import getl.data.Dataset
import groovy.transform.InheritConstructors

@InheritConstructors
class TFSDriver extends CSVDriver {
	@Override
	protected void processWriteFile(String fileName, File fileTemp) {
		super.processWriteFile(fileName, fileTemp)
		if ((connection as TFS).deleteOnExit) {
			new File(fileName).deleteOnExit()
		}
	}
}
