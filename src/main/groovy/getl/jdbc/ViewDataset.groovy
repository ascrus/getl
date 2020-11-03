package getl.jdbc

import com.fasterxml.jackson.annotation.JsonIgnore
import groovy.transform.InheritConstructors

/**
 * View dataset class
 * @author Alexsey Konstantinov
 *
 */
class ViewDataset extends TableDataset {
	@SuppressWarnings("UnnecessaryQualifiedReference")
	ViewDataset() {
		super()
		sysParams.isView = true
		type = viewType
	}

	/** Use specified connection */
	JDBCConnection useConnection(JDBCConnection value) {
		setConnection(value)
		return value
	}

	/**
	 * Validation exists view
	 * @return
	 */
	@Override
	@JsonIgnore
	Boolean isExists() {
		def ds = currentJDBCConnection.retrieveDatasets(dbName: dbName, schemaName: schemaName,
					tableName: tableName, type: ["VIEW"])
		
		(!ds.isEmpty())
	}
}