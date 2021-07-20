package getl.jdbc

import com.fasterxml.jackson.annotation.JsonIgnore
import groovy.transform.InheritConstructors

/**
 * View dataset class
 * @author Alexsey Konstantinov
 *
 */
@InheritConstructors
class ViewDataset extends TableDataset {
	@Override
	protected void registerParameters() {
		super.registerParameters()
		methodParams.register('createView', [])
	}

	@Override
	protected void initParams() {
		super.initParams()

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
		
		return (!ds.isEmpty())
	}

	/**
	 * Create view in database
	 * @param params creation options
	 */
	void createView(Map procParams) {
		validConnection()

		procParams = procParams?:[:]
		methodParams.validation('createView', procParams,
				[connection.driver.methodParams.params('createView')])

		currentJDBCConnection.currentJDBCDriver.createView(this, procParams)
	}
}