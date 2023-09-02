//file:noinspection unused
package getl.tfs

import com.fasterxml.jackson.annotation.JsonIgnore
import getl.driver.Driver
import getl.jdbc.TableDataset
import getl.utils.BoolUtils
import getl.utils.FileUtils
import getl.h2.*
import groovy.transform.InheritConstructors
import org.h2.tools.DeleteDbFiles

/**
 * Temporary data storage manager class
 * @author Alexsey Konstantinov
 *
 */
@InheritConstructors
class TDS extends H2Connection {
	/** Global temporary database connection object */
	static public final TDS storage
	/** Global locker object */
	static private final Object lock
	/** Global temporary database name */
	static public final String storageDatabaseName = '_GETL_EMBEDDED'

	static {
		lock = new Object()
		storage = new TDS(connectDatabase: storageDatabaseName)
	}

	@Override
	protected Class<Driver> driverClass() { TDSDriver }

	@Override
	protected void initParams() {
		super.initParams()
		params.inMemory = true
		//connectDatabase = 'getl_' + StringUtils.RandomStr().replace('-', '')
		connectDatabase = storageDatabaseName
		login = "easyloader"
		password = "easydata"
		connectProperty.PAGE_SIZE = 8192
		config = "getl_tds"
		extensionForSqlScripts = true
	}

	protected void doInitConnection () {
		super.doInitConnection()
		if (sqlHistoryFile == null && storage?.sqlHistoryFile != null)
			sqlHistoryFile = storage.sqlHistoryFile
	}

    /** Temp path of database file */
    private String tempPath = TFS.systemPath
	/** Temp path of database file */
	@JsonIgnore
	String getTempPath() { tempPath }
	
	/** Internal name in config section */
	protected String internalConfigName() { "getl_tds" }
	
	/* Default parameters */
	//static public Map<String, Object> defaultParams = [:] as Map<String, Object>
	
	@Override
	protected void doBeforeConnect () {
		super.doBeforeConnect()
		if (connectHost == null) {
			connectProperty."IFEXISTS" = "FALSE"
		}
		if (inMemory && connectProperty."DB_CLOSE_DELAY" == null)
			connectProperty."DB_CLOSE_DELAY" = -1
//		autoCommit = true
	}

    @Override
    protected void doDoneDisconnect () {
        super.doDoneDisconnect()
        if (!inMemory && connectURL == null && connectDatabase != null) {
            DeleteDbFiles.execute(tempPath, FileUtils.FileName(connectDatabase()), true)
        }
    }
	
	/** Generate new table from temporary data stage */
	static TDSTable dataset () {
		def res = new TDSTable()
		res.connection = NewDefaultConnection()
		return res
	}

	@Override
	void setInMemory (Boolean value) {
		def oldValue = inMemory
		super.setInMemory(value)
		if (BoolUtils.IsValue(value) == oldValue) return

		//noinspection GroovySynchronizationOnNonFinalField
		synchronized (tempPath) {
			if (BoolUtils.IsValue(inMemory)) {
				connectURL = null
				connectDatabase = "getl"
			} else {
				connectDatabase = "$tempPath/getl"
				new File(connectDatabase() + '.mv.db').deleteOnExit()
				new File(connectDatabase() + '.trace.db').deleteOnExit()
			}
		}
	}

	@Override
	protected Class<TableDataset> getTableClass() { TDSTable }

	/** Create new default connection */
	static TDS NewDefaultConnection() { new TDS(connectDatabase: storageDatabaseName) }

	/** Shutdown default TDS database and close all sessions */
	static void ShutdownDefaultDatabase(Boolean immediately = false) {
		NewDefaultConnection().shutdownDatabase((immediately)?'IMMEDIATELY':null)
	}
}