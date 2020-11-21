package getl.tfs

import com.fasterxml.jackson.annotation.JsonIgnore
import getl.jdbc.TableDataset
import getl.utils.BoolUtils
import getl.utils.FileUtils
import getl.h2.*
import org.h2.tools.DeleteDbFiles

/**
 * Temporary data storage manager class
 * @author Alexsey Konstantinov
 *
 */
class TDS extends H2Connection {
	TDS() {
		super()
		
		if (connectURL == null && params."inMemory" == null) params.inMemory = true
		if (connectURL == null && connectDatabase == null) connectDatabase = "getl"
		if (login == null && password == null) {
			login = "easyloader"
			password = "easydata"
		}
		if (connectProperty."PAGE_SIZE" == null) {
			connectProperty."PAGE_SIZE" = 8192
		}
		if (connectProperty."LOG" == null) {
			connectProperty."LOG" = 0
		}
		if (connectProperty."UNDO_LOG" == null) {
			connectProperty."UNDO_LOG" = 0
		}

		synchronized (TDS.lock) {
			if (sqlHistoryFile == null && TDS.storage?.sqlHistoryFile != null)
				sqlHistoryFile = TDS.storage.sqlHistoryFile
		}

		config = "getl_tds"
	}
	
	TDS(Map initParams) {
		super(initParams?:[:])
		
		if (this.getClass().name == 'getl.tfs.TDS') methodParams.validation("Super", initParams?:[:])
		
		if (connectURL == null && params."inMemory" == null) params.inMemory = true

		synchronized (TDS.lock) {
			if (connectURL == null && connectDatabase == null) {
				if (inMemory) {
					connectDatabase = "getl"
				} else {
					tempPath = TFS.systemPath
					connectDatabase = "$tempPath/getl"
					new File(connectDatabase + '.mv.db').deleteOnExit()
					new File(connectDatabase + '.trace.db').deleteOnExit()
				}
			} else if (connectDatabase != null) {
				tempPath = FileUtils.PathFromFile(connectDatabase)
				new File(connectDatabase + '.mv.db').deleteOnExit()
				new File(connectDatabase + '.trace.db').deleteOnExit()
			}

			if (sqlHistoryFile == null && TDS.storage?.sqlHistoryFile != null)
				sqlHistoryFile = TDS.storage.sqlHistoryFile
		}

		if (login == null && password == null) {
			login = "easyloader"
			password = "easydata"
		}
		if (connectProperty."PAGE_SIZE" == null) {
			connectProperty."PAGE_SIZE" = 8192
		}
		if (connectProperty."LOG" == null) {
			connectProperty."LOG" = 0
		}
		if (connectProperty."UNDO_LOG" == null) {
			connectProperty."UNDO_LOG" = 0
		}
		if (params."config" == null) config = "getl_tds"
	}

	/** Global temporary database connection object */
	static public final TDS storage
	/** Global locker object */
	static private final Object lock

	static {
		lock = new Object()
		storage = new TDS([:])
	}

    /** Temp path of database file */
    private String tempPath = TFS.systemPath
	/** Temp path of database file */
	@JsonIgnore
	String getTempPath() { tempPath }
	
	/** Internal name in config section */
	protected String internalConfigName() { "getl_tds" }
	
	/** Default parameters */
	static public Map<String, Object> defaultParams = [:] as Map<String, Object>
	
	@Override
	protected void onLoadConfig (Map configSection) {
		super.onLoadConfig(configSection)
		if (this.getClass().name == 'getl.tfs.TDS') methodParams.validation("Super", params)
	}
	
	@Override
	protected void doBeforeConnect () {
		super.doBeforeConnect()
		if (connectHost == null) {
			connectProperty."IFEXISTS" = "FALSE"
		}
		if (inMemory && connectProperty."DB_CLOSE_DELAY" == null) connectProperty."DB_CLOSE_DELAY" = -1
//		autoCommit = true
	}

    @Override
    protected void doDoneDisconnect () {
        super.doDoneDisconnect()
        if (!inMemory && connectURL == null && connectDatabase != null) {
            DeleteDbFiles.execute(tempPath, FileUtils.FileName(connectDatabase), true)
        }
    }
	
	/** Generate new table from temporary data stage */
	static TDSTable dataset () {
		def res = new TDSTable()
		res.connection = new TDS()
		return res
	}

	@Override
	void setInMemory (Boolean value) {
		def oldValue = inMemory
		super.setInMemory(value)
		if (BoolUtils.IsValue(value) == oldValue) return

		synchronized (tempPath) {
			if (BoolUtils.IsValue(inMemory)) {
				connectURL = null
				connectDatabase = "getl"
			} else {
				tempPath = TFS.systemPath
				connectDatabase = "$tempPath/getl"
				new File(connectDatabase + '.mv.db').deleteOnExit()
				new File(connectDatabase + '.trace.db').deleteOnExit()
			}
		}
	}

	@Override
	protected Class<TableDataset> getTableClass() { TDSTable }
}