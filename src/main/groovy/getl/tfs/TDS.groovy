/*
 GETL - based package in Groovy, which automates the work of loading and transforming data. His name is an acronym for "Groovy ETL".

 GETL is a set of libraries of pre-built classes and objects that can be used to solve problems unpacking,
 transform and load data into programs written in Groovy, or Java, as well as from any software that supports
 the work with Java classes.
 
 Copyright (C) EasyData Company LTD

 This program is free software: you can redistribute it and/or modify
 it under the terms of the GNU Lesser General Public License as published by
 the Free Software Foundation, either version 3 of the License, or
 (at your option) any later version.

 This program is distributed in the hope that it will be useful,
 but WITHOUT ANY WARRANTY; without even the implied warranty of
 MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 GNU General Public License for more details.

 You should have received a copy of the GNU General Public License and
 GNU Lesser General Public License along with this program.
 If not, see <http://www.gnu.org/licenses/>.
*/

package getl.tfs

import getl.utils.BoolUtils
import getl.utils.FileUtils
import groovy.transform.InheritConstructors
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
		config = "getl_tds"
	}
	
	TDS(Map initParams) {
		super(initParams?:[:])
		
		if (this.getClass().name == 'getl.tfs.TDS') methodParams.validation("Super", initParams?:[:])
		
		if (connectURL == null && params."inMemory" == null) params.inMemory = true

		synchronized (tempPath) {
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
	public static final TDS storage = new TDS([:])

    /** Temp path of database file */
    String tempPath = TFS.systemPath
	
	/** Internal name in config section */
	protected String internalConfigName() { "getl_tds" }
	
	/** Default parameters */
	public static Map defaultParams = [:]
	
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
}