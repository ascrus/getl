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

import groovy.transform.InheritConstructors
import getl.csv.*
import getl.data.*
import getl.driver.Driver
import getl.exception.ExceptionGETL
import getl.utils.FileUtils

/**
 * Temporary File Storage manager class
 * @author Alexsey Konstantinov
 *
 */
@InheritConstructors
class TFS extends CSVConnection {
	private static String _systemPath

	static def getSystemPath() {
        if (_systemPath == null) {
			_systemPath = "${FileUtils.SystemTempDir()}/getl/${FileUtils.UniqueFileName()}"
			FileUtils.ValidPath(_systemPath)
			new File(_systemPath).deleteOnExit()
		}
        return _systemPath
    }
	
	/**
	 * Global Temporary File Storage connection object
	 */
	public static final TFS storage = new TFS([:])
	
	TFS () {
		super(driver: TFSDriver)

		methodParams.register("Super", ["deleteOnExit"])
		if (this.getClass().name == 'getl.tfs.TFS') methodParams.validation("Super", params)

		initParams()
	}
	
	TFS (Map params) {
		super(new HashMap([driver: TFSDriver]) + params)
		
		methodParams.register("Super", ["deleteOnExit"])
		if (this.getClass().name == 'getl.tfs.TFS') methodParams.validation("Super", params)

		initParams()
	}
	
	/**
	 * Init on creating
	 */
	protected void initParams() {
		if (params.fieldDelimiter == null) fieldDelimiter = "|"
		if (params.rowDelimiter == null) rowDelimiter = "\n"
		if (params.autoSchema == null) autoSchema = true
		
		if (params.deleteOnExit == null) params.deleteOnExit = true
		setPath((params.path as String)?:systemPath)
	}
	
	/**
	 * Delete temporary directories and files after exit is program
	 * @return
	 */
	boolean getDeleteOnExit () { params.deleteOnExit }

	void setDeleteOnExit (boolean value) { params.deleteOnExit = value }
	
	@Override
	void setPath (String value) {
		super.setPath(value)
		if (value != null) {
			def f = new File(value)
			f.mkdirs()
			if (deleteOnExit) {
				f.deleteOnExit()
			}
		}
	}
	
	/**
	 * Create new temporary named dataset object
	 * @param name - name of object
	 * @param validExists - object required is exists 
	 * @return
	 */
	static TFSDataset dataset(String name, boolean validExists) {
		dataset(storage, name, validExists)
	}
	
	/**
	 * Create new temporary named dataset object
	 * @param name - name of object
	 * @return
	 */
	static TFSDataset dataset(String name) {
		dataset(name, false)
	}

	/**
	 * Create new temporary unnamed dataset object	
	 * @return
	 */
	static TFSDataset dataset() {
		dataset(FileUtils.UniqueFileName(), false)
	}
	
	/**
	 * Create new temporary named dataset object
	 * @param connection - TFS connection
	 * @param name - name of object
	 * @param validExists - object required is exists 
	 * @return
	 */
	static TFSDataset dataset(TFS connection, String name, boolean validExists) {
		TFSDataset ds = new TFSDataset(connection: connection, fileName: name)
		if (validExists && !ds.existsFile()) throw new ExceptionGETL("Temporary file \"${name}\" not exists")
		
		ds
	}

	/**
	 * Create new temporary named dataset object
	 * @param connection - TFS connection
	 * @param name - name of object
	 * @return
	 */
	static TFSDataset dataset(TFS connection, String name) {
		dataset(connection, name, false)
	}
	
	/**
	 * Create new temporary unnamed dataset object
	 * @param connection - TFS connection
	 * @return
	 */
	static TFSDataset dataset(TFS connection) {
		dataset(connection, FileUtils.UniqueFileName(), false)
	}
}