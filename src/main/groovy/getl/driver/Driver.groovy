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

package getl.driver

import getl.csv.CSVDataset
import getl.data.*
import getl.utils.FileUtils
import getl.utils.ParamMethodValidator

/**
 * Base driver class
 * @author Alexsey Konstantinov
 *
 */
abstract class Driver {
	public ParamMethodValidator methodParams = new ParamMethodValidator()
	
	Driver () {
		methodParams.register("retrieveObjects", [])
		methodParams.register("createDataset", [])
		methodParams.register("dropDataset", [])
		methodParams.register("clearDataset", [])
		methodParams.register("bulkLoadFile", [])
		methodParams.register("rows", [])
		methodParams.register("eachRow", [])
		methodParams.register("openWrite", [])
		methodParams.register("executeCommand", [])
	}
	
	protected Connection connection

    static enum Support {
		CONNECT, TRANSACTIONAL, SQL, LOCAL_TEMPORARY, GLOBAL_TEMPORARY, BATCH, CREATEIFNOTEXIST, DROPIFEXIST, EACHROW,
		WRITE, SEQUENCE, AUTOLOADSCHEMA, AUTOSAVESCHEMA, INDEX, DATE, TIME, TIMESTAMP_WITH_TIMEZONE, BOOLEAN, BLOB,
		CLOB, MEMORY, NOT_NULL_FIELD, PRIMARY_KEY, DEFAULT_VALUE, COMPUTE_FIELD, UUID, MULTIDATABASE, BULKLOADMANYFILES
	}

	@SuppressWarnings("UnnecessaryQualifiedReference")
	abstract List<Driver.Support> supported ()

	@SuppressWarnings("UnnecessaryQualifiedReference")
	boolean isSupport(Driver.Support feature) {
		(supported().indexOf(feature) != -1)
	}

    static enum Operation {
		CREATE, DROP, CLEAR, BULKLOAD, EXECUTE, RETRIEVEFIELDS, INSERT, UPDATE, DELETE, MERGE,
		READ_METADATA
	}

	@SuppressWarnings("UnnecessaryQualifiedReference")
	abstract List<Driver.Operation> operations ()

	@SuppressWarnings("UnnecessaryQualifiedReference")
	boolean isOperation(Driver.Operation operation) {
		(operations().indexOf(operation) != -1)
	}

    void prepareField (Field field) { }

    abstract boolean isConnected()

    abstract void connect ()

    abstract void disconnect ()

    abstract List<Object> retrieveObjects (Map params, Closure<Boolean> filter)

    abstract  List<Field> fields (Dataset dataset)

    abstract void startTran ()

    abstract void commitTran ()

    abstract void rollbackTran ()

    abstract void createDataset (Dataset dataset, Map params)

    void dropDataset (Dataset dataset, Map params) {
		if (dataset.autoSchema && !isResourceFileNameSchema(dataset)) {
			def name = fullFileNameSchema(dataset)
			if (name != null) {
				def s = new File(name)
				if (s.exists()) s.delete()
			}
		}
	}

    abstract long eachRow (Dataset dataset, Map params, Closure prepareCode, Closure code)

    abstract void openWrite(Dataset dataset, Map params, Closure prepareCode)

    abstract void write (Dataset dataset, Map row)

    abstract void doneWrite (Dataset dataset)

    abstract void closeWrite (Dataset dataset)

    abstract void bulkLoadFile (CSVDataset source, Dataset dest, Map params, Closure prepareCode)

    abstract void clearDataset (Dataset dataset, Map params)

    abstract long executeCommand (String command, Map params)

    abstract long getSequence(String sequenceName)
	
	/**
	 * Full file schema name
	 * @param dataset
	 * @return
	 */
	String fullFileNameSchema(Dataset dataset) {
		FileUtils.ResourceFileName(dataset.schemaFileName)
	}

	/**
	 * Determine that the schema file is stored in resources
	 * @param dataset - source dataset
	 * @return
	 */
	static Boolean isResourceFileNameSchema(Dataset dataset) {
		FileUtils.IsResourceFileName(dataset.schemaFileName)
	}

	/**
	 * Configure the file to work and upload to the table
	 * @param csvFile CSV dataset
	 */
	void prepareCsvTempFile(Dataset source, CSVDataset csvFile) { }

	/**
	 * Check CSV file settings for bulk loading
	 * @param csvFile CSV dataset
	 */
	void validCsvTempFile(Dataset source, CSVDataset csvFile) { }
}