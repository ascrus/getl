/*
 GETL - based package in Groovy, which automates the work of loading and transforming data. His name is an acronym for "Groovy ETL".

 GETL is a set of libraries of pre-built classes and objects that can be used to solve problems unpacking,
 transform and load data into programs written in Groovy, or Java, as well as from any software that supports
 the work with Java classes.
 
 Copyright (C) 2013-2015  Alexsey Konstantonov (ASCRUS)

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
	
	public enum Support {
		CONNECT, TRANSACTIONAL, SQL, LOCAL_TEMPORARY, GLOBAL_TEMPORARY, BATCH, EACHROW, WRITE, SEQUENCE,
		AUTOLOADSCHEMA, AUTOSAVESCHEMA, INDEX, BLOB, CLOB, MEMORY, NOT_NULL_FIELD, PRIMARY_KEY,
		DEFAULT_VALUE, COMPUTE_FIELD}
	public abstract List<Driver.Support> supported ()
	public boolean isSupport(Driver.Support feature) {
		(supported().indexOf(feature) != -1)
	}
	
	public enum Operation {CREATE, DROP, CLEAR, BULKLOAD, EXECUTE, RETRIEVEFIELDS, MERGE}
	public abstract List<Driver.Operation> operations ()
	public boolean isOperation(Driver.Operation operation) {
		(operations().indexOf(operation) != -1)
	}

	public void prepareField (Field field) { }
	
	public abstract boolean isConnected()

	public abstract void connect ()

	public abstract void disconnect ()
	
	public abstract List<Object> retrieveObjects (Map params, Closure filter)

	public abstract  List<Field> fields (Dataset dataset)

	public abstract void startTran ()

	public abstract void commitTran ()

	public abstract void rollbackTran ()

	public abstract void createDataset (Dataset dataset, Map params)

	public void dropDataset (Dataset dataset, Map params) {
		if (dataset.autoSchema) {
			def name = fullFileNameSchema(dataset)
			if (name != null) {
				def s = new File(name)
				if (s.exists()) s.delete()
			}
		}
	}

	public abstract long eachRow (Dataset dataset, Map params, Closure prepareCode, Closure code)

	public abstract void openWrite(Dataset dataset, Map params, Closure prepareCode)

	public abstract void write (Dataset dataset, Map row)

	public abstract void doneWrite (Dataset dataset)

	public abstract void closeWrite (Dataset dataset)

	public abstract void bulkLoadFile (CSVDataset source, Dataset dest, Map params, Closure prepareCode)

	public abstract void clearDataset (Dataset dataset, Map params)

	public abstract long executeCommand (String command, Map params)
	
	public abstract long getSequence(String sequenceName)
	
	/**
	 * Full file schema name
	 * @param dataset
	 * @return
	 */
	public String fullFileNameSchema(Dataset dataset) {
		dataset.schemaFileName
	}
}
