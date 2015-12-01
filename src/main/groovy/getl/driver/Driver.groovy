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
import getl.data.Field.Type
import getl.utils.ParamMethodValidator

/**
 * Base driver class
 * @author Alexsey Konstantinov
 *
 */
abstract class Driver {
	protected ParamMethodValidator methodParams = new ParamMethodValidator()
	
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
	
	public enum Support {CONNECT, TRANSACTIONAL, SQL, TEMPORARY, BATCH, EACHROW, WRITE, SEQUENCE, AUTOLOADSCHEMA, AUTOSAVESCHEMA, INDEX}
	public abstract List<Driver.Support> supported ()
	public boolean isSupport(Driver.Support feature) {
		(supported().indexOf(feature) != -1)
	}
	
	public enum Operation {CREATE, DROP, CLEAR, BULKLOAD, EXECUTE, RETRIEVEFIELDS}
	public abstract List<Driver.Operation> operations ()
	public boolean isOperation(Driver.Operation operation) {
		(operations().indexOf(operation) != -1)
	}
	
	protected void prepareField (Field field) { }
	
	protected abstract boolean isConnect ()
	
	protected abstract void connect ()
	
	protected abstract void disconnect ()
	
	protected abstract List<Object> retrieveObjects (Map params, Closure filter)

	protected abstract  List<Field> fields (Dataset dataset)
	
	protected abstract void startTran ()
	
	protected abstract void commitTran ()
	
	protected abstract void rollbackTran ()
	
	protected abstract void createDataset (Dataset dataset, Map params)
	
	protected void dropDataset (Dataset dataset, Map params) {
		if (dataset.autoSchema) {
			def name = fullFileNameSchema(dataset)
			if (name != null) {
				def s = new File(name)
				if (s.exists()) s.delete()
			}
		}
	}
	
	protected abstract long eachRow (Dataset dataset, Map params, Closure prepareCode, Closure code)
	
	protected abstract void openWrite(Dataset dataset, Map params, Closure prepareCode)
	
	protected abstract void write (Dataset dataset, Map row)
	
	protected abstract void doneWrite (Dataset dataset)
	
	protected abstract void closeWrite (Dataset dataset)
	
	protected abstract void bulkLoadFile (CSVDataset source, Dataset dest, Map params, Closure prepareCode)
	
	protected abstract void clearDataset (Dataset dataset, Map params)
	
	protected abstract long executeCommand (String command, Map params)
	
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
