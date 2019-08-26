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
import getl.exception.ExceptionGETL
import groovy.transform.InheritConstructors

/**
 * Base virtual dataset driver 
 * @author Alexsey Konstantinov
 *
 */
@InheritConstructors
abstract class VirtualDatasetDriver extends Driver {
	@Override
	List<Driver.Support> supported() {
		[Driver.Support.WRITE]
	}

	@Override
	List<Driver.Operation> operations() {
		[]
	}

	@Override
	boolean isConnected() {
		true
	}

	@Override
	void connect() { }

	@Override
	void disconnect() { }

	@Override
	List<Object> retrieveObjects(Map params, Closure filter) {
		throw new ExceptionGETL('Not support this features!')
	}
	
	protected static Dataset getDestinition(Dataset dataset) {
		Dataset ds = dataset.params.dest
		if (ds == null) throw new ExceptionGETL("Required set param \"dest\" with dataset")
		
		return ds
	}

	@Override

	List<Field> fields(Dataset dataset) {
		throw new ExceptionGETL('Not support this features!')
	}

	@Override

	void startTran() {
		throw new ExceptionGETL('Not support this features!')
	}

	@Override

	void commitTran() {
		throw new ExceptionGETL('Not support this features!')
	}

	@Override

	void rollbackTran() {
		throw new ExceptionGETL('Not support this features!')
	}

	@Override

	void createDataset(Dataset dataset, Map params) {
		throw new ExceptionGETL('Not support this features!')
	}

	@Override

	void dropDataset(Dataset dataset, Map params) {
		throw new ExceptionGETL('Not support this features!')
	}

	@Override

	long eachRow(Dataset dataset, Map params, Closure prepareCode, Closure code) {
		throw new ExceptionGETL('Not support this features!')
	}
	
	@Override

	void bulkLoadFile(CSVDataset source, Dataset dest, Map params, Closure prepareCode) {
		throw new ExceptionGETL('Not support this features!')
	}

	@Override

	void clearDataset(Dataset dataset, Map params) {
		throw new ExceptionGETL('Not support this features!')
	}

	@Override

	long executeCommand(String command, Map params) {
		throw new ExceptionGETL('Not support this features!')
	}

	@Override
	long getSequence(String sequenceName) {
		throw new ExceptionGETL('Not support this features!')
	}
}
