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

package getl.transform

import groovy.transform.InheritConstructors
import getl.data.Connection
import getl.data.Dataset
import getl.utils.*

/**
 * Multiple dataset writer class
 * @author Alexsey Konstantinov
 *
 */
class MultipleDataset extends Dataset {
	MultipleDataset () {
		super()
		
		Map<String, Dataset> d = [:]
		params.dest = d
		
		Map<String, Closure> cond = [:]
		params.condition = cond
		connection = new Connection([driver: MutlipleDatasetDriver])
	}
	
	/**
	 * Destinition datasets (alias:dataset)
	 * @return
	 */
	Map<String, Dataset> getDest () { params.dest as Map<String, Dataset> }

    void setDest (Map<String, Dataset> value) { params.dest = value }
	
	/**
	 * Conditions for filter rows to datasets (alias:condition)
	 * @return
	 */
	Map<String, Closure> getCondition () { params.condition as Map<String, Closure> }

    void setCondition (Map<String, Closure> value) { params.condition = value }
	
	@Override
    List<String> excludeSaveParams () {
		super.excludeSaveParams() + ["dest", "condition"]
	}
	
	@Override
    String getObjectName() { (dest != null)?dest.keySet().toList().join(', '):null }
}