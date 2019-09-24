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

package getl.jdbc

import getl.data.Connection
import getl.driver.Driver
import getl.exception.ExceptionGETL
import groovy.transform.Synchronized

/**
 * Sequence manager class
 * @author Alexsey Konstantinov
 *
 */
class Sequence {
	/**
	 * Connection for use 
	 */
	public Connection connection
	
	/**
	 * Sequence name
	 */
	public String name
	
	/**
	 * Sequence cache interval
	 */
	public long cache = 1
	
	private long current = 0
	private long offs = 0
	
	/**
	 * Clone sequence as new instance with current connection
	 * @return
	 */
	Sequence newSequence () {
		Sequence res = getClass().newInstance() as Sequence
		res.connection = this.connection
		res.name = this.name
		res.cache = this.cache
		
		res
	} 
	
	/**
	 * Clone sequence as new instance with other connection
	 * @param con
	 * @return
	 */
	Sequence newSequence (Connection con) {
		Sequence res = getClass().newInstance() as Sequence
		res.connection = con
		res.name = this.name
		res.cache = this.cache
		
		res
	}
	
	/**
	 * Get next sequence value with synchronized
	 * @return
	 */
	@Synchronized
	long getNextValue() {
		nextValueFast
	}

	/**
	 * Get next sequence value without synchronized	
	 * @return
	 */
	long getNextValueFast() {
		if (!connection.driver.isSupport(Driver.Support.SEQUENCE)) throw new ExceptionGETL("Driver not support sequences")
		if ((current == 0) || (offs >= cache)) {
				connection.tryConnect()
				current = connection.driver.getSequence(name)
				offs = 0
		}

		offs++
		
		return (current + offs - 1)
	}
	
	@Override
	String toString() {
		name
	}
}
