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
import getl.data.sub.WithConnection
import getl.driver.Driver
import getl.exception.ExceptionGETL
import getl.jdbc.opts.DropSpec
import getl.jdbc.opts.SequenceCreateSpec
import getl.lang.Getl
import getl.lang.opts.BaseSpec
import getl.lang.sub.GetlRepository
import getl.utils.CloneUtils
import groovy.transform.Synchronized
import groovy.transform.stc.ClosureParams
import groovy.transform.stc.SimpleType

/**
 * Sequence manager class
 * @author Alexsey Konstantinov
 *
 */
class Sequence implements Cloneable, GetlRepository, WithConnection {
	Sequence() {
		initParams()
	}

	/** Save point manager parameters */
	final Map<String, Object> params = [:] as Map<String, Object>

	/**
	 * Initialization parameters
	 */
	protected void initParams() {
		params.attributes = [:] as Map<String, Object>
	}

	/** Save point manager parameters */
	Map getParams() { params }
	/** Save point manager parameters */
	void setParams(Map value) {
		params.clear()
		initParams()
		if (value != null) params.putAll(value)
	}

	/** Extended attributes */
	Map getAttributes() { params.attributes as Map }
	/** Extended attributes */
	void setAttributes(Map value) {
		attributes.clear()
		if (value != null) attributes.putAll(value)
	}

	/** System parameters */
	final Map<String, Object> sysParams = [:] as Map<String, Object>

	/** System parameters */
	Map<String, Object> getSysParams() { sysParams }

	String getDslNameObject() { sysParams.dslNameObject as String }
	void setDslNameObject(String value) { sysParams.dslNameObject = value }

	Getl getDslCreator() { sysParams.dslCreator as Getl }
	void setDslCreator(Getl value) { sysParams.dslCreator = value }

	/** Connection */
	JDBCConnection connection
	/** Connection */
	Connection getConnection() { connection }
	/** Connection */
	void setConnection(Connection value) {
		if (value != null && !(value instanceof JDBCConnection))
			throw new ExceptionGETL('Only work with JDBC connections is supported!')

		if (value != null && !value.driver.isSupport(Driver.Support.SEQUENCE))
			throw new ExceptionGETL("At connection \"$connection\" the driver does not support sequence!")

		connection = value
	}
	/** Use specified connection */
	void useConnection(JDBCConnection value) {
		setConnection(value)
	}

	/** Current JDBC connection */
	JDBCConnection getCurrentJDBCConnection() { connection }

	/** Sequence name */
	String getName() { params.name as String }
	/** Sequence name */
	void setName(String value) { params.name = value }

	/** Sequence name */
	String getSchema() {
		def res = params.schema as String
		if (res  == null && name.indexOf('.') == -1)
			res = (connection as JDBCConnection).schemaName

		return res
	}
	/** Sequence name */
	void setSchema(String value) { params.schema = value }

	/** Sequence cache interval */
	Long getCache() { params.cache as Long }
	/** Sequence cache interval */
	void setCache(Long value) { params.cache = value }

	/** last received sequence value */
	private long current = 0
	/** Offset relative to the last received value */
	private long offs = 0
	
	/** Clone sequenced and its connection */
	@Synchronized
	Sequence cloneSequenceConnection() {
		cloneSequence(connection?.cloneConnection() as JDBCConnection)
	}
	
	/**
	 * Clone sequenced by establishing the specified connection
	 * @param con establish a connection (null value leaves the current connection)
	 * @return
	 */
	@Synchronized
	Sequence cloneSequence(JDBCConnection con = null) {
		Sequence res = getClass().newInstance() as Sequence
		res.connection = con
		res.params.putAll(CloneUtils.CloneMap(params))

		return res
	}

	@Override
	Object clone() {
		return cloneSequence()
	}

	Object cloneConnection() {
		return cloneSequenceConnection()
	}

	/** Sequence full name */
	String getFullName() {
		return (schema != null)?"${schema}.$name":name
	}

	/** Get next sequence value */
	Long getNextValue() {
		return nextValueFast
	}

	/** Get next sequence value with synchronized */
	@Synchronized
	Long getNextValueSynch() {
		return nextValueFast
	}

	/** Get next sequence value without synchronized */
	long getNextValueFast() {
		if ((current == 0) || (offs >= cache)) {
			connection.tryConnect()
			current = connection.driver.getSequence(fullName)
			offs = 0
		}

		offs++
		
		return (current + offs - 1)
	}
	
	@Override
	String toString() {
		return fullName
	}

	/** System method */
	void dslCleanProps() {
		sysParams.dslNameObject = null
		sysParams.dslCreator = null
	}

	/**
	 * Create sequence in database
	 * @param ifNotExists create if not exists
	 * @param cl process create options
	 */
	void createSequence(boolean ifNotExists = false,
						@DelegatesTo(SequenceCreateSpec)
						@ClosureParams(value = SimpleType, options = ['getl.jdbc.opts.SequenceCreateSpec']) Closure cl = null) {
		def parent = new SequenceCreateSpec(this)
		parent.runClosure(cl)
		connection.currentJDBCDriver.createSequence(fullName, ifNotExists, parent)
	}

	/**
	 * Create sequence in database
	 * @param cl process create options
	 */
	void createSequence(@DelegatesTo(SequenceCreateSpec)
						@ClosureParams(value = SimpleType, options = ['getl.jdbc.opts.SequenceCreateSpec']) Closure cl) {
		createSequence(false, cl)
	}

	/**
	 * Drop sequence from database
	 * @param ifExists drop if exists
	 */
	void dropSequence(boolean ifExists = false) {
		connection.currentJDBCDriver.dropSequence(fullName, ifExists)
	}
}