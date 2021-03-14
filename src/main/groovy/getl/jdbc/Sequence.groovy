package getl.jdbc

import com.fasterxml.jackson.annotation.JsonIgnore
import getl.data.Connection
import getl.data.sub.WithConnection
import getl.driver.Driver
import getl.exception.ExceptionGETL
import getl.jdbc.opts.DropSpec
import getl.jdbc.opts.SequenceCreateSpec
import getl.lang.Getl
import getl.lang.opts.BaseSpec
import getl.lang.sub.GetlRepository
import getl.lang.sub.GetlValidate
import getl.utils.CloneUtils
import getl.utils.MapUtils
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

	/** Initialization parameters */
	protected void initParams() {
		params.attributes = [:] as Map<String, Object>
	}

	/** Save point manager parameters */
	private final Map<String, Object> params = [:] as Map<String, Object>

	/** Save point manager parameters */
	@JsonIgnore
	Map<String, Object> getParams() { params }
	/** Save point manager parameters */
	@JsonIgnore
	void setParams(Map<String, Object> value) {
		params.clear()
		initParams()
		if (value != null) params.putAll(value)
	}

	/** Extended attributes */
	Map<String, Object> getAttributes() { params.attributes as Map<String, Object> }
	/** Extended attributes */
	void setAttributes(Map<String, Object> value) {
		attributes.clear()
		if (value != null) attributes.putAll(value)
	}

	/** System parameters */
	private final Map<String, Object> sysParams = [:] as Map<String, Object>

	/** System parameters */
	@JsonIgnore
	Map<String, Object> getSysParams() { sysParams }

	@JsonIgnore
	String getDslNameObject() { sysParams.dslNameObject as String }
	void setDslNameObject(String value) { sysParams.dslNameObject = value }

	@JsonIgnore
	Getl getDslCreator() { sysParams.dslCreator as Getl }
	@JsonIgnore
	void setDslCreator(Getl value) { sysParams.dslCreator = value }

	/** Connection */
	private JDBCConnection connection
	/** Connection */
	@JsonIgnore
	Connection getConnection() { connection }
	/** Connection */
	void setConnection(Connection value) {
		if (value != null && !(value instanceof JDBCConnection))
			throw new ExceptionGETL('Only work with JDBC connections is supported!')

		useConnection(value as JDBCConnection)
	}
	/** Use specified connection */
	JDBCConnection useConnection(JDBCConnection value) {
		if (value != null && !value.driver.isSupport(Driver.Support.SEQUENCE))
			throw new ExceptionGETL("At connection \"$connection\" the driver does not support sequence!")

		this.connection = value
		return value
	}

	/** The name of the connection in the repository */
	String getConnectionName() { connection?.dslNameObject }
	/** The name of the connection in the repository */
	void setConnectionName(String value) {
		if (value != null) {
			GetlValidate.IsRegister(this)
			def con = dslCreator.jdbcConnection(value)
			useConnection(con)
		}
		else
			useConnection(null)
	}

	/** Current JDBC connection */
	@JsonIgnore
	JDBCConnection getCurrentJDBCConnection() { connection }

	/** Sequence name */
	String getName() { params.name as String }
	/** Sequence name */
	void setName(String value) { params.name = value }

	/** Sequence name */
	String getSchema() {
		def res = params.schema as String
		if (res  == null && name?.indexOf('.') == -1)
			res = (connection as JDBCConnection).schemaName

		return res
	}
	/** Sequence name */
	void setSchema(String value) { params.schema = value }

	/** Sequence cache interval */
	Long getCache() { params.cache as Long }
	/** Sequence cache interval */
	void setCache(Long value) { params.cache = value } /* TODO: automatic read metadata if cache is not set */

	/** Description of sequence */
	String getDescription() { params.description as String }
	/** Description of sequence */
	void setDescription(String value) { params.description = value }

	/** last received sequence value */
	private Long current = 0
	/** Offset relative to the last received value */
	private Long offs = 0
	
	/** Clone sequenced and its connection */
	@Synchronized
	Sequence cloneSequenceConnection(Map otherParams = [:]) {
		cloneSequence(connection?.cloneConnection() as JDBCConnection, otherParams)
	}
	
	/**
	 * Clone sequenced by establishing the specified connection
	 * @param con establish a connection (null value leaves the current connection)
	 * @return
	 */
	@Synchronized
	Sequence cloneSequence(JDBCConnection con = null, Map otherParams = [:]) {
		Map p = CloneUtils.CloneMap(this.params, false)
		if (otherParams != null) MapUtils.MergeMap(p, otherParams)
		Sequence res = getClass().newInstance() as Sequence
		res.connection = con
		res.params.putAll(p)

		return res
	}

	@Override
	Object clone() {
		return cloneSequence()
	}

	Object cloneWithConnection() {
		return cloneSequenceConnection()
	}

	/** Sequence full name */
	@JsonIgnore
	String getFullName() {
		return (schema != null)?"${schema}.$name":name
	}

	/** Get next sequence value */
	@JsonIgnore
	Long getNextValue() {
		return nextValueFast
	}

	/** Get next sequence value with synchronized */
	@Synchronized
	@JsonIgnore
	Long getNextValueSynch() {
		return nextValueFast
	}

	/** Get next sequence value without synchronized */
	@JsonIgnore
	Long getNextValueFast() {
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
	void createSequence(Boolean ifNotExists = false,
						@DelegatesTo(SequenceCreateSpec)
						@ClosureParams(value = SimpleType, options = ['getl.jdbc.opts.SequenceCreateSpec']) Closure cl = null) {
		def parent = new SequenceCreateSpec(this)
		parent.runClosure(cl)
		connection.tryConnect()
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
	void dropSequence(Boolean ifExists = false) {
		connection.tryConnect()
		connection.currentJDBCDriver.dropSequence(fullName, ifExists)
	}
}