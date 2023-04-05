//file:noinspection unused
//file:noinspection DuplicatedCode
package getl.jdbc

import com.fasterxml.jackson.annotation.JsonIgnore
import getl.data.Connection
import getl.data.sub.WithConnection
import getl.driver.Driver
import getl.exception.NotSupportError
import getl.exception.RequiredParameterError
import getl.exception.SequenceError
import getl.jdbc.opts.SequenceCreateSpec
import getl.lang.Getl
import getl.lang.sub.GetlRepository
import getl.lang.sub.GetlValidate
import getl.utils.CloneUtils
import getl.utils.Logs
import getl.utils.MapUtils
import getl.utils.StringUtils
import groovy.transform.Synchronized
import groovy.transform.stc.ClosureParams
import groovy.transform.stc.SimpleType

/**
 * Sequence manager class
 * @author Alexsey Konstantinov
 *
 */
class Sequence implements GetlRepository, WithConnection {
	Sequence() {
		initParams()
	}

	/** Initialization parameters */
	protected void initParams() {
		params.clear()

		params.attributes = new HashMap<String, Object>()
	}

	/**
	 * Import parameters to current sequence
	 * @param importParams imported parameters
	 * @return current sequence
	 */
	Sequence importParams(Map<String, Object> importParams) {
		initParams()
		MapUtils.MergeMap(params, importParams)
		return this
	}

	/** Save point manager parameters */
	private final Map<String, Object> params = new HashMap<String, Object>()

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
	/** Write value to extended attribute */
	@Synchronized
	void saveAttribute(String name, Object value) {
		if (name == null)
			throw new RequiredParameterError(this, 'name')

		attributes.put(name, value)
	}
	/** Extended attribute value */
	Object attribute(String name) {
		if (name == null)
			throw new RequiredParameterError(this, 'name')

		return attributes.get(name)
	}
	/** Extended attribute value with evaluate variables */
	String attributeValue(String name, Map vars = null) {
		def val = attribute(name)
		if (val == null)
			return null

		return StringUtils.EvalMacroString(val.toString(), vars?:[:], true)
	}

	/** System parameters */
	private final Map<String, Object> sysParams = new HashMap<String, Object>()

	/** System parameters */
	@JsonIgnore
	Map<String, Object> getSysParams() { sysParams }

	@JsonIgnore
	@Override
	String getDslNameObject() { sysParams.dslNameObject as String }
	@Override
	void setDslNameObject(String value) { sysParams.dslNameObject = value }

	@JsonIgnore
	@Override
	Getl getDslCreator() { sysParams.dslCreator as Getl }
	@JsonIgnore
	@Override
	void setDslCreator(Getl value) { sysParams.dslCreator = value }

	@JsonIgnore
	@Override
	Date getDslRegistrationTime() { sysParams.dslRegistrationTime as Date }
	@Override
	void setDslRegistrationTime(Date value) { sysParams.dslRegistrationTime = value }

	@Override
	void dslCleanProps() {
		sysParams.dslNameObject = null
		sysParams.dslCreator = null
		sysParams.dslRegistrationTime = null
	}

	/** Current logger */
	@JsonIgnore
	Logs getLogger() { (dslCreator?.logging?.manager != null)?dslCreator.logging.manager:Logs.global }

	/** Connection */
	private JDBCConnection localConnection

	/** Connection */
	@JsonIgnore
	@Override
	Connection getConnection() {
		(dslCreator != null && connectionName != null)?dslCreator.jdbcConnection(connectionName):this.localConnection
	}
	/** Connection */
	@Override
	void setConnection(Connection value) {
		if (value != null && !(value instanceof JDBCConnection))
			throw new SequenceError(this, '#jdbc.connection.only')

		useConnection(value as JDBCConnection)
	}
	/** Use specified connection */
	JDBCConnection useConnection(JDBCConnection value) {
		if (value != null && !value.driver.isSupport(Driver.Support.SEQUENCE))
			throw new NotSupportError(value, 'sequence', 'useConnection')

		if (value != null && dslCreator != null && value.dslCreator != null && value.dslNameObject != null) {
			params.connection = value.dslNameObject
			this.localConnection = null
		} else {
			this.localConnection = value
			params.connection = null
		}

		return value
	}

	/** The name of the connection in the repository */
	String getConnectionName() { params.connection as String }
	/** The name of the connection in the repository */
	void setConnectionName(String value) {
		if (value != null) {
			GetlValidate.IsRegister(this, false)
			def con = dslCreator.jdbcConnection(value)
			if (!con.driver.isSupport(Driver.Support.SEQUENCE))
				throw new NotSupportError(con, 'sequence', 'useConnection')
			value = con.dslNameObject
		}

		params.connection = value
		this.localConnection = null
	}

	/** Current JDBC connection */
	@JsonIgnore
	JDBCConnection getCurrentJDBCConnection() { getConnection() as JDBCConnection }

	/** Sequence name */
	String getName() { params.name as String }
	/** Sequence name */
	void setName(String value) { params.name = value }

	/** Sequence schema */
	String getSchema() {
		def res = params.schema as String
		if (res  == null && name?.indexOf('.') == -1)
			res = currentJDBCConnection?.schemaName()

		return res
	}
	/** Sequence schema */
	void setSchema(String value) { params.schema = value }

	/** Database name */
	String getDbName() { params.dbName as String }
	/** Sequence name */
	void setDbName(String value) { params.dbName = value }

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
	Sequence cloneSequenceConnection(Map otherParams = new HashMap()) {
		cloneSequence(connection?.cloneConnection() as JDBCConnection, otherParams)
	}
	
	/**
	 * Clone sequenced by establishing the specified connection
	 * @param con establish a connection (null value leaves the current connection)
	 * @return
	 */
	@Synchronized
	Sequence cloneSequence(JDBCConnection con = null, Map otherParams = new HashMap(), Getl getl = null) {
		Map p = CloneUtils.CloneMap(this.params, false)

		if (otherParams != null)
			MapUtils.MergeMap(p, otherParams)

		def res = getClass().getConstructor().newInstance() as Sequence
		res.sysParams.dslCreator = dslCreator?:getl
		res.sysParams.dslNameObject = dslNameObject
		res.params.putAll(p)

		if (con != null)
			res.connection = con

		return res
	}

	@Override
	Object clone() {
		return cloneSequence()
	}

	@Override
	Object cloneWithConnection() {
		return cloneSequenceConnection()
	}

	/** Sequence full name */
	@JsonIgnore
	String getFullName() {
		def res = name

		if (schema != null)
			res = schema + '.' + res

		if (dbName != null)
			res = dbName + '.' + res

		return res
	}

	/** Check that the connection is specified */
	private void validConnection() {
		if (connection == null)
			throw new RequiredParameterError(this, 'connection')
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
			validConnection()
			def con = currentJDBCConnection
			con.tryConnect()
			current = con.driver.getSequence(fullName)
			offs = 0
		}

		offs++
		
		return (current + offs - 1)
	}

	/** Last executed SQL statement */
	@JsonIgnore
	String getLastSqlStatement() { sysParams.lastSqlStatement as String }
	
	@Override
	String toString() {
		return (dslNameObject != null)?(dslNameObject + ' [' + fullName + ']'):(getClass().simpleName + ' [' + fullName + ']')
	}

	/**
	 * Create sequence in database
	 * @param ifNotExists create if not exists
	 * @param cl process create options
	 */
	void createSequence(Boolean ifNotExists = false,
						@DelegatesTo(SequenceCreateSpec)
						@ClosureParams(value = SimpleType, options = ['getl.jdbc.opts.SequenceCreateSpec']) Closure cl = null) {
		validConnection()
		def parent = new SequenceCreateSpec(this)
		parent.runClosure(cl)
		def con = currentJDBCConnection
		con.tryConnect()
		con.currentJDBCDriver.createSequence(this, ifNotExists, parent)
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
	void dropSequence(Boolean ifExists = false, Boolean ddlOnly = false) {
		validConnection()
		def con = currentJDBCConnection
		con.tryConnect()
		con.currentJDBCDriver.dropSequence(this, ifExists, ddlOnly)
	}

	/**
	 * Restart sequence value
	 * @param newValue new value
	 */
	void restartWith(Long newValue) {
		validConnection()
		def con = currentJDBCConnection
		con.tryConnect()
		con.currentJDBCDriver.restartSequence(this, newValue)
	}
}