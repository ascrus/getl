/*
 GETL - based package in Groovy, which automates the work of loading and transforming data. His name is an acronym for "Groovy ETL".

 GETL is a set of libraries of pre-built classes and objects that can be used to solve problems unpacking,
 transform and load data into programs written in Groovy, or Java, as well as from any software that supports
 the work with Java classes.
 
 Copyright (C) 2013-2017  Alexsey Konstantonov (ASCRUS)

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

package getl.h2

import groovy.transform.InheritConstructors

import getl.csv.*
import getl.data.*
import getl.driver.Driver
import getl.exception.ExceptionGETL
import getl.files.FileManager
import getl.jdbc.*
import getl.utils.*

/**
 * H2 driver class
 * @author Alexsey Konstantinov
 *
 */
@InheritConstructors
class H2Driver extends JDBCDriver {
	H2Driver() {
		super()
		sqlAutoIncrement = "auto_increment"
		commitDDL = false

		caseObjectName = "UPPER"
		defaultSchemaName = "PUBLIC"
		connectionParamBegin = ";"
		connectionParamJoin = ";"

		methodParams.register("createDataset", ["transactional", "not_persistent"])
		methodParams.register('bulkLoadFile', ['expression'])
	}

	@Override
	public List<Driver.Support> supported() {
		return super.supported() +
				[Driver.Support.GLOBAL_TEMPORARY, Driver.Support.LOCAL_TEMPORARY, Driver.Support.MEMORY,
				 Driver.Support.SEQUENCE, Driver.Support.BLOB, Driver.Support.CLOB, Driver.Support.INDEX,
				 Driver.Support.UUID, Driver.Support.TIME, Driver.Support.DATE, Driver.Support.BOOLEAN]
	}

	@Override
	public List<Driver.Operation> operations() {
		return super.operations() +
				[Driver.Operation.CLEAR, Driver.Operation.DROP, Driver.Operation.EXECUTE, Driver.Operation.CREATE,
				 Driver.Operation.BULKLOAD, Driver.Operation.MERGE]
	}

	@Override
	public String defaultConnectURL() {
		def con = connection as H2Connection
		def url
		if (con.inMemory) {
			url = (con.connectHost != null) ? "jdbc:h2:tcp://{host}/mem:{database}" : "jdbc:h2:mem:{database}"
			if (con.connectDatabase == null) url = url.replace('{database}', 'memory_database')
		} else {
			url = (con.connectHost != null) ? "jdbc:h2:tcp://{host}/{database}" : "jdbc:h2://{database}"
		}

		return url
	}

	/**
	 * Prepare object name by prefix
	 * @param name
	 * @param prefix
	 * @return
	 */
	@Override
	public String prepareObjectNameWithPrefix(String name, String prefix, String prefixEnd = null) {
		if (name == null) return null

		String res

		def m = name =~ /([^a-zA-Z0-9_])/
		if (m.size() > 0) res = prefix + name + prefix else res = prefix + name.toUpperCase() + (prefixEnd ?: prefix)

		return res
	}

	@Override
	protected String createDatasetExtend(Dataset dataset, Map params) {
		String result = ""
		def temporary = ((dataset.sysParams.type as JDBCDataset.Type) in [JDBCDataset.Type.GLOBAL_TEMPORARY, JDBCDataset.Type.LOCAL_TEMPORARY])
		if (BoolUtils.IsValue(params."not_persistent")) result += "NOT PERSISTENT "
		if (temporary && params.transactional != null && params.transactional) result += "TRANSACTIONAL "

		return result
	}

	@Override
	public void bulkLoadFile(CSVDataset source, Dataset dest, Map params, Closure prepareCode) {
		if (params.compressed != null) throw new ExceptionGETL("H2 bulk load dont support compression files")

		params = bulkLoadFilePrepare(source, dest as JDBCDataset, params, prepareCode)

		List<Map> map = params.map
		boolean autoCommit = (params.autoCommit != null) ? params.autoCommit : (dest.connection.tranCount == 0 && !dest.connection.autoCommit)

		Map<String, String> expression = params.expression ?: [:]
		expression.each { String fieldName, String expr ->
			if (dest.fieldByName(fieldName) == null) throw new ExceptionGETL("Unknown field \"$fieldName\" in \"expression\" parameter")
		}

		StringBuilder sb = new StringBuilder()
		List<String> columns = []
		List<String> fields = []
		List<String> headers = []
		List<String> fparm = []
		map.each { Map<String, Object> m ->
			Field f = m.field as Field
			if (f != null && !f.isReadOnly) {
				headers << f.name.toUpperCase()
				columns << fieldPrefix + f.name.toUpperCase() + fieldPrefix
				def expr = expression.get(f.name.toLowerCase())
				if (expr == null) {
					fields << fieldPrefix + f.name.toUpperCase() + fieldPrefix
				} else {
					fields << expr
				}
			}
		}
		def cols = columns.join(', ')
		def flds = fields.join(', ')
		def heads = (!source.header) ? "'" + headers.join(source.fieldDelimiter) + "'" : "null"
		fparm << "charset=${source.codePage}"
		fparm << "fieldSeparator=${source.fieldDelimiter}"
		if (source.quoteStr != null) fparm << "fieldDelimiter=${StringUtils.EscapeJava(source.quoteStr)}"
		def functionParms = fparm.join(" ")

		sb <<
"""INSERT INTO ${fullNameDataset(dest)} (
$cols
)
SELECT $flds 
FROM CSVREAD('{file_name}', ${heads}, '${functionParms}')
"""
        def sql = sb.toString()
		//println sb.toString()

        def sourceConnection = source.connection as CSVConnection
        def files = [] as List<String>
        if (params.files != null) {
            files.addAll(params.files)
        }
        else if (params.fileMask != null) {
            def fm = new FileManager(rootPath: sourceConnection.path)
            fm.connect()
            try {
                fm.list(params.fileMask).each { Map f -> files << f.filename }
            }
            finally {
                fm.disconnect()
            }
        }
        else {
            files << source.fileName
        }

        dest.writeRows = 0
		dest.updateRows = 0
		if (autoCommit) dest.connection.startTran()
		long count = 0
		try {
            files.each { String fileName ->
                def loadFile = "${sourceConnection.path}/$fileName"
                count += executeCommand(sql.replace('{file_name}', loadFile), [isUpdate: true])
            }
		}
		catch (Exception e) {
			if (autoCommit) dest.connection.rollbackTran()
			throw e
		}
		if (autoCommit) dest.connection.commitTran()
		dest.writeRows = count
		dest.updateRows = count
	}
	
	@Override
	protected String unionDatasetMergeSyntax () {
        return
'''MERGE INTO {target} ({fields})
  KEY ({keys})
    SELECT {values}
	FROM {source} s'''
	}

	@Override
	protected String sessionID() {
		String res = null
		def rows = sqlConnect.rows("SELECT SESSION_ID() AS session_id;")
		if (!rows.isEmpty()) res = rows[0].session_id.toString()
		
		return res
	}

    @Override
    protected String getChangeSessionPropertyQuery() { return 'SET {name} {value}' }

	@Override
	protected String openWriteMergeSql(JDBCDataset dataset, Map params, List<Field> fields, List<String> statFields) {
		def excludeFields = []
		fields.each { Field f ->
			if (f.isReadOnly) {
                excludeFields << f.name
            }
            else {
                statFields << f.name
            }
		}

		String res = """
MERGE INTO ${dataset.fullNameDataset()} (${GenerationUtils.SqlFields(dataset.connection as JDBCConnection, fields, null, excludeFields).join(", ")})
VALUES(${GenerationUtils.SqlFields(dataset.connection as JDBCConnection, fields, "?", excludeFields).join(", ")})
"""
		
		return res
	}

	@Override
	public void prepareField (Field field) {
		super.prepareField(field)

		if (field.typeName != null) {
			if (field.typeName.matches("(?i)UUID")) {
				field.type = Field.Type.UUID
				field.dbType = java.sql.Types.VARCHAR
				field.length = 36
				field.precision = null
				return
			}

			if (field.typeName.matches("(?i)BLOB")) {
				field.type = Field.Type.BLOB
				field.dbType = java.sql.Types.BLOB
				field.precision = null
				return
			}

			if (field.typeName.matches("(?i)CLOB")) {
				field.type = Field.Type.TEXT
				field.dbType = java.sql.Types.CLOB
				field.precision = null
				return
			}
		}
	}
}