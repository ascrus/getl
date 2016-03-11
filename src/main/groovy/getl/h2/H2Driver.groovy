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

package getl.h2

import java.util.Map;

import getl.csv.CSVDataset
import getl.data.Dataset
import getl.data.Field
import getl.driver.Driver
import getl.exception.ExceptionGETL
import getl.jdbc.JDBCConnection
import getl.jdbc.JDBCDriver
import getl.jdbc.JDBCDataset
import getl.jdbc.TableDataset
import getl.utils.*
import groovy.transform.InheritConstructors

/**
 * H2 driver class
 * @author Alexsey Konstantinov
 *
 */
@InheritConstructors
class H2Driver extends JDBCDriver {
	H2Driver () {
		super()
		sqlAutoIncrement = "auto_increment"
		commitDDL = false
		
		caseObjectName = "UPPER"
		defaultSchemaName = "PUBLIC"
		connectionParamBegin = ";"
		connectionParamJoin = ";"
		
		methodParams.register("createDataset", ["transactional", "not_persistent"])
	}
	
	@Override
	public List<Driver.Support> supported() {
		List<Driver.Support> result = super.supported()
		result << Driver.Support.TEMPORARY
		result << Driver.Support.INDEX
		result
	}
	
	@Override
	public List<Driver.Operation> operations() {
		List<Driver.Operation> result = super.operations()
		result << Driver.Operation.BULKLOAD
		result << Driver.Operation.CREATE
		result
	}
	
	@Override
	public String defaultConnectURL () {
		H2Connection con = connection 
		if (con.inMemory) {
			return (con.connectHost != null)?"jdbc:h2:tcp://{host}/mem:{database}":"jdbc:h2:mem:{database}" 
		}
		
		(con.connectHost != null)?"jdbc:h2:tcp://{host}/{database}":"jdbc:h2://{database}"
	}
	
	/**
	 * Prepare object name by prefix
	 * @param name
	 * @param prefix
	 * @return
	 */
	@Override
	public String prepareObjectNameWithPrefix(String name, String prefix) {
		if (name == null) return null
		
		String res
		
		def m = name =~ /([^a-zA-Z0-9_])/
		if (m.size() > 0) res = prefix + name + prefix else res = name.toUpperCase()
		
		res
	}
	
	@Override
	protected String createDatasetExtend(Dataset dataset, Map params) {
		String result = ""
		def temporary = (dataset.sysParams.type in [JDBCDataset.Type.GLOBAL_TEMPORARY, JDBCDataset.Type.LOCAL_TEMPORARY])
		if (BoolUtils.IsValue(params."not_persistent", false)) result += "NOT PERSISTENT "
		if (temporary && params.transactional != null && params.transactional) result += "TRANSACTIONAL "
		
		result
	}
	
	@Override
	protected void bulkLoadFile(CSVDataset source, Dataset dest, Map params, Closure prepareCode) {
		if (params.compressed != null) throw new ExceptionGETL("H2 bulk load dont support compression files")
		
		params = bulkLoadFilePrepare(source, dest, params, prepareCode)
		
		List<Map> map = params.map
		boolean autoCommit = (params.autoCommit != null)?params.autoCommit:(dest.connection.tranCount == 0 && !dest.connection.autoCommit)
		
		StringBuilder sb = new StringBuilder()
		List columns = []
		List headers = []
		List fparm = []
		map.each { Map f ->
			if (f.field != null) {
				columns << fieldPrefix + f.field.name.toUpperCase() + fieldPrefix
				headers << f.field.name.toUpperCase()
			}
		}
		def cols = columns.join(", ")
		def heads = (!source.header)?"'" + headers.join(source.fieldDelimiter) + "'":"null"
		fparm << "charset=${source.codePage}"
		fparm << "fieldSeparator=${source.fieldDelimiter}"
//		fparm << "rowSeparator=${StringUtils.EscapeJava(source.rowDelimiter)}"
		if (source.quoteStr != null) fparm << "fieldDelimiter=${StringUtils.EscapeJava(source.quoteStr)}"
		def functionParms = fparm.join(" ") 
		
		sb << """
INSERT INTO ${fullNameDataset(dest)} (
${cols}
)
SELECT ${cols} 
FROM CSVREAD('${source.fullFileName()}', ${heads}, '${functionParms}')
""" 
		
		//println sb.toString()
		
		dest.writeRows = 0
		dest.updateRows = 0
		if (autoCommit) dest.connection.startTran()
		long count
		try {
			count = executeCommand(sb.toString(), [isUpdate: true])
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
		'''MERGE INTO {target} ({fields})
  KEY ({keys})
    SELECT {values}
	FROM {source} s'''
	}

	/*
	@Override	
	protected String unionDatasetMerge (JDBCDataset source, JDBCDataset target, Map map, List<String> keyField, Map procParams) {
			if (!source instanceof TableDataset) throw new ExceptionGETL("Source dataset must be \"TableDataset\"")
			if (keyField.isEmpty()) throw new ExceptionGETL("For MERGE operation required key fields by table")
			
			def insertFields = []
			def insertValues = []
			map.each { targetField, sourceField ->
				insertFields << "$fieldPrefix$targetField$fieldPrefix"
				insertValues << "$sourceField"
			}
			
			"""MERGE INTO ${target.fullNameDataset()}
  (${insertFields.join(", ")})
  KEY (${keyField.join(", ")})
    SELECT ${insertValues.join(", ")}
	FROM ${source.fullNameDataset()} s"""
	}
	*/
	
	@Override
	protected String sessionID() {
		String res
		def rows = sqlConnect.rows("SELECT SESSION_ID() AS session_id;")
		if (!rows.isEmpty()) res = rows[0].session_id.toString()
		
		res
	}
	
	@Override
	protected String openWriteMergeSql(JDBCDataset dataset, Map params, List<Field> fields) {
		def excludeFields = []
		fields.each { Field f -> 
			if (f.isAutoincrement || f.isReadOnly) excludeFields << f 
		}
		
		String res = """
MERGE INTO ${dataset.fullNameDataset()} (${GenerationUtils.SqlFields(dataset.connection, fields, null, excludeFields).join(", ")})
VALUES(${GenerationUtils.SqlFields(dataset.connection, fields, "?", excludeFields).join(", ")})
"""
		
		res
	}
}
