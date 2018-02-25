package getl.salesforce

import com.sforce.soap.partner.Connector
import com.sforce.soap.partner.Field as sfField
import com.sforce.soap.partner.DescribeGlobalResult
import com.sforce.soap.partner.DescribeGlobalSObjectResult
import com.sforce.soap.partner.DescribeSObjectResult
import com.sforce.soap.partner.FieldType
import com.sforce.soap.partner.PartnerConnection
import com.sforce.soap.partner.QueryResult
import com.sforce.soap.partner.sobject.SObject
import com.sforce.ws.ConnectionException
import com.sforce.ws.ConnectorConfig
import getl.csv.CSVDataset
import getl.data.Dataset
import getl.data.Field
import getl.driver.Driver
import getl.exception.ExceptionGETL
import getl.utils.ListUtils
import groovy.transform.CompileStatic
import groovy.transform.InheritConstructors

/**
 * SalesForce Driver class
 * @author Dmitry Shaldin
 */
@InheritConstructors
class SalesForceDriver extends Driver {
	private ConnectorConfig config
	private PartnerConnection partnerConnection
	private boolean connected = false

	SalesForceDriver () {
		methodParams.register('eachRow', ['limit'])
		methodParams.register('retrieveObjects', [])
		methodParams.register('rows', ['limit'])
	}

	@Override
	List<Support> supported() { return [Support.EACHROW, Support.CONNECT, Support.CLOB, Support.AUTOLOADSCHEMA] }

	@Override
	List<Operation> operations() { return [Operation.RETRIEVEFIELDS] }

	@Override
	boolean isConnected() {
		return connected
	}

	@Override
	void connect() {
		SalesForceConnection con = connection as SalesForceConnection
		try {
			config = new ConnectorConfig()
			config.setUsername(con.login)
			config.setPassword(con.password)
			config.setAuthEndpoint(con.connectURL)

			partnerConnection = Connector.newConnection(config)
			this.connected = true
		} catch (ConnectionException ce) {
			ce.printStackTrace()
		}
	}

	@Override
	void disconnect() {
		try {
			partnerConnection.logout()
			this.connected = false
		} catch (ConnectionException ce) {
			ce.printStackTrace()
		}
	}

	@Override @CompileStatic
	List<Object> retrieveObjects(Map params, Closure filter) {
		DescribeGlobalResult describeGlobalResult = partnerConnection.describeGlobal()
		DescribeGlobalSObjectResult[] sobjectResults = describeGlobalResult.sobjects
		List<Map> objects = []

		sobjectResults.each { DescribeGlobalSObjectResult row ->
			def t = [:]

			t.'objectName' = row.name
			t.'label' = row.label
			t.'isCustom' = row.custom

			if (filter == null || filter(t)) objects << t
		}

		return objects as List<Object>
	}

	@Override @CompileStatic
	List<Field> fields(Dataset dataset) {
		List<Field> result = []

		DescribeSObjectResult describeSObjectResults = partnerConnection.describeSObject((dataset as SalesForceDataset).params.sfObjectName as String)

		describeSObjectResults.fields.each { sfField field ->
			Field f = new Field()

			f.name = field.name
			f.dbType = field.type
			f.typeName = field.type.toString()
			f.isKey = field.type == FieldType.id

			if (field.type in [FieldType._double, FieldType.percent, FieldType.currency]) {
				f.length = field.precision
				f.precision = field.scale
			} else {
				f.length = field.byteLength
				f.precision = field.scale
			}

			if (f.length <= 0) f.length = null
			if (f.precision < 0) f.precision = null

			f.isNull = field.nillable
			f.isAutoincrement = field.autoNumber
			f.description = field.label
			if (f.description == '') f.description = null

			result << f
		}

		return result
	}

	@Override @CompileStatic
	void prepareField (Field field) {
		if (field.dbType == null) return
		if (field.type != null && field.type != Field.Type.STRING) return

		switch (field.dbType) {
			case FieldType._int:
				field.type = Field.Type.INTEGER
				break

			case [ FieldType.url, FieldType.address, FieldType.id,
				   FieldType.reference, FieldType.reference, FieldType.multipicklist,
				   FieldType.phone, FieldType.picklist ]:
				field.type = Field.Type.STRING
				break

			case FieldType.textarea:
				field.length = null
				field.precision = null
				field.type = Field.Type.TEXT
				break

			case [FieldType._double, FieldType.currency, FieldType.percent]:
				field.type = Field.Type.DOUBLE
				break

			case FieldType.datetime:
				field.type = Field.Type.DATETIME
				break

			case FieldType.date:
				field.type = Field.Type.DATE
				break

			case FieldType.time:
				field.type = Field.Type.TIME
				break

			case FieldType._boolean:
				field.type = Field.Type.BOOLEAN
				break

			case FieldType.anyType:
				field.type = Field.Type.STRING
				break
		}
	}

	@Override @CompileStatic
	long eachRow(Dataset dataset, Map params, Closure prepareCode, Closure code) {
		String sfObjectName = dataset.params.sfObjectName
		Integer limit = ListUtils.NotNullValue([params.limit, dataset.params.limit, 0]) as Integer

		if (dataset.field.isEmpty()) dataset.retrieveFields()
		List<String> fields = dataset.field*.name
		if (prepareCode != null) fields = (ArrayList<String>)prepareCode.call(dataset.field)

		String soqlQuery = "SELECT ${fields.join(', ')} FROM $sfObjectName"
		if (limit > 0) soqlQuery += " limit ${limit.toString()}"

		long countRec = 0
		try {
			QueryResult qr = partnerConnection.query(soqlQuery)
			if (qr.size > 0) {
				Boolean done = false

				while (!done) {
					SObject[] records = qr.records

					records.each { SObject record ->
						Map row = [:]
						fields.each {
							row[it] = record.getSObjectField(it)
						}

						code(row)
						countRec++
					}

					if (qr.done) done = true
					else qr = partnerConnection.queryMore(qr.queryLocator)
				}
			}
		} catch (ConnectionException ce) {
			ce.printStackTrace()
		}

		return countRec
	}

	@Override
	void startTran() { throw new ExceptionGETL("Not supported") }

	@Override
	void commitTran() { throw new ExceptionGETL("Not supported") }

	@Override
	void rollbackTran() { throw new ExceptionGETL("Not supported") }

	@Override
	void createDataset(Dataset dataset, Map params) { throw new ExceptionGETL("Not supported") }

	@Override
	void openWrite(Dataset dataset, Map params, Closure prepareCode) { throw new ExceptionGETL("Not supported") }

	@Override
	void write(Dataset dataset, Map row) { throw new ExceptionGETL("Not supported") }

	@Override
	void doneWrite(Dataset dataset) { throw new ExceptionGETL("Not supported") }

	@Override
	void closeWrite(Dataset dataset) { throw new ExceptionGETL("Not supported") }

	@Override
	void bulkLoadFile(CSVDataset source, Dataset dest, Map params, Closure prepareCode) { throw new ExceptionGETL("Not supported") }

	@Override
	void clearDataset(Dataset dataset, Map params) { throw new ExceptionGETL("Not supported") }

	@Override
	long executeCommand(String command, Map params) { throw new ExceptionGETL("Not supported") }

	@Override
	long getSequence(String sequenceName) { throw new ExceptionGETL("Not supported") }
}
