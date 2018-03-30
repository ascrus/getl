package getl.salesforce

import com.sforce.async.AsyncApiException
import com.sforce.async.BatchInfo
import com.sforce.async.BatchStateEnum
import com.sforce.async.BulkConnection
import com.sforce.async.ConcurrencyMode
import com.sforce.async.ContentType
import com.sforce.async.JobInfo
import com.sforce.async.JobStateEnum
import com.sforce.async.OperationEnum
import com.sforce.async.QueryResultList
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
import getl.tfs.TFS
import getl.tfs.TFSDataset
import getl.utils.BoolUtils
import getl.utils.ConvertUtils
import getl.utils.DateUtils
import getl.utils.ListUtils
import getl.utils.Logs
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
    private BulkConnection bulkConnection
	private boolean connected = false

	SalesForceDriver () {
		methodParams.register('eachRow', ['limit', 'where', 'readAsBulk', 'orderBy'])
		methodParams.register('retrieveObjects', [])
        methodParams.register('bulkUnload', ['where', 'limit', 'fileName', 'orderBy'])
		methodParams.register('rows', ['limit', 'where', 'readAsBulk', 'orderBy'])
	}

	@Override
	List<Support> supported() { return [Support.EACHROW, Support.CONNECT, Support.CLOB, Support.AUTOLOADSCHEMA] }

	@Override
	List<Operation> operations() { return [Operation.RETRIEVEFIELDS] }

	@Override
	boolean isConnected() {
		return connected
	}

    boolean getIsBulkConnected() {
        return bulkConnection != null
    }

	@Override
	void connect() {
		SalesForceConnection con = connection as SalesForceConnection
		try {
            this.config = new ConnectorConfig()
            this.config.setUsername(con.login)
            this.config.setPassword(con.password)
            this.config.setAuthEndpoint(con.connectURL)
            // This should only be false when doing debugging.
            this.config.setCompression(true)
            // Set this to true to see HTTP requests and responses on stdout
            this.config.setTraceMessage(false)

            this.partnerConnection = Connector.newConnection(config)
            this.partnerConnection.setQueryOptions(con.batchSize)
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
            this.bulkConnection = null
		} catch (ConnectionException ce) {
			ce.printStackTrace()
            throw new ExceptionGETL(ce.toString())
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

		describeSObjectResults.fields.eachWithIndex { sfField field, int idx ->
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

			f.extended.ordinalPosition = idx + 1
			f.extended.calculatedFormula = field.calculatedFormula
			f.extended.referenceTo = field.referenceTo.size() > 0 ? field.referenceTo.toList().join(', ') : null

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
				   FieldType.phone, FieldType.picklist, FieldType.anyType ]:
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
                field.format = "yyyy-MM-dd'T'HH:mm:ss"
				break

			case FieldType.date:
				field.type = Field.Type.DATE
                field.format = 'yyyy-MM-dd'
				break

			case FieldType.time:
				field.type = Field.Type.TIME
				break

			case FieldType._boolean:
				field.type = Field.Type.BOOLEAN
                field.format = 'true|false'
				break
		}
	}

	@Override @CompileStatic
	long eachRow(Dataset dataset, Map params, Closure prepareCode, Closure code) {
		String sfObjectName = dataset.params.sfObjectName
		Integer limit = ListUtils.NotNullValue([params.limit, dataset.params.limit, 0]) as Integer
        String where = (params.where) ?: ''
        Map<String, String> orderBy = ((params.orderBy) ?: [:]) as Map<String, String>

        Boolean readAsBulk = BoolUtils.IsValue(params.readAsBulk)

		if (dataset.field.isEmpty()) dataset.retrieveFields()
		List<String> fields = dataset.field*.name
		if (prepareCode != null) fields = (ArrayList<String>)prepareCode.call(dataset.field)

        // SOQL Query generation
		String soqlQuery = "SELECT ${fields.join(', ')}\nFROM $sfObjectName"
        if (where.size() > 0) soqlQuery += "\nWHERE $where"
        if (orderBy.size() > 0) {
            soqlQuery += '\nORDER BY '
            List<String> result = []
            orderBy.each { k, v ->
                result.add("$k $v".toString())
            }

            soqlQuery += result.join(', ')
        }
		if (limit > 0) soqlQuery += "\nLIMIT ${limit.toString()}"

		long countRec = 0

        if (readAsBulk) {
            TFSDataset csv = TFS.dataset()
            csv.fieldDelimiter = ','
            csv.quoteStr = '"'
            csv.field = dataset.field

            bulkUnload(dataset, params + [fileName: csv.fullFileName()])

            csv.eachRow { Map row ->
                code(row)
                countRec++
            }

            csv.drop()
        } else {
            try {
                QueryResult qr = partnerConnection.query(soqlQuery)
                if (qr.size > 0) {
                    Boolean done = false

                    while (!done) {
                        SObject[] records = qr.records

                        records.each { SObject record ->
                            Map row = [:]
                            fields.each {
                                row[it.toLowerCase()] = parseTypes(record.getSObjectField(it), dataset.fieldByName(it))
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
        }


		return countRec
	}

    private void initBulkConnection() {
        if (!connected) connect()
        try {
            String connectURL = connection.params.connectURL
            String restEndpoint = config.serviceEndpoint.substring(0, config.serviceEndpoint.indexOf('Soap/') - 1)
            String apiVersion = connectURL.substring(connectURL.lastIndexOf('/') + 1)
            this.config.setRestEndpoint("$restEndpoint/async/$apiVersion")
            this.bulkConnection = new BulkConnection(config)
        } catch (AsyncApiException aae) {
            aae.printStackTrace()
            throw new ExceptionGETL(aae.toString())
        }
    }

    @CompileStatic
    protected void bulkUnload(Dataset dataset, Map params) {
        if (!isBulkConnected) initBulkConnection()

        String fileName = "${params.fileName}.getltemp"
        File tmpFile = new File(fileName)

        String sfObjectName = dataset.params.sfObjectName
        Integer limit = ListUtils.NotNullValue([params.limit, dataset.params.limit, 0]) as Integer
        String where = (params.where) ?: ''

        if (dataset.field.isEmpty()) dataset.retrieveFields()
        List<String> fields = dataset.field*.name

        JobInfo job = createJob(bulkConnection, sfObjectName)

        try {
            job = bulkConnection.getJobStatus(job.getId())

            // SOQL Query generation
            String soqlQuery = "SELECT ${fields.join(', ')}\nFROM $sfObjectName"
            if (where.size() > 0) soqlQuery += "\nwhere $where"
            if (limit > 0) soqlQuery += "\nlimit ${limit.toString()}"

            BatchInfo info
            ByteArrayInputStream bout = new ByteArrayInputStream(soqlQuery.getBytes())
            info = bulkConnection.createBatchFromStream(job, bout)

            String[] queryResults = null

            for (int i = 0; i < 10000; i++) {
                Thread.sleep(30000) //30 sec
                info = bulkConnection.getBatchInfo(job.getId(), info.getId())

                if (info.getState() == BatchStateEnum.Completed) {
                    QueryResultList list = bulkConnection.getQueryResultList(job.getId(), info.getId())
                    queryResults = list.getResult()
                    break
                } else if (info.getState() == BatchStateEnum.Failed) {
                    throw new ExceptionGETL(info.toString())
                } else {
                    Logs.Info("Batch ID: ${info.getId()}, Batch Status: ${info.getState()}")
                }
            }

            queryResults.each { String resultId ->
                InputStream inputStream = bulkConnection.getQueryResultStream(job.getId(), info.getId(), resultId)
                try {
                    tmpFile.withOutputStream { outputStream ->
                        inputStream.eachByte(1024 * 8) { byte[] data, int len ->
                            outputStream.write(data, 0, len)
                        }
                    }
                } finally {
                    inputStream.close()
                }
            }
        } catch (e) {
            tmpFile.delete()
            throw new ExceptionGETL(e)
        } finally {
            closeJob(bulkConnection, job.getId())
            tmpFile.renameTo(params.fileName as String)
        }
    }

    @CompileStatic
    private static JobInfo createJob(BulkConnection connection, String sfObjectName) {
        JobInfo job = new JobInfo()

        job.with {
            object = sfObjectName
            operation = OperationEnum.query
            concurrencyMode = ConcurrencyMode.Parallel
            contentType = ContentType.CSV
        }

        job = connection.createJob(job)
        assert job.getId() != null

        return job
    }

	@CompileStatic
	private static void closeJob(BulkConnection connection, String jobId) {
		JobInfo job = new JobInfo()
		job.setId(jobId)
		job.setState(JobStateEnum.Closed)
		connection.updateJob(job)
	}

	@CompileStatic
	private static Object parseTypes(Object value, final Field field) {
		switch (field.type) {
			case 'STRING':
				value = ConvertUtils.Object2String(value)
				break

			case 'BOOLEAN':
				value = ConvertUtils.String2Boolean(value as String, 'false')
				break

			case 'INTEGER':
				value = ConvertUtils.Object2Int(value)
				break

			case 'TEXT':
				value = ConvertUtils.Object2String(value)
				break

			case 'DOUBLE':
				value = ConvertUtils.Object2Double(value)
				break

			case 'DATETIME':
				value = DateUtils.ParseDate("yyyy-MM-dd'T'HH:mm:ss", value)
				break

			case 'DATE':
				value = DateUtils.ParseDate("yyyy-MM-dd", value)
				break

			case 'TIME':
				break
		}

		return value
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
