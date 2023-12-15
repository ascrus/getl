//file:noinspection DuplicatedCode
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
import getl.exception.DatasetError
import getl.exception.ExceptionGETL
import getl.tfs.TFS
import getl.tfs.TFSDataset
import getl.utils.BoolUtils
import getl.utils.ConvertUtils
import getl.utils.DateUtils
import getl.utils.ListUtils
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
    private Boolean connected = false

    @Override
	List<Support> supported() { return [Support.EACHROW, Support.CONNECT, Support.CLOB, Support.AUTOLOADSCHEMA] }

	@Override
	List<Operation> operations() { return [Operation.RETRIEVEFIELDS] }

    /** Current SalesForce connection */
    @SuppressWarnings('unused')
    SalesForceConnection getCurrentSalesForceConnection() { connection as SalesForceConnection }

	@Override
    Boolean isConnected() {
		return connected
	}

    Boolean getIsBulkConnected() {
        return bulkConnection != null
    }

	@Override
	void connect() {
		SalesForceConnection con = connection as SalesForceConnection
		try {
            this.config = new ConnectorConfig()
            this.config.setUsername(con.login)
            this.config.setPassword(con.loginManager.currentDecryptPassword())
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

	@Override
	List<Object> retrieveObjects(Map params, Closure<Boolean> filter) {
		DescribeGlobalResult describeGlobalResult = partnerConnection.describeGlobal()
		DescribeGlobalSObjectResult[] sObjectResults = describeGlobalResult.sobjects
		List<Map> objects = []

		sObjectResults.each { DescribeGlobalSObjectResult row ->
			def t = new HashMap()

			t.objectName = row.name
			t.label = row.label
			t.isCustom = row.custom

			if (filter == null || filter(t)) objects << t
		}

		return objects as List<Object>
	}

	@Override
	List<Field> fields(Dataset dataset) {
		List<Field> result = []

		DescribeSObjectResult describeSObjectResults = partnerConnection.describeSObject((dataset as SalesForceDataset).params.sfObjectName as String)

		describeSObjectResults.fields.eachWithIndex { sfField field, Integer idx ->
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
			f.extended.referenceAlias = field.relationshipName
			f.extended.referenceOrder = field.relationshipOrder

			result << f
		}

		return result
	}

    @SuppressWarnings('SpellCheckingInspection')
    @Override
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

	@Override
    Long eachRow(Dataset dataset, Map params, Closure prepareCode, Closure code) {
		String sfObjectName = dataset.params.sfObjectName
		def limit = ListUtils.NotNullValue([ConvertUtils.Object2Int(params.limit), dataset.params.limit, 0]) as Integer
        String where = (params.where)?:''
        Map<String, String> orderBy = ((params.orderBy)?:new HashMap<String, String>()) as Map<String, String>

        Boolean readAsBulk = BoolUtils.IsValue(params.readAsBulk)

		Boolean isCustomQuery = (dataset.params?.query as String)?.length() > 0
        String soqlQuery
        List<String> fields = []

        if (isCustomQuery) {
            if (readAsBulk) throw new ExceptionGETL("Custom Query isn't supported with bulk API.")

            if (dataset.field.isEmpty()) throw new ExceptionGETL("Field names must be declared when custom query is used.")
            else fields = dataset.field*.name

            soqlQuery = dataset.params.query
        } else {
            if (dataset.field.isEmpty()) dataset.retrieveFields()
            if (prepareCode != null) {
                prepareCode.call(dataset.field)
            }

            fields = dataset.field*.name

            // SOQL Query generation
            soqlQuery = "SELECT ${fields.join(', ')}\nFROM $sfObjectName"
            if (where.size() > 0) soqlQuery += "\nWHERE $where"
            if (orderBy.size() > 0) soqlQuery += "\nORDER BY ${orderBy.collect { k, v -> "$k $v".toString() }.join(', ')}"
            if (limit > 0) soqlQuery += "\nLIMIT ${limit.toString()}"
        }

		def countRec = 0L

        if (readAsBulk) {
            List<TFSDataset> tfsDatasetList = bulkUnload(dataset, params)
            tfsDatasetList.each { TFSDataset tDataset ->
                tDataset.eachRow { Map row ->
                    code.call(row)
                    countRec++
                }

                tDataset.drop()
            }
        } else {
            try {
                QueryResult qr = partnerConnection.query(soqlQuery)
                if (qr.size > 0) {
                    Boolean done = false

                    while (!done) {
                        SObject[] records = qr.records

                        records.each { SObject record ->
                            Map row = new HashMap()
                            fields.each {
                                row[it.toLowerCase()] = parseTypes(record.getSObjectField(it), dataset.fieldByName(it))
                            }

                            code.call(row)
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

    protected List<TFSDataset> bulkUnload(Dataset dataset, Map params) {
        if (!isBulkConnected) initBulkConnection()

        String sfObjectName = dataset.params.sfObjectName
        def limit = ListUtils.NotNullValue([ConvertUtils.Object2Int(params.limit), dataset.params.limit, 0]) as Integer
        String where = (params.where) ?: ''
        String bulkJobId = params.bulkJobId
        Map<String, String> orderBy = ((params.orderBy)?:new HashMap<String, String>()) as Map<String, String>

        Integer chunkSize = ListUtils.NotNullValue([params.chunkSize, dataset.params.chunkSize, 0]) as Integer

        if (chunkSize > 0) {
            this.bulkConnection.addHeader("Sforce-Enable-PKChunking","chunkSize=$chunkSize")

            if (where.size() > 0 || orderBy.size() > 0 || limit > 0)
                throw new ExceptionGETL('Limit, where, orderBy is not allowed with chunk size')
        }

        if (dataset.field.isEmpty()) dataset.retrieveFields()
        List<String> fields = dataset.field*.name

        JobInfo job = getOrCreateJob(bulkConnection, sfObjectName, bulkJobId)
        if (bulkJobId == null) {
            connection.logger.info("Job '${job.id}' created.")
        } else {
            connection.logger.info("Reusing Job '${job.id}'.")
        }

        List<TFSDataset> tfsDatasetList = []

        try {
            job = bulkConnection.getJobStatus(job.id)
            Boolean isJobClosed = job.state == JobStateEnum.Closed

            // SOQL Query generation
            String soqlQuery = "SELECT ${fields.join(', ')}\nFROM $sfObjectName"
            if (where.size() > 0) soqlQuery += "\nWHERE $where"
            if (orderBy.size() > 0) soqlQuery += "\nORDER BY ${orderBy.collect { k, v -> "$k $v".toString() }.join(', ')}"
            if (limit > 0) soqlQuery += "\nLIMIT ${limit.toString()}"

            ByteArrayInputStream bout = new ByteArrayInputStream(soqlQuery.bytes)

            BatchInfo mainBatch
            if (!isJobClosed) {
                mainBatch = bulkConnection.createBatchFromStream(job, bout)
            } else {
                def batches = bulkConnection.getBatchInfoList(job.id).batchInfo
                mainBatch = batches.find { it.state === BatchStateEnum.NotProcessed } ?:
                        batches.find { it.state === BatchStateEnum.Completed } ?:
                                batches[0]
            }

            // Trick to get the real number of batches because of async API
            if (chunkSize > 0 && !isJobClosed) {
                def batchCounter = 0
                while (bulkConnection.getBatchInfo(job.id, mainBatch.id).state != BatchStateEnum.NotProcessed) {
                    batchCounter++
                }
            }

            BatchInfo[] batchInfos = bulkConnection.getBatchInfoList(job.id).batchInfo

            TFS tfsArea = new TFS()
            tfsArea.header = true
            tfsArea.fieldDelimiter = ','
            tfsArea.deleteOnExit = true
            tfsArea.quoteStr = '"'
            tfsArea.path += "/${job.id}"

            if (batchInfos.length > 1) {
                for (Integer bn = 0; bn < batchInfos.length; bn++) {
                    BatchInfo info = batchInfos[bn]
                    if (info.id == mainBatch.id) continue
                    tfsDatasetList.addAll(processBatch(job, info, tfsArea, dataset.field))
                }
            } else {
                BatchInfo info = batchInfos[0]
                tfsDatasetList.addAll(processBatch(job, info, tfsArea, dataset.field))
            }
        } catch (e) {
            throw new DatasetError(dataset, 'SalesForceError', e)
        } finally {
            if (job.state === JobStateEnum.Open) {
                closeJob(bulkConnection, job.getId())
            }
        }

        return tfsDatasetList
    }

    private List<TFSDataset> processBatch(JobInfo job, BatchInfo info, TFS tfsArea, List<Field> fields) {
        String[] queryResults = null
        String batchId = info.id

        for (Integer i = 0; i < 10000; i++) {
            info = bulkConnection.getBatchInfo(job.id, batchId)

            if (info.state == BatchStateEnum.Completed) {
                QueryResultList list = bulkConnection.getQueryResultList(job.id, batchId)
                queryResults = list.result
                connection.logger.info("Batch ID: ${batchId}, Batch Status: ${info.state}")
                break
            } else if (info.state == BatchStateEnum.Failed) {
                throw new ExceptionGETL(info.toString())
            } else {
                connection.logger.info("Batch ID: ${batchId}, Batch Status: ${info.state}")
                Thread.sleep(10000) // 10 sec
            }
        }

        List<TFSDataset> tfsDatasetList = []
        int queryResultsSize = queryResults.size()
        connection.logger.info("Batch ID: ${batchId}. Found Query Result(s): ${queryResultsSize}")

        queryResults.eachWithIndex { String resultId, int idx ->
            connection.logger.info("Batch ID: ${batchId}. Working on Query Result: ${resultId}")
            InputStream inputStream = bulkConnection.getQueryResultStream(job.id, batchId, resultId)
            TFSDataset tDataset = new TFSDataset(connection: tfsArea, fileName: resultId, field: fields)

            try {
                new File(tDataset.getObjectFullName()).withOutputStream { outputStream ->
                    inputStream.eachByte(1024 * 8) { byte[] data, Integer len ->
                        outputStream.write(data, 0, len)
                    }
                }
            } finally {
                inputStream.close()
                tfsDatasetList.add(tDataset)
            }

            connection.logger.info("Batch ID: ${batchId}. Query Result '${resultId}' is processed.")
            connection.logger.info("Batch ID: ${batchId}. Processed ${idx + 1} of ${queryResults.size()} Query Results.")
        }

        return tfsDatasetList
    }

    private static JobInfo getOrCreateJob(BulkConnection bulkConnection, String sfObjectName, String jobId) {
        JobInfo job = new JobInfo()

        job.object = sfObjectName
        job.operation = OperationEnum.query
        job.concurrencyMode = ConcurrencyMode.Parallel
        job.contentType = ContentType.CSV

        if (jobId != null) {
            job = bulkConnection.getJobStatus(jobId)
        } else {
            job = bulkConnection.createJob(job)
        }

        assert job.id != null

        return job
    }

	static private void closeJob(BulkConnection connection, String jobId) {
		JobInfo job = new JobInfo()
		job.id = jobId
		job.state = JobStateEnum.Closed
		connection.updateJob(job)
	}

    @SuppressWarnings('SpellCheckingInspection')
    static private Object parseTypes(Object value, Field field) {
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
				value = DateUtils.ParseSQLTimestamp("yyyy-MM-dd'T'HH:mm:ss", value?.toString())
				break

			case 'DATE':
				value = DateUtils.ParseSQLDate("yyyy-MM-dd", value)
				break

			case 'TIME':
				break
		}

		return value
	}

	@Override
	void startTran(Boolean useSqlOperator = false) { throw new ExceptionGETL('Not support this features!') }

	@Override
	void commitTran(Boolean useSqlOperator = false) { throw new ExceptionGETL('Not support this features!') }

	@Override
	void rollbackTran(Boolean useSqlOperator = false) { throw new ExceptionGETL('Not support this features!') }

	@Override
	void createDataset(Dataset dataset, Map params) { throw new ExceptionGETL('Not support this features!') }

	@Override
	void openWrite(Dataset dataset, Map params, Closure prepareCode) { throw new ExceptionGETL('Not support this features!') }

	@Override
	void write(Dataset dataset, Map row) { throw new ExceptionGETL('Not support this features!') }

	@Override
	void doneWrite(Dataset dataset) { throw new ExceptionGETL('Not support this features!') }

	@Override
	void closeWrite(Dataset dataset) { throw new ExceptionGETL('Not support this features!') }

	@Override
	void bulkLoadFile(CSVDataset source, Dataset dest, Map params, Closure prepareCode) { throw new ExceptionGETL('Not support this features!') }

	@Override
	void clearDataset(Dataset dataset, Map params) { throw new ExceptionGETL('Not support this features!') }

	@Override
    Long executeCommand(String command, Map params, StringBuffer commandBuffer = null) {
        throw new ExceptionGETL('Not support this features!')
    }

	@Override
    Long getSequence(String sequenceName) { throw new ExceptionGETL('Not support this features!') }
}
