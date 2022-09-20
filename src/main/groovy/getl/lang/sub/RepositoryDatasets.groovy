package getl.lang.sub

import getl.clickhouse.ClickHouseTable
import getl.csv.CSVDataset
import getl.data.Connection
import getl.data.Dataset
import getl.data.StructureFileDataset
import getl.db2.DB2Table
import getl.dbf.DBFDataset
import getl.excel.ExcelDataset
import getl.exception.ExceptionDSL
import getl.exception.ExceptionGETL
import getl.firebird.FirebirdTable
import getl.h2.H2Table
import getl.hive.HiveTable
import getl.impala.ImpalaTable
import getl.jdbc.QueryDataset
import getl.jdbc.TableDataset
import getl.jdbc.ViewDataset
import getl.json.JSONDataset
import getl.kafka.KafkaDataset
import getl.mssql.MSSQLTable
import getl.mysql.MySQLTable
import getl.netezza.NetezzaTable
import getl.netsuite.NetsuiteTable
import getl.oracle.OracleTable
import getl.postgresql.PostgreSQLTable
import getl.proc.sub.ExecutorListElement
import getl.salesforce.SalesForceDataset
import getl.salesforce.SalesForceQueryDataset
import getl.sap.HanaTable
import getl.tfs.TDSTable
import getl.tfs.TFS
import getl.tfs.TFSDataset
import getl.transform.ArrayDataset
import getl.utils.GenerationUtils
import getl.utils.MapUtils
import getl.vertica.VerticaTable
import getl.xml.XMLDataset
import getl.yaml.YAMLDataset
import groovy.transform.InheritConstructors
import groovy.transform.stc.ClosureParams
import groovy.transform.stc.SimpleType

/**
 * Repository datasets manager
 * @author Alexsey Konstantinov
 */
@InheritConstructors
@SuppressWarnings('SpellCheckingInspection')
class RepositoryDatasets extends RepositoryObjectsWithConnection<Dataset> {
    static public final String CSVDATASET = CSVDataset.name
    static public final String CSVTEMPDATASET = TFSDataset.name
    static public final String CLICKHOUSETABLE = ClickHouseTable.name
    static public final String DB2TABLE = DB2Table.name
    static public final String DBFDATASET = DBFDataset.name
    static public final String EXCELDATASET = ExcelDataset.name
    static public final String FIREBIRDTABLE = FirebirdTable.name
    static public final String H2TABLE = H2Table.name
    static public final String HANATABLE = HanaTable.name
    static public final String HIVETABLE = HiveTable.name
    static public final String IMPALATABLE = ImpalaTable.name
    static public final String TABLEDATASET = TableDataset.name
    static public final String JSONDATASET = JSONDataset.name
    static public final String KAFKADATASET = KafkaDataset.name
    static public final String MSSQLTABLE = MSSQLTable.name
    static public final String MYSQLTABLE = MySQLTable.name
    static public final String NETEZZATABLE = NetezzaTable.name
    static public final String NETSUITETABLE = NetsuiteTable.name
    static public final String ORACLETABLE = OracleTable.name
    static public final String QUERYDATASET = QueryDataset.name
    static public final String POSTGRESQLTABLE = PostgreSQLTable.name
    static public final String SALESFORCEDATASET = SalesForceDataset.name
    static public final String SALESFORCEQUERYDATASET = SalesForceQueryDataset.name
    static public final String EMBEDDEDTABLE = TDSTable.name
    static public final String VIEWDATASET = ViewDataset.name
    static public final String VERTICATABLE = VerticaTable.name
    static public final String XMLDATASET = XMLDataset.name
    static public final String YAMLDATASET = YAMLDataset.name
    static public final String ARRAYDATASET = ArrayDataset.name

    /** List of allowed dataset classes */
    static public final List<String> LISTDATASETS = [
        CSVDATASET, CSVTEMPDATASET, CLICKHOUSETABLE, DB2TABLE, DBFDATASET, EXCELDATASET, FIREBIRDTABLE, H2TABLE, HANATABLE, HIVETABLE, IMPALATABLE, TABLEDATASET,
        JSONDATASET, KAFKADATASET, MSSQLTABLE, MYSQLTABLE, NETEZZATABLE, NETSUITETABLE, ORACLETABLE, QUERYDATASET, POSTGRESQLTABLE,
        SALESFORCEDATASET, SALESFORCEQUERYDATASET, EMBEDDEDTABLE, VIEWDATASET, VERTICATABLE, XMLDATASET, YAMLDATASET, ARRAYDATASET
    ]

    /** List of allowed jdbc dataset classes */
    static public List<String> LISTJDBCTABLES = [
            CLICKHOUSETABLE, DB2TABLE, EMBEDDEDTABLE, FIREBIRDTABLE, H2TABLE, HANATABLE, HIVETABLE, IMPALATABLE, TABLEDATASET, MSSQLTABLE, MYSQLTABLE,
            NETEZZATABLE, NETSUITETABLE, ORACLETABLE, QUERYDATASET, POSTGRESQLTABLE, VERTICATABLE, VIEWDATASET
    ]

    /** List of allowed file dataset classes */
    static public List<String> LISTFILES = [
        CSVDATASET, CSVTEMPDATASET, DBFDATASET, EXCELDATASET, JSONDATASET, XMLDATASET, YAMLDATASET
    ]

    /** List of allowed other dataset classes */
    static public List<String> LISTOTHER = [
            SALESFORCEDATASET, SALESFORCEQUERYDATASET, KAFKADATASET, ARRAYDATASET
    ]

    /** List of allowed dataset classes */
    @Override
    List<String> getListClasses() { LISTDATASETS }

    /** List of allowed jdbc dataset classes */
    @SuppressWarnings('GrMethodMayBeStatic')
    List<String> getListJdbcClasses() { LISTJDBCTABLES }

    /** List of allowed file dataset classes */
    @SuppressWarnings('GrMethodMayBeStatic')
    List<String> getListFileClasses() { LISTFILES }

    /** List of allowed other dataset classes */
    @SuppressWarnings('GrMethodMayBeStatic')
    List<String> getListOtherClasses() { LISTOTHER }

    @Override
    protected Dataset createObject(String className) {
        Dataset.CreateDataset(dataset: className)
    }

    @Override
    Map exportConfig(GetlRepository repObj) {
        def obj = repObj as Dataset
        if (obj.connection == null)
            throw new ExceptionDSL("No connection specified for dataset \"${obj.dslNameObject}\"!")

        def res = [dataset: obj.getClass().name] + MapUtils.Copy(obj.params, ['field', 'attributeField']) +
                GenerationUtils.Fields2Map(obj.field, 'fields')
        if (obj instanceof StructureFileDataset)
            res.putAll(GenerationUtils.Fields2Map((obj as StructureFileDataset).attributeField, 'attributeFields'))

        if (obj.connection.dslNameObject == null) {
            if (!(obj instanceof TFSDataset))
                throw new ExceptionDSL("Connection for dataset \"${obj.dslNameObject}\" must be registered in the repository!")
        }
        else
            res.connection = obj.connection.dslNameObject

        return res
    }

    @Override
    GetlRepository importConfig(Map config, GetlRepository existObject, String objectName) {
        def connectionName = config.connection as String
        Connection con = null
        if (connectionName != null) {
            try {
                con = dslCreator.registerConnection(null, connectionName, false, false) as Connection
            }
            catch (Exception e) {
                dslCreator.logError("Invalid connection \"$connectionName\" for dataset \"$objectName\"", e)
                throw new ExceptionDSL("Invalid connection \"$connectionName\" for dataset \"$objectName\": ${e.message}")
            }
        }

        def importParams = MapUtils.Copy(config, ['connection', 'fields', 'attributeFields']) as Map<String, Object>
        def obj = (existObject != null)?(existObject as Dataset).importParams(importParams):
                Dataset.CreateDataset(importParams)

        if (con == null) {
            if (!(obj instanceof TFSDataset))
                throw new ExceptionGETL('No dataset connection specified in configuration!')
            else
                con = TFS.storage
        }

        if (con != null)
            obj.setConnection(con)

        obj.field = GenerationUtils.Map2Fields(config, 'fields')
        if (obj instanceof StructureFileDataset)
            obj.attributeField = GenerationUtils.Map2Fields(config, 'attributeFields')

        return obj
    }

    /**
     * Return a list of objects associated with the names of two groups
     * @param sourceList List of name source objects
     * @param destGroup List of name destination objects
     * @param filter filtering objects
     * @return list of names of related objects
     */
    static List<ExecutorListElement> linkDatasets(List sourceList, List destList,
                                                  @ClosureParams(value = SimpleType, options = ['java.lang.String'])
                                                   Closure<Boolean> filter = null) {
        if (sourceList == null)
            throw new ExceptionGETL('Required to specify the value of the source group name!')
        if (destList == null)
            throw new ExceptionGETL('Required to specify the value of the destination group name!')

        def parse = new ParseObjectName()

        def sourceTables = new HashMap<String, String>()
        sourceList.each { name ->
            parse.name = name as String
            sourceTables.put(parse.objectName, parse.name)
        }

        def destTables = new HashMap<String, String>()
        destList.each { name ->
            parse.name = name as String
            destTables.put(parse.objectName, parse.name)
        }

        def res = [] as List<ExecutorListElement>
        sourceTables.each { smallName, sourceFullName ->
            def destFullName = destTables.get(smallName)
            if (destFullName != null)
                if (filter == null || filter.call(smallName))
                    res << new ExecutorListElement(source: sourceFullName, destination: destFullName)
        }

        return res
    }


}