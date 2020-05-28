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
package getl.lang.sub

import getl.csv.CSVDataset
import getl.data.Dataset
import getl.db2.DB2Table
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
import getl.mssql.MSSQLTable
import getl.mysql.MySQLTable
import getl.netezza.NetezzaTable
import getl.netsuite.NetsuiteTable
import getl.oracle.OracleTable
import getl.postgresql.PostgreSQLTable
import getl.proc.sub.ExecutorListElement
import getl.salesforce.SalesForceDataset
import getl.salesforce.SalesForceQueryDataset
import getl.tfs.TDSTable
import getl.tfs.TFSDataset
import getl.utils.GenerationUtils
import getl.utils.MapUtils
import getl.vertica.VerticaTable
import getl.xero.XeroDataset
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
class RepositoryDatasets extends RepositoryObjectsWithConnection<Dataset> {
    public static final String CSVDATASET = CSVDataset.name
    public static final String CSVTEMPDATASET = TFSDataset.name
    public static final String DB2TABLE = DB2Table.name
    public static final String EXCELDATASET = ExcelDataset.name
    public static final String FIREBIRDTABLE = FirebirdTable.name
    public static final String H2TABLE = H2Table.name
    public static final String HIVETABLE = HiveTable.name
    public static final String IMPALATABLE = ImpalaTable.name
    public static final String TABLEDATASET = TableDataset.name
    public static final String JSONDATASET = JSONDataset.name
    public static final String MSSQLTABLE = MSSQLTable.name
    public static final String MYSQLTABLE = MySQLTable.name
    public static final String NETEZZATABLE = NetezzaTable.name
    public static final String NETSUITETABLE = NetsuiteTable.name
    public static final String ORACLETABLE = OracleTable.name
    public static final String QUERYDATASET = QueryDataset.name
    public static final String POSTGRESQLTABLE = PostgreSQLTable.name
    public static final String SALESFORCEDATASET = SalesForceDataset.name
    public static final String SALESFORCEQUERYDATASET = SalesForceQueryDataset.name
    public static final String EMBEDDEDTABLE = TDSTable.name
    public static final String VIEWDATASET = ViewDataset.name
    public static final String VERTICATABLE = VerticaTable.name
    public static final String XERODATASET = XeroDataset.name
    public static final String XMLDATASET = XMLDataset.name
    public static final String YAMLDATASET = YAMLDataset.name

    /** List of allowed dataset classes */
    public static final List<String> LISTDATASETS = [
        CSVDATASET, CSVTEMPDATASET, DB2TABLE, EXCELDATASET, FIREBIRDTABLE, H2TABLE, HIVETABLE, IMPALATABLE, TABLEDATASET,
        JSONDATASET, MSSQLTABLE, MYSQLTABLE, NETEZZATABLE, NETSUITETABLE, ORACLETABLE, QUERYDATASET, POSTGRESQLTABLE,
        SALESFORCEDATASET, SALESFORCEQUERYDATASET, EMBEDDEDTABLE, VIEWDATASET, VERTICATABLE, XERODATASET,
        XMLDATASET, YAMLDATASET
    ]

    /** List of allowed jdbc dataset classes */
    public static List<String> LISTJDBCTABLES = [
        DB2TABLE, EMBEDDEDTABLE, FIREBIRDTABLE, H2TABLE, HIVETABLE, IMPALATABLE, TABLEDATASET, MSSQLTABLE, MYSQLTABLE,
        NETEZZATABLE, NETSUITETABLE, ORACLETABLE, QUERYDATASET, POSTGRESQLTABLE, VERTICATABLE
    ]

    /** List of allowed file dataset classes */
    public static List<String> LISTFILES = [
        CSVDATASET, CSVTEMPDATASET, EXCELDATASET, JSONDATASET, XMLDATASET, YAMLDATASET
    ]

    /** List of allowed other dataset classes */
    public static List<String> LISTOTHER = [
            SALESFORCEDATASET, SALESFORCEQUERYDATASET, XERODATASET
    ]

    /** List of allowed dataset classes */
    @Override
    List<String> getListClasses() { LISTDATASETS }

    /** List of allowed jdbc dataset classes */
    List<String> getListJdbcClasses() { LISTJDBCTABLES }

    /** List of allowed file dataset classes */
    List<String> getListFileClasses() { LISTFILES }

    /** List of allowed other dataset classes */
    List<String> getListOtherClasses() { LISTOTHER }

    @Override
    protected Dataset createObject(String className) {
        Dataset.CreateDataset(dataset: className)
    }

    @Override
    Map exportConfig(String name) {
        def obj = find(name)
        if (obj == null)
            throw new ExceptionDSL("Dataset \"$name\" not found!")
        if (obj.connection == null)
            throw new ExceptionDSL("No connection specified for dataset \"$name\"!")
        if (obj.connection.dslNameObject == null)
            throw new ExceptionDSL("Connection for dataset \"$name\" not found in repository!")
        if (obj.field.isEmpty())
            throw new ExceptionDSL("Dataset \"$name\" does not have a description of the fields!")

        def fields = GenerationUtils.Fields2Map(obj.field)

        return [dataset: obj.class.name, connection: obj.connection.dslNameObject] + obj.params + fields
    }

    @Override
    GetlRepository importConfig(Map config) {
        def connectionName = config.connection as String
        def con = dslCreator.connection(connectionName)
        def obj = Dataset.CreateDataset(MapUtils.Copy(config, ['connection', 'fields']))
        obj.setConnection(con)
        obj.field = GenerationUtils.Map2Fields(config)
        return obj
    }

    /**
     * Return a list of objects associated with the names of two groups
     * @param sourceList List of name source objects
     * @param destGroup List of name destination objects
     * @param filter filtering objects
     * @return list of names of related objects
     */
    protected List<ExecutorListElement> linkDatasets(List sourceList, List destList,
                                           @ClosureParams(value = SimpleType, options = ['java.lang.String'])
                                                   Closure<Boolean> filter = null) {
        if (sourceList == null)
            throw new ExceptionGETL('Required to specify the value of the source group name!')
        if (destList == null)
            throw new ExceptionGETL('Required to specify the value of the destination group name!')

        def parse = new ParseObjectName()

        def sourceTables = [:] as Map<String, String>
        sourceList.each { name ->
            parse.name = name as String
            sourceTables.put(parse.objectName, parse.name)
        }

        def destTables = [:] as Map<String, String>
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