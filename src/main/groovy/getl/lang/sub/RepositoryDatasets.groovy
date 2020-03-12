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

import getl.data.Connection
import getl.data.Dataset
import getl.exception.ExceptionGETL
import getl.h2.H2Connection
import getl.h2.H2Table
import getl.proc.sub.ExecutorListElement
import groovy.transform.InheritConstructors
import groovy.transform.Synchronized
import groovy.transform.stc.ClosureParams
import groovy.transform.stc.SimpleType

/**
 * Repository datasets manager
 * @author Alexsey Konstantinov
 */
@InheritConstructors
class RepositoryDatasets extends RepositoryObjectsWithConnection<Dataset> {
    public static final String CSVDATASET = 'getl.csv.CSVDataset'
    public static final String CSVTEMPDATASET = 'getl.tfs.TFSDataset'
    public static final String DB2TABLE = 'getl.db2.DB2Table'
    public static final String EXCELDATASET = 'getl.excel.ExcelDataset'
    public static final String FIREBIRDTABLE = 'getl.firebird.FirebirdTable'
    public static final String H2TABLE = 'getl.h2.H2Table'
    public static final String HIVETABLE = 'getl.hive.HiveTable'
    public static final String IMPALATABLE = 'getl.impala.ImpalaTable'
    public static final String TABLEDATASET = 'getl.jdbc.TableDataset'
    public static final String JSONDATASET = 'getl.json.JSONDataset'
    public static final String MSSQLTABLE = 'getl.mssql.MSSQLTable'
    public static final String MYSQLTABLE = 'getl.mysql.MySQLTable'
    public static final String NETEZZATABLE = 'getl.netezza.NetezzaTable'
    public static final String NETSUITETABLE = 'getl.netsuite.NetsuiteTable'
    public static final String ORACLETABLE = 'getl.oracle.OracleTable'
    public static final String QUERYDATASET = 'getl.jdbc.QueryDataset'
    public static final String POSTGRESQLTABLE = 'getl.postgresql.PostgreSQLTable'
    public static final String SALESFORCEDATASET = 'getl.salesforce.SalesForceDataset'
    public static final String SALESFORCEQUERYDATASET = 'getl.salesforce.SalesForceQueryDataset'
    public static final String EMBEDDEDTABLE = 'getl.tfs.TDSTable'
    public static final String VIEWDATASET = 'getl.jdbc.ViewDataset'
    public static final String VERTICATABLE = 'getl.vertica.VerticaTable'
    public static final String XERODATASET = 'getl.xero.XeroDataset'
    public static final String XMLDATASET = 'getl.xml.XMLDataset'

    /** List of allowed dataset classes */
    public static final List<String> LISTDATASETS = [
        CSVDATASET, CSVTEMPDATASET, DB2TABLE, EXCELDATASET, FIREBIRDTABLE, H2TABLE, HIVETABLE, IMPALATABLE, TABLEDATASET,
        JSONDATASET, MSSQLTABLE, MYSQLTABLE, NETEZZATABLE, NETSUITETABLE, ORACLETABLE, QUERYDATASET, POSTGRESQLTABLE,
        SALESFORCEDATASET, SALESFORCEQUERYDATASET, EMBEDDEDTABLE, VIEWDATASET, VERTICATABLE, XERODATASET,
        XMLDATASET
    ]

    /** List of allowed jdbc dataset classes */
    public static List<String> LISTJDBCTABLES = [
        DB2TABLE, EMBEDDEDTABLE, FIREBIRDTABLE, H2TABLE, HIVETABLE, IMPALATABLE, TABLEDATASET, MSSQLTABLE, MYSQLTABLE,
        NETEZZATABLE, NETSUITETABLE, ORACLETABLE, QUERYDATASET, POSTGRESQLTABLE, VERTICATABLE
    ]

    /** List of allowed file dataset classes */
    public static List<String> LISTFILES = [
        CSVDATASET, CSVTEMPDATASET, EXCELDATASET, JSONDATASET, XMLDATASET
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
    protected String getNameCloneCollection() { 'datasets' }

    @Override
    protected String getTypeObject() { 'Dataset' }

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
