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

package getl.lang

import getl.config.*
import getl.config.opts.*
import getl.csv.*
import getl.data.*
import getl.db2.*
import getl.deploy.Version
import getl.driver.Driver
import getl.excel.*
import getl.exception.ExceptionGETL
import getl.files.*
import getl.h2.*
import getl.hive.*
import getl.jdbc.*
import getl.json.*
import getl.lang.opts.*
import getl.mssql.*
import getl.mysql.*
import getl.netsuite.*
import getl.oracle.*
import getl.postgresql.*
import getl.proc.*
import getl.proc.opts.*
import getl.salesforce.*
import getl.stat.*
import getl.tfs.*
import getl.utils.*
import getl.vertica.*
import getl.xero.*
import getl.xml.*
import groovy.transform.InheritConstructors
import groovy.transform.Synchronized
import groovy.transform.stc.ClosureParams
import groovy.transform.stc.SimpleType

import java.util.concurrent.ConcurrentHashMap

/**
 * Getl language script
 * @author Alexsey Konstantinov
 *
 */
@InheritConstructors
class Getl extends Script {
    protected Getl() {
        super()
        init()
    }

    protected Getl(Binding binding) {
        super(binding)
        init()
    }

    private void init() {
        Version.SayInfo()

        _params.executedClasses = new SynchronizeObject()

        _params.repConnections = new ConcurrentHashMap<String, Connection>()
        _params.repDatasets = new ConcurrentHashMap<String, Dataset>()
        _params.repHistoryPoints = new ConcurrentHashMap<String, SavePointManager>()
        _params.repFileManagers = new ConcurrentHashMap<String, Manager>()

        _connections = _params.repConnections
        _datasets = _params.repDatasets
        _historypoints = _params.repHistoryPoints
        _filemanagers = _params.repFileManagers

        //useEmbeddedConnection()
        //useCsvTempConnection()
    }

    @Override
    Object run() { return this }

    /** Instance DSL */
    protected static Getl _getl

    /* Owner object for instance DSL */
    private _ownerObject

    /** Run DSL script on getl share object */
    static Getl Dsl(def ownerObject, Map parameters,
                    @DelegatesTo(Getl)
                    @ClosureParams(value = SimpleType, options = ['getl.lang.Getl']) Closure cl) {
        if (_getl == null) _getl = new Getl()
        return _getl.runDsl(ownerObject, parameters, cl)
    }

    /** Run DSL script on getl share object */
    static Getl Dsl(def thisObject,
                    @DelegatesTo(Getl)
                    @ClosureParams(value = SimpleType, options = ['getl.lang.Getl']) Closure cl) {
        Dsl(thisObject, null, cl)
    }

    /** Run DSL script on getl share object */
    static Getl Dsl(@DelegatesTo(Getl)
                    @ClosureParams(value = SimpleType, options = ['getl.lang.Getl']) Closure cl) {
        Dsl(null, null, cl)
    }

    /** Run DSL script on getl share object */
    static Getl Dsl() {
        Dsl(null, null, null)
    }

    /** Clean Getl instance */
    static void CleanGetl() {
        _getl = null
    }

    /** Run DSL script */
    Getl runDsl(def ownerObject, Map parameters,
                @DelegatesTo(Getl)
                @ClosureParams(value = SimpleType, options = ['getl.lang.Getl']) Closure cl) {
        if (ownerObject != null) _ownerObject = ownerObject
        if (cl != null) {
            def code = cl.rehydrate(this, this, _ownerObject?:this)
            code.resolveStrategy = childDelegate
            if (parameters != null) code.properties.putAll(parameters)
            code.call(this)
        }

        return this
    }

    /** Run DSL script */
    Getl runDsl(def ownerObject,
                @DelegatesTo(Getl)
                @ClosureParams(value = SimpleType, options = ['getl.lang.Getl']) Closure cl) {
        runDsl(ownerObject, null, cl)
    }

    /** Run DSL script */
    Getl runDsl(@DelegatesTo(Getl)
                @ClosureParams(value = SimpleType, options = ['getl.lang.Getl']) Closure cl) {
        runDsl(null, null, cl)
    }

    /** Owner object for child objects  */
    def getChildOwnerObject() { this }
    /** This object for child objects */
    def getChildThisObject() { _ownerObject?:this }
    /** Delegate method for child objects */
    protected static int childDelegate = Closure.DELEGATE_FIRST

    protected Map<String, Object> _params = new ConcurrentHashMap<String, Object>()
    /** Set language parameters */
    protected void setGetlParams(Map<String, Object> importParams) {
        _params = importParams
        _connections = _params.repConnections
        _datasets = _params.repDatasets
        _historypoints = _params.repHistoryPoints
        _filemanagers = _params.repFileManagers
    }

    public final String CSVCONNECTION = 'getl.csv.CSVConnection'
    public final String CSVTEMPCONNECTION = 'getl.tfs.TFS'
    public final String DB2CONNECTION = 'getl.db2.DB2Connection'
    public final String EXCELCONNECTION = 'getl.excel.ExcelConnection'
    public final String H2CONNECTION = 'getl.h2.H2Connection'
    public final String HIVECONNECTION = 'getl.hive.HiveConnection'
    public final String JSONCONNECTION = 'getl.json.JSONConnection'
    public final String MSSQLCONNECTION = 'getl.mssql.MSSQLConnection'
    public final String MYSQLCONNECTION = 'getl.mysql.MySQLConnection'
    public final String NETSUITECONNECTION = 'getl.netsuite.NetsuiteConnection'
    public final String ORACLECONNECTION = 'getl.oracle.OracleConnection'
    public final String POSTGRESQLCONNECTION = 'getl.postgresql.PostgreSQLConnection'
    public final String SALESFORCECONNECTION = 'getl.salesforce.SalesForceConnection'
    public final String EMBEDDEDCONNECTION = 'getl.tfs.TDS'
    public final String VERTICACONNECTION = 'getl.vertica.VerticaConnection'
    public final String XEROCONNECTION = 'getl.xero.XeroConnection'
    public final String XMLCONNECTION = 'getl.xml.XMLConnection'

    /** List of allowed connection classes */
    public final List<String> listConnectionClasses = [
            CSVCONNECTION, CSVTEMPCONNECTION, DB2CONNECTION, EMBEDDEDCONNECTION, EXCELCONNECTION, H2CONNECTION,
            HIVECONNECTION, JSONCONNECTION, MSSQLCONNECTION, MYSQLCONNECTION, NETSUITECONNECTION, ORACLECONNECTION,
            POSTGRESQLCONNECTION, SALESFORCECONNECTION, VERTICACONNECTION, XEROCONNECTION, XMLCONNECTION
    ]

    /** List of allowed jdbc connection classes */
    public final List<String> listJdbcConectionClasses = [
            DB2CONNECTION, H2CONNECTION, HIVECONNECTION, MSSQLCONNECTION, MYSQLCONNECTION, NETSUITECONNECTION,
            ORACLECONNECTION, POSTGRESQLCONNECTION, VERTICACONNECTION
    ]

    public final String CSVDATASET = 'getl.csv.CSVDataset'
    public final String CSVTEMPDATASET = 'getl.tfs.TFSDataset'
    public final String DB2TABLE = 'getl.db2.DB2Table'
    public final String EXCELDATASET = 'getl.excel.ExcelDataset'
    public final String H2TABLE = 'getl.h2.H2Table'
    public final String HIVETABLE = 'getl.hive.HiveTable'
    public final String JSONDATASET = 'getl.json.JSONDataset'
    public final String MSSQLTABLE = 'getl.mssql.MSSQLTable'
    public final String MYSQLTABLE = 'getl.mysql.MySQLTable'
    public final String NETSUITETABLE = 'getl.netsuite.NetsuiteTable'
    public final String ORACLETABLE = 'getl.oracle.OracleTable'
    public final String QUERYDATASET = 'getl.jdbc.QueryDataset'
    public final String POSTGRESQLTABLE = 'getl.postgresql.PostgreSQLTable'
    public final String SALESFORCEDATASET = 'getl.salesforce.SalesForceDataset'
    public final String SALESFORCEQUERYDATASET = 'getl.salesforce.SalesForceQueryDataset'
    public final String EMBEDDEDTABLE = 'getl.tfs.TDSTable'
    public final String VIEWDATASET = 'getl.jdbc.ViewDataset'
    public final String VERTICATABLE = 'getl.vertica.VerticaTable'
    public final String XERODATASET = 'getl.xero.XeroDataset'
    public final String XMLDATASET = 'getl.xml.XMLDataset'

    /** List of allowed dataset classes */
    public final List<String> listDatasetClasses = [
            CSVDATASET, CSVTEMPDATASET, DB2TABLE, EXCELDATASET, H2TABLE, HIVETABLE, JSONDATASET, MSSQLTABLE,
            MYSQLTABLE, NETSUITETABLE, ORACLETABLE, QUERYDATASET, POSTGRESQLTABLE, SALESFORCEDATASET,
            SALESFORCEQUERYDATASET, EMBEDDEDTABLE, VIEWDATASET, VERTICATABLE, XERODATASET, XMLDATASET
    ]

    /** List of allowed jdbc dataset classes */
    public final List<String> listJdbcTableClasses = [
            DB2TABLE, H2TABLE, HIVETABLE, MSSQLTABLE, MYSQLTABLE, NETSUITETABLE, ORACLETABLE, POSTGRESQLTABLE,
            VERTICATABLE
    ]

    public final String FILEMANAGER = 'getl.files.FileManager'
    public final String FTPMANAGER = 'getl.files.FTPManager'
    public final String HDFSMANAGER = 'getl.files.HDFSManager'
    public final String SFTPMANAGER = 'getl.files.SFTPManager'

    /** List of allowed file manager classes */
    public final List<String> listFileManagerClasses = [
            FILEMANAGER, FTPMANAGER, HDFSMANAGER, SFTPMANAGER
    ]

    /** Fix start process */
    ProcessTime startProcess(String name) {
        return (langOpts.processTimeTracing)?new ProcessTime(name: name):null
    }

    /** Fix finish process */
    static void finishProcess(ProcessTime pt, Long countRow = null) {
        if (pt != null) pt.finish(countRow)
    }

    /** GETL DSL options */
    void profile(String name,
                 @DelegatesTo(ProfileSpec)
                 @ClosureParams(value = SimpleType, options = ['getl.lang.opts.ProfileSpec']) Closure cl) {
        def stat = new ProfileSpec(profileName: name)
        stat.startProfile()
        runClosure(stat, cl)
        stat.finishProfile()
    }


    /** GETL DSL options */
    private final def _langOpts = new LangSpec()

    /** GETL DSL options */
    protected LangSpec getLangOpts() { _langOpts }
    /** GETL DSL options */
    protected setLangOpts(LangSpec value) {
        _langOpts.params.clear()
        _langOpts.params.putAll(value.params)
    }

    /** list of executed script classes and call parameters */
    protected SynchronizeObject getExecutedClasses() { _params.executedClasses as SynchronizeObject }

    /** Specified filter when searching for objects */
    private String _filteringGroup
    /** Specified filter when searching for objects */
    @Synchronized
    String getFilteringGroup() { _filteringGroup }

    /** Use the specified filter by group name when searching for objects */
    @Synchronized
    void forGroup(String group) {
        if (group == null || group.trim().length() == 0)
            throw new ExceptionGETL('Filter group required!')
        if (Thread.currentThread() instanceof ExecutorThread)
            throw new ExceptionGETL('Using group filtering within a threads is not allowed!')

        _filteringGroup = group.trim().toLowerCase()
    }

    /** Reset filter to search for objects */
    @Synchronized
    void clearGroupFilter() { _filteringGroup = null }

    /** Repository object name */
    String repObjectName(String name) {
        def names = new ParseObjectName(name)
        if (filteringGroup != null && names.groupName == null)
            names.groupName = filteringGroup

        return names.name
    }

    /** Class for parsing object name*/
    class ParseObjectName {
        ParseObjectName() {
            super()
        }

        ParseObjectName(String repName) {
            super()
            setName(repName)
        }

        ParseObjectName(String groupName, objectName) {
            super()
            setGroupName(groupName)
            setObjectName(objectName)
        }

        /** Name object in repository */
        String _name
        /** Name object in repository */
        String getName() { _name }
        /** Name object in repository */
        void setName(String value) {
            if (value == null) {
                _name = null
                _groupName = null
                _objectName = null
                return
            }

            value = value.trim().toLowerCase()
            if (value.length() == 0)
                throw new ExceptionGETL('The naming value cannot be empty!')

            def i = value.indexOf(':')
            if (i > -1) {
                if (i == 0)
                    throw new ExceptionGETL("Invalid name \"$value\"")

                _groupName = value.substring(0, i)
                if (i < value.length() - 1)
                    _objectName = value.substring(i + 1)
                else
                    _objectName = null
            }
            else {
                _groupName = null
                _objectName = value
            }

            _name = value
        }

        /** Group name */
        String _groupName
        /** Group name */
        String getGroupName() { _groupName }
        /** Group name */
        void setGroupName(String value) {
            value = value?.trim().toLowerCase()
            if (value != null && value.length() == 0)
                throw new ExceptionGETL('The group naming value cannot be empty!')

            if (value == null) {
                _name = _objectName
            }
            else if (_objectName != null) {
                _name = value + ':' + _objectName
            }
            else {
                _name = null
            }
            _groupName = value
        }

        /** Object name */
        String _objectName
        /** Object name */
        String getObjectName() { _objectName }
        /** Object name */
        void setObjectName(String value) {
            value = value?.trim().toLowerCase()
            if (value != null && value.length() == 0)
                throw new ExceptionGETL('The object naming value cannot be empty!')

            if (value == null) {
                _name = null
            }
            else if (_groupName != null) {
                _name = _groupName + ':' + value
            }
            else {
                _name = value
            }
            _objectName = value
        }
    }

    /** Parsing the name of an object from the repository into a group and the name itself */
    ParseObjectName parseName(String name) {
        return new ParseObjectName(name)
    }

    /** Object link to connections repository */
    private Map<String, Connection> _connections

    /** Connections repository */
    protected Map<String, Connection> getConnections() { _connections }

    /**
     * Return list of repository connections for specified mask, classes and filter
     * @param mask filter mask (use Path expression syntax)
     * @param connectionClasses connection class list
     * @param filter object filtering code
     * @return list of connection names according to specified conditions
     */
    @Synchronized
    List<String> listConnections(String mask = null, List connectionClasses = null,
                                 @ClosureParams(value = SimpleType, options = ['java.lang.String', 'getl.data.Connection'])
                                                   Closure<Boolean> filter = null) {
        (connectionClasses as List<String>)?.each {
            if (!(it in listConnectionClasses))
                throw new ExceptionGETL("\"$it\" is not connection class!")
        }

        def res = [] as List<String>
        def classList = connectionClasses as List<String>

        def masknames = parseName(mask)
        def maskgroup = masknames.groupName?:filteringGroup
        def maskobject = masknames.objectName
        def path = (maskobject != null)?new Path(mask: maskobject):null
        def names = new ParseObjectName()

        connections.each { name, obj ->
            names.name = name
            if (maskgroup == null || maskgroup == names.groupName) {
                if (path == null || path.match(names.objectName)) {
                    if (classList == null || obj.getClass().name in classList) {
                        if (filter == null || BoolUtils.IsValue(filter.call(name, obj))) res << name
                    }
                }
            }
        }

        return res
    }

    /**
     * Return list of repository connections for specified classes
     * @param connectionClasses connection class list
     * @param filter object filtering code
     * @return list of connection names according to specified conditions
     */
    List<String> listConnections(List connectionClasses,
                                 @ClosureParams(value = SimpleType, options = ['java.lang.String', 'getl.data.Connection'])
                                        Closure<Boolean> filter = null) {
        listConnections(null, connectionClasses, filter)
    }

    /**
     * Return list of repository connections for specified filter
     * @param filter object filtering code
     * @return list of connection names according to specified conditions
     */
    List<String> listConnections(@ClosureParams(value = SimpleType, options = ['java.lang.String', 'getl.data.Connection'])
                                        Closure<Boolean> filter) {
        listConnections(null, null, filter)
    }

    /**
     * Process repository connections for specified mask and class
     * @param mask filter mask (use Path expression syntax)
     * @param connectionClasses connection class list
     * @param cl processing code
     */
    void processConnections(String mask, List connectionClasses,
                            @ClosureParams(value = SimpleType, options = ['java.lang.String']) Closure cl) {
        if (cl == null)
            throw new ExceptionGETL('Process required closure code!')

        def code = PrepareClosure(childOwnerObject, childThisObject, cl.delegate, cl)
        def list = listConnections(mask, connectionClasses)
        list.each { name ->
            code.call(name)
        }
    }

    /**
     * Process repository connections for specified mask
     * @param mask filter mask (use Path expression syntax)
     * @param cl processing code
     */
    void processConnections(String mask,
                            @ClosureParams(value = SimpleType, options = ['java.lang.String']) Closure cl) {
        processConnections(mask, null, cl)
    }

    /**
     * Process repository connections for specified class
     * @param connectionClasses connection class list
     * @param cl processing code
     */
    void processConnections(List connectionClasses,
                            @ClosureParams(value = SimpleType, options = ['java.lang.String']) Closure cl) {
        processConnections(null, connectionClasses, cl)
    }

    /**
     * Process all repository connections
     * @param cl processing code
     */
    void processConnections(@ClosureParams(value = SimpleType, options = ['java.lang.String']) Closure cl) {
        processConnections(null, null, cl)
    }

    /**
     * Return list of repository jdbc connections for specified mask and filter
     * @param mask filter mask (use Path expression syntax)
     * @param filter object filtering code
     * @return list of jdbc connection names according to specified conditions
     */
    List<String> jdbcConnectionList(String mask = null, Closure<Boolean> filter = null) {
        return listConnections(mask, listJdbcConectionClasses, filter)
    }

    /**
     * Process repository jdbc connections for specified mask
     * @param mask filter mask (use Path expression syntax)
     * @param cl processing code
     */
    void jdbcConnectionProcess(String mask,
                           @ClosureParams(value = SimpleType, options = ['java.lang.String']) Closure cl) {
        processConnections(mask, listJdbcConectionClasses, cl)
    }

    /**
     * Process repository all jdbc connections
     * @param cl processing code
     */
    void jdbcConnectionProcess(@ClosureParams(value = SimpleType, options = ['java.lang.String']) Closure cl) {
        jdbcConnectionProcess(null, cl)
    }

    /** Register connection in repository */
    @Synchronized
    protected Connection registerConnection(String connectionClassName, String name, Boolean registration = false) {
        registration = BoolUtils.IsValue(registration)

        if (connectionClassName ==  null && registration)
            throw new ExceptionGETL('Connection class name cannot be null!')

        if (connectionClassName != null && !(connectionClassName in listConnectionClasses))
            throw new ExceptionGETL("$connectionClassName is not connection class!")

        if (name == null) {
            def c = Connection.CreateConnection(connection: connectionClassName) as Connection
            c.sysParams.dslThisObject = childThisObject
            c.sysParams.dslOwnerObject = childOwnerObject
            return c
        }

        def repName = repObjectName(name)
        def obj = connections.get(repName)

        if (obj == null) {
            if ((!registration && langOpts.validRegisterObjects) || Thread.currentThread() instanceof ExecutorThread)
                throw new ExceptionGETL("Connection \"$name\" is not registered!")

            obj = Connection.CreateConnection(connection: connectionClassName) as Connection
            obj.sysParams.dslThisObject = childThisObject
            obj.sysParams.dslOwnerObject = childOwnerObject
            connections.put(repName, obj)
        }
        else {
            if (registration)
                throw new ExceptionGETL("Connection \"$name\" already registered for class \"${obj.getClass().name}\"!")
            else {
                if (connectionClassName != null && obj.getClass().name != connectionClassName)
                    throw new ExceptionGETL("The requested connection \"$name\" of the class \"$connectionClassName\" is already registered for the class \"${obj.getClass().name}\"!")
            }
        }

        if (langOpts.useThreadModelConnection && Thread.currentThread() instanceof ExecutorThread) {
            def thread = Thread.currentThread() as ExecutorThread
            obj = thread.registerCloneObject('connections', obj,
                    {
                        def c = (it as Connection).cloneConnection()
                        c.sysParams.dslThisObject = childThisObject
                        c.sysParams.dslOwnerObject = childOwnerObject
                        return c
                    }
            )
        }

        return obj
    }

    /**
     * Register connection object in repository
     * @param obj object for registration
     * @param name name object in repository
     * @param validExist checking if an object is registered in the repository (default true)
     */
    @Synchronized
    Connection registerConnectionObject(Connection obj, String name = null, Boolean validExist = true) {
        if (obj == null) throw new ExceptionGETL("Connection object cannot be null!")

        def className = obj.getClass().name
        if (!(className in listConnectionClasses))
            throw new ExceptionGETL("Unknown connection class $className!")

        if (name == null) {
            obj.sysParams.dslThisObject = childThisObject
            obj.sysParams.dslOwnerObject = childOwnerObject
            return obj
        }

        validExist = BoolUtils.IsValue(validExist, true)
        def repName = repObjectName(name)

        if (validExist) {
            def exObj = connections.get(repName)
            if (exObj != null)
                throw new ExceptionGETL("Connection \"$name\" already registered for class \"${exObj.getClass().name}\"!")
        }

        obj.sysParams.dslThisObject = childThisObject
        obj.sysParams.dslOwnerObject = childOwnerObject

        connections.put(repName, obj)

        return obj
    }

    /**
     * Unregister connections by a given mask or a list of their classes
     * @param mask mask of objects (in Path format)
     * @param connectionClasses list of processed connection classes
     * @param filter filter for detect connections to unregister
     */
    @Synchronized
    void unregisterConnection(String mask = null, List connectionClasses = null,
                              @ClosureParams(value = SimpleType, options = ['java.lang.String', 'getl.data.Connection'])
                                      Closure<Boolean> filter = null) {
        def list = listConnections(mask, connectionClasses, filter)
        list.each { name ->
            connections.remove(name)
        }
    }

    /** Tables repository link object */
    private Map<String, Dataset> _datasets

    /** Tables repository */
    protected Map<String, Dataset> getDatasets() { _datasets }

    /**
     * Return list of repository datasets for specified mask, class and filter
     * @param mask filter mask (use Path expression syntax)
     * @param datasetClasses dataset class list
     * @param filter object filtering code
     * @return list of dataset names according to specified conditions
     */
    @Synchronized
    List<String> listDatasets(String mask = null, List datasetClasses = null,
                              @ClosureParams(value = SimpleType, options = ['java.lang.String', 'getl.data.Dataset'])
                                             Closure<Boolean> filter = null) {
        (datasetClasses as List<String>)?.each {
            if (!(it in listDatasetClasses))
                throw new ExceptionGETL("\"$it\" is not dataset class!")
        }

        def res = [] as List<String>
        def classList = datasetClasses as List<String>

        def masknames = parseName(mask)
        def maskgroup = masknames.groupName?:filteringGroup
        def maskobject = masknames.objectName
        def path = (maskobject != null)?new Path(mask: maskobject):null
        def names = new ParseObjectName()

        datasets.each { name, obj ->
            names.name = name
            if (maskgroup == null || maskgroup == names.groupName) {
                if (path == null || path.match(names.objectName)) {
                    if (classList == null || obj.getClass().name in classList) {
                        if (filter == null || BoolUtils.IsValue(filter.call(name, obj))) res << name
                    }
                }
            }
        }

        return res
    }

    /**
     * Return list of repository datasets for specified class and filter
     * @param datasetClasses dataset class list
     * @param filter object filtering code
     * @return list of dataset names according to specified conditions
     */
    List<String> listDatasets(List datasetClasses,
                              @ClosureParams(value = SimpleType, options = ['java.lang.String', 'getl.data.Dataset'])
                                     Closure<Boolean> filter = null) {
        listDatasets(null, datasetClasses, filter)
    }

    /**
     * Return list of repository datasets for specified filter
     * @param filter object filtering code
     * @return list of dataset names according to specified conditions
     */
    List<String> listDatasets(@ClosureParams(value = SimpleType, options = ['java.lang.String', 'getl.data.Dataset'])
                                     Closure<Boolean> filter) {
        listDatasets(null, null, filter)
    }

    /**
     * Process repository datasets for specified mask and class
     * @param mask filter mask (use Path expression syntax)
     * @param datasetClasses dataset class list
     * @param cl processing code
     */
    void processDatasets(String mask, List datasetClasses,
                         @ClosureParams(value = SimpleType, options = ['java.lang.String']) Closure cl) {
        if (cl == null)
            throw new ExceptionGETL('Process required closure code!')

        def code = PrepareClosure(childOwnerObject, childThisObject, cl.delegate, cl)
        def list = listDatasets(mask, datasetClasses)
        list.each { name ->
            code.call(name)
        }
    }

    /**
     * Process repository datasets for specified mask
     * @param mask filter mask (use Path expression syntax)
     * @param cl processing code
     */
    void processDatasets(String mask,
                         @ClosureParams(value = SimpleType, options = ['java.lang.String']) Closure cl) {
        processDatasets(mask, null, cl)
    }

    /**
     * Process repository datasets for specified mask
     * @param datasetClasses dataset class list
     * @param cl processing code
     */
    void processDatasets(List datasetClasses,
                         @ClosureParams(value = SimpleType, options = ['java.lang.String']) Closure cl) {
        processDatasets(null, datasetClasses, cl)
    }

    /**
     * Process all repository datasets
     * @param cl processing code
     */
    void processDatasets(@ClosureParams(value = SimpleType, options = ['java.lang.String']) Closure cl) {
        processDatasets(null, null, cl)
    }

    /**
     * Return a list of objects associated with the names of two groups
     * @param sourceList List of name source objects
     * @param destGroup List of name destination objects
     * @param filter filtering objects
     * @return list of names of related objects
     */
    List<ExecutorListElement> linkDatasets(List sourceList, List destList,
                                           @ClosureParams(value = SimpleType, options = ['java.lang.String'])
                                               Closure<Boolean> filter = null) {
        if (sourceList == null) throw new ExceptionGETL('Required to specify the value of the source group name!')
        if (destList == null) throw new ExceptionGETL('Required to specify the value of the destination group name!')

        def parse = new ParseObjectName()

        def sourceTables = [:] as Map<String, String>
        sourceList.each { name ->
            parse.name = name as String
            sourceTables.put(parse.objectName, name)
        }

        def destTables = [:] as Map<String, String>
        destList.each { name ->
            parse.name = name as String
            destTables.put(parse.objectName, name)
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

    /**
     * Return a list of objects associated with the names of two groups
     * @param sourceGroup name of the group of source objects
     * @param destGroup name of the group of destination objects
     * @param filter filtering objects
     * @return list of names of related objects
     */
    List<ExecutorListElement> linkDatasets(String sourceGroup, String destGroup,
                                           @ClosureParams(value = SimpleType, options = ['java.lang.String'])
                                                   Closure<Boolean> filter = null) {
        if (sourceGroup == null) throw new ExceptionGETL('Required to specify the value of the source group name!')
        if (destGroup == null) throw new ExceptionGETL('Required to specify the value of the destination group name!')

        return linkDatasets(listDatasets(sourceGroup + ':'), listDatasets(destGroup + ':'), filter)
    }

    /**
     * Return list of repository jdbc tables for specified mask and filter
     * @param mask filter mask (use Path expression syntax)
     * @param filter object filtering code
     * @return list of jdbc table names according to specified conditions
     */
    List<String> jdbcTableList(String mask = null, Closure<Boolean> filter = null) {
        return listDatasets(mask, listJdbcTableClasses, filter)
    }

    /**
     * Return list of repository jdbc tables for specified filter
     * @param filter object filtering code
     * @return list of jdbc table names according to specified conditions
     */
    List<String> jdbcTableList(Closure<Boolean> filter) {
        return listDatasets(null, listJdbcTableClasses, filter)
    }

    /**
     * Process repository tables for specified mask
     * @param mask filter mask (use Path expression syntax)
     * @param cl processing code
     */
    void jdbcTableProcess(String mask,
                          @ClosureParams(value = SimpleType, options = ['java.lang.String']) Closure cl) {
        if (cl == null)
            throw new ExceptionGETL('Process required closure code!')

        def code = PrepareClosure(childOwnerObject, childThisObject, cl.delegate, cl)
        def list = jdbcTableList(mask)
        list.each { name ->
            code.call(name)
        }
    }

    /**
     * Process all repository tables
     * @param cl processing code
     */
    void jdbcTableProcess(@ClosureParams(value = SimpleType, options = ['java.lang.String']) Closure cl) {
        jdbcTableProcess(null, cl)
    }

    /** Set default connection for use in datasets */
    @Synchronized
    protected void setDefaultConnection(String datasetClassName, Dataset ds) {
        if (datasetClassName ==  null)
            throw new ExceptionGETL('Dataset class name cannot be null!')
        if (!(datasetClassName in listDatasetClasses))
            throw new ExceptionGETL("$datasetClassName is not dataset class!")

        if (ds instanceof JDBCDataset) {
            def con = defaultJdbcConnection(datasetClassName)
            if (con != null) ds.connection = con
        }
        else if (ds instanceof FileDataset) {
            def con = defaultFileConnection(datasetClassName)
            if (con != null) ds.connection = con
        }
        else {
            def con = defaultOtherConnection(datasetClassName)
            if (con != null) ds.connection = con
        }
    }

    /** Last used JDBC default connection */
    private JDBCConnection _lastJDBCDefaultConnection

    /** Last used JDBC default connection */
    JDBCConnection getLastJdbcDefaultConnection() { _lastJDBCDefaultConnection }

    // /** Default JDBC connection for datasets */
    private def _defaultJDBCConnection = new ConcurrentHashMap<String, JDBCConnection>()

    /** Default JDBC connection for datasets */
    JDBCConnection defaultJdbcConnection(String datasetClassName = null) {
        JDBCConnection res
        if (datasetClassName == null)
            res = lastJdbcDefaultConnection
        else {
            if (!(datasetClassName in listDatasetClasses))
                throw new ExceptionGETL("$datasetClassName is not dataset class!")

            res = _defaultJDBCConnection.get(datasetClassName)
            if (res == null && lastJdbcDefaultConnection != null && datasetClassName == QUERYDATASET)
                res = lastJdbcDefaultConnection
        }

        return res
    }

    /** Use specified JDBC connection as default */
    JDBCConnection useJdbcConnection(String datasetClassName, JDBCConnection value) {
        if (Thread.currentThread() instanceof ExecutorThread)
            throw new ExceptionGETL('Specifying the default connection is not allowed in streams!')

        if (datasetClassName != null) {
            if (!(datasetClassName in listDatasetClasses))
                throw new ExceptionGETL("$datasetClassName is not dataset class!")
            _defaultJDBCConnection.put(datasetClassName, value)
        }
        _lastJDBCDefaultConnection = value

        return value
    }

    /** Last used file default connection */
    private FileConnection _lastFileDefaultConnection

    /** Last used file default connection */
    FileConnection getLastFileDefaultConnection() { _lastFileDefaultConnection }

    private def _defaultFileConnection = new ConcurrentHashMap<String, FileConnection>()

    /** Default file connection for datasets */
    FileConnection defaultFileConnection(String datasetClassName = null) {
        FileConnection res
        if (datasetClassName == null)
            res = lastFileDefaultConnection
        else {
            if (!(datasetClassName in listDatasetClasses))
                throw new ExceptionGETL("$datasetClassName is not dataset class!")

            res = _defaultFileConnection.get(datasetClassName)
        }

        return res
    }

    /** Use specified file connection as default */
    FileConnection useFileConnection(String datasetClassName, FileConnection value) {
        if (Thread.currentThread() instanceof ExecutorThread)
            throw new ExceptionGETL('Specifying the default connection is not allowed in streams!')

        if (datasetClassName != null) {
            if (!(datasetClassName in listDatasetClasses))
                throw new ExceptionGETL("$datasetClassName is not dataset class!")

            _defaultFileConnection.put(datasetClassName, value)
        }
        _lastFileDefaultConnection = value

        return value
    }

    /** Last used other type default connection */
    private Connection _lastOtherDefaultConnection

    /** Last used other type default connection */
    Connection getLastOtherDefaultConnection() { _lastOtherDefaultConnection }

    private def _defaultOtherConnection = new ConcurrentHashMap<String, Connection>()

    /** Default other type connection for datasets */
    Connection defaultOtherConnection(String datasetClassName = null) {
        Connection res
        if (datasetClassName == null)
            res = lastOtherDefaultConnection
        else {
            if (!(datasetClassName in listDatasetClasses))
                throw new ExceptionGETL("$datasetClassName is not dataset class!")

            res = _defaultOtherConnection.get(datasetClassName)
        }

        return res
    }

    /** Use specified other type connection as default */
    Connection useOtherConnection(String datasetClassName, Connection value) {
        if (Thread.currentThread() instanceof ExecutorThread)
            throw new ExceptionGETL('Specifying the default connection is not allowed in streams!')

        if (datasetClassName != null) {
            if (!(datasetClassName in listDatasetClasses))
                throw new ExceptionGETL("$datasetClassName is not dataset class!")

            _defaultOtherConnection.put(datasetClassName, value)
        }
        _lastOtherDefaultConnection = value

        return value
    }

    /** Register dataset in repository */
    @Synchronized
    protected Dataset registerDataset(Connection connection, String datasetClassName, String name, Boolean registration = false) {
        registration = BoolUtils.IsValue(registration)

        if (registration && datasetClassName ==  null)
            throw new ExceptionGETL('Dataset class name cannot be null!')
        if (datasetClassName != null && !(datasetClassName in listDatasetClasses))
            throw new ExceptionGETL("$datasetClassName is not dataset class!")

        Dataset obj
        if (name == null) {
            obj = Dataset.CreateDataset(dataset: datasetClassName) as Dataset
            if (connection != null)
                obj.connection = connection
            else
                setDefaultConnection(datasetClassName, obj)

            obj.sysParams.dslThisObject = childThisObject
            obj.sysParams.dslOwnerObject = childOwnerObject

            if (obj.connection != null && langOpts.useThreadModelConnection && Thread.currentThread() instanceof ExecutorThread) {
                def thread = Thread.currentThread() as ExecutorThread
                obj.connection = thread.registerCloneObject('connections', obj.connection,
                        {
                            def c = (it as Connection).cloneConnection()
                            if (connection == null) {
                                c.sysParams.dslThisObject = childThisObject
                                c.sysParams.dslOwnerObject = childOwnerObject
                            }
                            return c
                        }
                ) as Connection
            }

            return obj
        }

        def repName = repObjectName(name)
        obj = datasets.get(repName)

        if (obj == null) {
            if ((!registration && langOpts.validRegisterObjects) || Thread.currentThread() instanceof ExecutorThread)
                throw new ExceptionGETL("Dataset \"$name\" is not registered!")

            obj = Dataset.CreateDataset(dataset: datasetClassName) as Dataset
            obj.sysParams.dslThisObject = childThisObject
            obj.sysParams.dslOwnerObject = childOwnerObject

            if (connection != null) {
                obj.connection = connection
            }
            else {
                setDefaultConnection(datasetClassName, obj)
            }
            datasets.put(repName, obj)
        }
        else {
            if (registration)
                throw new ExceptionGETL("Dataset \"$name\" already registered for class \"${obj.getClass().name}\"!")
            else {
                if (datasetClassName != null && obj.getClass().name != datasetClassName)
                    throw new ExceptionGETL("The requested dataset \"$name\" of the class \"$datasetClassName\" is already registered for the class \"${obj.getClass().name}\"!")
            }
        }

        if (langOpts.useThreadModelConnection && Thread.currentThread() instanceof ExecutorThread) {
            def thread = Thread.currentThread() as ExecutorThread
            if (obj.connection != null) {
                def cloneConnection = thread.registerCloneObject('connections', obj.connection,
                        {
                            def c = (it as Connection).cloneConnection()
                            if (connection == null) {
                                c.sysParams.dslThisObject = childThisObject
                                c.sysParams.dslOwnerObject = childOwnerObject
                            }
                            return c
                        }
                ) as Connection

                obj = thread.registerCloneObject('datasets', obj,
                        {
                            def d = (it as Dataset).cloneDataset(cloneConnection)
                            d.sysParams.dslThisObject = childThisObject
                            d.sysParams.dslOwnerObject = childOwnerObject
                            return d
                        }
                ) as Dataset
            }
            else {
                obj = thread.registerCloneObject('datasets', obj,
                        {
                            def d = (it as Dataset).cloneDataset()
                            d.sysParams.dslThisObject = childThisObject
                            d.sysParams.dslOwnerObject = childOwnerObject
                            return d
                        }
                ) as Dataset
            }
        }

        return obj
    }

    /** Register dataset in repository */
    protected Dataset registerDataset(String datasetClassName, String name, Boolean registration = false) {
        registerDataset(null, datasetClassName, name, registration)
    }

    /**
     * Register dataset in repository
     * @param obj object for registration
     * @param name name object in repository
     * @param validExist checking if an object is registered in the repository (default true)
     */
    @Synchronized
    Dataset registerDatasetObject(Dataset obj, String name = null, Boolean validExist = true) {
        if (obj == null) throw new ExceptionGETL("Dataset object cannot be null!")

        def className = obj.getClass().name
        if (!(className in listDatasetClasses))
            throw new ExceptionGETL("Unknown dataset class $className!")

        if (name == null) {
            obj.sysParams.dslThisObject = childThisObject
            obj.sysParams.dslOwnerObject = childOwnerObject
            return obj
        }

        validExist = BoolUtils.IsValue(validExist, true)
        def repName = repObjectName(name)

        if (validExist) {
            def exObj = datasets.get(repName)
            if (exObj != null)
                throw new ExceptionGETL("Dataset \"$name\" already registered for class \"${exObj.getClass().name}\"!")
        }

        obj.sysParams.dslThisObject = childThisObject
        obj.sysParams.dslOwnerObject = childOwnerObject

        datasets.put(repName, obj)

        return obj
    }

    /**
     * Remove dataset from repository
     * @param mask filter mask (use Path expression syntax)
     * @param datasetClasses dataset class list
     * @param filter object filtering code
     */
    @Synchronized
    void unregisterDataset(String mask = null, List datasetClasses = null,
                              @ClosureParams(value = SimpleType, options = ['java.lang.String', 'getl.data.Dataset'])
                                      Closure<Boolean> filter = null) {
        def list = listDatasets(mask, datasetClasses, filter)
        list.each { name ->
            datasets.remove(name)
        }
    }

    /** History points repository link object */
    private Map<String, SavePointManager> _historypoints

    /** History points repository */
    protected Map<String, SavePointManager> getHistoryPoints() { _historypoints }

    /**
     * Return list of repository history point manager
     * @param mask filter mask (use Path expression syntax)
     * @param filter object filtering code
     * @return list of history point manager names according to specified conditions
     */
    @Synchronized
    List<String> listHistorypoints(String mask = null, Closure<Boolean> filter = null) {
        def res = [] as List<String>

        def masknames = parseName(mask)
        def maskgroup = masknames.groupName?:filteringGroup
        def maskobject = masknames.objectName
        def path = (maskobject != null)?new Path(mask: maskobject):null
        def names = new ParseObjectName()

        historyPoints.each { String name, SavePointManager obj ->
            names.name = name
            if (maskgroup == null || maskgroup == names.groupName) {
                if (path == null || path.match(names.objectName)) {
                    if (filter == null || BoolUtils.IsValue(filter.call(name, obj))) res << name
                }
            }
        }

        return res
    }

    /**
     * Return list of repository history point manager
     * @param filter object filtering code
     * @return list of history point manager names according to specified conditions
     */
    List<String> listHistorypoints(Closure<Boolean> filter) {
        listHistorypoints(null, filter)
    }

    /**
     * Process repository history point managers for specified mask and filter
     * @param mask filter mask (use Path expression syntax)
     * @param filter object filtering code
     */
    void processHistorypoints(String mask,
                              @ClosureParams(value = SimpleType, options = ['java.lang.String']) Closure cl) {
        if (cl == null)
            throw new ExceptionGETL('Process required closure code!')

        def code = PrepareClosure(childOwnerObject, childThisObject, cl.delegate, cl)
        listHistorypoints(mask).each { name ->
            code.call(name)
        }
    }

    /** Process all repository history point managers */
    void processHistorypoints(@ClosureParams(value = SimpleType, options = ['java.lang.String']) Closure cl) {
        processHistorypoints(null, cl)
    }

    /**
     * Register history point in repository
     * @param name name in repository
     * @registration registering
     */
    @Synchronized
    protected SavePointManager registerHistoryPoint(String name, Boolean registration = false) {
        SavePointManager obj
        if (name == null) {
            obj = new SavePointManager()
            obj.sysParams.dslThisObject = childThisObject
            obj.sysParams.dslOwnerObject = childOwnerObject
            if (lastJdbcDefaultConnection != null) obj.connection = lastJdbcDefaultConnection
            if (obj.connection != null && langOpts.useThreadModelConnection && Thread.currentThread() instanceof ExecutorThread) {
                def thread = Thread.currentThread() as ExecutorThread
                obj.connection = thread.registerCloneObject('connections', obj.connection,
                        {
                            def p = (it as Connection).cloneConnection()
                            p.sysParams.dslThisObject = childThisObject
                            p.sysParams.dslOwnerObject = childOwnerObject
                            return p
                        }
                ) as JDBCConnection
            }
        }
        else {
            def repName = repObjectName(name)
            obj = historyPoints.get(repName)
            if (obj == null) {
                if ((!BoolUtils.IsValue(registration) && langOpts.validRegisterObjects) || Thread.currentThread() instanceof ExecutorThread)
                    throw new ExceptionGETL("History point \"$name\" is not exist!")

                obj = new SavePointManager()
                obj.sysParams.dslThisObject = childThisObject
                obj.sysParams.dslOwnerObject = childOwnerObject
                if (lastJdbcDefaultConnection != null) obj.connection = lastJdbcDefaultConnection
                historyPoints.put(repName, obj)
            }

            if (langOpts.useThreadModelConnection && Thread.currentThread() instanceof ExecutorThread) {
                def thread = Thread.currentThread() as ExecutorThread
                if (obj.connection != null) {
                    def cloneConnection = thread.registerCloneObject('connections', obj.connection,
                            {
                                def c = (it as Connection).cloneConnection()
                                c.sysParams.dslThisObject = childThisObject
                                c.sysParams.dslOwnerObject = childOwnerObject
                                return c
                            }
                    ) as JDBCConnection
                    obj = thread.registerCloneObject('historypoints', obj,
                            {
                                def p = (it as SavePointManager).cloneSavePointManager(cloneConnection)
                                p.sysParams.dslThisObject = childThisObject
                                p.sysParams.dslOwnerObject = childOwnerObject
                                return p
                            }
                    ) as SavePointManager
                }
                else {
                    obj = thread.registerCloneObject('historypoints', obj,
                            {
                                def p = (it as SavePointManager).cloneSavePointManager()
                                p.sysParams.dslThisObject = childThisObject
                                p.sysParams.dslOwnerObject = childOwnerObject
                                return p
                            }
                    ) as SavePointManager
                }
            }
        }

        return obj
    }

    /**
     * Register history point in repository
     * @param obj object for registration
     * @param name name object in repository
     * @param validExist checking if an object is registered in the repository (default true)
     */
    @Synchronized
    SavePointManager registerHistoryPointObject(SavePointManager obj, String name = null, Boolean validExist = true) {
        if (obj == null) throw new ExceptionGETL("History point manager object cannot be null!")

        if (name == null) {
            obj.sysParams.dslThisObject = childThisObject
            obj.sysParams.dslOwnerObject = childOwnerObject
            return obj
        }

        validExist = BoolUtils.IsValue(validExist, true)
        def repName = repObjectName(name)

        if (validExist) {
            def exObj = historyPoints.get(repName)
            if (exObj != null)
                throw new ExceptionGETL("History point manager \"$name\" already registered!")
        }

        obj.sysParams.dslThisObject = childThisObject
        obj.sysParams.dslOwnerObject = childOwnerObject

        historyPoints.put(repName, obj)

        return obj
    }

    /**
     * Unregister history point manager
     * @param mask filter mask (use Path expression syntax)
     * @param filter object filtering code
     */
    @Synchronized
    void unregisterHistoryPoint(String mask = null,
                                @ClosureParams(value = SimpleType, options = ['java.lang.String', 'getl.jdbc.SavePointManager'])
                                        Closure<Boolean> filter = null) {
        def list = listHistorypoints(mask, filter)
        list.each { name ->
            historyPoints.remove(name)
        }
    }

    /** File managers repository link object */
    private Map<String, Manager> _filemanagers

    /** File managers repository */
    protected Map<String, Manager> getFileManagers() { _filemanagers }

    /**
     * Return list of repository file managers for specified mask, class and filter
     * @param mask filter mask (use Path expression syntax)
     * @param filemanagerClasses file manager class list
     * @param filter object filtering code
     * @return list of file manager names according to specified conditions
     */
    @Synchronized
    List<String> listFilemanagers(String mask = null, List filemanagerClasses = null,
                                  @ClosureParams(value = SimpleType, options = ['java.lang.String', 'getl.files.Manager'])
                                                 Closure<Boolean> filter = null) {
        (filemanagerClasses as List<String>)?.each {
            if (!(it in listFileManagerClasses))
                throw new ExceptionGETL("\"$it\" is not file manager class!")
        }

        def res = [] as List<String>
        def classList = filemanagerClasses as List<String>

        def masknames = parseName(mask)
        def maskgroup = masknames.groupName?:filteringGroup
        def maskobject = masknames.objectName
        def path = (maskobject != null)?new Path(mask: maskobject):null
        def names = new ParseObjectName()

        fileManagers.each { String name, Manager obj ->
            names.name = name
            if (maskgroup == null || maskgroup == names.groupName) {
                if (path == null || path.match(names.objectName)) {
                    if (classList == null || obj.getClass().name in classList) {
                        if (filter == null || BoolUtils.IsValue(filter.call(name, obj))) res << name
                    }
                }
            }
        }

        return res
    }

    /**
     * Return list of repository file managers for specified class
     * @param filemanagerClasses file manager class list
     * @param filter object filtering code
     * @return list of file manager names according to specified conditions
     */
    List<String> listFilemanagers(List filemanagerClasses,
                                  @ClosureParams(value = SimpleType, options = ['java.lang.String', 'getl.files.Manager'])
                                         Closure<Boolean> filter = null) {
        listFilemanagers(null, filemanagerList, filter)
    }

    /**
     * Return list of repository file managers for specified class
     * @param filter object filtering code
     * @return list of file manager names according to specified conditions
     */
    List<String> listFilemanagers(@ClosureParams(value = SimpleType, options = ['java.lang.String', 'getl.files.Manager'])
                                          Closure<Boolean> filter) {
        listFilemanagers(null, null, filter)
    }

    /**
     * Process repository file managers for specified mask and classes
     * @param mask filter mask (use Path expression syntax)
     * @param filemanagerClasses file manager class list
     * @param cl processing code
     */
    void processFilemanagers(String mask, List filemanagerClasses,
                             @ClosureParams(value = SimpleType, options = ['java.lang.String']) Closure cl) {
        if (cl == null)
            throw new ExceptionGETL('Process required closure code!')

        def code = PrepareClosure(childOwnerObject, childThisObject, cl.delegate, cl)
        listFilemanagers(mask, filemanagerClasses).each { name ->
            code.call(name)
        }
    }

    /**
     * Process repository file managers for specified mask
     * @param mask filter mask (use Path expression syntax)
     * @param cl processing code
     */
    void processFilemanagers(String mask,
                             @ClosureParams(value = SimpleType, options = ['java.lang.String']) Closure cl) {
        processFilemanagers(mask, null, cl)
    }

    /**
     * Process repository file managers for specified classes
     * @param filemanagerClasses file manager class list
     * @param cl processing code
     */
    void processFilemanagers(List filemanagerClasses,
                             @ClosureParams(value = SimpleType, options = ['java.lang.String']) Closure cl) {
        processFilemanagers(null, filemanagerClasses, cl)
    }

    /**
     * Process all repository file managers
     * @param cl processing code
     */
    void processFilemanagers(@ClosureParams(value = SimpleType, options = ['java.lang.String']) Closure  cl) {
        processFilemanagers(null, null, cl)
    }

    /** Register file manager in repository */
    @Synchronized
    protected Manager registerFileManager(String fileManagerClassName, String name, Boolean registration = false) {
        registration = BoolUtils.IsValue(registration)

        if (registration && fileManagerClassName ==  null)
            throw new ExceptionGETL('File manager class name cannot be null!')
        if (fileManagerClassName != null && !(fileManagerClassName in listFileManagerClasses))
            throw new ExceptionGETL("$fileManagerClassName is not file manager class!")

        Manager obj

        if (name == null) {
            obj = FileManager.CreateManager(manager: fileManagerClassName) as Manager
            obj.sysParams.dslThisObject = childThisObject
            obj.sysParams.dslOwnerObject = childOwnerObject
            return obj
        }

        def repName = repObjectName(name)
        obj = fileManagers.get(repName)

        if (obj == null) {
            if ((!registration && langOpts.validRegisterObjects) || Thread.currentThread() instanceof ExecutorThread)
                throw new ExceptionGETL("File manager \"$name\" with class \"$fileManagerClassName\" is not registered!")

            obj = Manager.CreateManager(manager: fileManagerClassName) as Manager
            obj.sysParams.dslThisObject = childThisObject
            obj.sysParams.dslOwnerObject = childOwnerObject
            fileManagers.put(repName, obj)
        }
        else {
            if (registration)
                throw new ExceptionGETL("File manager \"$name\" already registered for class \"${obj.getClass().name}\"!")
            else {
                if (fileManagerClassName != null && obj.getClass().name != fileManagerClassName)
                    throw new ExceptionGETL("The requested file manager \"$name\" of the class \"$fileManagerClassName\" is already registered for the class \"${obj.getClass().name}\"!")
            }
        }

        if (langOpts.useThreadModelConnection && Thread.currentThread() instanceof ExecutorThread) {
            def thread = Thread.currentThread() as ExecutorThread
            obj = thread.registerCloneObject('filemanagers', obj,
                    {
                        def f = (it as FileManager).cloneManager()
                        f.sysParams.dslThisObject = childThisObject
                        f.sysParams.dslOwnerObject = childOwnerObject
                        return f
                    }
            )
        }

        return obj
    }

    /**
     * Register file manager object in repository
     * @param obj object for registration
     * @param name name object in repository
     * @param validExist checking if an object is registered in the repository (default true)
     */
    @Synchronized
    Manager registerFileManagerObject(Manager obj, String name = null, Boolean validExist = true) {
        if (obj == null) throw new ExceptionGETL("File manager object cannot be null!")
        def className = obj.getClass().name
        if (!(className in listFileManagerClasses))
            throw new ExceptionGETL("$className is not file manager class!")

        if (name == null) {
            obj.sysParams.dslThisObject = childThisObject
            obj.sysParams.dslOwnerObject = childOwnerObject
            return obj
        }

        validExist = BoolUtils.IsValue(validExist, true)
        def repName = repObjectName(name)

        if (validExist) {
            def exObj = fileManagers.get(repName)
            if (exObj != null)
                throw new ExceptionGETL("File manager \"$name\" already registered for class \"${exObj.getClass().name}\"!")
        }

        obj.sysParams.dslThisObject = childThisObject
        obj.sysParams.dslOwnerObject = childOwnerObject

        fileManagers.put(repName, obj)

        return obj
    }

    @Synchronized
    void unregisterFileManager(String mask = null, List filemanagerClasses = null,
                           @ClosureParams(value = SimpleType, options = ['java.lang.String', 'getl.files.Manager'])
                                   Closure<Boolean> filter = null) {
        def list = listFilemanagers(mask, filemanagerClasses, filter)
        list.each { name ->
            fileManagers.remove(name)
        }
    }

    /** Load and run groovy script file */
    void runGroovyFile(String fileName, Boolean runOnce, Map vars = [:]) {
        File sourceFile = new File(fileName)
        def groovyClass = new GroovyClassLoader(getClass().getClassLoader()).parseClass(sourceFile)
        runGroovyClass(groovyClass, runOnce, vars)
    }

    /** Load and run groovy script file */
    void runGroovyFile(String fileName, Map vars = [:]) {
        runGroovyFile(fileName, false, vars)
    }

    /** Load and run groovy script file */
    void runGroovyFile(String fileName, Boolean runOnce, Closure vars) {
        File sourceFile = new File(fileName)
        def groovyClass = new GroovyClassLoader(getClass().getClassLoader()).parseClass(sourceFile)
        runGroovyClass(groovyClass, runOnce, vars)
    }

    /** Load and run groovy script file */
    void runGroovyFile(String fileName, Closure vars) {
        runGroovyFile(fileName, false, vars)
    }

    /** Script call arguments */
//    protected final def _scriptArgs = new ConcurrentHashMap<String, Object>()
//    Map<String, Object> getScriptArgs() { _scriptArgs }

    /** Load and run groovy script by class */
    void runGroovyClass(Class groovyClass, Boolean runOnce, Map vars = [:]) {
        def className = groovyClass.name
        def previouslyRun = (executedClasses.indexOfListItem(className) != -1)
        if (previouslyRun && BoolUtils.IsValue(runOnce)) return

        def script = (GroovyObject)groovyClass.newInstance() as Script
        if (script instanceof Getl) {
            def scriptGetl = script as Getl
            scriptGetl.setGetlParams(_params)
            scriptGetl.setLangOpts(langOpts)
            if (vars != null && !vars.isEmpty()) {
//                scriptGetl._scriptArgs.putAll(vars)
                fillFieldFromVars(scriptGetl, vars)
            }
        }
        else if (vars != null && !vars.isEmpty()) {
            script.binding = new Binding(vars)
        }

        def pt = startProcess("Execution groovy class $className")
        script.run()
        pt.finish()

        if (!previouslyRun) executedClasses.addToList(className)
    }

    /** Load and run groovy script by class */
    void runGroovyClass(Class groovyClass, Map vars = [:]) {
        runGroovyClass(groovyClass, false, vars)
    }

    /** Load and run groovy script by class */
    void runGroovyClass(Class groovyClass, Boolean runOnce, Closure vars) {
        def cfg = new groovy.util.ConfigSlurper()
        def cl = PrepareClosure(childOwnerObject, childThisObject, vars, vars)
        def map = cfg.parse(new ClosureScript(closure: cl))
        runGroovyClass(groovyClass, runOnce, map)
    }

    /** Load and run groovy script by class */
    void runGroovyClass(Class groovyClass, Closure vars) {
        runGroovyClass(groovyClass, false, vars)
    }

    /** Run getl script */
    void runGetl(Getl script, Boolean runOnce = false, Map vars = [:]) {
        def className = script.getClass().name
        def previouslyRun = (executedClasses.indexOfListItem(className) != -1)
        if (previouslyRun && BoolUtils.IsValue(runOnce)) return

        script.setGetlParams(_params)
        script.setLangOpts(langOpts)
        if (vars != null && !vars.isEmpty()) {
//            script._scriptArgs.putAll(vars)
            fillFieldFromVars(script, vars)
        }

        def pt = startProcess("Execution groovy class $className")
        script.run()
        pt.finish()

        if (!previouslyRun) executedClasses.addToList(className)
    }

    /** Run getl script */
    void runGetl(Getl script, Map vars) {
        runGetl(script, false, vars)
    }

    /** Run getl script */
    void runGetl(Getl script, Boolean runOnce, Closure vars) {
        def cfg = new groovy.util.ConfigSlurper()
        def cl = PrepareClosure(childOwnerObject, childThisObject, vars, vars)
        def map = cfg.parse(new ClosureScript(closure: cl))
        runGetl(script, runOnce, map)
    }

    /** Run getl script */
    void runGetl(Getl script, Closure vars) {
        runGetl(script, false, vars)
    }

    /** Fill script field property from external arguments */
    static protected void fillFieldFromVars(Getl script, Map vars) {
        vars.each { key, value ->
            MetaProperty prop = script.hasProperty(key)
            if (prop == null)
                throw new ExceptionGETL("Field \"$key\" not defined in script!")

            prop.setProperty(script, value)
        }
    }

    /** Detect delegate object for closure code */
    static Object DetectClosureDelegate(Object obj) {
        while (obj instanceof Closure) obj = (obj as Closure).delegate
        return obj
    }

    /** Preparing closure code for specified object */
    static Closure PrepareClosure(def ownerObject, def thisObject, def parent, Closure cl) {
        if (cl == null) return null
        def code = cl.rehydrate(parent, ownerObject, thisObject)
        code.resolveStrategy = childDelegate
        return code
    }

    /** Run closure with call parent parameter */
    static void RunClosure(Object ownerObject, Object thisObject, Object parent, Closure cl) {
        if (cl == null) return

        def code = cl.rehydrate(parent, ownerObject, thisObject)
        code.resolveStrategy = childDelegate
        code.call(parent)
    }

    /** Run closure with call one parameter */
    static void RunClosure(Object ownerObject, Object thisObject, Object parent, Closure cl, Object param) {
        if (cl == null) return

        def code = cl.rehydrate(parent, ownerObject, thisObject)
        code.resolveStrategy = childDelegate
        code.call(param)
    }

    /** Run closure with call two parameters */
    static void RunClosure(Object ownerObject, Object thisObject, Object parent, Closure cl, Object... params) {
        if (cl == null) return

        def code = cl.rehydrate(parent, ownerObject, thisObject)
        code.resolveStrategy = childDelegate
        code.call(params)
    }

    /** Run closure with call parent parameter */
    protected void runClosure(Object parent, Closure cl) {
        if (cl == null) return

        def code = cl.rehydrate(parent, childOwnerObject, childThisObject)
        code.resolveStrategy = childDelegate
        code.call(parent)
    }

    /** Run closure with call one parameter */
    protected void runClosure(Object parent, Closure cl, Object param) {
        if (cl == null) return

        def code = cl.rehydrate(parent, childOwnerObject, childThisObject)
        code.resolveStrategy = childDelegate
        code.call(param)
    }

    /** Run closure with call two parameters */
    protected void runClosure(Object parent, Closure cl, Object... params) {
        if (cl == null) return

        def code = cl.rehydrate(parent, childOwnerObject, childThisObject)
        code.resolveStrategy = childDelegate
        code.call(params)
    }

    /** Current configuration content */
    static Map<String, Object> getConfigContent() { Config.content }

    /** Current configuration vars */
    static Map<String, Object> getConfigVars() { Config.vars }

    /** Write message as level the INFO to log */
    static void logInfo(def msg) { Logs.Info(msg.toString()) }

    /** Write message as level the WARNING to log */
    static void logWarn(def msg) { Logs.Warning(msg.toString()) }

    /** Write message as level the SEVERE to log */
    static void logError(def msg) { Logs.Severe(msg.toString()) }

    /** Write message as level the FINE to log */
    static void logFine(def msg) { Logs.Fine(msg.toString()) }

    /** Write message as level the FINER to log */
    static void logFiner(def msg) { Logs.Finer(msg.toString()) }

    /** Write message as level the FINEST to log */
    static void logFinest(def msg) { Logs.Finest(msg.toString()) }

    /** Write message as level the CONFIG to log */
    static void logConfig(def msg) { Logs.Config(msg.toString()) }

    /** System temporary directory */
    static String getSystemTempPath() { TFS.systemPath }

    /** GETL DSL options */
    LangSpec options(@DelegatesTo(LangSpec)
                     @ClosureParams(value = SimpleType, options = ['getl.lang.opts.LangSpec']) Closure cl = null) {
        runClosure(langOpts, cl)

        return langOpts as LangSpec
    }

    /** Configuration options */
    ConfigSpec configuration(@DelegatesTo(ConfigSpec)
                             @ClosureParams(value = SimpleType, options = ['getl.lang.opts.ConfigSpec']) Closure cl = null) {
        if (!(Config.configClassManager instanceof ConfigSlurper)) Config.configClassManager = new ConfigSlurper()
        def parent = new ConfigSpec(childOwnerObject, childThisObject, false, null)
        parent.runClosure(cl)

        return parent
    }

    /** Log options */
    LogSpec logging(@DelegatesTo(LogSpec)
                    @ClosureParams(value = SimpleType, options = ['getl.lang.opts.LogSpec']) Closure cl = null) {
        def parent = new LogSpec(childOwnerObject, childThisObject, false, null)
        parent.runClosure(cl)

        return parent
    }

    /**
     * Existing connection from repository
     * @param name name in repository
     * @param cl object processing code
     * @return connection object
     */
    Connection connection(String name,
                                  @DelegatesTo(Connection)
                                  @ClosureParams(value = SimpleType, options = ['getl.data.Connection']) Closure cl = null) {
        if (name == null) throw new ExceptionGETL('Need connection name value!')

        def parent = registerConnection(null, name, false) as Connection
        runClosure(parent, cl)

        return parent
    }

    /**
     * Clone connection object
     * @param obj object to clone
     * @return clone object
     */
    Connection cloneConnection(Connection obj) {
        if (obj == null) throw new ExceptionGETL('Need object value!')
        return obj.cloneConnection()
    }

    /**
     * Existing dataset from repository
     * @param name name in repository
     * @param cl object processing code
     * @return dataset object
     */
    Dataset dataset(String name,
                    @DelegatesTo(Dataset)
                    @ClosureParams(value = SimpleType, options = ['getl.data.Dataset']) Closure cl = null) {
        if (name == null) throw new ExceptionGETL('Need table name value!')

        def parent = registerDataset(null, name, false) as Dataset
        runClosure(parent, cl)

        return parent
    }

    /**
     * Clone dataset object
     * @param obj object to clone
     * @param con used connection for new dataset
     * @return clone object
     */
    Dataset cloneDataset(Dataset obj, Connection con = null) {
        if (obj == null) throw new ExceptionGETL('Need object value!')
        return obj.cloneDataset(con)
    }

    /**
     * Existing jdbc connection from repository
     * @param name name in repository
     * @param cl object processing code
     * @return jdbc connection object
     */
    JDBCConnection jdbcConnection(String name,
                               @DelegatesTo(JDBCConnection)
                               @ClosureParams(value = SimpleType, options = ['getl.jdbc.JDBCConnection']) Closure cl = null) {
        if (name == null) throw new ExceptionGETL('Need connection name value!')

        def parent = registerConnection(null, name, false) as Connection
        if (!(parent instanceof JDBCConnection)) throw new ExceptionGETL("$name is not jdbc connection!")

        runClosure(parent, cl)

        return parent
    }

    /**
     * Existing jdbc table from repository
     * @param name name in repository
     * @param cl object processing code
     * @return jdbc table object
     */
    TableDataset jdbcTable(String name,
                                   @DelegatesTo(TableDataset)
                                   @ClosureParams(value = SimpleType, options = ['getl.jdbc.TableDataset']) Closure cl = null) {
        if (name == null) throw new ExceptionGETL('Need table name value!')

        def parent = registerDataset(null, name, false) as TableDataset
        runClosure(parent, cl)

        return parent
    }

    /** JDBC view dataset */
    ViewDataset view(String name = null, JDBCConnection connection = null, Boolean registration = false,
                     @DelegatesTo(ViewDataset)
                     @ClosureParams(value = SimpleType, options = ['getl.jdbc.ViewDataset']) Closure cl = null) {
        def parent = registerDataset(connection, VIEWDATASET, name, registration) as ViewDataset
        runClosure(parent, cl)

        return parent
    }

    /** JDBC view dataset */
    ViewDataset view(String name, Boolean registration,
                     @DelegatesTo(ViewDataset)
                     @ClosureParams(value = SimpleType, options = ['getl.jdbc.ViewDataset']) Closure cl = null) {
        view(name, null, registration, cl)
    }

    /** JDBC view dataset */
    ViewDataset view(String name,
                     @DelegatesTo(ViewDataset)
                     @ClosureParams(value = SimpleType, options = ['getl.jdbc.ViewDataset']) Closure cl) {
        view(name, null, false, cl)
    }

    /** JDBC view dataset */
    ViewDataset view(@DelegatesTo(ViewDataset)
                     @ClosureParams(value = SimpleType, options = ['getl.jdbc.ViewDataset']) Closure cl) {
        view(null, null, false, cl)
    }

    /** JDBC view dataset */
    ViewDataset view(JDBCConnection connection,
                     @DelegatesTo(ViewDataset)
                     @ClosureParams(value = SimpleType, options = ['getl.jdbc.ViewDataset']) Closure cl) {
        view(null, connection, false, cl)
    }

    /** H2 connection */
    H2Connection h2Connection(String name, Boolean registration,
                              @DelegatesTo(H2Connection)
                              @ClosureParams(value = SimpleType, options = ['getl.h2.H2Connection']) Closure cl) {
        def parent = registerConnection(H2CONNECTION, name, registration) as H2Connection
        runClosure(parent, cl)

        return parent
    }

    /** H2 connection */
    H2Connection h2Connection(String name,
                              @DelegatesTo(H2Connection)
                              @ClosureParams(value = SimpleType, options = ['getl.h2.H2Connection']) Closure cl = null) {
        h2Connection(name, false, cl)
    }

    /** H2 connection */
    H2Connection h2Connection(@DelegatesTo(H2Connection)
                              @ClosureParams(value = SimpleType, options = ['getl.h2.H2Connection']) Closure cl) {
        h2Connection(null, false, cl)
    }

    /** H2 current connection */
    H2Connection h2Connection() {
        defaultJdbcConnection(H2TABLE) as H2Connection
    }

    /** Use default H2 connection for new datasets */
    H2Connection useH2Connection(H2Connection connection) {
        useJdbcConnection(H2TABLE, connection) as H2Connection
    }

    /** Use default H2 connection for new datasets */
    TDS useEmbeddedConnection(TDS connection = TDS.storage) {
        useJdbcConnection(EMBEDDEDTABLE, connection) as TDS
    }

    /** H2 table */
    H2Table h2Table(String name, Boolean registration,
                    @DelegatesTo(H2Table)
                    @ClosureParams(value = SimpleType, options = ['getl.h2.H2Table']) Closure cl) {
        def parent = registerDataset(H2TABLE, name, registration) as H2Table
        runClosure(parent, cl)

        return parent
    }

    /** H2 table */
    H2Table h2Table(String name,
                    @DelegatesTo(H2Table)
                    @ClosureParams(value = SimpleType, options = ['getl.h2.H2Table']) Closure cl = null) {
        h2Table(name, false, cl)
    }

    /** H2 table */
    H2Table h2Table(@DelegatesTo(H2Table)
                    @ClosureParams(value = SimpleType, options = ['getl.h2.H2Table']) Closure cl) {
        h2Table(null, false, cl)
    }

    /** DB2 connection */
    DB2Connection db2Connection(String name, Boolean registration,
                                @DelegatesTo(DB2Connection)
                                @ClosureParams(value = SimpleType, options = ['getl.db2.DB2Connection']) Closure cl) {
        def parent = registerConnection(DB2CONNECTION, name, registration) as DB2Connection
        runClosure(parent, cl)

        return parent

    }

    /** DB2 connection */
    DB2Connection db2Connection(String name,
                                @DelegatesTo(DB2Connection)
                                @ClosureParams(value = SimpleType, options = ['getl.db2.DB2Connection']) Closure cl = null) {
        db2Connection(name, false, cl)
    }

    /** DB2 connection */
    DB2Connection db2Connection(@DelegatesTo(DB2Connection)
                                @ClosureParams(value = SimpleType, options = ['getl.db2.DB2Connection']) Closure cl) {
        db2Connection(null, false, cl)
    }

    /** DB2 current connection */
    DB2Connection db2Connection() {
        defaultJdbcConnection(DB2TABLE) as DB2Connection
    }

    /** Use default DB2 connection for new datasets */
    DB2Connection useDb2Connection(DB2Connection connection) {
        useJdbcConnection(DB2TABLE, connection) as DB2Connection
    }

    /** DB2 database table */
    DB2Table db2Table(String name, Boolean registration,
                      @DelegatesTo(DB2Table)
                      @ClosureParams(value = SimpleType, options = ['getl.db2.DB2Table']) Closure cl) {
        def parent = registerDataset(DB2TABLE, name, registration) as DB2Table
        runClosure(parent, cl)

        return parent
    }

    /** DB2 database table */
    DB2Table db2Table(String name,
                      @DelegatesTo(DB2Table)
                      @ClosureParams(value = SimpleType, options = ['getl.db2.DB2Table']) Closure cl = null) {
        db2Table(name, false, cl)
    }

    /** DB2 database table */
    DB2Table db2Table(@DelegatesTo(DB2Table)
                      @ClosureParams(value = SimpleType, options = ['getl.db2.DB2Table']) Closure cl) {
        db2Table(null, false, cl)
    }

    /** Hive connection */
    HiveConnection hiveConnection(String name, Boolean registration,
                                  @DelegatesTo(HiveConnection)
                                  @ClosureParams(value = SimpleType, options = ['getl.hive.HiveConnection']) Closure cl) {
        def parent = registerConnection(HIVECONNECTION, name, registration) as HiveConnection
        runClosure(parent, cl)

        return parent
    }

    /** Hive connection */
    HiveConnection hiveConnection(String name,
                                  @DelegatesTo(HiveConnection)
                                  @ClosureParams(value = SimpleType, options = ['getl.hive.HiveConnection']) Closure cl = null) {
        hiveConnection(name, false, cl)
    }

    /** Hive connection */
    HiveConnection hiveConnection(@DelegatesTo(HiveConnection)
                                  @ClosureParams(value = SimpleType, options = ['getl.hive.HiveConnection']) Closure cl) {
        hiveConnection(null, false, cl)
    }

    /** Hive current connection */
    HiveConnection hiveConnection() {
        defaultJdbcConnection(HIVETABLE) as HiveConnection
    }

    /** Use default Hive connection for new datasets */
    HiveConnection useHiveConnection(HiveConnection connection) {
        useJdbcConnection(HIVETABLE, connection) as HiveConnection
    }

    /** Hive table */
    HiveTable hiveTable(String name, Boolean registration,
                        @DelegatesTo(HiveTable)
                        @ClosureParams(value = SimpleType, options = ['getl.hive.HiveTable']) Closure cl) {
        def parent = registerDataset(HIVETABLE, name, registration) as HiveTable
        runClosure(parent, cl)

        return parent
    }

    /** Hive table */
    HiveTable hiveTable(String name,
                        @DelegatesTo(HiveTable)
                        @ClosureParams(value = SimpleType, options = ['getl.hive.HiveTable']) Closure cl = null) {
        hiveTable(name, false, cl)
    }

    /** Hive table */
    HiveTable hiveTable(@DelegatesTo(HiveTable)
                        @ClosureParams(value = SimpleType, options = ['getl.hive.HiveTable']) Closure cl) {
        hiveTable(null, false, cl)
    }

    /** MSSQL connection */
    MSSQLConnection mssqlConnection(String name, Boolean registration,
                                    @DelegatesTo(MSSQLConnection)
                                    @ClosureParams(value = SimpleType, options = ['getl.mssql.MSSQLConnection']) Closure cl) {
        def parent = registerConnection(MSSQLCONNECTION, name, registration) as MSSQLConnection
        runClosure(parent, cl)

        return parent
    }

    /** MSSQL connection */
    MSSQLConnection mssqlConnection(String name,
                                    @DelegatesTo(MSSQLConnection)
                                    @ClosureParams(value = SimpleType, options = ['getl.mssql.MSSQLConnection']) Closure cl = null) {
        mssqlConnection(name, false, cl)
    }

    /** MSSQL connection */
    MSSQLConnection mssqlConnection(@DelegatesTo(MSSQLConnection)
                                    @ClosureParams(value = SimpleType, options = ['getl.mssql.MSSQLConnection']) Closure cl) {
        mssqlConnection(null, false, cl)
    }

    /** MSSQL current connection */
    MSSQLConnection mssqlConnection() {
        defaultJdbcConnection(MSSQLTABLE) as MSSQLConnection
    }

    /** Use default MSSQL connection for new datasets */
    MSSQLConnection useMssqlConnection(MSSQLConnection connection) {
        useJdbcConnection(MSSQLTABLE, connection) as MSSQLConnection
    }

    /** MSSQL database table */
    MSSQLTable mssqlTable(String name, Boolean registration,
                          @DelegatesTo(MSSQLTable)
                          @ClosureParams(value = SimpleType, options = ['getl.mssql.MSSQLTable']) Closure cl) {
        def parent = registerDataset(MSSQLTABLE, name, registration) as MSSQLTable
        runClosure(parent, cl)

        return parent
    }

    /** MSSQL database table */
    MSSQLTable mssqlTable(String name,
                          @DelegatesTo(MSSQLTable)
                          @ClosureParams(value = SimpleType, options = ['getl.mssql.MSSQLTable']) Closure cl = null) {
        mssqlTable(name, false, cl)
    }

    /** MSSQL database table */
    MSSQLTable mssqlTable(@DelegatesTo(MSSQLTable)
                          @ClosureParams(value = SimpleType, options = ['getl.mssql.MSSQLTable']) Closure cl) {
        mssqlTable(null, cl)
    }

    /** MySQL connection */
    MySQLConnection mysqlConnection(String name, Boolean registration,
                                    @DelegatesTo(MySQLConnection)
                                    @ClosureParams(value = SimpleType, options = ['getl.mysql.MySQLConnection']) Closure cl) {
        def parent = registerConnection(MYSQLCONNECTION, name, registration) as MySQLConnection
        runClosure(parent, cl)

        return parent
    }

    /** MySQL connection */
    MySQLConnection mysqlConnection(String name,
                                    @DelegatesTo(MySQLConnection)
                                    @ClosureParams(value = SimpleType, options = ['getl.mysql.MySQLConnection']) Closure cl = null) {
        mysqlConnection(name, false, cl)
    }

    /** MySQL connection */
    MySQLConnection mysqlConnection(@DelegatesTo(MySQLConnection)
                                    @ClosureParams(value = SimpleType, options = ['getl.mysql.MySQLConnection']) Closure cl) {
        mysqlConnection(null, false, cl)
    }

    /** MySQL current connection */
    MySQLConnection mysqlConnection() {
        defaultJdbcConnection(MYSQLTABLE) as MySQLConnection
    }

    /** Use default MySQL connection for new datasets */
    MySQLConnection useMysqlConnection(MySQLConnection connection) {
        useJdbcConnection(MYSQLTABLE, connection) as MySQLConnection
    }

    /** MySQL database table */
    MySQLTable mysqlTable(String name, Boolean registration,
                          @DelegatesTo(MySQLTable)
                          @ClosureParams(value = SimpleType, options = ['getl.mysql.MySQLTable']) Closure cl) {
        def parent = registerDataset(MYSQLTABLE, name, registration) as MySQLTable
        runClosure(parent, cl)

        return parent
    }

    /** MySQL database table */
    MySQLTable mysqlTable(String name,
                          @DelegatesTo(MySQLTable)
                          @ClosureParams(value = SimpleType, options = ['getl.mysql.MySQLTable']) Closure cl = null) {
        mysqlTable(name, false, cl)
    }

    /** MySQL database table */
    MySQLTable mysqlTable(@DelegatesTo(MySQLTable)
                          @ClosureParams(value = SimpleType, options = ['getl.mysql.MySQLTable']) Closure cl) {
        mysqlTable(null, false, cl)
    }

    /** Oracle connection */
    OracleConnection oracleConnection(String name, Boolean registration,
                                      @DelegatesTo(OracleConnection)
                                      @ClosureParams(value = SimpleType, options = ['getl.oracle.OracleConnection']) Closure cl) {
        def parent = registerConnection(ORACLECONNECTION, name, registration) as OracleConnection
        runClosure(parent, cl)

        return parent
    }

    /** Oracle connection */
    OracleConnection oracleConnection(String name,
                                      @DelegatesTo(OracleConnection)
                                      @ClosureParams(value = SimpleType, options = ['getl.oracle.OracleConnection']) Closure cl = null) {
        oracleConnection(name, false, cl)
    }

    /** Oracle connection */
    OracleConnection oracleConnection(@DelegatesTo(OracleConnection)
                                      @ClosureParams(value = SimpleType, options = ['getl.oracle.OracleConnection']) Closure cl) {
        oracleConnection(null, false, cl)
    }

    /** Oracle current connection */
    OracleConnection oracleConnection() {
        defaultJdbcConnection(ORACLETABLE) as OracleConnection
    }

    /** Use default Oracle connection for new datasets */
    OracleConnection useOracleConnection(OracleConnection connection) {
        useJdbcConnection(ORACLETABLE, connection) as OracleConnection
    }

    /** Oracle table */
    OracleTable oracleTable(String name, Boolean registration,
                            @DelegatesTo(OracleTable)
                            @ClosureParams(value = SimpleType, options = ['getl.oracle.OracleTable']) Closure cl) {
        def parent = registerDataset(ORACLETABLE, name, registration) as OracleTable
        runClosure(parent, cl)

        return parent
    }

    /** Oracle table */
    OracleTable oracleTable(String name,
                            @DelegatesTo(OracleTable)
                            @ClosureParams(value = SimpleType, options = ['getl.oracle.OracleTable']) Closure cl = null) {
        oracleTable(name, false, cl)
    }

    /** Oracle table */
    OracleTable oracleTable(@DelegatesTo(OracleTable)
                            @ClosureParams(value = SimpleType, options = ['getl.oracle.OracleTable']) Closure cl) {
        oracleTable(null, false, cl)
    }

    /** PostgreSQL connection */
    PostgreSQLConnection postgresqlConnection(String name, Boolean registration,
                                              @DelegatesTo(PostgreSQLConnection)
                                              @ClosureParams(value = SimpleType, options = ['getl.postgresql.PostgreSQLConnection']) Closure cl) {
        def parent = registerConnection(POSTGRESQLCONNECTION, name, registration) as PostgreSQLConnection
        runClosure(parent, cl)

        return parent
    }

    /** PostgreSQL connection */
    PostgreSQLConnection postgresqlConnection(String name,
                                              @DelegatesTo(PostgreSQLConnection)
                                              @ClosureParams(value = SimpleType, options = ['getl.postgresql.PostgreSQLConnection']) Closure cl = null) {
        postgresqlConnection(name, false, cl)
    }

    /** PostgreSQL connection */
    PostgreSQLConnection postgresqlConnection(@DelegatesTo(PostgreSQLConnection)
                                              @ClosureParams(value = SimpleType, options = ['getl.postgresql.PostgreSQLConnection']) Closure cl) {
        postgresqlConnection(null, false, cl)
    }

    /** PostgreSQL default connection */
    PostgreSQLConnection postgresqlConnection() {
        defaultJdbcConnection(POSTGRESQLTABLE) as PostgreSQLConnection
    }

    /** Use default PostgreSQL connection for new datasets */
    PostgreSQLConnection usePostgresqlConnection(PostgreSQLConnection connection) {
        useJdbcConnection(POSTGRESQLTABLE, connection) as PostgreSQLConnection
    }

    /** MySQL database table */
    PostgreSQLTable postgresqlTable(String name, Boolean registration,
                                    @DelegatesTo(PostgreSQLTable)
                                    @ClosureParams(value = SimpleType, options = ['getl.postgresql.PostgreSQLTable']) Closure cl) {
        def parent = registerDataset(POSTGRESQLTABLE, name, registration) as PostgreSQLTable
        runClosure(parent, cl)

        return parent
    }


    /** MySQL database table */
    PostgreSQLTable postgresqlTable(String name,
                                    @DelegatesTo(PostgreSQLTable)
                                    @ClosureParams(value = SimpleType, options = ['getl.postgresql.PostgreSQLTable']) Closure cl = null) {
        postgresqlTable(name, false, cl)
    }

    /** MySQL database table */
    PostgreSQLTable postgresqlTable(@DelegatesTo(PostgreSQLTable)
                                    @ClosureParams(value = SimpleType, options = ['getl.postgresql.PostgreSQLTable']) Closure cl) {
        postgresqlTable(null, false, cl)
    }

    /** Vertica connection */
    VerticaConnection verticaConnection(String name, Boolean registration,
                                        @DelegatesTo(VerticaConnection)
                                        @ClosureParams(value = SimpleType, options = ['getl.vertica.VerticaConnection']) Closure cl) {
        def parent = registerConnection(VERTICACONNECTION, name, registration) as VerticaConnection
        runClosure(parent, cl)

        return parent
    }

    /** Vertica connection */
    VerticaConnection verticaConnection(String name,
                                        @DelegatesTo(VerticaConnection)
                                        @ClosureParams(value = SimpleType, options = ['getl.vertica.VerticaConnection']) Closure cl = null) {
        verticaConnection(name, false, cl)
    }

    /** Vertica connection */
    VerticaConnection verticaConnection(@DelegatesTo(VerticaConnection)
                                        @ClosureParams(value = SimpleType, options = ['getl.vertica.VerticaConnection']) Closure cl) {
        verticaConnection(null, false, cl)
    }

    /** Vertica default connection */
    VerticaConnection verticaConnection() {
        defaultJdbcConnection(VERTICATABLE) as VerticaConnection
    }

    /** Use default Vertica connection for new datasets */
    VerticaConnection useVerticaConnection(VerticaConnection connection) {
        useJdbcConnection(VERTICATABLE, connection) as VerticaConnection
    }

    /** Vertica table */
    VerticaTable verticaTable(String name, Boolean registration,
                              @DelegatesTo(VerticaTable)
                              @ClosureParams(value = SimpleType, options = ['getl.vertica.VerticaTable']) Closure cl) {
        def parent = registerDataset(VERTICATABLE, name, registration) as VerticaTable
        runClosure(parent, cl)

        return parent
    }

    /** Vertica table */
    VerticaTable verticaTable(String name,
                              @DelegatesTo(VerticaTable)
                              @ClosureParams(value = SimpleType, options = ['getl.vertica.VerticaTable']) Closure cl = null) {
        verticaTable(name, false, cl)
    }

    /** Vertica table */
    VerticaTable verticaTable(@DelegatesTo(VerticaTable)
                              @ClosureParams(value = SimpleType, options = ['getl.vertica.VerticaTable']) Closure cl) {
        verticaTable(null, false, cl)
    }

    /** NetSuite connection */
    NetsuiteConnection netsuiteConnection(String name, Boolean registration,
                                          @DelegatesTo(NetsuiteConnection)
                                          @ClosureParams(value = SimpleType, options = ['getl.netsuite.NetsuiteConnection']) Closure cl) {
        def parent = registerConnection(NETSUITECONNECTION, name, registration) as NetsuiteConnection
        runClosure(parent, cl)

        return parent
    }

    /** NetSuite connection */
    NetsuiteConnection netsuiteConnection(String name,
                                          @DelegatesTo(NetsuiteConnection)
                                          @ClosureParams(value = SimpleType, options = ['getl.netsuite.NetsuiteConnection']) Closure cl = null) {
        netsuiteConnection(name, false, cl)
    }

    /** NetSuite connection */
    NetsuiteConnection netsuiteConnection(@DelegatesTo(NetsuiteConnection)
                                          @ClosureParams(value = SimpleType, options = ['getl.netsuite.NetsuiteConnection']) Closure cl) {
        netsuiteConnection(null, false, cl)
    }

    /** NetSuite default connection */
    NetsuiteConnection netsuiteConnection() {
        defaultJdbcConnection(NETSUITETABLE) as NetsuiteConnection
    }

    /** Use default Netsuite connection for new datasets */
    NetsuiteConnection useNetsuiteConnection(NetsuiteConnection connection) {
        useJdbcConnection(NETSUITETABLE, connection) as NetsuiteConnection
    }

    /** Netsuite table */
    NetsuiteTable netsuiteTable(String name, Boolean registration,
                                @DelegatesTo(NetsuiteTable)
                                @ClosureParams(value = SimpleType, options = ['getl.netsuite.NetsuiteTable']) Closure cl) {
        def parent = registerDataset(NETSUITETABLE, name, registration) as NetsuiteTable
        runClosure(parent, cl)

        return parent
    }

    /** Netsuite table */
    NetsuiteTable netsuiteTable(String name, @DelegatesTo(NetsuiteTable)
    @ClosureParams(value = SimpleType, options = ['getl.netsuite.NetsuiteTable']) Closure cl = null) {
        netsuiteTable(name, false, cl)
    }

    /** Netsuite table */
    NetsuiteTable netsuiteTable(@DelegatesTo(NetsuiteTable)
                                @ClosureParams(value = SimpleType, options = ['getl.netsuite.NetsuiteTable']) Closure cl) {
        netsuiteTable(null, false, cl)
    }

    /** Temporary database connection */
    TDS embeddedConnection(String name, Boolean registration,
                           @DelegatesTo(TDS)
                           @ClosureParams(value = SimpleType, options = ['getl.tfs.TDS']) Closure cl) {
        def parent = registerConnection(EMBEDDEDCONNECTION, name, registration) as TDS
        if (parent.sqlHistoryFile == null) parent.sqlHistoryFile = langOpts.tempDBSQLHistoryFile
        runClosure(parent, cl)

        return parent
    }

    /** Temporary database connection */
    TDS embeddedConnection(String name,
                           @DelegatesTo(TDS)
                           @ClosureParams(value = SimpleType, options = ['getl.tfs.TDS']) Closure cl = null) {
        embeddedConnection(name, false, cl)
    }

    /** Temporary database connection */
    TDS embeddedConnection(@DelegatesTo(TDS)
                           @ClosureParams(value = SimpleType, options = ['getl.tfs.TDS']) Closure cl) {
        embeddedConnection(null, false, cl)
    }

    /** Temporary database default connection */
    TDS embeddedConnection() {
//        defaultJdbcConnection(EMBEDDEDTABLE) as TDS
        TDS.storage
    }

    /** Table with temporary database */
    TDSTable embeddedTable(String name, Boolean registration,
                           @DelegatesTo(TDSTable)
                           @ClosureParams(value = SimpleType, options = ['getl.tfs.TDSTable']) Closure cl) {
        def parent = registerDataset(defaultJdbcConnection(EMBEDDEDTABLE)?:TDS.storage, EMBEDDEDTABLE, name, registration) as TDSTable
        if ((parent.connection as TDS).sqlHistoryFile == null)
            (parent.connection as TDS).sqlHistoryFile = langOpts.tempDBSQLHistoryFile

        runClosure(parent, cl)

        return parent
    }

    /** Table with temporary database */
    TDSTable embeddedTable(String name,
                           @DelegatesTo(TDSTable)
                           @ClosureParams(value = SimpleType, options = ['getl.tfs.TDSTable']) Closure cl = null) {
        embeddedTable(name, false, cl)
    }

    /** Table with temporary database */
    TDSTable embeddedTable(@DelegatesTo(TDSTable)
                           @ClosureParams(value = SimpleType, options = ['getl.tfs.TDSTable']) Closure cl) {
        embeddedTable(null, false, cl)
    }

    /** Table with temporary database */
    TDSTable embeddedTableWithDataset(String name, Dataset sourceDataset,
                                      @DelegatesTo(TDSTable)
                                      @ClosureParams(value = SimpleType, options = ['getl.tfs.TDSTable']) Closure cl = null) {
        if (sourceDataset == null) throw new ExceptionGETL("Source dataset cannot be null!")
        if (sourceDataset.field.isEmpty()) {
            sourceDataset.retrieveFields()
            if (sourceDataset.field.isEmpty()) throw new ExceptionGETL("Required field from dataset $sourceDataset")
        }

        TDSTable parent = new TDSTable(connection: defaultJdbcConnection(EMBEDDEDTABLE)?:TDS.storage)
        parent.field = sourceDataset.field
        if ((parent.connection as TDS).sqlHistoryFile == null)
            (parent.connection as TDS).sqlHistoryFile = langOpts.tempDBSQLHistoryFile

        registerDatasetObject(parent, name, true)
        runClosure(parent, cl)

        return parent
    }

    /** Table with temporary database */
    TDSTable embeddedTableWithDataset(Dataset sourceDataset,
                                      @DelegatesTo(TDSTable)
                                      @ClosureParams(value = SimpleType, options = ['getl.tfs.TDSTable']) Closure cl = null) {
        embeddedTableWithDataset(null, sourceDataset, cl)
    }

    /** JDBC query dataset */
    QueryDataset query(String name, JDBCConnection connection, Boolean registration,
                       @DelegatesTo(QueryDataset)
                       @ClosureParams(value = SimpleType, options = ['getl.jdbc.QueryDataset']) Closure cl) {
        def parent = registerDataset(connection, QUERYDATASET, name, registration) as QueryDataset
        runClosure(parent, cl)

        return parent
    }

    /** JDBC query dataset */
    QueryDataset query(String name, Boolean registration,
                       @DelegatesTo(QueryDataset)
                       @ClosureParams(value = SimpleType, options = ['getl.jdbc.QueryDataset']) Closure cl) {
        query(name, null, registration, cl)
    }

    /** JDBC query dataset */
    QueryDataset query(String name,
                       @DelegatesTo(QueryDataset)
                       @ClosureParams(value = SimpleType, options = ['getl.jdbc.QueryDataset']) Closure cl = null) {
        query(name, null, false, cl)
    }

    /** JDBC query dataset */
    QueryDataset query(@DelegatesTo(QueryDataset)
                       @ClosureParams(value = SimpleType, options = ['getl.jdbc.QueryDataset']) Closure cl) {
        query(null, null, false, cl)
    }

    /** JDBC query dataset */
    QueryDataset query(JDBCConnection connection,
                       @DelegatesTo(QueryDataset)
                       @ClosureParams(value = SimpleType, options = ['getl.jdbc.QueryDataset']) Closure cl) {
        query(null, connection, false, cl)
    }

    /** JDBC query dataset */
    QueryDataset sqlQuery(JDBCConnection connection, String sql, Map vars = [:]) {
        def parent = registerDataset(connection, QUERYDATASET, null, false) as QueryDataset
        parent.query = sql
        parent.queryParams.putAll(vars)

        return parent
    }

    /** JDBC query dataset */
    QueryDataset sqlQuery(String sql, Map vars = [:]) {
        sqlQuery(null, sql, vars)
    }

    /** Return the first row from query */
    Map<String, Object> sqlQueryRow(JDBCConnection connection, String sql, Map vars = [:]) {
        def query = sqlQuery(connection, sql, vars)
        def rows = query.rows(limit: 1)
        return (!rows.isEmpty())?rows[0]:null
    }

    /** Return the first row from query */
    Map<String, Object> sqlQueryRow(String sql, Map vars = [:]) {
        sqlQueryRow(null, sql, vars)
    }

    /** CSV connection */
    CSVConnection csvConnection(String name, Boolean registration,
                                @DelegatesTo(CSVConnection)
                                @ClosureParams(value = SimpleType, options = ['getl.csv.CSVConnection']) Closure cl) {
        def parent = registerConnection(CSVCONNECTION, name, registration) as CSVConnection
        runClosure(parent, cl)

        return parent
    }

    /** CSV connection */
    CSVConnection csvConnection(String name,
                                @DelegatesTo(CSVConnection)
                                @ClosureParams(value = SimpleType, options = ['getl.csv.CSVConnection']) Closure cl = null) {
        csvConnection(name, false, cl)
    }

    /** CSV connection */
    CSVConnection csvConnection(@DelegatesTo(CSVConnection)
                                @ClosureParams(value = SimpleType, options = ['getl.csv.CSVConnection']) Closure cl) {
        csvConnection(null, false, cl)
    }

    /** CSV default connection */
    CSVConnection csvConnection() {
        defaultFileConnection(CSVDATASET) as CSVConnection
    }

    /** Use default CSV connection for new datasets */
    CSVConnection useCsvConnection(CSVConnection connection) {
        useFileConnection(CSVDATASET, connection) as CSVConnection
    }

    /** CSV delimiter file */
    CSVDataset csv(String name, Boolean registration,
                   @DelegatesTo(CSVDataset)
                   @ClosureParams(value = SimpleType, options = ['getl.csv.CSVDataset']) Closure cl) {
        def parent = registerDataset(CSVDATASET, name, registration) as CSVDataset
        if (parent.connection == null) parent.connection = new CSVConnection()
        runClosure(parent, cl)

        return parent
    }

    /** CSV delimiter file */
    CSVDataset csv(String name,
                   @DelegatesTo(CSVDataset)
                   @ClosureParams(value = SimpleType, options = ['getl.csv.CSVDataset']) Closure cl = null) {
        csv(name, false, cl)
    }

    /** CSV delimiter file */
    CSVDataset csv(@DelegatesTo(CSVDataset)
                   @ClosureParams(value = SimpleType, options = ['getl.csv.CSVDataset']) Closure cl) {
        csv(null, false, cl)
    }

    /** CSV file with exists dataset */
    CSVDataset csvWithDataset(String name, Dataset sourceDataset, Boolean registration,
                              @DelegatesTo(CSVDataset)
                              @ClosureParams(value = SimpleType, options = ['getl.csv.CSVDataset']) Closure cl) {
        if (sourceDataset == null) throw new ExceptionGETL("Dataset cannot be null!")
        def parent = registerDataset(CSVDATASET, name, registration) as CSVDataset
        parent.field = sourceDataset.field

        runClosure(parent, cl)

        return parent
    }

    /** CSV file with exists dataset */
    CSVDataset csvWithDataset(String name, Dataset sourceDataset,
                              @DelegatesTo(CSVDataset)
                              @ClosureParams(value = SimpleType, options = ['getl.csv.CSVDataset']) Closure cl = null) {
        csvWithDataset(name, sourceDataset, false, cl)
    }

    /** CSV file with exists dataset */
    CSVDataset csvWithDataset(Dataset sourceDataset,
                              @DelegatesTo(CSVDataset)
                              @ClosureParams(value = SimpleType, options = ['getl.csv.CSVDataset']) Closure cl = null) {
        csvWithDataset(null, sourceDataset, false, cl)
    }

    /** Excel connection */
    ExcelConnection excelConnection(String name, Boolean registration,
                                    @DelegatesTo(ExcelConnection)
                                    @ClosureParams(value = SimpleType, options = ['getl.excel.ExcelConnection']) Closure cl) {
        def parent = registerConnection(EXCELCONNECTION, name, registration) as ExcelConnection
        runClosure(parent, cl)

        return parent
    }

    /** Excel connection */
    ExcelConnection excelConnection(String name,
                                    @DelegatesTo(ExcelConnection)
                                    @ClosureParams(value = SimpleType, options = ['getl.excel.ExcelConnection']) Closure cl = null) {
        excelConnection(name, false, cl)
    }

    /** Excel connection */
    ExcelConnection excelConnection(@DelegatesTo(ExcelConnection)
                                    @ClosureParams(value = SimpleType, options = ['getl.excel.ExcelConnection']) Closure cl) {
        excelConnection(null, false, cl)
    }

    /** Excel default connection */
    ExcelConnection excelConnection() {
        defaultFileConnection(EXCELDATASET) as ExcelConnection
    }

    /** Use default Excel connection for new datasets */
    ExcelConnection useExcelConnection(ExcelConnection connection) {
        useOtherConnection(EXCELDATASET, connection) as ExcelConnection
    }

    /** Excel file */
    ExcelDataset excel(String name, Boolean registration,
                       @DelegatesTo(ExcelDataset)
                       @ClosureParams(value = SimpleType, options = ['getl.excel.ExcelDataset']) Closure cl) {
        def parent = registerDataset(EXCELDATASET, name, registration) as ExcelDataset
        if (parent.connection == null) parent.connection = new ExcelConnection()
        runClosure(parent, cl)

        return parent
    }

    /** Excel file */
    ExcelDataset excel(String name,
                       @DelegatesTo(ExcelDataset)
                       @ClosureParams(value = SimpleType, options = ['getl.excel.ExcelDataset']) Closure cl = null) {
        excel(name, false, cl)
    }

    /** Excel file */
    ExcelDataset excel(@DelegatesTo(ExcelDataset)
                       @ClosureParams(value = SimpleType, options = ['getl.excel.ExcelDataset']) Closure cl = null) {
        excel(null, false, cl)
    }

    /** JSON connection */
    JSONConnection jsonConnection(String name, Boolean registration,
                                  @DelegatesTo(JSONConnection)
                                  @ClosureParams(value = SimpleType, options = ['getl.json.JSONConnection']) Closure cl) {
        def parent = registerConnection(JSONCONNECTION, name, registration) as JSONConnection
        runClosure(parent, cl)

        return parent
    }

    /** JSON connection */
    JSONConnection jsonConnection(String name,
                                  @DelegatesTo(JSONConnection)
                                  @ClosureParams(value = SimpleType, options = ['getl.json.JSONConnection']) Closure cl = null) {
        jsonConnection(name, false, cl)
    }

    /** JSON connection */
    JSONConnection jsonConnection(@DelegatesTo(JSONConnection)
                                  @ClosureParams(value = SimpleType, options = ['getl.json.JSONConnection']) Closure cl) {
        jsonConnection(null, false, cl)
    }

    /** JSON default connection */
    JSONConnection jsonConnection() {
        defaultFileConnection(JSONDATASET) as JSONConnection
    }

    /** Use default Json connection for new datasets */
    JSONConnection useJsonConnection(JSONConnection connection) {
        useFileConnection(JSONDATASET, connection) as JSONConnection
    }

    /** JSON file */
    JSONDataset json(String name, Boolean registration,
                     @DelegatesTo(JSONDataset)
                     @ClosureParams(value = SimpleType, options = ['getl.json.JSONDataset']) Closure cl) {
        def parent = registerDataset(JSONDATASET, name, registration) as JSONDataset
        if (parent.connection == null) parent.connection = new JSONConnection()
        runClosure(parent, cl)

        return parent
    }

    /** JSON file */
    JSONDataset json(String name,
                     @DelegatesTo(JSONDataset)
                     @ClosureParams(value = SimpleType, options = ['getl.json.JSONDataset']) Closure cl = null) {
        json(name, false, cl)
    }

    /** JSON file */
    JSONDataset json(@DelegatesTo(JSONDataset)
                     @ClosureParams(value = SimpleType, options = ['getl.json.JSONDataset']) Closure cl) {
        json(null, false, cl)
    }

    /** XML connection */
    XMLConnection xmlConnection(String name, Boolean registration,
                                @DelegatesTo(XMLConnection)
                                @ClosureParams(value = SimpleType, options = ['getl.xml.XMLConnection']) Closure cl) {
        def parent = registerConnection(XMLCONNECTION, name, registration) as XMLConnection
        runClosure(parent, cl)

        return parent
    }

    /** XML connection */
    XMLConnection xmlConnection(String name,
                                @DelegatesTo(XMLConnection)
                                @ClosureParams(value = SimpleType, options = ['getl.xml.XMLConnection']) Closure cl = null) {
        xmlConnection(name, false, cl)
    }

    /** XML connection */
    XMLConnection xmlConnection(@DelegatesTo(XMLConnection)
                                @ClosureParams(value = SimpleType, options = ['getl.xml.XMLConnection']) Closure cl) {
        xmlConnection(null, false, cl)
    }

    /** XML default connection */
    XMLConnection xmlConnection() {
        defaultFileConnection(XMLDATASET) as XMLConnection
    }

    /** Use default XML connection for new datasets */
    XMLConnection useXmlConnection(XMLConnection connection) {
        useFileConnection(XMLCONNECTION, connection) as XMLConnection
    }

    /** XML file */
    XMLDataset xml(String name, Boolean registration,
                   @DelegatesTo(XMLDataset)
                   @ClosureParams(value = SimpleType, options = ['getl.xml.XMLDataset']) Closure cl) {
        def parent = registerDataset(XMLDATASET, name, registration) as XMLDataset
        if (parent.connection == null) parent.connection = new XMLConnection()
        runClosure(parent, cl)

        return parent
    }

    /** XML file */
    XMLDataset xml(String name,
                   @DelegatesTo(XMLDataset)
                   @ClosureParams(value = SimpleType, options = ['getl.xml.XMLDataset']) Closure cl = null) {
        xml(name, false, cl)
    }

    /** XML file */
    XMLDataset xml(@DelegatesTo(XMLDataset)
                   @ClosureParams(value = SimpleType, options = ['getl.xml.XMLDataset']) Closure cl) {
        xml(null, false, cl)
    }

    /** SalesForce connection */
    SalesForceConnection salesforceConnection(String name, Boolean registration,
                                              @DelegatesTo(SalesForceConnection)
                                              @ClosureParams(value = SimpleType, options = ['getl.salesforce.SalesForceConnection']) Closure cl) {
        def parent = registerConnection(SALESFORCECONNECTION, name, registration) as SalesForceConnection
        runClosure(parent, cl)

        return parent
    }

    /** SalesForce connection */
    SalesForceConnection salesforceConnection(String name,
                                              @DelegatesTo(SalesForceConnection)
                                              @ClosureParams(value = SimpleType, options = ['getl.salesforce.SalesForceConnection']) Closure cl = null) {
        salesforceConnection(name, false, cl)
    }

    /** SalesForce connection */
    SalesForceConnection salesforceConnection(@DelegatesTo(SalesForceConnection)
                                              @ClosureParams(value = SimpleType, options = ['getl.salesforce.SalesForceConnection']) Closure cl) {
        salesforceConnection(null, false, cl)
    }

    /** SalesForce default connection */
    SalesForceConnection salesforceConnection() {
        defaultOtherConnection(SALESFORCEDATASET) as SalesForceConnection
    }

    /** Use default SalesForce connection for new datasets */
    SalesForceConnection useSalesforceConnection(SalesForceConnection connection) {
        useOtherConnection(SALESFORCEDATASET, connection) as SalesForceConnection
    }

    /** SalesForce table */
    SalesForceDataset salesforce(String name, Boolean registration,
                                 @DelegatesTo(SalesForceDataset)
                                 @ClosureParams(value = SimpleType, options = ['getl.salesforce.SalesForceDataset']) Closure cl) {
        def parent = registerDataset(SALESFORCEDATASET, name, registration) as SalesForceDataset
        runClosure(parent, cl)

        return parent
    }

    /** SalesForce table */
    SalesForceDataset salesforce(String name,
                                 @DelegatesTo(SalesForceDataset)
                                 @ClosureParams(value = SimpleType, options = ['getl.salesforce.SalesForceDataset']) Closure cl = null) {
        salesforce(name, false, cl)
    }

    /** SalesForce table */
    SalesForceDataset salesforce(@DelegatesTo(SalesForceDataset)
                                 @ClosureParams(value = SimpleType, options = ['getl.salesforce.SalesForceDataset']) Closure cl) {
        salesforce(null, false, cl)
    }

    /** SalesForce query */
    SalesForceQueryDataset salesforceQuery(String name, Boolean registration,
                                           @DelegatesTo(SalesForceQueryDataset)
                                           @ClosureParams(value = SimpleType, options = ['getl.salesforce.SalesForceQueryDataset']) Closure cl) {
        def parent = registerDataset(SALESFORCEQUERYDATASET, name, registration) as SalesForceQueryDataset
        runClosure(parent, cl)

        return parent
    }

    /** SalesForce query */
    SalesForceQueryDataset salesforceQuery(String name,
                                           @DelegatesTo(SalesForceQueryDataset)
                                           @ClosureParams(value = SimpleType, options = ['getl.salesforce.SalesForceQueryDataset']) Closure cl = null) {
        salesforceQuery(name, false, cl)
    }

    /** SalesForce query */
    SalesForceQueryDataset salesforceQuery(@DelegatesTo(SalesForceQueryDataset)
                                           @ClosureParams(value = SimpleType, options = ['getl.salesforce.SalesForceQueryDataset']) Closure cl) {
        salesforceQuery(null, false, cl)
    }

    /** Xero connection */
    XeroConnection xeroConnection(String name, Boolean registration,
                                  @DelegatesTo(XeroConnection)
                                  @ClosureParams(value = SimpleType, options = ['getl.xero.XeroConnection']) Closure cl) {
        def parent = registerConnection(XEROCONNECTION, name, registration) as XeroConnection
        runClosure(parent, cl)

        return parent
    }

    /** Xero connection */
    XeroConnection xeroConnection(String name,
                                  @DelegatesTo(XeroConnection)
                                  @ClosureParams(value = SimpleType, options = ['getl.xero.XeroConnection']) Closure cl = null) {
        xeroConnection(name, false, cl)
    }

    /** Xero connection */
    XeroConnection xeroConnection(@DelegatesTo(XeroConnection)
                                  @ClosureParams(value = SimpleType, options = ['getl.xero.XeroConnection']) Closure cl) {
        xeroConnection(null, false, cl)
    }

    /** Xero default connection */
    XeroConnection xeroConnection() {
        defaultOtherConnection(XERODATASET) as XeroConnection
    }

    /** Use default Xero connection for new datasets */
    XeroConnection useXeroConnection(XeroConnection connection) {
        useOtherConnection(XERODATASET, connection) as XeroConnection
    }

    /** Xero table */
    XeroDataset xero(String name, Boolean registration,
                     @DelegatesTo(XeroDataset)
                     @ClosureParams(value = SimpleType, options = ['getl.xero.XeroDataset']) Closure cl) {
        def parent = registerDataset(XERODATASET, name, registration) as XeroDataset
        runClosure(parent, cl)

        return parent
    }

    /** Xero table */
    XeroDataset xero(String name,
                     @DelegatesTo(XeroDataset)
                     @ClosureParams(value = SimpleType, options = ['getl.xero.XeroDataset']) Closure cl = null) {
        xero(name, false, cl)
    }

    /** Xero table */
    XeroDataset xero(@DelegatesTo(XeroDataset)
                     @ClosureParams(value = SimpleType, options = ['getl.xero.XeroDataset']) Closure cl) {
        xero(null, false, cl)
    }

    /** Temporary CSV file connection */
    TFS csvTempConnection(String name, Boolean registration,
                          @DelegatesTo(TFS)
                          @ClosureParams(value = SimpleType, options = ['getl.tfs.TFS']) Closure cl) {
        def parent = registerConnection(CSVTEMPCONNECTION, name, registration) as TFS
        runClosure(parent, cl)

        return parent
    }

    /** Temporary CSV file connection */
    TFS csvTempConnection(String name,
                          @DelegatesTo(TFS)
                          @ClosureParams(value = SimpleType, options = ['getl.tfs.TFS']) Closure cl = null) {
        csvTempConnection(name, false, cl)
    }

    /** Temporary CSV file connection */
    TFS csvTempConnection(@DelegatesTo(TFS)
                          @ClosureParams(value = SimpleType, options = ['getl.tfs.TFS']) Closure cl) {
        csvTempConnection(null, false, cl)
    }

    /** Temporary CSV file current connection */
    TFS csvTempConnection() {
//        defaultFileConnection(CSVTEMPDATASET) as TFS
        TFS.storage
    }

    /** Use default CSV temporary connection for new datasets */
    TFS useCsvTempConnection(TFS connection = TFS.storage) {
        useFileConnection(CSVTEMPDATASET, connection) as TFS
    }

    /** Create CSV temporary dataset for dataset */
    void createCsvTemp(String name, Dataset dataset) {
        if (name ==  null) throw new ExceptionGETL('Name cannot be null!')
        if (dataset ==  null) throw new ExceptionGETL('Dataset cannot be null!')
        TFSDataset csvTemp = dataset.csvTempFile
        registerDatasetObject(csvTemp, name, true)
    }

    /** Temporary CSV file */
    TFSDataset csvTemp(String name, Boolean registration,
                       @DelegatesTo(TFSDataset)
                       @ClosureParams(value = SimpleType, options = ['getl.tfs.TFSDataset']) Closure cl) {
        TFSDataset parent = registerDataset(defaultFileConnection(CSVTEMPDATASET)?:TFS.storage, CSVTEMPDATASET, name, registration) as TFSDataset
        runClosure(parent, cl)

        return parent
    }

    /** Temporary CSV file */
    TFSDataset csvTemp(String name,
                       @DelegatesTo(TFSDataset)
                       @ClosureParams(value = SimpleType, options = ['getl.tfs.TFSDataset']) Closure cl = null) {
        csvTemp(name, false, cl)
    }

    /** Temporary CSV file */
    TFSDataset csvTemp(@DelegatesTo(TFSDataset)
                       @ClosureParams(value = SimpleType, options = ['getl.tfs.TFSDataset']) Closure cl) {
        csvTemp(null, false, cl)
    }

    /** Temporary CSV file */
    TFSDataset csvTempWithDataset(String name, Dataset sourceDataset,
                                  @DelegatesTo(TFSDataset)
                                  @ClosureParams(value = SimpleType, options = ['getl.tfs.TFSDataset']) Closure cl = null) {
        if (sourceDataset == null) throw new ExceptionGETL("Dataset cannot be null!")
        def parent = sourceDataset.csvTempFile.cloneDataset() as TFSDataset
        parent.connection = defaultFileConnection(CSVTEMPDATASET)?:TFS.storage
        registerDatasetObject(parent, name, true)
        runClosure(parent, cl)

        return parent
    }

    /** Temporary CSV file */
    TFSDataset csvTempWithDataset(Dataset sourceDataset,
                                  @DelegatesTo(TFSDataset)
                                  @ClosureParams(value = SimpleType, options = ['getl.tfs.TFSDataset']) Closure cl = null) {
        csvTempWithDataset(null, sourceDataset, cl)
    }

    /**
     * Copy rows from source to destination dataset
     * <br>Closure gets two parameters: source and destination datasets
     */
    void copyRows(Dataset source, Dataset destination,
                  @DelegatesTo(FlowCopySpec)
                  @ClosureParams(value = SimpleType, options = ['getl.proc.opts.FlowCopySpec']) Closure cl = null) {
        if (source == null)
            throw new ExceptionGETL('Source dataset cannot be null!')
        if (destination == null)
            throw new ExceptionGETL('Destination dataset cannot be null!')

        def pt = startProcess("Copy rows from $source to $destination")
        def parent = new FlowCopySpec(childOwnerObject, childThisObject, false, null)
        parent.source = source
        parent.destination = destination
        parent.runClosure(cl)
        if (!parent.isProcessed) parent.copyRow(null)
        finishProcess(pt, parent.countRow)
    }

    /** Write rows to destination dataset */
    void rowsTo(Dataset destination,
                @DelegatesTo(FlowWriteSpec)
                @ClosureParams(value = SimpleType, options = ['getl.proc.opts.FlowWriteSpec']) Closure cl) {
        if (destination == null)
            throw new ExceptionGETL('Destination dataset cannot be null!')
        if (cl == null)
            throw new ExceptionGETL('Required closure code!')

        def pt = startProcess("Write rows to $destination")
        def parent = new FlowWriteSpec(childOwnerObject, childThisObject, false, null)
        parent.destination = destination
        parent.runClosure(cl)
        finishProcess(pt, parent.countRow)
    }

    /** Write rows to destination dataset */
    void rowsTo(@DelegatesTo(FlowWriteSpec)
                @ClosureParams(value = SimpleType, options = ['getl.proc.opts.FlowWriteSpec']) Closure cl) {
        if (cl == null)
            throw new ExceptionGETL('Required closure code!')
        def destination = DetectClosureDelegate(cl)
        if (destination == null || !(destination instanceof Dataset))
            throw new ExceptionGETL('Can not detect destination dataset!')

        rowsTo(destination, cl)
    }

    /** Write rows to many destination datasets */
    void rowsToMany(Map destinations,
                    @DelegatesTo(FlowWriteManySpec)
                    @ClosureParams(value = SimpleType, options = ['getl.proc.opts.FlowWriteManySpec']) Closure cl) {
        if (destinations == null || destinations.isEmpty())
            throw new ExceptionGETL('Destination datasets cannot be null or empty!')
        if (cl == null)
            throw new ExceptionGETL('Required closure code!')

        def destNames = [] as List<String>
        (destinations as Map<String, Dataset>).each { destName, ds -> destNames.add("$destName: ${ds.toString()}".toString())}
        def pt = startProcess("Write rows to $destNames")
        def parent = new FlowWriteManySpec(childOwnerObject, childThisObject, false, null)
        parent.destinations = destinations
        parent.runClosure(cl)
        finishProcess(pt)
    }

    /** Process rows from source dataset */
    void rowProcess(Dataset source,
                    @DelegatesTo(FlowProcessSpec)
                    @ClosureParams(value = SimpleType, options = ['getl.proc.opts.FlowProcessSpec']) Closure cl) {
        if (source == null)
            throw new ExceptionGETL('Source dataset cannot be null!')
        if (cl == null)
            throw new ExceptionGETL('Required closure code!')
        def pt = startProcess("Read rows from $source")
        def parent = new FlowProcessSpec(childOwnerObject, childThisObject, false, null)
        parent.source = source
        parent.runClosure(cl)
        finishProcess(pt, parent.countRow)
    }

    /** Process rows from source dataset */
    void rowProcess(@DelegatesTo(FlowProcessSpec)
                    @ClosureParams(value = SimpleType, options = ['getl.proc.opts.FlowProcessSpec']) Closure cl) {
        if (cl == null)
            throw new ExceptionGETL('Required closure code!')
        def source = DetectClosureDelegate(cl)
        if (source == null || !(source instanceof Dataset))
            throw new ExceptionGETL('Can not detect source dataset!')
        rowProcess(source, cl)
    }

    /** SQL scripter */
    SQLScripter sql(JDBCConnection connection,
                    @DelegatesTo(SQLScripter)
                    @ClosureParams(value = SimpleType, options = ['getl.jdbc.SQLScripter']) Closure cl) {
        def parent = new SQLScripter()
        parent.connection = connection?:defaultJdbcConnection()
        parent.extVars = configContent
        def pt = startProcess("Execution SQL script${(parent.connection != null)?' on [' + parent.connection + ']':''}")
        runClosure(parent, cl)
        finishProcess(pt, parent.rowCount)

        return parent
    }

    /** SQL scripter */
    SQLScripter sql(@DelegatesTo(SQLScripter)
                    @ClosureParams(value = SimpleType, options = ['getl.jdbc.SQLScripter']) Closure cl) {
        sql(null, cl)
    }

    /** Exist file manager */
    Manager filemanager(String name,
                        @DelegatesTo(Manager)
                        @ClosureParams(value = SimpleType, options = ['getl.files.Manager']) Closure cl = null) {
        if (name == null) throw new ExceptionGETL('Need file manager name value!')

        def parent = registerFileManager(null, name, false) as Manager
        runClosure(parent, cl)

        return parent
    }

    /**
     * Clone file manager object
     * @param obj object to clone
     * @return clone object
     */
    Manager cloneFilemanager(Manager obj) {
        if (obj == null) throw new ExceptionGETL('Need object value!')
        return obj.cloneManager()
    }

    /** Process local file system */
    FileManager files(String name, Boolean registration,
                      @DelegatesTo(FileManager)
                      @ClosureParams(value = SimpleType, options = ['getl.files.FileManager']) Closure cl) {
        def parent = registerFileManager(FILEMANAGER, name, registration) as FileManager
        if (parent.rootPath == null) parent.rootPath = new File('.').absolutePath
        if (parent.localDirectory == null) parent.localDirectory = TFS.storage.path
        if (cl != null) {
            def pt = startProcess("Do commands on [$parent]")
            try {
                runClosure(parent, cl)
            }
            finally {
                if (parent.connected) parent.disconnect()
            }
            pt.name = "Do commands on [$parent]"
            finishProcess(pt)
        }

        return parent
    }

    /** Process local file system */
    FileManager files(String name, @DelegatesTo(FileManager)
    @ClosureParams(value = SimpleType, options = ['getl.files.FileManager']) Closure cl = null) {
        files(name, false, cl)
    }

    /** Process local file system */
    FileManager files(@DelegatesTo(FileManager)
                      @ClosureParams(value = SimpleType, options = ['getl.files.FileManager']) Closure cl) {
        files(null, false, cl)
    }

    /** Process ftp file system */
    FTPManager ftp(String name, Boolean registration,
                   @DelegatesTo(FTPManager)
                   @ClosureParams(value = SimpleType, options = ['getl.files.FTPManager']) Closure cl) {
        def parent = registerFileManager(FTPMANAGER, name, registration) as FTPManager
        if (parent.localDirectory == null) parent.localDirectory = TFS.storage.path
        if (cl != null) {
            def pt = startProcess("Do commands on [$parent]")
            try {
                runClosure(parent, cl)
            }
            finally {
                parent.disconnect()
            }
            pt.name = "Do commands on [$parent]"
            finishProcess(pt)
        }

        return parent
    }

    /** Process ftp file system */
    FTPManager ftp(String name,
                   @DelegatesTo(FTPManager)
                   @ClosureParams(value = SimpleType, options = ['getl.files.FTPManager']) Closure cl = null) {
        ftp(name, false, cl)
    }

    /** Process ftp file system */
    FTPManager ftp(@DelegatesTo(FTPManager)
                   @ClosureParams(value = SimpleType, options = ['getl.files.FTPManager']) Closure cl) {
        ftp(null, false, cl)
    }

    /** Process sftp file system */
    SFTPManager sftp(String name, Boolean registration,
                     @DelegatesTo(SFTPManager)
                     @ClosureParams(value = SimpleType, options = ['getl.files.SFTPManager']) Closure cl) {
        def parent = registerFileManager(SFTPMANAGER, name, registration) as SFTPManager
        if (parent.localDirectory == null) parent.localDirectory = TFS.storage.path
        if (cl != null) {
            def pt = startProcess("Do commands on [$parent]")
            try {
                runClosure(parent, cl)
            }
            finally {
                parent.disconnect()
            }
            pt.name = "Do commands on [$parent]"
            finishProcess(pt)
        }

        return parent
    }

    /** Process sftp file system */
    SFTPManager sftp(String name,
                     @DelegatesTo(SFTPManager)
                     @ClosureParams(value = SimpleType, options = ['getl.files.SFTPManager']) Closure cl = null) {
        sftp(name, false, cl)
    }

    /** Process sftp file system */
    SFTPManager sftp(@DelegatesTo(SFTPManager)
                     @ClosureParams(value = SimpleType, options = ['getl.files.SFTPManager']) Closure cl) {
        sftp(null, false, cl)
    }

    /** Process sftp file system */
    HDFSManager hdfs(String name, Boolean registration,
                     @DelegatesTo(HDFSManager)
                     @ClosureParams(value = SimpleType, options = ['getl.files.HDFSManager']) Closure cl) {
        def parent = registerFileManager(HDFSMANAGER, name, registration) as HDFSManager
        if (parent.localDirectory == null) parent.localDirectory = TFS.storage.path
        if (cl != null) {
            def pt = startProcess("Do commands on [$parent]")
            try {
                runClosure(parent, cl)
            }
            finally {
                parent.disconnect()
            }
            pt.name = "Do commands on [$parent]"
            finishProcess(pt)
        }

        return parent
    }

    /** Process sftp file system */
    HDFSManager hdfs(String name,
                     @DelegatesTo(HDFSManager)
                     @ClosureParams(value = SimpleType, options = ['getl.files.HDFSManager']) Closure cl = null) {
        hdfs(name, false, cl)
    }

    /** Process sftp file system */
    HDFSManager hdfs(@DelegatesTo(HDFSManager)
                     @ClosureParams(value = SimpleType, options = ['getl.files.HDFSManager']) Closure cl) {
        hdfs(null, false, cl)
    }

    /** Run code in multithread mode */
    Executor thread(@DelegatesTo(Executor)
                    @ClosureParams(value = SimpleType, options = ['getl.proc.Executor']) Closure cl) {
        def disposeConnections = { Map<String, List<ExecutorThread.CloneObject>> list ->
            (list?.connections as List<ExecutorThread.CloneObject>)?.each { ExecutorThread.CloneObject cloneObject ->
                def con = cloneObject.cloneObject as Connection
                if (con != null && con.driver.isSupport(Driver.Support.CONNECT)) con.connected = false
            }

            (list?.filemanagers as List<ExecutorThread.CloneObject>)?.each { ExecutorThread.CloneObject cloneObject ->
                def man = cloneObject.cloneObject as FileManager
                if (man != null) man.connected = false
            }
        }

        def parent = new Executor()
        parent.disposeThreadResource(disposeConnections)
        def pt = startProcess('Execution threads')
        runClosure(parent, cl)
        finishProcess(pt)

        return parent
    }

    /** Run code in multithread mode */
    EMailer mail(@DelegatesTo(EMailer)
                 @ClosureParams(value = SimpleType, options = ['getl.utils.EMailer']) Closure cl) {
        def parent = new EMailer()
        def pt = startProcess('Mailer')
        runClosure(parent, cl)
        finishProcess(pt)

        return parent
    }

    /**
     * Processing text file
     * @param file file object or string file name
     * @cl process code
     */
    String textFile(def file,
                    @DelegatesTo(FileTextSpec)
                    @ClosureParams(value = SimpleType, options = ['getl.lang.opts.FileTextSpec']) Closure cl) {
        def parent = new FileTextSpec(childOwnerObject, childThisObject, false, null)
        if (file != null) {
            parent.fileName = (file instanceof File)?((file as File).path):file.toString()
        }
        def pt = startProcess("Processing text file${(parent.fileName != null)?(' "' + parent.fileName + '"'):''}")
        pt.objectName = 'byte'
        parent.runClosure(cl)
        parent.save()
        pt.name = "Processing text file${(parent.fileName != null)?(' "' + parent.fileName + '"'):''}"
        finishProcess(pt, parent.countBytes)

        return parent.fileName
    }

    /** Processing text file */
    String textFile(@DelegatesTo(FileTextSpec)
                    @ClosureParams(value = SimpleType, options = ['getl.lang.opts.FileTextSpec']) Closure cl) {
        textFile(null, cl)
    }

    /** File path parser */
    Path filePath(@DelegatesTo(Path)
                  @ClosureParams(value = SimpleType, options = ['getl.utils.Path']) Closure cl) {
        def parent = new Path()
        parent.sysParams.dslThisObject = childThisObject
        parent.sysParams.dslOwnerObject = childOwnerObject
        runClosure(parent, cl)

        return parent
    }

    /** File path parser */
    @SuppressWarnings("GrMethodMayBeStatic")
    Path filePath(String mask) {
        return new Path(mask: mask)
    }

    /**
     * Incremenal history point manager
     */
    SavePointManager historypoint(String name = null, Boolean registration = false,
                                  @DelegatesTo(SavePointManager)
                                  @ClosureParams(value = SimpleType, options = ['getl.jdbc.SavePointManager']) Closure cl = null) {
        def parent = registerHistoryPoint(name, registration) as SavePointManager
        runClosure(parent, cl)

        return parent
    }

    /**
     * Incremenal history point manager
     */
    SavePointManager historypoint(String name,
                                  @DelegatesTo(SavePointManager)
                                  @ClosureParams(value = SimpleType, options = ['getl.jdbc.SavePointManager']) Closure cl) {
        historypoint(name, false, cl)
    }

    /** Incremenal history point manager */
    SavePointManager historypoint(@DelegatesTo(SavePointManager)
                                  @ClosureParams(value = SimpleType, options = ['getl.jdbc.SavePointManager']) Closure cl) {
        historypoint(null, false, cl)
    }

    /**
     * Clone history point manager object
     * @param obj object to clone
     * @param con used connection for new history point manager
     * @return clone object
     */
    SavePointManager cloneHistorypoint(SavePointManager obj, Connection con = null) {
        if (obj == null) throw new ExceptionGETL('Need object value!')
        return obj.cloneSavePointManager(con)
    }

    /** Copying files according to the specified rules */
    FileCopier fileCopier(Manager source, Manager destination,
                          @DelegatesTo(FileCopier)
                          @ClosureParams(value = SimpleType, options = ['getl.proc.FileCopier']) Closure cl) {
        if (source == null) throw new ExceptionGETL('Source file manager cannot be null!')
        if (destination == null) throw new ExceptionGETL('Destination file manager cannot be null!')
        def parent = new FileCopier()
        parent.sysParams.dslThisObject = childThisObject
        parent.sysParams.dslOwnerObject = childOwnerObject
        parent.source = source
        parent.destination = destination
        runClosure(parent, cl)

        def pt = startProcess("Copy files from [$source] to [$destination]")
        try {
            parent.copy()
        }
        finally {
            try {
                source.disconnect()
            }
            finally {
                destination.disconnect()
            }
        }
        finishProcess(pt, parent.countFiles)

        return parent
    }

    /** Test case instance */
    protected GroovyTestCase _testCase

    /** Run test case code */
    GroovyTestCase testCase(@DelegatesTo(GroovyTestCase)
                            @ClosureParams(value = SimpleType, options = ['junit.framework.TestCase']) Closure cl) {
        def parent = _testCase?:new GroovyTestCase()
        RunClosure(cl.delegate, this, parent, cl)
        return parent
    }
}