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

package getl.hive

import getl.csv.CSVDataset
import getl.data.Dataset
import getl.data.Field
import getl.driver.Driver
import getl.exception.ExceptionGETL
import getl.jdbc.JDBCDriver
import getl.utils.BoolUtils

/**
 * Hive driver class
 * @author Alexsey Konstantinov
 */
class HiveDriver extends JDBCDriver {
    HiveDriver() {
        super()

        tablePrefix = ''
        fieldPrefix = ''

        connectionParamBegin = ';'

        allowLocalTemporaryTable = true
        localTemporaryTablePrefix = 'TEMPORARY'

        defaultTransactionIsolation = java.sql.Connection.TRANSACTION_READ_UNCOMMITTED

        methodParams.register('createDataset',
                ['clustered', 'skewed', 'rowFormat', 'storedAs', 'location',
                 'tblproperties', 'select'])
    }

    @Override
    public List<Driver.Operation> operations() {
        return super.operations() +
                [Driver.Operation.CLEAR, Driver.Operation.DROP, Driver.Operation.EXECUTE, Driver.Operation.CREATE]
    }

    @Override
    public List<Driver.Support> supported() {
        return super.supported() +
                [Driver.Support.BATCH, Driver.Support.WRITE, Driver.Support.TRANSACTIONAL,
                 Driver.Support.BLOB]
    }

    @Override
    public String defaultConnectURL () {
        return 'jdbc:hive2://{host}/{database}'
    }

    @Override
    public Map getSqlType () {
        Map res = super.getSqlType()
        res.STRING.name = 'string'
        res.STRING.useLength = JDBCDriver.sqlTypeUse.SOMETIMES
        res.BLOB.name = 'binary'

        return res
    }

    @Override
    protected String createDatasetAddColumn(Field f, boolean useNativeDBType) {
        return (!f.isPartition)?super.createDatasetAddColumn(f, useNativeDBType):null
    }

    @Override
    protected String createDatasetExtend(Dataset dataset, Map params) {
        def sb = new StringBuilder()

        def partitionFields = [] as List<Field>
        dataset.field.each { Field f ->
            if (f.isPartition) partitionFields << f
        }

        if (!partitionFields.isEmpty()) {
            partitionFields.sort(true) { Field a, Field b -> (a.ordPartition?:999999999) <=> (b.ordPartition?:999999999) }
            def partitionCols = [] as List<String>
            partitionFields.each { Field f ->
                partitionCols << generateColumnDefinition(f, false)
            }
            sb << "PARTITIONED BY (${partitionCols.join(', ')})"
            sb << '\n'
        }

        if (params.clustered != null) {
            if (!(params.clustered instanceof Map)) throw new ExceptionGETL('Required map type for parameter "clustered"')
            def clustered = params.clustered as Map

            def allowed = ['by', 'sortedBy', 'intoBuckets']
            def unknown = clustered.keySet().toList() - allowed
            if (!unknown.isEmpty()) throw new ExceptionGETL("Found unknown parameter for \"clustered\" option: $unknown")

            if (clustered.by == null) throw new ExceptionGETL('Required value for parameter "clustered.by"')
            if (!(clustered.by instanceof List)) throw new ExceptionGETL('Required list type for parameter "clustered.by"')
            def by = clustered.by as List
            sb << "CLUSTERED BY (${by.join(', ')})"

            if (clustered.sorterBy != null) {
                if (!(clustered.sorterBy instanceof List)) throw new ExceptionGETL('Required list type for parameter "clustered.sorterBy"')
                def sortedBy = clustered.sorterBy as List
                sb << " SORTED BY (${sortedBy.join(', ')})"
            }

            if (clustered.intoBuckets != null) {
                if (!(clustered.intoBuckets instanceof Integer)) throw new ExceptionGETL('Required integer type for parameter "clustered.intoBuckets"')
                def intoBuckets = clustered.intoBuckets as Integer
                sb << " INTO $intoBuckets BUCKETS"
            }

            sb << '\n'
        }

        if (params.skewed != null) {
            if (!(params.skewed instanceof Map)) throw new ExceptionGETL('Required map type for parameter "skewed"')
            def skewed = params.skewed as Map

            def allowed = ['by', 'on', 'storedAsDirectories']
            def unknown = skewed.keySet().toList() - allowed
            if (!unknown.isEmpty()) throw new ExceptionGETL("Found unknown parameter for \"skewed\" option: $unknown")

            if (skewed.by == null) throw new ExceptionGETL('Required value for parameter "skewed.by"')
            if (!(skewed.by instanceof List)) throw new ExceptionGETL('Required list type for parameter "skewed.by"')
            def by = skewed.by as List
            sb << "SKEWED BY (${by.join(', ')})"

            if (skewed.on == null) throw new ExceptionGETL('Required value for parameter "skewed.on"')
            if (!(skewed.on instanceof List)) throw new ExceptionGETL('Required list type for parameter "skewed.on"')
            def on = skewed.on as List
            def onCols = [] as List<String>
            on.each { onCol ->
                if (!(onCol instanceof List)) throw new ExceptionGETL('Required list type for item by "skewed.on"')
                onCols << "(${(onCol as List).join(', ')})"
            }
            sb << " ON ${onCols.join(', ')}"

            if (BoolUtils.IsValue(skewed.storedAsDirectories)) sb << ' STORED AS DIRECTORIES'

            sb << '\n'
        }

        if (params.rowFormat != null) {
            sb << "ROW FORMAT ${params.rowFormat}"
            sb << '\n'
        }

        if (params.storedAs != null) {
            sb << "STORED AS ${params.storedAs}"

            sb << '\n'
        }

        if (params.location != null) {
            sb << "LOCATION '${params.location}'"

            sb << '\n'
        }

        if (params.tblproperties != null) {
            if (!(params.tblproperties instanceof Map)) throw new ExceptionGETL('Required map type for parameter "tblproperties"')
            def tblproperties = params.tblproperties as Map
            def props = [] as List<String>
            tblproperties.each { k, v ->
                props << "\"$k\"=\"$v\""
            }
            sb << "TBLPROPERTIES(${props.join(', ')})"

            sb << '\n'
        }

        if (params.select != null) {
            sb << "AS"
            sb << '\n'
            sb << "${params.select}"

            sb << '\n'
        }

        return sb.toString()
    }

    /*
    @Override
    public void bulkLoadFile(CSVDataset source, Dataset dest, Map bulkParams, Closure prepareCode) {

    }*/


}