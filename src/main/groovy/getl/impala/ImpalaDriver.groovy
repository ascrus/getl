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

package getl.impala

import getl.data.Dataset
import getl.data.Field
import getl.driver.Driver
import getl.exception.ExceptionGETL
import getl.jdbc.JDBCDataset
import getl.jdbc.JDBCDriver
import getl.jdbc.TableDataset
import getl.utils.BoolUtils
import getl.utils.Logs
import groovy.transform.InheritConstructors

/**
 * Impala driver class
 * @author Alexsey Konstantinov
 */
@InheritConstructors
class ImpalaDriver extends JDBCDriver {
    ImpalaDriver() {
        super()

        tablePrefix = '`'
        fieldPrefix = '`'

        caseObjectName = "LOWER"
        connectionParamBegin = ';'
        connectionParamJoin = ';'

        syntaxPartitionKeyInColumns = false

        defaultTransactionIsolation = java.sql.Connection.TRANSACTION_READ_UNCOMMITTED

        methodParams.register('createDataset',
                ['sortBy', 'rowFormat', 'storedAs', 'location', 'tblproperties', 'serdeproperties',
                 'fieldsTerminated', 'escapedBy', 'linesTerminatedBy', 'select'])
        methodParams.register('openWrite', ['overwrite'])
        methodParams.register('bulkLoadFile', ['overwrite', 'hdfsHost', 'hdfsPort', 'hdfsLogin',
                                               'hdfsDir', 'processRow', 'expression'])
    }

    @SuppressWarnings("UnnecessaryQualifiedReference")
    @Override
    List<Driver.Support> supported() {
        return super.supported() +
                [Driver.Support.BOOLEAN, Driver.Support.CREATEIFNOTEXIST, Driver.Support.DROPIFEXIST,
                 Driver.Support.EXTERNAL, Driver.Support.BULKLOADMANYFILES] -
                [Driver.Support.PRIMARY_KEY, Driver.Support.NOT_NULL_FIELD,
                 Driver.Support.DEFAULT_VALUE, Driver.Support.COMPUTE_FIELD]
    }

    @SuppressWarnings("UnnecessaryQualifiedReference")
    @Override
    List<Driver.Operation> operations() {
        return super.operations() +
                [Driver.Operation.CLEAR, Driver.Operation.DROP, Driver.Operation.EXECUTE, Driver.Operation.CREATE/*,
                 Driver.Operation.BULKLOAD*/] -
                [Driver.Operation.READ_METADATA, Driver.Operation.UPDATE, Driver.Operation.DELETE]
    }

    @Override
    String defaultConnectURL () {
        return 'jdbc:impala://{host}/{database}'
    }

    @SuppressWarnings("UnnecessaryQualifiedReference")
    @Override
    Map getSqlType () {
        Map res = super.getSqlType()
        res.STRING.name = 'string'
        res.STRING.useLength = JDBCDriver.sqlTypeUse.NEVER
        res.DOUBLE.name = 'double'
        /*res.BLOB.name = 'binary'
        res.BLOB.useLength = JDBCDriver.sqlTypeUse.NEVER*/

        return res
    }

    /** Impala connection */
    ImpalaConnection getImpalaConnection() { connection as ImpalaConnection } /* TODO: added to all drivers */

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

        def sortBy = params.sortBy as List<String>
        if (sortBy != null && !(sortBy.isEmpty())) {
            sb << "SORT BY (${sortBy.join(', ')})"
            sb << '\n'
        }

        if (params.rowFormat != null) {
            sb << "ROW FORMAT ${params.rowFormat}"
            sb << '\n'
        }

        if (params.fieldsTerminated != null) {
            sb << "FIELDS TERMINATED BY '${params.fieldsTerminated}'"
            sb << '\n'
        }

        if (params.escapedBy != null) {
            sb << "ESCAPED BY '${params.escapedBy}'"
            sb << '\n'
        }

        if (params.linesTerminatedBy != null) {
            sb << "LINES TERMINATED BY '${params.linesTerminatedBy}'"
            sb << '\n'
        }

        if (params.serdeproperties != null) {
            if (!(params.serdeproperties instanceof Map)) throw new ExceptionGETL('Required map type for parameter "serdeproperties"')
            def serdeproperties = params.serdeproperties as Map
            if (!serdeproperties.isEmpty()) {
                def props = [] as List<String>
                serdeproperties.each { k, v ->
                    props << "\"$k\"=\"$v\"".toString()
                }
                sb << "WITH SERDEPROPERTIES(${props.join(', ')})"

                sb << '\n'
            }
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
            if (!tblproperties.isEmpty()) {
                def props = [] as List<String>
                tblproperties.each { k, v ->
                    props << "\"$k\"=\"$v\"".toString()
                }
                sb << "TBLPROPERTIES(${props.join(', ')})"

                sb << '\n'
            }
        }

        if (params.select != null) {
            sb << "AS"
            sb << '\n'
            sb << "${params.select}"

            sb << '\n'
        }

        return sb.toString()
    }

    @Override
    protected String syntaxInsertStatement(Dataset dataset, Map params) {
        String into = (BoolUtils.IsValue([params.overwrite, (dataset as TableDataset).params.overwrite]))?'OVERWRITE':'INTO'
        return ((dataset.fieldListPartitions.isEmpty()))?
                "INSERT $into TABLE {table} ({columns}) VALUES({values})":
                "INSERT $into TABLE {table} ({columns}) PARTITION ({partition}) VALUES({values})"
    }

    @groovy.transform.CompileStatic
    protected void saveBatch (Dataset dataset, JDBCDriver.WriterParams wp) {
        try {
            super.saveBatch(dataset, wp)
        }
        catch (AssertionError e) {
            Logs.Dump(e, getClass().name, dataset.toString(), "operation:${wp.operation}, batch size: ${wp.batchSize}, query:\n${wp.query}\n\nstatement: ${wp.statement}")
            throw e
        }
    }

    @Override
    String getSysDualTable() { impalaConnection.dualTable?:'(SELECT 1 AS row_num) AS x' }

    @Override
    protected void initOpenWrite(JDBCDataset dataset, Map params, String query) {
        super.initOpenWrite(dataset, params, query)
        def compression = (params.compression as String)?.toLowerCase()
        if (compression == null) compression = 'none'
        dataset.currentJDBCConnection.executeCommand("set COMPRESSION_CODEC=$compression;")
    }

    /* TODO: Where bulk load? */
}