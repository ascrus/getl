//file:noinspection unused
package getl.lang.opts

import getl.data.Dataset
import getl.data.Field
import getl.exception.DatasetError
import getl.exception.DslError
import getl.jdbc.TableDataset
import getl.lang.Getl
import getl.proc.opts.FlowCopySpec
import getl.proc.opts.FlowProcessSpec
import getl.proc.opts.FlowWriteManySpec
import getl.proc.opts.FlowWriteSpec
import getl.stat.ProcessTime
import groovy.transform.InheritConstructors
import groovy.transform.stc.ClosureParams
import groovy.transform.stc.SimpleType

/** Etl specification manager */
@InheritConstructors
class EtlSpec extends BaseSpec {
    /** Getl instance */
    protected Getl getGetl() { _owner as Getl }

    /**
     * Copy rows from source to destination dataset
     * @param sourceName source name dataset
     * @param destinationName destination name dataset
     * @param cl processing closure code
     * @return operation result
     */
    FlowCopySpec copyRows(String sourceName, String destinationName,
                          @DelegatesTo(FlowCopySpec)
                          @ClosureParams(value = SimpleType, options = ['getl.proc.opts.FlowCopySpec']) Closure cl = null) {
        copyRows(getl.dataset(sourceName), getl.dataset(destinationName), cl)
    }

    /**
     * Copy rows from source to destination dataset
     * @param source source dataset
     * @param destination destination dataset
     * @param cl processing closure code
     * @return operation result
     */
    FlowCopySpec copyRows(Dataset source, Dataset destination,
                  @DelegatesTo(FlowCopySpec)
                  @ClosureParams(value = SimpleType, options = ['getl.proc.opts.FlowCopySpec']) Closure cl = null) {
        if (source == null)
            throw new DslError(getl,'#params.required', [param: 'source', detail: 'etl.copyRows'])
        if (destination == null)
            if (source == null)
                throw new DslError(getl,'#params.required', [param: 'destination', detail: 'etl.copyRows'])

        ProcessTime pt = getGetl().startProcess("Copy rows from \"$source\" to \"$destination\"")
        def parent = new FlowCopySpec(getl)
        parent.source = source
        parent.destination = destination
        runClosure(parent, cl)
        if (!parent.isProcessed)
            parent.copyRow(null)
        getGetl().finishProcess(pt, parent.countRow)

        return parent
    }

    /**
     * Write rows to destination dataset
     * @param destinationName destination name dataset
     * @param cl processing closure code
     * @return operation result
     */
    FlowWriteSpec rowsTo(String destinationName,
                         @DelegatesTo(FlowWriteSpec)
                         @ClosureParams(value = SimpleType, options = ['getl.proc.opts.FlowWriteSpec']) Closure cl) {
        rowsTo(getl.dataset(destinationName), cl)
    }

    /**
     * Write rows to destination dataset
     * @param destination destination dataset
     * @param cl processing closure code
     * @return operation result
     */
    FlowWriteSpec rowsTo(Dataset destination,
                @DelegatesTo(FlowWriteSpec)
                @ClosureParams(value = SimpleType, options = ['getl.proc.opts.FlowWriteSpec']) Closure cl) {
        if (destination == null)
            throw new DslError(getl,'#params.required', [param: 'destination', detail: 'etl.rowsTo'])
        if (cl == null)
            throw new DslError(getl,'#params.required', [param: 'closure code', detail: 'etl.rowsTo'])

        def pt = getGetl().startProcess("Write rows to \"$destination\"")
        def parent = new FlowWriteSpec(getl)
        parent.destination = destination
        runClosure(parent, cl)
        getGetl().finishProcess(pt, parent.countRow)

        return parent
    }

    /**
     * Write rows to destination dataset
     * @param cl processing closure code
     * @return operation result
     */
    FlowWriteSpec rowsTo(@DelegatesTo(FlowWriteSpec)
                @ClosureParams(value = SimpleType, options = ['getl.proc.opts.FlowWriteSpec']) Closure cl) {
        if (cl == null)
            throw new DslError(getl,'#params.required', [param: 'closure code', detail: 'etl.rowsTo'])
        def destination = DetectClosureDelegate(cl, true)
        if (destination == null || !(destination instanceof Dataset))
            throw new DslError(getl,'#params.required', [param: 'destination', detail: 'etl.rowsTo'])

        return rowsTo(destination, cl)
    }

    /**
     * Write rows to many destination datasets
     * @param destinations list of destination datasets (name: dataset)
     * @param cl processing closure code
     * @return operation result
     */
    FlowWriteManySpec rowsToMany(Map<String, Dataset> destinations,
                    @DelegatesTo(FlowWriteManySpec)
                    @ClosureParams(value = SimpleType, options = ['getl.proc.opts.FlowWriteManySpec']) Closure cl) {
        if (destinations == null || destinations.isEmpty())
            throw new DslError(getl,'#params.required', [param: 'destination', detail: 'etl.rowsToMany'])
        if (cl == null)
            throw new DslError(getl,'#params.required', [param: 'closure code', detail: 'etl.rowsToMany'])

        def destNames = [] as List<String>
        (destinations as Map<String, Dataset>).each { destName, ds -> destNames.add("$destName: ${ds.toString()}".toString()) }
        def pt = getGetl().startProcess("Write rows to $destNames")
        def parent = new FlowWriteManySpec(getl)
        parent.destinations = destinations
        runClosure(parent, cl)
        getGetl().finishProcess(pt)

        return parent
    }

    /**
     * Process rows from source dataset
     * @param sourceName source name dataset
     * @param cl processing closure code
     * @return operation result
     */
    @SuppressWarnings('unused')
    FlowProcessSpec rowsProcess(String sourceName,
                                @DelegatesTo(FlowProcessSpec)
                                @ClosureParams(value = SimpleType, options = ['getl.proc.opts.FlowProcessSpec']) Closure cl) {
        rowsProcess(getl.dataset(sourceName), cl)
    }

    /**
     * Process rows from source dataset
     * @param source source dataset
     * @param cl processing closure code
     * @return operation result
     */
    FlowProcessSpec rowsProcess(Dataset source,
                     @DelegatesTo(FlowProcessSpec)
                     @ClosureParams(value = SimpleType, options = ['getl.proc.opts.FlowProcessSpec']) Closure cl) {
        if (source == null)
            throw new DslError(getl,'#params.required', [param: 'source', detail: 'etl.rowsProcess'])
        if (cl == null)
            throw new DslError(getl,'#params.required', [param: 'closure code', detail: 'etl.rowsProcess'])
        def pt = getGetl().startProcess("Read rows from \"$source\"")
        def parent = new FlowProcessSpec(getl)
        parent.source = source
        runClosure(parent, cl)
        getGetl().finishProcess(pt, parent.countRow)

        return parent
    }

    /**
     * Process rows from source dataset
     * @return operation result
     */
    FlowProcessSpec rowsProcess(@DelegatesTo(FlowProcessSpec)
                     @ClosureParams(value = SimpleType, options = ['getl.proc.opts.FlowProcessSpec']) Closure cl) {
        if (cl == null)
            throw new DslError(getl,'#params.required', [param: 'closure code', detail: 'etl.rowsProcess'])
        def source = DetectClosureDelegate(cl, true)
        if (source == null || !(source instanceof Dataset))
            throw new DslError(getl,'#params.required', [param: 'source', detail: 'etl.rowsProcess'])

        return rowsProcess(source, cl)
    }

    /**
     * Add row to dataset
     * @param dataset destination dataset
     * @param row row to add with field values
     */
    void addRow(Dataset dataset, Map<String, Object> row) {
        if (dataset == null)
            throw new DslError(getl,'#params.required', [param: 'dataset', detail: 'etl.addRow'])
        if (row == null || row.isEmpty())
            throw new DslError(getl,'#params.required', [param: 'row', detail: 'etl.addRow'])

        def pt = getGetl().startProcess("Insert row to $dataset")
        dataset.connection.transaction(true) {
            dataset.openWrite()
            try {
                dataset.write(row)
                dataset.doneWrite()
            }
            finally {
                dataset.closeWrite()
            }
        }

        getGetl().finishProcess(pt, 1)
    }

    /**
     * Update row in JDBC table
     * @param table destination table
     * @param row values of key and updatable fields for a modified record
     */
    void updateRow(TableDataset table, Map<String, Object> row) {
        if (table == null)
            throw new DslError(getl,'#params.required', [param: 'table', detail: 'etl.updateRow'])
        if (row == null || row.isEmpty())
            throw new DslError(getl,'#params.required', [param: 'row', detail: 'etl.updateRow'])

        def pt = getGetl().startProcess("Update row from $table")
        table.connection.transaction(true) {
            table.openWrite(operation: 'UPDATE', updateField: row.keySet().toList())
            try {
                table.write(row)
                table.doneWrite()
                if (table.updateRows != 1)
                    throw new DslError(getl, '#dsl.etl.fail_update_row',
                            [table: table.dslNameObject?:table.fullTableName, count: getl.Numeric2String(table.updateRows)])
            }
            finally {
                table.closeWrite()
            }
        }

        getGetl().finishProcess(pt, 1)
    }

    /**
     * Delete row in JDBC table
     * @param table destination table
     * @param row values of key fields for the deleted record
     */
    void deleteRow(TableDataset table, Map<String, Object> row) {
        if (table == null)
            throw new DslError(getl,'#params.required', [param: 'table', detail: 'etl.deleteRow'])
        if (row == null || row.isEmpty())
            throw new DslError(getl,'#params.required', [param: 'row', detail: 'etl.deleteRow'])

        def pt = getGetl().startProcess("Delete row from $table")
        table.connection.transaction(true) {
            table.openWrite(operation: 'DELETE')
            try {
                table.write(row)
                table.doneWrite()
                if (table.updateRows != 1)
                    throw new DslError(getl, '#dsl.etl.fail_delete_row',
                            [table: table.dslNameObject?:table.fullTableName, count: getl.Numeric2String(table.updateRows)])
            }
            finally {
                table.closeWrite()
            }
        }

        getGetl().finishProcess(pt, 1)
    }

    /**
     * Return one row by specified field values
     * @param table source table
     * @param row row search field values (search in fields with blob, array and object types is not supported)
     */
    @SuppressWarnings('GroovyFallthrough')
    Map<String, Object> findRow(TableDataset table, Map<String, Object> row) {
        if (table == null)
            throw new DslError(getl,'#params.required', [param: 'table', detail: 'etl.findRow'])
        if (row == null || row.isEmpty())
            throw new DslError(getl,'#params.required', [param: 'row', detail: 'etl.findRow'])

        def find = [] as List<String>
        row.each { name, value ->
            def field = table.fieldByName(name)
            if (field == null)
                throw new DatasetError(table, 'dataset.field_not_found', [field: name, detail: 'etl.findRow'])

            String fn = table.sqlObjectName(field.name)
            String val
            switch (field.type) {
                case Field.integerFieldType: case Field.bigintFieldType: case Field.numericFieldType:
                case Field.doubleFieldType: case Field.booleanFieldType:
                    val = value.toString()
                    break
                case Field.dateFieldType: case Field.timeFieldType:
                case Field.datetimeFieldType: case Field.timestamp_with_timezoneFieldType:
                    val = table.currentJDBCConnection.expressionString2Timestamp(value)
                    break
                case Field.blobFieldType: case Field.arrayFieldType: case Field.objectFieldType:
                    throw new DslError(getl, '#dsl.etl.fail_find_row_field_type', [field: name, type: field.type, table: table.dslNameObject?:table.fullTableName])
                default:
                    val = "'${value.toString()}'"
            }

            find.add("$fn = $val")
        }

        def pt = getGetl().startProcess("Find row in $table")
        def rows = table.rows(where: find.join(' AND '))
        if (rows.size() > 1)
            throw new DslError(getl, '#dsl.etl.fail_find_row',
                    [table: table.dslNameObject?:table.fullTableName, count: getl.Numeric2String(rows.size())])
        getGetl().finishProcess(pt, rows.size())

        return (!rows.isEmpty())?rows[0]:null
    }
}