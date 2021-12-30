//file:noinspection unused
package getl.impala

import com.fasterxml.jackson.annotation.JsonIgnore
import getl.csv.CSVDataset
import getl.data.Connection
import getl.data.Field
import getl.exception.ExceptionGETL
import getl.impala.opts.ImpalaBulkLoadSpec
import getl.impala.opts.ImpalaCreateSpec
import getl.impala.opts.ImpalaWriteSpec
import getl.jdbc.TableDataset
import getl.jdbc.opts.BulkLoadSpec
import getl.jdbc.opts.CreateSpec
import getl.jdbc.opts.WriteSpec
import groovy.transform.InheritConstructors
import groovy.transform.stc.ClosureParams
import groovy.transform.stc.SimpleType

/**
 * Impala database table
 * @author Alexsey Konstantinov
 *
 */
@InheritConstructors
class ImpalaTable extends TableDataset {
    @Override
    void setConnection(Connection value) {
        if (value != null && !(value instanceof ImpalaConnection))
            throw new ExceptionGETL('Connection to ImpalaConnection class is allowed!')

        super.setConnection(value)
    }

    /** Use specified connection */
    ImpalaConnection useConnection(ImpalaConnection value) {
        setConnection(value)
        return value
    }

    /** Current Impala connection */
    @JsonIgnore
    ImpalaConnection getCurrentImpalaConnection() { connection as ImpalaConnection }

    @Override
    protected CreateSpec newCreateTableParams(Boolean useExternalParams, Map<String, Object> opts) {
        new ImpalaCreateSpec(this, useExternalParams, opts)
    }

    /** Options for creating Impala table */
    ImpalaCreateSpec getCreateOpts() { new ImpalaCreateSpec(this, true, createDirective) }

    /** Options for creating Impala table */
    ImpalaCreateSpec createOpts(@DelegatesTo(ImpalaCreateSpec)
                              @ClosureParams(value = SimpleType, options = ['getl.impala.opts.ImpalaCreateSpec'])
                                      Closure cl = null) {
        genCreateTable(cl) as ImpalaCreateSpec
    }

    @Override
    protected WriteSpec newWriteTableParams(Boolean useExternalParams, Map<String, Object> opts) {
        new ImpalaWriteSpec(this, useExternalParams, opts)
    }

    /** Options for writing to Hive table */
    ImpalaWriteSpec getWriteOpts() { new ImpalaWriteSpec(this, true, writeDirective) }

    /** Options for writing to Hive table */
    ImpalaWriteSpec writeOpts(@DelegatesTo(ImpalaWriteSpec)
                            @ClosureParams(value = SimpleType, options = ['getl.impala.opts.ImpalaWriteSpec'])
                                    Closure cl = null) {
        genWriteDirective(cl) as ImpalaWriteSpec
    }

    @Override
    protected BulkLoadSpec newBulkLoadTableParams(Boolean useExternalParams, Map<String, Object> opts) {
        new ImpalaBulkLoadSpec(this, useExternalParams, opts)
    }

    /** Options for loading csv files to Hive table */
    ImpalaBulkLoadSpec getBulkLoadOpts() { new ImpalaBulkLoadSpec(this, true, bulkLoadDirective) }

    /** Options for loading csv files to Hive table */
    ImpalaBulkLoadSpec bulkLoadOpts(@DelegatesTo(ImpalaBulkLoadSpec)
                                  @ClosureParams(value = SimpleType, options = ['getl.impala.opts.ImpalaBulkLoadSpec'])
                                          Closure cl = null) {
        genBulkLoadDirective(cl) as ImpalaBulkLoadSpec
    }

    /**
     * Load specified csv files to Vertica table
     * @param source File to load
     * @param cl Load setup code
     */
    ImpalaBulkLoadSpec bulkLoadCsv(CSVDataset source,
                                 @DelegatesTo(ImpalaBulkLoadSpec)
                                 @ClosureParams(value = SimpleType, options = ['getl.impala.opts.ImpalaBulkLoadSpec'])
                                         Closure cl = null) {
        doBulkLoadCsv(source, cl) as ImpalaBulkLoadSpec
    }

    /**
     * Load specified csv files to Hive table
     * @param cl Load setup code
     */
    ImpalaBulkLoadSpec bulkLoadCsv(@DelegatesTo(ImpalaBulkLoadSpec)
                                 @ClosureParams(value = SimpleType, options = ['getl.impala.opts.ImpalaBulkLoadSpec'])
                                         Closure cl) {
        doBulkLoadCsv(null, cl) as ImpalaBulkLoadSpec
    }

    /**
     * Convert fields from another DBMS to the appropriate Impala field types
     * @param table source table
     * @return fields for Impala
     */
    static List<Field> ProcessFields(TableDataset table) {
        if (table.field.isEmpty())
            table.retrieveFields()

        def res = [] as List<Field>
        table.field.each { verField ->
            def field = verField.clone() as Field
            field.typeName = null
            field.isAutoincrement = false
            field.isPartition = false
            field.isKey = false
            field.isNull = true

            switch (field.type) {
                case Field.dateFieldType:
                    field.type = Field.datetimeFieldType
                    break
                case Field.booleanFieldType:
                    field.type = Field.integerFieldType
                    break
            }

            res << field
        }

        return res
    }

    /**
     * Convert fields from another DBMS to the appropriate Impala field types
     * @param table source table
     */
    void useFieldsFromTable(TableDataset table) {
        setField(ProcessFields(table))
    }

    /**
     * Generate a list of expressions in INSERT SELECT with casting if necessary
     * @param source source Impala table
     * @param dest destination Impala table
     * @return List of SQL expressions
     */
    static List<String> SelectListForInsert(ImpalaTable source, ImpalaTable dest) {
        def res = [] as List<String>
        source.field.each { ef ->
            def df = dest.fieldByName(ef.name)
            if (df == null)
                return

            def fn = dest.currentImpalaConnection.currentImpalaDriver.prepareFieldNameForSQL(ef.name)
            def et = source.currentImpalaConnection.currentImpalaDriver.type2sqlType(ef, true)
            def dt = dest.currentImpalaConnection.currentImpalaDriver.type2sqlType(df, true)
            if (et == dt) {
                res << fn
            }
            else {
                res << "CAST($fn AS ${dt})".toString()
            }
        }

        return res
    }
}