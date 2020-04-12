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

import getl.csv.CSVDataset
import getl.data.Connection
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
            throw new ExceptionGETL('Сonnection to ImpalaConnection class is allowed!')

        super.setConnection(value)
    }

    /** Use specified connection */
    ImpalaConnection useConnection(ImpalaConnection value) {
        setConnection(value)
        return value
    }

    /** Current Impala connection */
    ImpalaConnection getCurrentImpalaConnection() { connection as ImpalaConnection }

    @Override
    protected CreateSpec newCreateTableParams(Boolean useExternalParams, Map<String, Object> opts) {
        new ImpalaCreateSpec(useExternalParams, opts)
    }

    ImpalaCreateSpec createOpts(@DelegatesTo(ImpalaCreateSpec)
                              @ClosureParams(value = SimpleType, options = ['getl.impala.opts.ImpalaCreateSpec'])
                                      Closure cl = null) {
        genCreateTable(cl) as ImpalaCreateSpec
    }

    @Override
    protected WriteSpec newWriteTableParams(Boolean useExternalParams, Map<String, Object> opts) {
        new ImpalaWriteSpec(useExternalParams, opts)
    }

    /** Options for writing to Hive table */
    ImpalaWriteSpec writeOpts(@DelegatesTo(ImpalaWriteSpec)
                            @ClosureParams(value = SimpleType, options = ['getl.impala.opts.ImpalaWriteSpec'])
                                    Closure cl = null) {
        genWriteDirective(cl) as ImpalaWriteSpec
    }

    @Override
    protected BulkLoadSpec newBulkLoadTableParams(Boolean useExternalParams, Map<String, Object> opts) {
        new ImpalaBulkLoadSpec(useExternalParams, opts)
    }

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
}