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

package getl.h2

import getl.csv.CSVDataset
import getl.data.Connection
import getl.exception.ExceptionGETL
import getl.h2.opts.*
import getl.jdbc.*
import getl.jdbc.opts.*
import getl.lang.opts.BaseSpec
import groovy.transform.InheritConstructors
import groovy.transform.stc.ClosureParams
import groovy.transform.stc.SimpleType

/**
 * H2 database table
 * @author Alexsey Konstantinov
 *
 */
@InheritConstructors
class H2Table extends TableDataset {
    @Override
    void setConnection(Connection value) {
        if (value != null && !(value instanceof H2Connection))
            throw new ExceptionGETL('Сonnection to H2Connection class is allowed!')

        super.setConnection(value)
    }

    /** Use specified H2 connection */
    H2Connection useConnection(H2Connection value) {
        setConnection(value)
        return value
    }

    @Override
    protected CreateSpec newCreateTableParams(def ownerObject, def thisObject, Boolean useExternalParams,
                                              Map<String, Object> opts) {
        return new H2CreateSpec(ownerObject, thisObject, useExternalParams, opts)
    }

    /** Options for creating Vertica table */
    H2CreateSpec createOpts(@DelegatesTo(H2CreateSpec)
                            @ClosureParams(value = SimpleType, options = ['getl.h2.opts.H2CreateSpec'])
                                    Closure cl = null) {
        genCreateTable(cl) as H2CreateSpec
    }

    @Override
    protected BulkLoadSpec newBulkLoadTableParams(def ownerObject, def thisObject, Boolean useExternalParams,
                                                  Map<String, Object> opts) {
        return new H2BulkLoadSpec(ownerObject, thisObject, useExternalParams, opts)
    }

    /** Options for loading csv files to Vertica table */
    H2BulkLoadSpec bulkLoadOpts(@DelegatesTo(H2BulkLoadSpec)
                                @ClosureParams(value = SimpleType, options = ['getl.h2.opts.H2BulkLoadSpec'])
                                        Closure cl = null) {
        genBulkLoadDirective(cl) as H2BulkLoadSpec
    }

    /**
     * Load specified csv files to H2 table
     * @param source File to load
     * @param cl Load setup code
     */
    H2BulkLoadSpec bulkLoadCsv(CSVDataset source,
                               @DelegatesTo(H2BulkLoadSpec)
                               @ClosureParams(value = SimpleType, options = ['getl.h2.opts.H2BulkLoadSpec'])
                                       Closure cl = null) {
        doBulkLoadCsv(source, cl) as H2BulkLoadSpec
    }

    /**
     * Load specified csv files to H2 table
     * @param cl Load setup code
     */
    H2BulkLoadSpec bulkLoadCsv(@DelegatesTo(H2BulkLoadSpec)
                               @ClosureParams(value = SimpleType, options = ['getl.h2.opts.H2BulkLoadSpec'])
                                       Closure cl) {
        doBulkLoadCsv(null, cl) as H2BulkLoadSpec
    }
}