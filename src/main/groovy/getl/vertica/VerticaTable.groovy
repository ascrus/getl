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

package getl.vertica

import getl.csv.CSVDataset
import getl.data.Connection
import getl.exception.ExceptionGETL
import getl.jdbc.*
import getl.jdbc.opts.*
import getl.vertica.opts.*
import groovy.transform.InheritConstructors
import groovy.transform.stc.ClosureParams
import groovy.transform.stc.SimpleType

/**
 * Vertica table
 * @author Alexsey Konstantinov
 *
 */
@InheritConstructors
class VerticaTable extends TableDataset {
    @Override
    void setConnection(Connection value) {
        if (value != null && !(value instanceof VerticaConnection))
            throw new ExceptionGETL('Сonnection to VerticaConnection class is allowed!')

        super.setConnection(value)
    }

    /** Use specified Vertica connection */
    VerticaConnection useConnection(VerticaConnection value) {
        setConnection(value)
        return value
    }

    @Override
    protected CreateSpec newCreateTableParams(def ownerObject, def thisObject, Boolean useExternalParams,
                                              Map<String, Object> opts) {
        new VerticaCreateSpec(ownerObject, thisObject, useExternalParams, opts)
    }

    /** Options for creating Vertica table */
    VerticaCreateSpec createOpts(@DelegatesTo(VerticaCreateSpec)
                                 @ClosureParams(value = SimpleType, options = ['getl.vertica.opts.VerticaCreateSpec'])
                                         Closure cl = null) {
        genCreateTable(cl) as VerticaCreateSpec
    }

    @Override
    protected ReadSpec newReadTableParams(def ownerObject, def thisObject, Boolean useExternalParams,
                                          Map<String, Object> opts) {
        new VerticaReadSpec(ownerObject, thisObject, useExternalParams, opts)
    }

    /** Options for reading from Vertica table */
    VerticaReadSpec readOpts(@DelegatesTo(VerticaReadSpec)
                             @ClosureParams(value = SimpleType, options = ['getl.vertica.opts.VerticaReadSpec'])
                                     Closure cl = null) {
        genReadDirective(cl) as VerticaReadSpec
    }

    @Override
    protected WriteSpec newWriteTableParams(def ownerObject, def thisObject, Boolean useExternalParams,
                                            Map<String, Object> opts) {
        new VerticaWriteSpec(ownerObject, thisObject, useExternalParams, opts)
    }

    /** Options for writing to Vertica table */
    VerticaWriteSpec writeOpts(@DelegatesTo(VerticaWriteSpec)
                               @ClosureParams(value = SimpleType, options = ['getl.vertica.opts.VerticaWriteSpec'])
                                       Closure cl = null) {
        genWriteDirective(cl) as VerticaWriteSpec
    }

    @Override
    protected BulkLoadSpec newBulkLoadTableParams(def ownerObject, def thisObject, Boolean useExternalParams,
                                                  Map<String, Object> opts) {
        new VerticaBulkLoadSpec(ownerObject, thisObject, useExternalParams, opts)
    }

    /** Options for loading csv files to Vertica table */
    VerticaBulkLoadSpec bulkLoadOpts(@DelegatesTo(VerticaBulkLoadSpec)
                                     @ClosureParams(value = SimpleType, options = ['getl.vertica.opts.VerticaBulkLoadSpec'])
                                             Closure cl = null) {
        genBulkLoadDirective(cl) as VerticaBulkLoadSpec
    }

    @Override
    void prepareCsvTempFile(CSVDataset csvFile) {
        csvFile.header = true
        csvTempFile.escaped = true
        csvTempFile.codePage = 'UTF-8'
        csvTempFile.nullAsValue = '<NULL>'
        csvFile.fieldDelimiter = '|'
        //csvTempFile.isGzFile = true
    }

    @Override
    void validCsvTempFile(CSVDataset csvFile) {
        if (!csvFile.escaped)
            throw new ExceptionGETL('The CSV file must be written in escape mode for bulk load!')

        if (!(csvFile.codePage.toLowerCase() in ['utf-8', 'utf8']))
            throw new ExceptionGETL('The file must be encoded in 8 for batch download!')

        if (csvFile.fieldDelimiter.length() > 1)
            throw new ExceptionGETL('The field separator must have only one character for bulk load!')
    }

    /**
     * Load specified csv files to Vertica table
     * @param source File to load
     * @param cl Load setup code
     */
    VerticaBulkLoadSpec bulkLoadCsv(CSVDataset source, @DelegatesTo(VerticaBulkLoadSpec) Closure cl = null) {
        doBulkLoadCsv(source, cl) as VerticaBulkLoadSpec
    }

    /**
     * Load specified csv files to Vertica table
     * @param cl Load setup code
     */
    VerticaBulkLoadSpec bulkLoadCsv(@DelegatesTo(VerticaBulkLoadSpec) Closure cl) {
        doBulkLoadCsv(null, cl) as VerticaBulkLoadSpec
    }
}