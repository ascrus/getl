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

package getl.mssql

import getl.data.Connection
import getl.exception.ExceptionGETL
import getl.jdbc.InternalTableDataset
import getl.jdbc.opts.ReadSpec
import getl.mssql.opts.MSSQLReadSpec
import groovy.transform.InheritConstructors
import groovy.transform.stc.ClosureParams
import groovy.transform.stc.SimpleType

/**
 * MS SQLServer table
 * @author Alexsey Konstantinov
 *
 */
@InheritConstructors
class MSSQLTable extends InternalTableDataset {
    @Override
    void setConnection(Connection value) {
        if (value != null && !(value instanceof MSSQLConnection))
            throw new ExceptionGETL('Сonnection to MSSQLConnection class is allowed!')

        super.setConnection(value)
    }

    /** Use specified connection */
    MSSQLConnection useConnection(MSSQLConnection value) {
        setConnection(value)
        return value
    }

    @Override
    protected ReadSpec newReadTableParams(def ownerObject, def thisObject, Boolean useExternalParams,
                                          Map<String, Object> opts) {
        new MSSQLReadSpec(ownerObject, thisObject, useExternalParams, opts)
    }

    /**
     * Read table options
     */
    MSSQLReadSpec readOpts(@DelegatesTo(MSSQLReadSpec)
                           @ClosureParams(value = SimpleType, options = ['getl.mssql.opts.MSSQLReadSpec>'])
                                   Closure cl = null) {
        genReadDirective(cl) as MSSQLReadSpec
    }

    /**
     * Perform operations on a table
     * @param cl closure code
     * @return source table
     */
    MSSQLTable dois(@DelegatesTo(MSSQLTable) Closure cl) {
        this.with(cl)
        return this
    }
}