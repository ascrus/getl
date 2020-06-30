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
package getl.models

import getl.exception.ExceptionDSL
import getl.jdbc.JDBCConnection
import getl.jdbc.TableDataset
import getl.models.opts.TableSpec
import getl.models.sub.DatasetsModel
import groovy.transform.InheritConstructors
import groovy.transform.stc.ClosureParams
import groovy.transform.stc.SimpleType

/**
 * Table list model
 * @author Alexsey Konstantinov
 */
@InheritConstructors
@InheritConstructors
class SetOfTables extends DatasetsModel<TableSpec> {
    /** Connection name in the repository */
    String getSourceConnectionName() { modelConnectionName }
    /** Use specified repository connection name */
    void useSourceConnection(String connectionName) {
        def con = dslCreator.connection(connectionName)
        if (!(con instanceof JDBCConnection))
            throw new ExceptionDSL("Connection \"$connectionName\" is not JDBC compatible!")
        useModelConnection(connectionName)
    }
    /** Use specified connection */
    void useSourceConnection(JDBCConnection connection) { useModelConnection(connection) }
    /** Used connection */
    JDBCConnection getSourceConnection() { modelConnection as JDBCConnection }

    /** Used tables */
    List<TableSpec> getUsedTables() { usedObjects as List<TableSpec> }

    /**
     * Use table in list
     * @param tableName repository table name
     * @param cl defining code
     * @return table spec
     */
    TableSpec table(String tableName,
                    @DelegatesTo(TableSpec)
                    @ClosureParams(value = SimpleType, options = ['getl.models.opts.TableSpec']) Closure cl = null) {
        super.dataset(tableName, cl)
    }

    /**
     * Use table in list
     * @param cl defining code
     * @return table spec
     */
    TableSpec table(@DelegatesTo(TableSpec)
                    @ClosureParams(value = SimpleType, options = ['getl.models.opts.TableSpec']) Closure cl) {
        def owner = DetectClosureDelegate(cl)
        if (owner instanceof TableDataset)
            return table((owner as TableDataset).dslNameObject)

        throw new ExceptionDSL('Required to specify jdbc compatible table!')
    }

    /**
     * Use table in list
     * @param table repository table
     * @param cl defining code
     * @return table spec
     */
    TableSpec table(TableDataset table,
                    @DelegatesTo(TableSpec)
                    @ClosureParams(value = SimpleType, options = ['getl.models.opts.TableSpec']) Closure cl = null) {
        super.useDataset(table, cl)
    }
}