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

package getl.netezza

import getl.data.Dataset
import getl.driver.Driver
import getl.jdbc.JDBCDataset
import getl.jdbc.JDBCDriver
import groovy.transform.InheritConstructors

/**
 * Netezza driver class
 * @author Alexsey Konstantinov
 *
 */
class NetezzaDriver extends JDBCDriver {
    NetezzaDriver() {
        super()

        connectionParamBegin = ";"
        connectionParamJoin = ";"

        dropSyntax = 'DROP {object} {name} {ifexists}'
    }

    @SuppressWarnings("UnnecessaryQualifiedReference")
    @Override
    List<Driver.Support> supported() {
        return super.supported() +
                [Driver.Support.LOCAL_TEMPORARY, Driver.Support.SEQUENCE,
                 Driver.Support.BLOB, Driver.Support.TIME, Driver.Support.DATE, Driver.Support.BOOLEAN,
                 Driver.Support.CREATEIFNOTEXIST, Driver.Support.DROPIFEXIST]
    }

    @SuppressWarnings("UnnecessaryQualifiedReference")
    @Override
    List<Driver.Operation> operations() {
        return super.operations() +
                [Driver.Operation.TRUNCATE, Driver.Operation.DROP, Driver.Operation.EXECUTE,
                 Driver.Operation.CREATE]
    }

    @Override
    Map getSqlType () {
        Map res = super.getSqlType()
        res.DOUBLE.name = 'double precision'
        res.BLOB.name = 'varbinary'
        res.NUMERIC.name = 'numeric'

        return res
    }

    @Override
    String defaultConnectURL () {
        return 'jdbc:netezza://{host}/{database}'
    }

    @Override
    void sqlTableDirective (JDBCDataset dataset, Map params, Map dir) {
        super.sqlTableDirective(dataset, params, dir)
        if (params.limit != null) {
            dir.afterOrderBy = ((dir.afterOrderBy != null) ? (dir.afterOrderBy + '\n') : '') + "LIMIT ${params.limit}"
            params.limit = null
        }

        if (params.offs != null) {
            dir.afterOrderBy = ((dir.afterOrderBy != null)?(dir.afterOrderBy + '\n'):'') + "OFFSET ${params.offs}"
            params.offs = null
        }
    }

    /* TODO: checking what syntax is correct
    @Override
    protected String sessionID() {
        String res = null
        def rows = sqlConnect.rows('SELECT qs_sessionid FROM _v_qrystat')
        if (!rows.isEmpty()) res = rows[0].qs_sessionid as String

        return res
    }
    */

    /** Next value sequence sql script */
    @Override
    protected String sqlSequenceNext(String sequenceName) { "SELECT NEXT VALUE FOR ${sequenceName} AS id" }
}