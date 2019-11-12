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
import getl.driver.Driver
import getl.jdbc.JDBCDriver
import getl.jdbc.TableDataset
import getl.utils.BoolUtils
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

        connectionParamBegin = ';'
        connectionParamJoin = ';'

        syntaxPartitionKeyInColumns = false
    }

    @SuppressWarnings("UnnecessaryQualifiedReference")
    @Override
    List<Driver.Support> supported() {
        return super.supported() +
                [Driver.Support.BOOLEAN, Driver.Support.CREATEIFNOTEXIST, Driver.Support.DROPIFEXIST] -
                [Driver.Support.PRIMARY_KEY, Driver.Support.NOT_NULL_FIELD,
                 Driver.Support.DEFAULT_VALUE, Driver.Support.COMPUTE_FIELD]
    }

    @SuppressWarnings("UnnecessaryQualifiedReference")
    @Override
    List<Driver.Operation> operations() {
        return super.operations() +
                [Driver.Operation.CLEAR, Driver.Operation.DROP, Driver.Operation.EXECUTE, Driver.Operation.CREATE] -
                [/*Driver.Operation.READ_METADATA, */Driver.Operation.UPDATE, Driver.Operation.DELETE]
    }

    @Override
    String defaultConnectURL () {
        return 'jdbc:impala://{host}/{database}'
    }

    @SuppressWarnings("UnnecessaryQualifiedReference")
    @Override
    Map getSqlType () {
        Map res = super.getSqlType()
        res.DOUBLE.name = 'double'
        res.BLOB.name = 'binary'
        res.BLOB.useLength = JDBCDriver.sqlTypeUse.NEVER

        return res
    }

    @Override
    protected String syntaxInsertStatement(Dataset dataset, Map params) {
        String into = (BoolUtils.IsValue([params.overwrite, (dataset as TableDataset).params.overwrite]))?'OVERWRITE':'INTO'
        return ((dataset.fieldListPartitions.isEmpty()))?
                "INSERT $into TABLE {table} ({columns}) VALUES({values})":
                "INSERT $into TABLE {table} ({columns}) PARTITION ({partition}) VALUES({values})"
    }
}