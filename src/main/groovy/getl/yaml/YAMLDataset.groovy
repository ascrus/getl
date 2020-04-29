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
package getl.yaml

import getl.data.Connection
import getl.data.StructureFileDataset
import getl.exception.ExceptionGETL
import getl.yaml.opts.YAMLReadSpec
import groovy.transform.stc.ClosureParams
import groovy.transform.stc.SimpleType

/**
 * YAML dataset class
 * @author Alexsey Konstantinov
 *
 */
class YAMLDataset extends StructureFileDataset {
    YAMLDataset() {
        super()
        driver_params = [:]
    }

    @Override
    void setConnection(Connection value) {
        if (value != null && !(value instanceof YAMLConnection))
            throw new ExceptionGETL('Сonnection to YAMLConnection class is allowed!')

        super.setConnection(value)
    }

    /** Use specified connection */
    YAMLConnection useConnection(YAMLConnection value) {
        setConnection(value)
        return value
    }

    /** Current YAML connection */
    YAMLConnection getCurrentYAMLConnection() { connection as YAMLConnection }

    /**
     * Read file options
     */
    YAMLReadSpec readOpts(@DelegatesTo(YAMLReadSpec)
                          @ClosureParams(value = SimpleType, options = ['getl.yaml.opts.YAMLReadSpec'])
                                  Closure cl = null) {
        def parent = new YAMLReadSpec(true, readDirective)
        parent.runClosure(cl)

        return parent
    }
}