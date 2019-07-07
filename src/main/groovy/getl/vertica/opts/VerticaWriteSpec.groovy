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

package getl.vertica.opts

import getl.exception.ExceptionGETL
import getl.jdbc.opts.WriteSpec
import groovy.transform.InheritConstructors

/**
 * Options for writing Vertica table
 * @author Alexsey Konstantinov
 *
 */
@InheritConstructors
class VerticaWriteSpec extends WriteSpec {
    /**
     * Auto detect how write rows
     */
    final static AUTO = 'auto'

    /**
     * Write rows to ROS
     */
    final static DIRECT = 'direct'

    /**
     * Write rows to WOS
     */
    final static TRICKLE = 'trickle'

    /**
     * Label vertica hint
     */
    String getLabel() { params.label as String }
    /**
     * Label vertica hint
     */
    void setLabel(String value) { params.label = value }

    /**
     * Direct vertica hint (AUTO, DIRECT, TRICKLE)
     */
    String getDirect() { params.direct as String }
    /**
     * Direct vertica hint (AUTO, DIRECT, TRICKLE)
     */
    void setDirect(String value) {
        if (value != null) {
            value = value.trim().toUpperCase()
            if (!(value in ['AUTO', 'DIRECT', 'TRICKLE']))
                throw new ExceptionGETL("Invalid direct option \"$value\", allowed: AUTO, DIRECT AND TRICKLE!")
        }

        params.direct = value
    }
}