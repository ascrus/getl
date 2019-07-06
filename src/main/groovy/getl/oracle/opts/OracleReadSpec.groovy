/*
 GETL - based package in Groovy, which automates the work of loading and transforming data. His name is an acronym for "Groovy ETL".

 GETL is a set of libraries of pre-built classes and objects that can be used to solve problems unpacking,
 transform and load data into programs written in Groovy, or Java, as well as from any software that supports
 the work with Java classes.

 Copyright (C) 2013-2017  Alexsey Konstantonov (ASCRUS)

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

package getl.oracle.opts

import getl.jdbc.opts.ReadSpec
import groovy.transform.InheritConstructors

/**
 * Oracle table read options
 * @author Alexsey Konstantinov
 *
 */
@InheritConstructors
class OracleReadSpec extends ReadSpec {
    /** Return the result on the specified transactional scn */
    Long getScn() { return params.scn as Long }
    /** Return the result on the specified transactional scn */
    void setScn(Long value) { params.scn = value }

    /** Return the result on the specified transactional timestamp */
    Date getTimestamp() { params.timestamp as Date }
    /** Return the result on the specified transactional timestamp */
    void setTimestamp(Date value) { params.timestamp = value }

    /** Using specified hints in the select statement */
    String getHints() { params.hints as String }
    /** Using specified hints in the select statement */
    void setHints(String value) { params.hints = value }

    /** Read data from the specified partition */
    String getUsePartition() { params.usePartition as String }
    /** Read data from the specified partition */
    void setUsePartition(String value) { params.usePartition = value }
}