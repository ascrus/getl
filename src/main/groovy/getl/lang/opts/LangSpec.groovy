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
package getl.lang.opts

import getl.utils.BoolUtils
import groovy.transform.InheritConstructors

/**
 * Getl language options
 * @author Alexsey Konstantinov
 *
 */
@InheritConstructors
class LangSpec extends BaseSpec {
    /** Fixing the execution time of processes in the log */
    boolean getProcessTimeTracing() { BoolUtils.IsValue(params.processTimeTracing, true) }
     /** Fixing the execution time of processes in the log */
    void setProcessTimeTracing(boolean value) {params.processTimeTracing = value }

    /** Use the multithreaded JDBC connection model */
    boolean getUseThreadModelConnection() { BoolUtils.IsValue(params.useThreadModelJDBCConnection, true) }
    /** Use the multithreaded JDBC connection model */
    void setUseThreadModelConnection(boolean value) { params.useThreadModelJDBCConnection = value }

    /** Write SQL command from temporary database connection to history file */
    String getTempDBSQLHistoryFile() { params.tempDBSQLHistoryFile as String }
    /** Write SQL command from temporary database connection to history file */
    void setTempDBSQLHistoryFile(String value) { params.tempDBSQLHistoryFile = value }

    /** Auto create CSV temp dataset for JDBC tables */
    Boolean getAutoCSVTempForJDBDTables() { BoolUtils.IsValue(params.autoCSVTempForJDBDTables) }
    /** Auto create CSV temp dataset for JDBC tables */
    void setAutoCSVTempForJDBDTables(Boolean value) { params.autoCSVTempForJDBDTables = value }

    /** Check on connection registration */
    Boolean getValidRegisterObjects() { BoolUtils.IsValue(params.validObjectExist, true) }
    /** Check on connection registration */
    void setValidRegisterObjects(Boolean value) { params.validObjectExist = value }
}