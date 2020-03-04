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

package getl.exception

import groovy.transform.InheritConstructors

/**
 * DSL GETL exception
 * @author Alexsey Konstantinov
 */
@InheritConstructors
class ExceptionDSL extends Throwable {
    ExceptionDSL(Integer typeCode, Integer exitCode, String message) {
        super(message)
        this.typeCode = typeCode
        this.exitCode = exitCode
    }

    ExceptionDSL(Integer typeCode, Integer exitCode) {
        super()
        this.typeCode = typeCode
        this.exitCode = exitCode
    }

    ExceptionDSL(Integer typeCode, String message) {
        super(message)
        this.typeCode = typeCode
    }

    ExceptionDSL(Integer typeCode) {
        super()
        this.typeCode = typeCode
    }

    ExceptionDSL(String message) {
        super(message)
        this.typeCode = 0
        this.exitCode = -1
    }

    ExceptionDSL() {
        super()
    }

    /** Stop code execution of the current class */
    static public final def STOP_CLASS = 1
    /**
     * Stop execution of current application code
     */
    static public final def STOP_APP = 2

    /** Type code */
    Integer typeCode
    /** Type code */
    Integer getTypeCode() { typeCode }

    /** Exit code */
    Integer exitCode
    /** Exit code */
    Integer getExitCode() { exitCode }
}