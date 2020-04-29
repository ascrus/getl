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
package getl.files.opts

import getl.exception.ExceptionGETL
import getl.files.Manager
import getl.lang.opts.BaseSpec

/**
 * Options and result of execution of launched processes
 * @author Alexsey Konstantinov
 */
class ManagerProcessSpec extends BaseSpec {
    ManagerProcessSpec(Manager owner) {
        if (owner == null)
            throw new ExceptionGETL('Owner required!')
        this.manager = owner
    }

    /** File system manager */
    Manager manager

    /** Console output */
    final def console = [] as List<String>
    /** Console output */
    List<String> getConsole() { console }

    /** Errors output */
    final def errors = [] as List<String>
    /** Errors output */
    List<String> getErrors() { errors }

    /** Result code */
    final def result = [] as List<Integer>
    /** Result code */
    List<Integer> getResult() { result }

    /** Last result code */
    Integer lastResult
    /** Last result code */
    Integer getLastResult() {
        if (result.isEmpty()) return null
        return result[result.size() - 1]
    }

    /** Last console output */
    String getLastConsole() {
        if (console.isEmpty()) return null
        return console[console.size() - 1]
    }

    /** Last errors output */
    String getLastErrors() {
        if (errors.isEmpty()) return null
        return errors[console.size() - 1]
    }

    /**
     * Run process
     * @param command process start command
     */
    Integer run(String command) {
        def out = new StringBuilder()
        def err = new StringBuilder()

        def res = manager.command(command, out, err)
        result.add(res)
        console.add(out.toString())
        errors.add(err.toString())

        return res
    }
}