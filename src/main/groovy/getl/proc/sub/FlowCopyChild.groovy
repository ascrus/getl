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
package getl.proc.sub

import getl.data.Dataset
import groovy.transform.CompileStatic

/**
 * Flow copy children class
 * @author Alexsey Konstantinov
 */
@CompileStatic
class FlowCopyChild {
    /** Dataset destination */
    public Dataset dataset

    /** Dataset destination */
    public Dataset writer

    /** Dataset write parameters */
    public Map datasetParams

    /** Use auto transaction write mode*/
    public Boolean autoTran

    /** Use synchronize write mode */
    public Boolean writeSynch

    /** Init code before open dataset */
    public Closure onInit

    /** Done code after close dataset */
    public Closure onDone

    /** Process write code*/
    public Closure process

    public Closure updater = { Map row ->
        if (!writeSynch) writer.write(row) else writer.writeSynch(row)
    }

    /** Process source row */
    void processRow(Map row) {
        process.call(updater, row)
    }

    /** Bulk load paramaters */
    public Map bulkParams
}