/*
 GETL - based package in Groovy, which automates the work of loading and transforming data. His name is an acronym for "Groovy ETL".

 GETL is a set of libraries of pre-built classes and objects that can be used to solve problems unpacking,
 transform and load data into programs written in Groovy, or Java, as well as from any software that supports
 the work with Java classes.

 Copyright (C) 2013-2019  Alexsey Konstantonov (ASCRUS)

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

package getl.proc.opts


import getl.data.Dataset
import getl.tfs.TFSDataset
import getl.utils.MapUtils

class FlowProcessSpec {
    FlowProcessSpec() {
        super()
    }

    FlowProcessSpec(Map<String, Object> params) {
        super()
        ImportFromMap(params, this)
    }

    /**
     * Source dataset
     */
    public Dataset source

    /**
     * Temporary source name
     */
    public String tempSourceName

    /**
     * Parameters for source read process
     */
    public Map<String, Object> sourceParams

    /**
     * Save assert errors to temporary dataset "errorsDataset"
     */
    public Boolean saveErrors

    /**
     * Initialization code on start process copying
     */
    public Closure onInit

    /**
     * Code to complete process copying
     */
    public Closure onDone

    /**
     * Closure code process row
     */
    public Closure process

    /**
     * Last count row
     */
    public Long countRow = 0

    /**
     * Error rows for "copy" process
     */
    public TFSDataset errorsDataset

    /**
     * Import from map parameters
     * @param params
     * @param opt
     */
    static void ImportFromMap(Map<String, Object> params, FlowProcessSpec opt) {
        opt.source = params.source as Dataset
        opt.sourceParams = MapUtils.GetLevel(params, "source_") as Map<String, Object>
        opt.tempSourceName = params.tempSource

        opt.saveErrors = params.saveErrors
        opt.onDone = params.onDone as Closure
        opt.onInit = params.onInit as Closure
    }
}