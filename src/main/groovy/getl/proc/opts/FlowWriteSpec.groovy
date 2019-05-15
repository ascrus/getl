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
import getl.data.Field
import getl.lang.opts.BaseSpec
import getl.utils.MapUtils

class FlowWriteSpec extends BaseSpec {
    FlowWriteSpec() {
        super()
    }

    FlowWriteSpec(Map<String, Object> params) {
        super()
        ImportFromMap(params, this)
    }

    /**
     * Destination dataset
     */
    public Dataset dest

    /**
     * Temporary destination name
     */
    public String tempDestName

    /**
     * List of field from destination dataset
     */
    public List<Field> tempFields

    /**
     * Parameters for destination write process
     */
    public Map<String, Object> destParams

    /**
     * Write with synchronize main thread
     */
    public Boolean writeSynch

    /**
     * Auto starting and finishing transaction for copy process
     */
    public Boolean autoTran

    /**
     * Clearing destination dataset before copy
     */
    public Boolean clear

    /**
     * Load to destination as bulk load (only is supported)
     */
    public Boolean bulkLoad

    /**
     * Convert bulk file to escaped format
     */
    public Boolean bulkEscaped

    /**
     * Compress bulk file from GZIP algorithm
     */
    public Boolean bulkAsGZIP

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
     * Import from map parameters
     * @param params
     * @param opt
     */
    static void ImportFromMap(Map<String, Object> params, FlowWriteSpec opt) {
        opt.dest = params.dest as Dataset
        opt.destParams = MapUtils.GetLevel(params, "dest_") as Map<String, Object>
        opt.tempDestName = params.tempDest
        opt.tempFields = params.tempFields as List<Field>

        opt.writeSynch = params.writeSynch
        opt.autoTran = params.autoTran

        opt.clear = params.clear

        opt.bulkAsGZIP = params.bulkAsGZIP
        opt.bulkEscaped = params.bulkEscaped
        opt.bulkLoad = params.bulkLoad

        opt.onDone = params.onDone as Closure
        opt.onInit = params.onInit as Closure
    }
}