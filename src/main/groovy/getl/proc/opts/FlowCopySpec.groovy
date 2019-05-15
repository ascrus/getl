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

import getl.data.*
import getl.lang.opts.BaseSpec
import getl.tfs.TFSDataset
import getl.utils.MapUtils

class FlowCopySpec extends BaseSpec {
    FlowCopySpec() {
        super()
    }

    FlowCopySpec(Map params) {
        super()
        ImportFromMap(params, this)
    }

    /**
     * Source dataset
     */
    public Dataset source

    /**
     * Destination dataset
     */
    public Dataset dest

    /**
     * List of destination dataset
     */
    public Map<Object, Dataset> destList

    /**
     * Temporary source name
     */
    public String tempSourceName

    /**
     * Temporary destination name
     */
    public String tempDestName

    /**
     * Destination fields inherit from source fields
     */
    public Boolean inheritFields

    /**
     * Create destination
     */
    public Boolean createDest

    /**
     * List of field from destination dataset
     */
    public List<Field> tempFields

    /**
     * Map card columns with syntax: [<destination field>:"<source field>:<convert format>"]
     */
    public Map map

    /**
     * Parameters for source read process
     */
    public Map sourceParams

    /**
     * Parameters for destination write process
     */
    public Map destParams

    /**
     * Write with synchronize main thread
     */
    public Boolean writeSynch

    /**
     * Auto mapping value from source fields to destination fields
     */
    public Boolean autoMap

    /**
     * Auto converting type value from source fields to destination fields
     */
    public Boolean autoConvert

    /**
     * Auto starting and finishing transaction for copy process
     */
    public Boolean autoTran

    /**
     * Clearing destination dataset before copy
     */
    public Boolean clear

    /**
     * Save assert errors to temporary dataset "errorsDataset"
     */
    public Boolean saveErrors

    /**
     * List of fields destination that do not need to use
     */
    public List<String> excludeFields

    /**
     * List of fields destination that do not need to converted
     */
    public Boolean notConverted

    /**
     * Filename  of mirror CSV dataset
     */
    public String mirrorCSV

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
     * Code executed before writing to destination dataset
     */
    public Closure onWrite

    /**
     * Code to complete process copying
     */
    public Closure onDone

    /**
     * save transformation code to dumn (default false)
     */
    public Boolean debug

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
    static void ImportFromMap(Map<String, Object> params, FlowCopySpec opt) {
        opt.source = params.source as Dataset
        opt.sourceParams = MapUtils.GetLevel(params, "source_") as Map<String, Object>
        opt.tempSourceName = params.tempSource

        opt.dest = params.dest as Dataset
        opt.destParams = MapUtils.GetLevel(params, "dest_") as Map<String, Object>

        opt.tempDestName = params.tempDest
        opt.tempFields = params.tempFields as List<Field>

        opt.debug = params.debug
        opt.saveErrors = params.saveErrors
        opt.mirrorCSV = params.mirrorCSV
        opt.writeSynch = params.writeSynch

        opt.map = params.map as Map<String, String>
        opt.autoConvert = params.autoConvert
        opt.autoTran = params.autoTran
        opt.autoMap = params.autoMap
        opt.notConverted = params.notConverted

        opt.clear = params.clear
        opt.createDest = params.createDest

        opt.excludeFields = params.excludeFields as List<String>
        opt.inheritFields = params.inheritFields

        opt.bulkAsGZIP = params.bulkAsGZIP
        opt.bulkEscaped = params.bulkEscaped
        opt.bulkLoad = params.bulkLoad

        opt.onDone = params.onDone as Closure
        opt.onInit = params.onInit as Closure
        opt.onWrite = params.onWrite as Closure
    }
}