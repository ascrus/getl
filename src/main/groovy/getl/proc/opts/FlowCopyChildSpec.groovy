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
import getl.lang.opts.BaseSpec
import groovy.transform.InheritConstructors

/**
 * Flow copy child options
 * @author Alexsey Konstantinov
 *
 */
@InheritConstructors
class FlowCopyChildSpec extends BaseSpec {
    FlowCopyChildSpec() {
        super()
        params.datasetParams = [:] as Map<String, Object>
    }

    FlowCopyChildSpec(Boolean useExternalParams = false, Map<String, Object> importParams) {
        super(useExternalParams, importParams)
        if (params.datasetParams == null) params.datasetParams = [:] as Map<String, Object>
    }

    /** Dataset for writing */
    Dataset getDataset() { params.dataset as Dataset }
    /** Dataset for writing */
    void setDataset(Dataset value) { params.dataset = value }

    /** Write options for the dataset */
    Map<String, Object> getDatasetParams() { params.datasetParams as Map<String, Object> }
    /** Write options for the dataset */
    void setDatasetParams(Map<String, Object> value) {
        datasetParams.clear()
        if (value != null) datasetParams.putAll(value)
    }

    /** The code for write to child dataset (parameters passed to the writer and the original source row) */
    void processRow(Closure value) { params.process = prepareClosure {value} }
}
