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
package getl.models.opts

import getl.data.Dataset
import getl.data.FileDataset
import getl.exception.ExceptionDSL
import getl.exception.ExceptionModel
import getl.jdbc.QueryDataset
import getl.jdbc.TableDataset
import getl.jdbc.ViewDataset
import getl.models.sub.DatasetsModel
import getl.utils.MapUtils

import java.util.concurrent.ConcurrentHashMap

/**
 * Base model dataset specification
 * @author ALexsey Konstantinov
 */
class DatasetSpec extends BaseSpec {
    DatasetSpec(DatasetsModel model, String tableName) {
        super(model)
        setDatasetName(tableName)
    }

    DatasetSpec(DatasetsModel model, Map importParams) {
        super(model, importParams)
    }

    /** Owner processing model */
    protected DatasetsModel getOwnerDatasetsModel() { ownerModel as DatasetsModel }

    @Override
    protected void initSpec() {
        super.initSpec()
        params.attrs = new ConcurrentHashMap<String, Object>()
    }

    /** Model dataset name */
    protected String getDatasetName() { params.datasetName as String }
    /** Model dataset name */
    protected void setDatasetName(String value) { params.datasetName = value }

    /** Model dataset */
    Dataset getModelDataset() { ownerModel.dslCreator.dataset(datasetName) }

    /** Mapping attributes */
    Map<String, Object> getAttrs() { params.attrs as Map<String, Object> }
    /** Mapping attributes */
    void setAttrs(Map<String, Object> value) {
        attrs.clear()
        if (value != null)
            attrs.putAll(value)
    }

    /**
     * Check attribute naming and generate an unknown error
     * @param allowAttrs list of allowed attribute names
     */
    void checkAttrs(List<String> allowAttrs) {
        if (allowAttrs == null)
            throw new ExceptionDSL('The list of attribute names in parameter "allowAttrs" is not specified!')

        def unknownKeys = MapUtils.Unknown(attrs, allowAttrs)
        if (!unknownKeys.isEmpty())
            throw new ExceptionDSL("Unknown attributes were detected in \"$datasetName\": $unknownKeys, allow attributes: $allowAttrs")
    }

    @Override
    String toString() { datasetName }
}