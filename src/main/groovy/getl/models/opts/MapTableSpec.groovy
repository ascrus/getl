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
import getl.data.Field
import getl.exception.ExceptionModel
import getl.models.MapTables
import groovy.transform.InheritConstructors

@InheritConstructors
class MapTableSpec extends TableSpec {
    @Override
    protected void initSpec() {
        super.initSpec()
        params.listPartitions = []
    }

    /** Owner processing model */
    protected MapTables getOwnerProc() { ownerModel as MapTables }

    /** Repository source dataset name */
    String getSourceName() { datasetName }
    /** Source dataset */
    Dataset getSource() { ownerModel.dslCreator.dataset(sourceName) }

    /** Repository destination dataset name */
    String getDestinationName() { params.destinationName as String }
    /** Repository destination dataset name */
    protected void setDestinationName(String value) { params.destinationName = value }
    /** Set destination table name */
    void linkTo(String destinationName) {
        if (destinationName == null)
            throw new ExceptionModel('Destination name can not be null!')

        def ds = ownerModel.dslCreator.dataset(destinationName)
        def name = ds.dslNameObject
        if (name == sourceName)
            throw new ExceptionModel('You cannot use the same dataset for source and destination!')

        params.destinationName = name
    }
    /** Set destination table name */
    void linkTo(Dataset destinationDataset) {
        if (destinationDataset == null)
            throw new ExceptionModel('Destination can not be null!')

        def name = destinationDataset.dslNameObject
        if (name == null)
            throw new ExceptionModel("The dataset $destinationDataset must be registered in the repository!")

        if (name == sourceName)
            throw new ExceptionModel('You cannot use the same dataset for source and destination!')

        params.destinationName = name
    }
    /** Destination dataset */
    Dataset getDestination() { ownerModel.dslCreator.dataset(destinationName) }

    /** Source dataset partition field name */
    String getPartitionFieldName() { params.partitionFieldName as String }
    /** Source dataset partition field name */
    void setPartitionFieldName(String value) { params.partitionFieldName = value }
    /** Source dataset partition field */
    Field getPartitionField() { source.fieldByName(partitionFieldName) }

    /** List of key values for partitions being processed */
    List getListPartitions() { params.listPartitions as List }
    /** List of key values for partitions being processed */
    void setListPartitions(List value) {
        listPartitions.clear()
        if (value != null)
            listPartitions.addAll(value)
    }
}