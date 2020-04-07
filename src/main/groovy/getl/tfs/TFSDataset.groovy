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

package getl.tfs

import getl.exception.ExceptionGETL
import getl.utils.FileUtils
import groovy.transform.InheritConstructors

import getl.csv.CSVDataset
import getl.data.*

/**
 * Temporary file storage dataset class
 * @author Alexsey Konstantinov
 *
 */
class TFSDataset extends CSVDataset {
	TFSDataset () {
		super()
		manualSchema = true
		isTemporaryFile = true
		if (fileName == null)
			fileName = FileUtils.UniqueFileName()
	}

	@Override
    void openWrite (Map procParams) {
        super.openWrite(procParams)

        if ((connection as TFS).deleteOnExit) {
            new File(fullFileName()).deleteOnExit()

            if (autoSchema) {
                File s = new File(fullFileSchemaName())
                if (s.exists()) s.deleteOnExit()
            }
        }
    }
	
	@Override
	void setConnection(Connection value) {
		if (value != null && !(value instanceof TFS))
			throw new ExceptionGETL('The tfs dataset only supports tfs connections!')
		super.setConnection(value)
	}
}