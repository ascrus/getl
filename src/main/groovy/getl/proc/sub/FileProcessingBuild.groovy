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

import getl.proc.FileProcessing
import getl.utils.NumericUtils
import groovy.transform.CompileStatic
import groovy.transform.InheritConstructors

/**
 * File processing build manager class
 * @author Alexsey Konstantinov
 */
@InheritConstructors
@CompileStatic
class FileProcessingBuild extends FileListProcessingBuild {
    /** File processing owner */
    FileProcessing getOwnerProcessing() { owner as FileProcessing }

    /** Segmented columns */
    List<String> threadGroupColumns

    @Override
    void init() {
        super.init()

        threadGroupColumns = ownerProcessing.threadGroupColumns*.toLowerCase()
    }

    @Override
    boolean prepare(Map file) {
        if (!super.prepare(file)) return false
        if (file.filetype != 'FILE') return true

        if (!threadGroupColumns.isEmpty()) {
            def  l = []
            threadGroupColumns.each { l << file.get(it) }
            file.put('_hash_', NumericUtils.Hash(l))
        }
        else {
            file.put('_hash_', NumericUtils.Hash(file.filepath))
        }

        return true
    }
}