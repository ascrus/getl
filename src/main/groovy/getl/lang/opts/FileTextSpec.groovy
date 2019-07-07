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
package getl.lang.opts

import getl.exception.ExceptionGETL
import getl.tfs.TFS
import getl.utils.BoolUtils
import getl.utils.FileUtils
import groovy.transform.InheritConstructors

/**
 * File text procession class
 * @author Alexsey Konstantinov
 *
 */
@InheritConstructors
class FileTextSpec extends BaseSpec {
    /** Text file name */
    String getFileName() { params.fileName as String }
    /** Text file name */
    void setFileName(String value) { params.fileName = value }

    /** Code page text file (default UTF-8) */
    String getCodePage() { params.codePage as String }
    void setCodePage(String value) { params.codePage = value }

    Boolean getTemporaryFile() { BoolUtils.IsValue(params.temporaryFile) }
    void setTemporaryFile(Boolean value) {
        params.temporaryFile = value
    }

    /** Text buffer */
    final private StringBuilder buffer = new StringBuilder()
    /** Text buffer */
    String getTextBuffer() { buffer.toString() }

    /** Write text buffer to file */
    void write(boolean append = false) {
        if (fileName == null && !temporaryFile) throw new ExceptionGETL("Required \"fileName\" value!")
        def file = (!temporaryFile)?new File(fileName):
                File.createTempFile('text.', '.getltemp', new File(TFS.storage.path))
        if (temporaryFile) {
            fileName = file.absolutePath
            file.deleteOnExit()
        }
        def writer = file.newWriter(codePage?:'UTF-8', append, false)
        try {
            writer.write(buffer.toString())
        }
        finally {
            writer.close()
        }

        clear()
    }

    /** Clear current text buffer */
    void clear() {
        buffer.setLength(0)
    }

    /** Write text and line feed */
    void text(String data) {
        buffer.append(data)
        buffer.append('\n')
    }

    /** Write string */
    void string(String data) {
        buffer.append(data)
    }

    /** Read file to text buffer */
    String read() {
        clear()
        if (fileName == null) throw new ExceptionGETL("Required \"fileName\" value!")
        buffer.append(new File(fileName).text)
        return buffer.toString()
    }
}