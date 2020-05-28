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

import getl.config.ConfigSlurper
import getl.exception.ExceptionGETL
import getl.tfs.TFS
import getl.utils.BoolUtils
import getl.utils.FileUtils
import getl.utils.MapUtils
import groovy.transform.InheritConstructors
import org.apache.groovy.io.StringBuilderWriter

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
    String getCodePage() { (params.codePage as String)?:'UTF-8' }
    void setCodePage(String value) { params.codePage = value }

    /** Delete file after stop program */
    Boolean getTemporaryFile() { BoolUtils.IsValue(params.temporaryFile) }
    /** Delete file after stop program */
    void setTemporaryFile(Boolean value) {
        params.temporaryFile = value
    }

    /** Append text to exist file */
    Boolean getAppend() { BoolUtils.IsValue(params.append) }
    /** Append text to exist file */
    void setAppend(Boolean value) { params.append = value }

    /** Text buffer */
    final private StringBuilder buffer = new StringBuilder()
    /** Text buffer */
    String getTextBuffer() { buffer.toString() }

    /** Count saved bytes */
    Long countBytes = 0

    /** Write text buffer to file */
    void save() {
        if (fileName == null && !temporaryFile) throw new ExceptionGETL("Required \"fileName\" value!")
        File file
        if (!temporaryFile) {
            file = new File(fileName)
        }
        else if (fileName != null) {
            file = new File(fileName)
            file.deleteOnExit()
        }
        else {
            file = File.createTempFile('text.', '.getltemp', new File(TFS.storage.path))
            file.deleteOnExit()
            fileName = file.absolutePath
        }

        def writer = file.newWriter(codePage?:'UTF-8', append, false)
        try {
            writer.write(buffer.toString())
        }
        finally {
            writer.close()
        }

        countBytes = buffer.length()

        clear()
    }

    /** Clear current text buffer */
    void clear() {
        buffer.setLength(0)
    }

    /** Write text and line feed */
    void writeln(String text) {
        buffer.append(text)
        buffer.append('\n')
    }

    /** Write string */
    void write(String text) {
        buffer.append(text)
    }

    /**
     * Write map as configuration
     * @param data stored data
     * @param convertVars convert ${variable} to ${vars.variable}
     */
    void write(Map data, Boolean convertVars = false) {
        ConfigSlurper.SaveMap(data, buffer, convertVars)
    }

    /**
     * Write configuration
     * @param convertVars convert $ {variable} to $ {vars.variable}
     * @param cl configuration code
     */
    void write(Boolean convertVars = false, Closure cl) {
        write(MapUtils.Closure2Map(cl), convertVars)
    }

    /** Read file to text buffer */
    static String read(String sourceFileName, String codePage = 'UTF-8') {
        if (sourceFileName == null) throw new ExceptionGETL("Required \"sourceFileName\" value!")
        return new File(sourceFileName).getText(codePage)
    }
}