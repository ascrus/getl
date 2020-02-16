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

import getl.exception.ExceptionFileListProcessing
import getl.exception.ExceptionFileProcessing
import getl.files.Manager
import getl.proc.FileProcessing
import getl.utils.ListUtils
import groovy.transform.CompileStatic

/**
 * Element for file processing
 * @author Alexsey Konstantinov
 */
@CompileStatic
class FileProcessingElement {
    static enum ResultType  { COMPLETE, ERROR, SKIP }
    static public final ResultType completeResult = ResultType.COMPLETE
    static public final ResultType errorResult = ResultType.ERROR
    static public final ResultType skipResult = ResultType.SKIP

    FileProcessingElement(FileProcessing.ListPoolElement sourceElement,
                          FileProcessing.ListPoolElement processedElement,
                          FileProcessing.ListPoolElement errorElement,
                          Map<String, Object> attr, File file) {
        this.sourceElement = sourceElement
        this.processedElement = processedElement
        this.errorElement = errorElement
        this.attr = attr
        this.file = file
    }

    protected FileProcessing.ListPoolElement sourceElement, processedElement, errorElement

    /** Source file manager */
    Manager getSource() { sourceElement.man }

    /** Storage for processed files */
    Manager getStorageProcessedFiles() {
        if (processedElement == null)
            throw new ExceptionFileListProcessing('There is no specified archive storage manager to write the file!')

        return processedElement.man
    }

    /** Storage for error files */
    Manager getStorageErrorFiles() {
        if (errorElement == null)
            throw new ExceptionFileListProcessing('There is no specified error storage manager to write the file!')

        return errorElement.man
    }

    Map<String, Object> attr
    /** File attribute */
    Map<String, Object> getAttr() { attr }

    File file
    /** File descriptor */
    File getFile() { file }

    /** Processing result */
    public ResultType result

    /** Error file name */
    public String errorFileName

    /** Error text */
    public String errorText

    /**
     * Upload file to specified directory in error storage
     * @param uploadFile file to upload
     * @param groupdirectory grouping directory in error root directory
     */
    void uploadFileToStorageError(File uploadFile) {
        if (errorElement == null)
            throw new ExceptionFileListProcessing('There is no specified error storage manager to write the file!')

        errorElement.uploadFile(uploadFile, attr.filepath as String)
    }

    /**
     * Upload text to specified file with directory in error manager
     * @param errorText text to write to a file
     * @param fileName file name to upload
     * @param groupdirectory grouping directory in error root directory
     */
    void uploadTextToStorageError(String errorText, String fileName = null) {
        if (errorElement == null)
            throw new ExceptionFileListProcessing('There is no specified error storage manager to write the file!')

        if (errorText == null)
            throw new ExceptionFileListProcessing('Error text must be specified!')

        errorElement.uploadText(errorText, attr.filepath as String,
                ListUtils.NotNullValue([fileName, this.errorFileName, (attr.filename as String) + '.other.txt']) as String)
    }

    /**
     * Upload error text to specified file with directory in error manager
     */
    void uploadTextToStorageError() {
        uploadTextToStorageError(this.errorText, errorFileName)
    }

    /**
     * Throw exception and save error text to error file
     * @param errorText text of error
     * @param fileName saved file name
     */
    void throwError(String errorText, String fileName = null) {
        if (errorText == null)
            throw new ExceptionFileListProcessing('Error text must be specified!')

        this.errorText = errorText
        this.errorFileName = fileName?:((attr.filename as String) + '.error.txt')
        this.result = errorResult
        throw new ExceptionFileProcessing(errorText)
    }
}