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
    /** File processed successfully */
    static public final ResultType completeResult = ResultType.COMPLETE
    /** File Processing Error */
    static public final ResultType errorResult = ResultType.ERROR
    /** No file processing required */
    static public final ResultType skipResult = ResultType.SKIP

    FileProcessingElement(FileProcessing.ListPoolElement sourceElement,
                          FileProcessing.ListPoolElement processedElement,
                          FileProcessing.ListPoolElement errorElement,
                          Map<String, Object> attr, File file,
                          Map threadAttr) {
        this.sourceElement = sourceElement
        this.processedElement = processedElement
        this.errorElement = errorElement
        this.attr = attr
        this.file = file
        this.threadAttr = threadAttr
    }

    void free() {
        sourceElement = null
        processedElement = null
        errorElement = null
        attr = null
        file = null
        threadAttr = null
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

    private Map<String, Object> threadAttr
    /** Thread attributes */
    Map<String, Object> getThreadAttr() { threadAttr }

    private Map<String, Object> attr
    /** File attribute */
    Map<String, Object> getAttr() { attr }

    private File file
    /** File descriptor */
    File getFile() { file }

    /** Processing result */
    public ResultType result

    /** File deletion required (if the option to delete source files is enabled) */
    public Boolean removeFile

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

    /**
     * Throw exception and save error text to error file
     * @param errorText text of error
     * @param fileName saved file name
     */
    void throwCriticalError(String errorText, String fileName = null) {
        if (errorText == null)
            throw new ExceptionFileListProcessing('Error text must be specified!')

        this.errorText = errorText
        this.errorFileName = fileName?:((attr.filename as String) + '.error.txt')
        this.result = errorResult
        throw new ExceptionFileListProcessing(errorText)
    }
}