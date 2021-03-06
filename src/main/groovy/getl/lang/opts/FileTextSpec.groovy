package getl.lang.opts

import getl.config.ConfigSlurper
import getl.exception.ExceptionGETL
import getl.tfs.TFS
import getl.utils.BoolUtils
import getl.utils.FileUtils
import getl.utils.MapUtils
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
    void setFileName(String value) { saveParamValue('fileName', value) }

    /** Code page text file (default UTF-8) */
    String getCodePage() { (params.codePage as String)?:'UTF-8' }
    void setCodePage(String value) { saveParamValue('codePage', value) }

    /** Delete file after stop program */
    Boolean getTemporaryFile() { BoolUtils.IsValue(params.temporaryFile) }
    /** Delete file after stop program */
    void setTemporaryFile(Boolean value) {
        saveParamValue('temporaryFile', value)
    }

    /** Append text to exist file */
    Boolean getAppend() { BoolUtils.IsValue(params.append) }
    /** Append text to exist file */
    void setAppend(Boolean value) { saveParamValue('append', value) }

    /** Text buffer */
    private final StringBuilder buffer = new StringBuilder()
    /** Text buffer */
    String getTextBuffer() { buffer.toString() }

    /** Count saved bytes */
    private Long countBytes = 0
    /** Count saved bytes */
    Long getCountBytes() { countBytes }

    /** File path */
    String filePath() { FileUtils.TransformFilePath(fileName) }

    /** Write text buffer to file */
    @SuppressWarnings('SpellCheckingInspection')
    void save() {
        if (fileName == null && !temporaryFile)
            throw new ExceptionGETL("Required \"fileName\" value!")

        File file
        if (!temporaryFile) {
            file = new File(filePath())
        }
        else if (fileName != null) {
            file = new File(filePath())
            file.deleteOnExit()
        }
        else {
            file = File.createTempFile('text.', '.getltemp', new File(TFS.storage.currentPath()))
            file.deleteOnExit()
            fileName = file.canonicalPath
        }

        FileUtils.ValidFilePath(file)

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
     * @param trimMap
     */
    void write(Map data, Boolean convertVars = false, Boolean trimMap = false) {
        ConfigSlurper.SaveMap(data, buffer, convertVars, trimMap)
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