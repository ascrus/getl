//file:noinspection unused
package getl.lang.opts

import com.fasterxml.jackson.annotation.JsonIgnore
import getl.config.ConfigSlurper
import getl.exception.DslError
import getl.lang.Getl
import getl.tfs.TFS
import getl.utils.BoolUtils
import getl.utils.DateUtils
import getl.utils.FileUtils
import getl.utils.MapUtils
import getl.utils.StringUtils
import groovy.transform.InheritConstructors

/**
 * File text procession class
 * @author Alexsey Konstantinov
 *
 */
@InheritConstructors
class FileTextSpec extends BaseSpec {
    private Getl getGetl() { ownerObject as Getl }

    /** Text file name */
    String getFileName() { params.fileName as String }
    /** Text file name */
    void setFileName(String value) {
        saveParamValue('fileName', value)
        _filePath = null
    }

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

    /** Delete file if empty  */
    Boolean getDeleteOnEmpty() { BoolUtils.IsValue(params.deleteOnEmpty) }
    /** Delete file if empty  */
    void setDeleteOnEmpty(Boolean value) { saveParamValue('deleteOnEmpty', value) }

    /** Output writeln text to log */
    Boolean getLogOutput() { BoolUtils.IsValue(params.logOutput) }
    /** Output writeln text to log */
    void setLogOutput(Boolean value) { saveParamValue('logOutput', value) }

    /** Current buffer text */
    @JsonIgnore
    String getBufferText() { buffer?.text }

    /** Text buffer */
    private final File buffer = FileUtils.CreateTempFile()

    /** Count saved bytes */
    private Long countBytes = 0
    /** Count saved bytes */
    Long getCountBytes() { countBytes }

    /** File path cache */
    private String _filePath

    /** File path */
    String filePath() {
        if (_filePath != null)
            return _filePath

        def fp = StringUtils.EvalMacroString(fileName, [date: DateUtils.FormatDate(DateUtils.Now())], false)
        def file = new File(FileUtils.TransformFilePath(fp, ownerObject as Getl))
        if (temporaryFile && file.parent == null)
            _filePath = "${TFS.storage.currentPath()}/${file.name}"
        else
            _filePath = file.absolutePath

        return _filePath
    }

    /** Write text buffer to file */
    @SuppressWarnings('SpellCheckingInspection')
    void save() {
        if (!buffer.exists())
            return

        if (fileName == null && !temporaryFile)
            throw new DslError(getl,'#params.required', [param: 'fileName', detail: 'save'])

        if (buffer.length() == 0 && deleteOnEmpty && !append) {
            delete(false)
            countBytes = 0L
            clear()
            return
        }

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
            writer.write(buffer.text)
        }
        finally {
            writer.close()
        }

        countBytes = buffer.length()
        clear()
    }

    /** Clear current text buffer */
    void clear() {
        buffer.delete()
        logBuffer = new StringBuilder()
    }

    /** Write text and line feed */
    void writeln(String text) {
        buffer.append(text)
        buffer.append('\n')
        addToLogBuffer(text + '\n')
    }

    private logBuffer = new StringBuilder()
    private void addToLogBuffer(String text) {
        if (!logOutput)
            return

        logBuffer.append(text)
        def i = logBuffer.lastIndexOf('\n')
        if (i > -1) {
            getl.logFine(logBuffer.substring(0, i))
            logBuffer.delete(0, i)
        }
    }

    /** Write string */
    void write(String text) {
        buffer.append(text)
        addToLogBuffer(text)
    }

    /**
     * Write map as configuration
     * @param data stored data
     * @param convertVars convert ${variable} to ${vars.variable}
     * @param trimMap
     */
    void write(Map data, Boolean convertVars = false, Boolean trimMap = false) {
        StringBuilder sb = new StringBuilder()
        ConfigSlurper.SaveMap(data, sb, convertVars, trimMap)
        def text = sb.toString()
        buffer.append(text)
        addToLogBuffer(text)
    }

    /**
     * Write configuration
     * @param convertVars convert $ {variable} to $ {vars.variable}
     * @param cl configuration code
     */
    void write(Boolean convertVars = false, Closure cl) {
        write(MapUtils.Closure2Map(cl), convertVars)
    }

    /** Read text from specified file */
    static String read(String sourceFileName, String codePage = null) {
        if (sourceFileName == null)
            throw new DslError('#params.required', [param: 'sourceFileName', detail: 'read'])

        return new File(sourceFileName).getText(codePage?:'utf-8')
    }

    /** Read file to text buffer */
    String readToBuffer(String sourceFileName = null, String encode = null) {
        def res = new File(sourceFileName?:filePath()).getText(encode?:codePage)
        buffer.append(res)
        addToLogBuffer(res)
        return res
    }

    /**
     * Delete file
     * @param throwIfNotExists throw if file not exists
     */
    void delete(Boolean throwIfNotExists = true) {
        def file = new File(filePath())
        if (!file.exists()) {
            if (throwIfNotExists)
                throw new DslError(getl, '#io.file.not_found', [path: filePath()])
        }
        else {
            if (!file.delete())
                throw new DslError(getl, '#io.file.fail_delete', [path: filePath()])
        }

        clear()
    }

    /** Check exists file */
    Boolean getExists() { new File(filePath()).exists() }
}