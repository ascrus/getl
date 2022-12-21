//file:noinspection unused
package getl.utils

import getl.lang.Getl
import getl.lang.sub.GetlRepository
import groovy.transform.CompileStatic

/**
 * Messages manager
 * @author Alexsey Konstantinov
 */
@CompileStatic
class Messages {
    Messages() {
        super()
    }

    Messages(String fileName) {
        super()
        attachFile(fileName)
    }

    Messages(List<String> files) {
        super()
        files.each {fileName -> attachFile(fileName) }
    }

    /** Global messages */
    static public final Messages manager = new Messages()

    /** Current messages language */
    private String lang = 'en'
    /** Current messages language */
    String getLang() { this.lang }
    /** Current messages language */
    void setLang(String value) {
        if (value == null || value.trim().length() == 0)
            throw new NullPointerException('Not set value!')

        this.lang = value.trim().toUpperCase()
        reloadAttachmentFiles()
    }

    /** List of messages  */
    private final Properties messages = new Properties()

    /** Message file handler */
    private final List<String> files = [] as List<String>

    /**
     * Read pattern message
     * @param message message or code with # prefix
     * @return text pattern
     */
    String message(String message) {
        if (message == null || message.length() == 0)
            throw new Error('Not set message or code!')

        if (message[0] != '#')
            return message

        def code = message.substring(1)
        def res = messages.get(code) as String
        if (res == null)
            throw new Error("Unknown message code \"$code\"!")

        return res
    }

    /** Clear loaded files and messages */
    void clear() {
        files.clear()
        messages.clear()
    }

    /** Reload attachment files for new language */
    void reloadAttachmentFiles() {
        def curFiles = [] as List<String>
        curFiles.addAll(files)
        clear()

        def mask = new Path('{name}.{lang}.properties')
        def l = this.lang.toLowerCase()
        for (String fileName in curFiles) {
            def isRepository = FileUtils.IsRepositoryFileName(fileName)
            def isResource = (!isRepository)?FileUtils.IsResourceFileName(fileName, false):false

            def path = FileUtils.PathFromFile(fileName, true, isRepository || isResource)
            if (path == null || path == '/')
                path = ''
            path += '/'

            def name = FileUtils.FileName(fileName, true)
            def params = mask.analyzeFile(name)
            if (params.name != null && params.lang != null) {
                def newName = (params.name as String) + '.' + l + '.properties'

                if (isResource) {
                    if (FileUtils.FileFromResources(path + newName) != null) {
                        attachFile('resource:' + path + newName)
                        continue
                    }
                }
                else {
                    if (isRepository)
                        path = 'repository:' + path

                    if (new File(FileUtils.ResourceFileName(path + newName)).exists()) {
                        attachFile(path + newName)
                        continue
                    }
                }
            }

            attachFile(fileName)
        }
    }

    /**
     * Attach properties file with messages
     * @param fileName file name
     */
    void attachFile(String fileName) {
        def fn = FileUtils.ResourceFileName(fileName)
        if (fn == null)
            throw new Error("Messages resource file \"$fileName\" not found!")

        fileName = FileUtils.ConvertToUnixPath(fileName)

        // Already loaded
        if (files.indexOf(fileName) != -1)
            return

        def file = new File(fn)
        if (!file.exists())
            throw new Error("Messages file \"$fileName\" not found!")
        def props = new Properties()
        try (def reader = file.newReader('utf-8')) {
            props.load(reader)
        }
        def newKeys = props.keySet().toList() as List<String>
        def alreadyKeys = (messages.keySet().toList() as List<String>).intersect(newKeys)
        if (!alreadyKeys.isEmpty())
            throw new Error("Message file \"$fileName\" already codes: ${alreadyKeys.join('; ')}")

        messages.putAll(props)
        files.add(fileName)
    }

    /**
     * Attach resource message file
     * @param name name of message file
     * @param classLoader class loader for search resource files
     */
    void attachResourceFile(String name, ClassLoader classLoader = null) {
        def fileName = name + '.' + lang.toLowerCase() + '.properties'
        def file = FileUtils.FileFromResources('/' + fileName, null, classLoader)
        if (file == null)
            throw new Error("Message file \"$fileName\" not found in resources!")

        attachFile('resource:/' + fileName)
    }

    /** Generate text from message or code */
    static String BuildText(String message, Map vars = null) {
        return StringUtils.EvalMacroString(manager.message(message), (vars?:[:]))
    }

    /** Generate text from message or code */
    static String BuildText(GetlRepository object, String message, Map vars = null) {
        return StringUtils.EvalMacroString('{objectClassName} [{object}]: ' + manager.message(message),
                (vars?:[:]) + [object: object.dslNameObject?:object.toString(), objectClassName: object.getClass().simpleName], false)
    }

    /** Generate text from message or code */
    static String BuildText(Getl getl, String message, Map vars = null) {
        return StringUtils.EvalMacroString('{getl}: ' + manager.message(message),
                (vars?:[:]) + [getl: (getl.getGetlSystemParameter('runMode') == 'workflow')?'Workflow [' + getl.getGetlSystemParameter('workflow') + ']':
                        'Script [' + (getl.getlMainClassName?:'DSL') + ']'], false)
    }
}