package getl.files

import com.fasterxml.jackson.annotation.JsonIgnore
import getl.exception.ExceptionGETL
import getl.files.sub.FileManagerList
import getl.files.sub.ResourceCatalogElem
import getl.utils.FileUtils
import getl.utils.Path
import groovy.transform.CompileStatic
import groovy.transform.InheritConstructors
import groovy.transform.Synchronized
import groovy.transform.stc.ClosureParams
import groovy.transform.stc.SimpleType
import net.lingala.zip4j.ZipFile

/**
 * Resource files manager
 * @author Alexsey Konstantinov
 */
@InheritConstructors
class ResourceManager extends Manager {
    /** Path to the resource directory */
    String getResourcePath() { params.resourcePath as String }
    /** Path to the resource directory */
    void setResourcePath(String value) {
        params.resourcePath = value
        if (connected) {
            validResourcePath()
            buildCatalog()
        }
    }
    /** Path to the resource directory */
    void useResourcePath(String value) { setResourcePath(value) }

    /** Resource file storage paths */
    List<String> getResourceDirectories() { params.resourceDirectories as List<String> }
    /** Resource file storage paths */
    void setResourceDirectories(List<String> value) { useResourceDirectories(value) }
    /** Resource file storage paths */
    void useResourceDirectories(List<String> value) {
        resourceDirectories.clear()
        if (value != null)
            resourceDirectories.addAll(value)
    }

    /** Use class loader to access resources */
    private ClassLoader classLoader
    /** Use class loader to access resources */
    @JsonIgnore
    ClassLoader getClassLoader() { classLoader }
    /** Use class loader to access resources */
    void useClassLoader(ClassLoader value) { classLoader = value }

    @Override
    protected void initParams() {
        super.initParams()
        params.resourceDirectories = [] as List<String>
    }

    @Override
    protected void registerParameters () {
        super.registerParameters()
        methodParams.register('super', ['resourcePath', 'classLoader', 'resourceDirectories'])
    }

    @Override
    Boolean isCaseSensitiveName() { return true }

    @Override
    @JsonIgnore
    String getHostOS() {
        return unixOS
    }

    @Override
    void setRootPath(String value) {
        super.setRootPath(value)
        if (connected)
            setCurrentDirectory(directoryFromPath(rootPath))
    }

    /** Connect status */
    private Boolean connected = false

    /** Catalog of directories and files */
    private ResourceCatalogElem rootNode

    /** Current directory */
    private ResourceCatalogElem currentDirectory
    /** Current directory */
    private setCurrentDirectory(ResourceCatalogElem value) {
        currentDirectory = value
        _currentPath = currentDirectory.filepath
    }

    private void validResourcePath() {
        if (resourcePath == null)
            throw new ExceptionGETL('Required to specify the path to the resource directory!')
    }

    /** Return catalog files from specified path */
    @SuppressWarnings('UnnecessaryQualifiedReference')
    static ResourceCatalogElem ListDirFiles(String path) {
        def filePath = new File(path)
        if (!filePath.directory)
            throw new ExceptionGETL("Directory \"$path\" not found!")

        def res = new ResourceCatalogElem()
        res.filename = '/'
        res.filepath = '/'
        res.type = Manager.directoryType
        res.files = ListDirFilesFromParent(path, res)

        return res
    }

    @SuppressWarnings(['UnnecessaryQualifiedReference', 'UnnecessaryQualifiedReference'])
    static private List<ResourceCatalogElem> ListDirFilesFromParent(String path, ResourceCatalogElem parentElem) {
        def filePath = new File(path)
        def res = [] as List<ResourceCatalogElem>
        filePath.listFiles().each { file ->
            def attr = new ResourceCatalogElem()
            attr.filename = file.name
            if (file.directory) {
                attr.with {
                    type = Manager.directoryType
                    parent = parentElem
                    filepath = ((parentElem.filepath != '/')?parentElem.filepath:'') + '/' + file.name
                    files = ListDirFilesFromParent(path + '/' + file.name, it)
                }
            }
            else if (file.file) {
                attr.with {
                    type = Manager.fileType
                    filedate = file.lastModified()
                    filesize = file.size()
                    parent = parentElem
                    filepath = ((parentElem.filepath != '/')?parentElem.filepath:'') + '/' + file.name
                }
            }
            res.add(attr)
        }
        return res
    }

    @SuppressWarnings(['UnnecessaryQualifiedReference', 'UnnecessaryQualifiedReference', 'UnnecessaryQualifiedReference'])
    static ResourceCatalogElem ListDirJar(String path) {
        def p = new Path(mask: 'file:{jar}!{dir}')
        def m = p.analyze(path, false)
        if (m == null || m.isEmpty()) {
            m = [jar: path, dir: '']
        }

        def jarFileName = m.jar as String
        def dirPath = new Path(mask: "${m.dir}/{name}")

        def zip = new ZipFile(jarFileName)

        def res = new ResourceCatalogElem()
        res.filename = '/'
        res.filepath = '/'
        res.type = Manager.directoryType
        res.files = [] as List<ResourceCatalogElem>

        try {
            zip.fileHeaders.each { head ->
                def attr = dirPath.analyze('/' + head.fileName, false)
                if (attr == null) return

                def relFilePath = attr.name as String
                def pathName = FileUtils.RelativePathFromFile(relFilePath, true)
                def fileName = (!head.directory) ? FileUtils.FileName(relFilePath) : null
                def paths = pathName.split('/')

                def parentDir = res
                paths.each { dir ->
                    def cur = (dir != '.') ? parentDir.files.find { elem -> (elem.filename == dir) } : parentDir
                    if (cur == null) {
                        cur = new ResourceCatalogElem()
                        cur.tap {
                            filename = dir
                            type = Manager.directoryType
                            files = [] as List<ResourceCatalogElem>
                            parent = parentDir
                            filepath = ((parentDir.filepath != '/') ? parentDir.filepath : '') + '/' + dir
                        }
                        parentDir.files.add(cur)
                    }
                    parentDir = cur
                }
                if (head.directory) return

                def elem = new ResourceCatalogElem()
                elem.with {
                    type = Manager.fileType
                    filename = fileName
                    parent = parentDir
                    filepath = ((parentDir.filepath != '/') ? parentDir.filepath : '') + '/' + fileName
                    filedate = head.lastModifiedTimeEpoch
                    filesize = head.uncompressedSize
                }
                parentDir.files.add(elem)
            }
        }
        finally {
            zip.close()
        }

        return res
    }

    private Boolean isZipFile = false

    private void buildCatalog() {
        def res = (classLoader != null)?classLoader.getResource(resourcePath):
                GroovyClassLoader.getResource(resourcePath)

        isZipFile = false
        if (res == null && FileUtils.ExistsFile(resourcePath)) {
            def file = new File(resourcePath)
            res = file.toURI().toURL()
            isZipFile = true
        }
        if (res == null)
            throw new ExceptionGETL("There is no directory \"$resourcePath\" in the resources!")

        rootNode = (res.protocol == 'file' && !isZipFile)?ListDirFiles(res.file):ListDirJar(res.file)
        setCurrentDirectory(directoryFromPath(currentRootPath))
    }

    @Override
    @Synchronized
    protected void doConnect() {
        if (connected)
            throw new ExceptionGETL('Manager already connected!')

        validResourcePath()
        buildCatalog()

        connected = true
    }

    @Override
    @Synchronized
    protected void doDisconnect() {
        if (!connected)
            throw new ExceptionGETL('Manager already disconnected!')

        connected = false
        rootNode = null
    }

    @Override
    @JsonIgnore
    Boolean isConnected() { connected }

    class ResourceFileList extends FileManagerList {
        List<ResourceCatalogElem> listFiles

        @CompileStatic
        @Override
        Integer size () {
            (listFiles != null)?listFiles.size():0
        }

        @SuppressWarnings('UnnecessaryQualifiedReference')
        @CompileStatic
        @Override
        Map item (Integer index) {
            def f = listFiles[index]

            Map<String, Object> m =  new HashMap<String, Object>()
            m.filename = f.filename
            m.type = f.type
            if (f.type == Manager.fileType) {
                m.filedate = new Date(f.filedate)
                m.filesize = f.filesize
            }

            return m
        }

        @CompileStatic
        @Override
        void clear () {
            listFiles = null
        }
    }

    @Override
    FileManagerList listDir(String mask = null) {
        validConnect()

        def dir = directoryFromPath(mask)

        List<ResourceCatalogElem> files
        def fileName = FileUtils.FileName(mask)
        if (fileName != null) {
            if (FileUtils.IsMaskFilePath(fileName)) {
                def p = new Path()
                p.compile(mask: fileName)
                files = dir.files.findAll { p.match(it.filename) }
            }
            else {
                files = [dir.files.find { it.filename == fileName}] as List<ResourceCatalogElem>
            }
        }
        else
            files = dir.files

        def res = new ResourceFileList()
        res.listFiles = files
        return res
    }

    /** Current directory */
    private ResourceCatalogElem curDir

    @Override
    @JsonIgnore
    String getCurrentPath() {
        return currentDirectory.filepath
    }

    @Override
    void setCurrentPath(String path) {
        if (path == null || path.length() == 0)
            throw new ExceptionGETL('Required to specify the path to change the directory!')

        if (path == '.') return

        if (path == '..') {
            changeDirectoryUp()
            return
        }

        if (path == '/') {
            setCurrentDirectory(rootNode)
            return
        }

        setCurrentDirectory(directoryFromPath(path))
    }

    @SuppressWarnings('UnnecessaryQualifiedReference')
    private ResourceCatalogElem directoryFromPath(String path) {
        def cp = FileUtils.ConvertToUnixPath(path)
        if (cp == null)
            return currentDirectory

        def isMask = FileUtils.IsMaskFileName(path)

        def dirs = cp.split('/')
        def size = dirs.length - ((isMask)?1:0)

        def cd = (cp[0] == '/')?rootNode:currentDirectory
        for (Integer i = 0; i < size; i++) {
            def dir = dirs[i]

            if (dir == '' || dir == '.')
                continue

            if (dir == '..') {
                cd = cd.parent
                continue
            }

            def child = cd.files.find { it.filename == dir }
            if (child == null)
                throw new ExceptionGETL("Path \"$path\" not found!")
            else if (child.type == Manager.fileType)
                break

            cd = child
        }

        return cd
    }

    @Override
    void changeDirectoryUp() {
        if (currentDirectory == rootNode)
            throw new ExceptionGETL('Unable to navigate above the root directory!')

        setCurrentDirectory(currentDirectory.parent)
    }

    @Override
    protected void validConnect () {
        if (!connected)
            connect()
    }

    @Override
    File download(String filePath, String localPath, String localFileName) {
        validConnect()

        def cd = directoryFromPath(FileUtils.RelativePathFromFile(filePath, true))
        def fileName = FileUtils.FileName(filePath)
        def file = cd.files.find { it.filename == fileName }
        if (file == null)
            throw new ExceptionGETL("File \"$filePath\" not found!")
        String fp
        if (!isZipFile)
            fp = resourcePath + file.filepath
        else
            fp = file.filepath.substring(1)

        def destFile = new File(localPath + '/' + localFileName)
        FileUtils.ValidFilePath(destFile)

        def destDir = destFile.parentFile
        def delDirs = [] as List<File>
        while (destDir.canonicalPath != localDirectoryFile.canonicalPath) {
            delDirs << destDir
            destDir = destDir.parentFile
        }
        delDirs.reverse().each { dir -> dir.deleteOnExit() }

        def res= FileUtils.FileFromResources(fp, resourceDirectories, classLoader, destFile)
        if (res == null)
            throw new ExceptionGETL("Resource file \"$fp\" not found!")

        return res
    }

    @Override
    void upload(String path, String fileName) {
        validWrite()
    }

    @Override
    void removeFile(String fileName) {
        validWrite()
    }

    @Override
    void createDir(String dirName) {
        validWrite()
    }

    @Override
    void removeDir(String dirName, Boolean recursive,
                   @ClosureParams(value = SimpleType, options = ['java.lang.String']) Closure onDelete = null) {
        validWrite()
    }

    @Override
    void rename(String fileName, String path) {
        validWrite()
    }

    @Override
    Long getLastModified(String fileName) {
        if (fileName == null)
            throw new ExceptionGETL('A file name is required!')

        def path = FileUtils.RelativePathFromFile(fileName, true)
        def name = FileUtils.FileName(fileName)

        ResourceCatalogElem cd = (path == '.')?currentDirectory:directoryFromPath(path)
        def file = cd.files.find { it.filename == name }
        if (file == null)
            throw new ExceptionGETL("File \"$fileName\" not found!")

        return file.filedate as Long
    }

    @Override
    void setLastModified(String fileName, Long time) {
        validWrite()
    }

    @Override
    String getObjectName() {
        return (rootPath != null)?"resource:/$rootPath":'resource'
    }

    @Override
    protected Boolean allowWrite() { false }
}