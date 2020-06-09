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
package getl.files

import getl.exception.ExceptionGETL
import getl.files.sub.FileManagerList
import getl.files.sub.ResourceCatalogElem
import getl.utils.FileUtils
import getl.utils.Path
import groovy.transform.CompileStatic
import groovy.transform.InheritConstructors
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
    void useResourceDirectories(List<String> value) {
        resourceDirectories.clear()
        if (value != null)
            resourceDirectories.addAll(value)
    }

    /** Use class loader to access resources */
    private ClassLoader classLoader
    /** Use class loader to access resources */
    ClassLoader getClassLoader() { classLoader }
    /** Use class loader to access resources */
    void useClassLoader(ClassLoader value) { classLoader = value }

    @Override
    protected void initParams() {
        super.initParams()
        params.resourceDirectories = [] as List<String>
    }

    @Override
    protected void initMethods () {
        super.initMethods()
        methodParams.register('super', ['resourcePath', 'classLoader', 'resourceDirectories'])
    }

    @Override
    boolean isCaseSensitiveName() { return true }

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
    private setCurrentDirectory(ResourceCatalogElem value) {
        currentDirectory = value
        _currentPath = currentDirectory.filepath
    }

    private void validResourcePath() {
        if (resourcePath == null)
            throw new ExceptionGETL('Required to specify the path to the resource directory!')
    }

    /** Return catalog files from specified path */
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

    static ResourceCatalogElem ListDirJar(String path) {
        def p = new Path(mask: 'file:{jar}!{dir}')
        def m = p.analize(path, false)
        if (m.isEmpty())
            throw new ExceptionGETL("Invalid path to resource file \"$path\"!")

        def jarFileName = m.jar as String
        def dirPath = new Path(mask: "${m.dir}/{name}")

        def zip = new ZipFile(jarFileName)

        def res = new ResourceCatalogElem()
        res.filename = '/'
        res.filepath = '/'
        res.type = Manager.directoryType
        res.files = [] as List<ResourceCatalogElem>

        zip.fileHeaders.each { head ->
            def attr = dirPath.analize('/' + head.fileName, false)
            if (attr == null) return

            def relFilePath = attr.name as String
            def pathName = FileUtils.RelativePathFromFile(relFilePath, true)
            def fileName = (!head.directory)?FileUtils.FileName(relFilePath):null
            def paths = pathName.split('/')

            def parentDir = res
            paths.each { dir ->
                def cur = parentDir.files.find { elem -> (elem.filename == dir) }
                if (cur == null) {
                    cur = new ResourceCatalogElem()
                    cur.with {
                        filename = dir
                        type = Manager.directoryType
                        files =  [] as List<ResourceCatalogElem>
                        parent = parentDir
                        filepath = ((parentDir.filepath != '/')?parentDir.filepath:'') + '/' + dir
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
                filepath = ((parentDir.filepath != '/')?parentDir.filepath:'') + '/' + fileName
                filedate = head.lastModifiedTime
                filesize = head.uncompressedSize
            }
            parentDir.files.add(elem)
        }

        return res
    }

    private void buildCatalog() {
        def res = GroovyClassLoader.getResource(resourcePath)
        if (res == null)
            throw new ExceptionGETL("There is no directory \"$resourcePath\" in the resources!")

        rootNode = (res.protocol == 'file')?ListDirFiles(res.file):ListDirJar(res.file)
        setCurrentDirectory(directoryFromPath(rootPath))
    }

    @Override
    void connect() {
        if (connected)
            throw new ExceptionGETL('Manager already connected!')

        validResourcePath()
        buildCatalog()

        connected = true
    }

    @Override
    void disconnect() {
        if (!connected)
            throw new ExceptionGETL('Manager already disconnected!')

        connected = false
        rootNode = null
    }

    @Override
    boolean isConnected() { connected }

    class ResourceFileList extends FileManagerList {
        List<ResourceCatalogElem> listFiles

        @CompileStatic
        @Override
        Integer size () {
            listFiles.size()
        }

        @CompileStatic
        @Override
        Map item (int index) {
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
    FileManagerList listDir(String mask) {
        validConnect()

        def dir = directoryFromPath(mask)

        Path p
        if (mask != null) {
            def strmask = FileUtils.MaskFile(mask)
            if (strmask != null) {
                p = new Path()
                p.compile(mask: strmask)
            }
        }

        def files = dir.files.findAll {
            return (p == null || p.match(it.filename))
        }

        def res = new ResourceFileList()
        res.listFiles = files
        return res
    }

    /** Current directory */
    private ResourceCatalogElem curDir

    @Override
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

    private ResourceCatalogElem directoryFromPath(String path) {
        def cp = FileUtils.ConvertToUnixPath(path)
        if (cp == null)
            return currentDirectory

        def mask = FileUtils.MaskFile(path)

        def dirs = cp.split('/')
        def size = dirs.length - ((mask != null)?1:0)

        def cd = (cp[0] == '/')?rootNode:currentDirectory
        for (int i = 0; i < size; i++) {
            def dir = dirs[i]

            if (dir == '' || dir == '.')
                continue

            if (dir == '..') {
                cd = cd.parent
                continue
            }

            def child = cd.files.find { it.filename == dir }
            if (child.type == Manager.fileType) {
                if (mask == null && i == size - 1) break
                child = null
            }
            if (child == null)
                throw new ExceptionGETL("Path \"$path\" not found!")

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
    void download(String filePath, String path, String localFileName) {
        validConnect()

        def cd = directoryFromPath(FileUtils.RelativePathFromFile(filePath, true))
        def fileName = FileUtils.FileName(filePath)
        def file = cd.files.find { it.filename == fileName }
        if (file == null)
            throw new ExceptionGETL("File \"$filePath\" not found!")
        def fp = resourcePath + file.filepath

        def destFile = new File(path + cd.filepath + '/' + fileName)
        FileUtils.ValidFilePath(destFile)

        def destDir = destFile.parentFile
        def delDirs = [] as List<File>
        while (destDir.canonicalPath != localDirFile.canonicalPath) {
            delDirs << destDir
            destDir = destDir.parentFile
        }
        delDirs.reverse().each { dir -> dir.deleteOnExit() }

        FileUtils.FileFromResources(fp, resourceDirectories, classLoader, destFile)
    }

    @Override
    void upload(String path, String fileName) {
        throw new ExceptionGETL('Not supported!')
    }

    @Override
    void removeFile(String fileName) {
        throw new ExceptionGETL('Not supported!')
    }

    @Override
    void createDir(String dirName) {
        throw new ExceptionGETL('Not supported!')
    }

    @Override
    void removeDir(String dirName, Boolean recursive) {
        throw new ExceptionGETL('Not supported!')
    }

    @Override
    void rename(String fileName, String path) {
        throw new ExceptionGETL('Not supported!')
    }

    @Override
    long getLastModified(String fileName) {
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
    void setLastModified(String fileName, long time) {
        throw new ExceptionGETL('Not supported!')
    }
}