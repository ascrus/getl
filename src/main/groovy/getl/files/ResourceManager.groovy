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
import groovy.transform.InheritConstructors
import net.lingala.zip4j.ZipFile

/**
 * Resource files manager
 * @author Alexsey Konstantinov
 */
@InheritConstructors
class ResourceManager extends Manager {
    /** Resource file storage paths */
    List<String> getResourcePaths() { params.resourcePaths as List<String> }
    /** Resource file storage paths */
    void useResourcePaths(List<String> value) {
        resourcePaths.clear()
        if (value != null)
            resourcePaths.addAll(value)
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
        params.resourcePaths = [] as List<String>
    }

    @Override
    protected void initMethods () {
        super.initMethods()
        methodParams.register("super", ["resourcePaths", "classLoader"])
    }

    @Override
    boolean isCaseSensitiveName() { return true }

    void setRootPath(String value) {
        super.setRootPath(value)
        if (connected) {
            validRootPath()
            buildCatalog()
        }
    }

    /** Connect status */
    private Boolean connected = false

    /** Catalog of directories and files */
    private ResourceCatalogElem rootNode

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
        def dirPath = new Path(mask: "${m.dir}/*")

        def zip = new ZipFile(jarFileName)

        def res = new ResourceCatalogElem()
        res.filename = '/'
        res.filepath = '/'
        res.type = Manager.directoryType
        res.files = [] as List<ResourceCatalogElem>

        zip.fileHeaders.each { head ->
            if (!dirPath.match(head.fileName)) return

            def pathName = FileUtils.RelativePathFromFile(head.fileName, true)
            def fileName = FileUtils.FileName(head.fileName)
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

            def elem = new ResourceCatalogElem()
            elem.with {
                filename = fileName
                parent = parentDir
                filepath = ((parentDir.filepath != '/')?parentDir.filepath:'') + '/' + fileName
            }
            if (head.directory) {
                elem.with {
                    type = Manager.directoryType
                    files = [] as List<ResourceCatalogElem>
                }
            }
            else {
                elem.with {
                    type = Manager.fileType
                    filedate = head.lastModifiedTime
                    filesize = head.uncompressedSize
                }
            }
            parentDir.files.add(elem)
        }

        return res
    }

    private void buildCatalog() {
        def res = GroovyClassLoader.getResource(rootPath)
        if (res == null)
            throw new ExceptionGETL("There is no directory \"$rootPath\" in the resources!")

        rootNode = (res.protocol == 'file')?ListDirFiles(res.file):ListDirJar(res.file)
    }

    @Override
    void connect() {
        if (connected)
            throw new ExceptionGETL('Manager already connected!')

        validRootPath()
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

    @Override
    FileManagerList listDir(String mask) {
        validConnect()
        return null
    }

    /** Current directory */
    private ResourceCatalogElem curDir

    @Override
    String getCurrentPath() {
        throw new ExceptionGETL('Not supported!')
    }

    @Override
    void setCurrentPath(String path) {
        throw new ExceptionGETL('Not supported!')
    }

    @Override
    void changeDirectoryUp() {
        throw new ExceptionGETL('Not supported!')
    }

    @Override
    protected void validConnect () {
        if (!connected)
            connect()
    }

    @Override
    void download(String fileName, String path, String localFileName) {
        validConnect()

        fileName = FileUtils.ConvertToUnixPath(fileName)
        if (fileName[0] != '/') fileName = '/' + fileName
        def filePath = fileName
        if (rootPath != null)
            filePath = rootPath + filePath

        def destFile = new File(path + '/' + localFileName)
        FileUtils.ValidFilePath(destFile)
        def destDir = destFile.parentFile
        while (destDir.canonicalPath != localDirFile.canonicalPath) {
            destDir.deleteOnExit()
            destDir = destDir.parentFile
        }

        FileUtils.FileFromResources(filePath, resourcePaths, classLoader, destFile)
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
        throw new ExceptionGETL('Not supported!')
    }

    @Override
    void setLastModified(String fileName, long time) {
        throw new ExceptionGETL('Not supported!')
    }
}