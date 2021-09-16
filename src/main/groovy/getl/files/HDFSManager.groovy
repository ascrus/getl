package getl.files

import com.fasterxml.jackson.annotation.JsonIgnore
import getl.exception.ExceptionGETL
import getl.files.sub.FileManagerList
import getl.lang.Getl
import getl.lang.sub.UserLogins
import getl.utils.FileUtils
import getl.utils.StringUtils
import getl.lang.sub.LoginManager
import getl.lang.sub.StorageLogins
import groovy.transform.CompileStatic
import groovy.transform.InheritConstructors
import groovy.transform.Synchronized
import org.apache.hadoop.fs.FileStatus
import org.apache.hadoop.fs.FileSystem
import getl.utils.Logs
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.Path
import org.apache.hadoop.fs.PathFilter
import org.apache.hadoop.security.UserGroupInformation

import java.security.PrivilegedExceptionAction

/**
 * HDFS manager
 * @author Alexsey Konstantinov
 */
@InheritConstructors
class HDFSManager extends Manager implements UserLogins {
    @Override
    void initParams() {
        super.initParams()
        loginManager = new LoginManager(this)
        params.storedLogins = new StorageLogins(loginManager)
    }

    @Override
    protected void registerParameters() {
        super.registerParameters()
        methodParams.register('super', ['server', 'port', 'login', 'password', 'storedLogins', 'replication'])
    }

    @Override
    protected void onLoadConfig(Map configSection) {
        super.onLoadConfig(configSection)
        loginManager.encryptObject()
    }

    /** Server address */
    String getServer() { params.server }
    /** Server address */
    void setServer(String value) { params.server = value }

    /** Server port */
    Integer getPort() { (params.port != null)?(params.port as Integer):8022 }
    /** Server port */
    void setPort(Integer value) { params.port = value }

    @Override
    String getLogin() { params.login }
    @Override
    void setLogin(String value) { params.login = value }

    @Override
    String getPassword() { params.password }
    @Override
    void setPassword(String value) { params.password = loginManager.encryptPassword(value) }

    @Override
    Map<String, String> getStoredLogins() { params.storedLogins as Map<String, String> }
    @Override
    void setStoredLogins(Map<String, String> value) {
        storedLogins.clear()
        if (value != null) storedLogins.putAll(value)
    }

    /** Replication parameter for root directory */
    Short getReplication() { params.replication as Short }
    /** Replication parameter for root directory */
    void setReplication(Short value) {
        if (value != null && value < 1)
            throw new ExceptionGETL("The replication parameter cannot be less than 1!")
        params.replication = value
    }

    /** File system driver */
    private FileSystem client

    /** Home directory by user */
    private String homeDirectory
    /** Home directory by user */
    @JsonIgnore
    String getHomeDirectory() { this.homeDirectory }

    @Override
    @JsonIgnore
    Boolean isCaseSensitiveName() {
        return true
    }

    @Override
    @JsonIgnore
    String getHostOS() {
        return unixOS
    }

    @Override
    @JsonIgnore
    Boolean isConnected() { client != null }

    class ConfigAction implements PrivilegedExceptionAction<Void> {
        ConfigAction(HDFSManager owner) {
            this.man = owner
        }

        private final HDFSManager man

        @Override
        Void run() throws Exception {
            Configuration conf = new Configuration()
            conf.set("fs.defaultFS", "hdfs://${man.server}:${man.port}")
            conf.set("hadoop.job.ugi", man.login)

            try {
                man.client = FileSystem.get(conf)
            }
            catch (Exception e) {
                if (writeErrorsToLog) logger.severe("Can not connect to ${man.server}:${man.port}")
                throw e
            }
            man.homeDirectory = client.homeDirectory
            man.currentPath = man.currentRootPath
            if (man.rootPath != null && man.replication != null)
                client.setReplication(new Path(man.rootPath), man.replication)

            return null
        }
    }

    @Override
    @Synchronized
    protected void doConnect() {
        if (connected)
            throw new ExceptionGETL('Manager already connected!')

        if (server == null || port == null)
            throw new ExceptionGETL("Required server host and port for connect")
        if (login == null)
            throw new ExceptionGETL("Required login for connect")

        writeScriptHistoryFile("Connect to hdfs $server:$port with login $login from session $sessionID")

        UserGroupInformation ugi = UserGroupInformation.createRemoteUser(login)
        ugi.doAs(new ConfigAction(this))
    }

    @Override
    @Synchronized
    protected void doDisconnect() {
        if (!connected)
            throw new ExceptionGETL('Manager already disconnected!')

        try {
            if (client != null) client.close()
        }
        finally {
            client = null
            homeDirectory = null
            _currentPath = null
        }
    }

    @Override
    @JsonIgnore
    String getCurrentPath() {
        return _currentPath
    }

    @Override
    void setCurrentPath(String path) {
        validConnect()

        if (path == _currentPath) return

        if (path == null || path == '/') {
            if (writeErrorsToLog) logger.severe('Invalid path: \"$path\"')
            throw new ExceptionGETL('Invalid null path')
        }
        path = fullName(path, null)
        def p = new Path(path)
        if (!client.exists(p)) {
            if (writeErrorsToLog) logger.severe("Path \"$path\" not found")
            throw new ExceptionGETL("Path \"$path\" not found")
        }
        if (!client.exists(p) || !client.getFileStatus(p).isDirectory()) {
            if (writeErrorsToLog) logger.severe("Path \"$path\" non directory")
            throw new ExceptionGETL("Path \"$path\" non directory")
        }
        _currentPath = path
    }

    private fullName(String dir, String file) {
        if (dir != null && dir[0] == '/' && StringUtils.LeftStr(dir, 6) != '/user/') dir = dir.substring(1)
        if (dir == null) dir = currentPath
        if (!((dir + '/').matches(rootPath + '/.*'))) dir = currentRootPath + '/' + dir
        return ((dir != null)?dir:'') + ((file != null)?"/$file":'')
    }

    private Path fullPath(String dir, String file) {
        new Path(fullName(dir, file))
    }

    class HDFSList extends FileManagerList {
        FileStatus[] listFiles

        @CompileStatic
        @Override
        Integer size() {
            listFiles.length
        }

        @CompileStatic
        @Override
        Map item(Integer index) {
            FileStatus f = listFiles[index]

            Map<String, Object> m = new HashMap<String, Object>()
            m.filename = f.path.name
            m.filedate = new Date(f.modificationTime)
            m.filesize = f.len
            if  (f.isSymlink()) m.link = f.symlink.name

            if (f.directory) {
                m.type = Manager.TypeFile.DIRECTORY
            }
            else if (f.file) {
                m.type = Manager.TypeFile.FILE
            }
            else if (f.symlink) {
                m.type = Manager.TypeFile.LINK
            }
            else {
                throw new ExceptionGETL("Unnknown type object ${m.filename}")
            }

            return m
        }

        @CompileStatic
        @Override
        void clear() {
            listFiles = null
        }
    }

    /** Filter class for list dir */
    class FilterDir implements PathFilter {
        FilterDir(String mask) {
            maskPath = new getl.utils.Path(mask: mask)
        }

        private final getl.utils.Path maskPath

        @Override
        boolean accept(Path path) {
            return maskPath.match(path.name)
        }
    }

    @Override
    FileManagerList listDir(String maskFiles = null) {
        validConnect()

        HDFSList res = new HDFSList()
        if (maskFiles != null)
            res.listFiles = client.listStatus(fullPath(_currentPath, null), new FilterDir(maskFiles))
        else
            res.listFiles = client.listStatus(fullPath(_currentPath, null))

        return res
    }

    @Override
    void changeDirectoryUp() {
        validConnect()

        if (_currentPath == currentRootPath) {
            if (writeErrorsToLog) logger.severe("Can not change directory to up with root directory \"$currentRootPath\"")
            throw new ExceptionGETL("Can not change directory to up with root directory \"$currentRootPath\"")
        }

        String[] l = _currentPath.split('/')
        def n = []
        for (Integer i = 0; i < l.length - 1; i++) {
            n << l[i]
        }
        def c = n.join('/')

        try {
            currentPath = c
        }
        catch (Exception e) {
            if (writeErrorsToLog) logger.severe("Can not change directory to up: ${e.message}")
            throw e
        }
    }

    @Override
    File download(String filePath, String localPath, String localFileName) {
        validConnect()

        File res
        def fn = ((localPath != null)?localPath + '/':'') + localFileName
        try {
            def p = fullPath(_currentPath, filePath)
            client.copyToLocalFile(false, p, new Path(fn), true)
            res = new File(fn)
            setLocalLastModified(res, getLastModified(filePath))
        }
        catch (Exception e) {
            if (writeErrorsToLog) logger.severe("Can not download file \"${fullName(_currentPath, filePath)}\" to \"$fn\"")
            throw e
        }

        return res
    }

    @Override
    void upload(String path, String fileName) {
        validConnect()

        def fn = ((path != null)?path + "/":"") + fileName
        try {
            def p = fullPath(_currentPath, fileName)
            client.copyFromLocalFile(new Path(fn), p)
            def f = new File(fn)
            setLastModified(fileName, f.lastModified())
        }
        catch (Exception e) {
            if (writeErrorsToLog) logger.severe("Can not upload file \"$fn\" to \"${fullName(_currentPath, fileName)}\"")
            throw e
        }
    }

    @Override
    void removeFile(String fileName) {
        validConnect()

        try {
            client.delete(fullPath(_currentPath, fileName), false)
        }
        catch (Exception e) {
            if (writeErrorsToLog) logger.severe("Can not remove file \"${fullName(_currentPath, fileName)}\"")
            throw e
        }
    }

    @Override
    void createDir(String dirName) {
        validConnect()

        try {
            client.mkdirs(fullPath(_currentPath, dirName))
        }
        catch (Exception e) {
            if (writeErrorsToLog) logger.severe("Can not create dir \"${fullName(_currentPath, dirName)}\"")
            throw e
        }
    }

    @Override
    void removeDir(String dirName, Boolean recursive) {
        validConnect()

        try {
            client.delete(fullPath(_currentPath, dirName), recursive)
        }
        catch (Exception e) {
            if (writeErrorsToLog) logger.severe("Can not remove dir \"${fullName(_currentPath, dirName)}\"")
            throw e
        }
    }

    @Override
    void rename(String fileName, String path) {
        validConnect()

        try {
            if (FileUtils.RelativePathFromFile(path, '/') == '.')
                path = fullPath(_currentPath, path)
            client.rename(fullPath(_currentPath, fileName), new Path(path))
        }
        catch (Exception e) {
            if (writeErrorsToLog) logger.severe("Can not rename file \"${fullName(_currentPath, fileName)}\" to \"$path\"")
            throw e
        }
    }

    @Override
    Boolean existsDirectory(String dirName) {
        validConnect()

        def path = fullPath(dirName, null)
        def res = client.exists(path)
        if (res)
            res = client.getFileStatus(path).directory

        return res
    }

    @Override
    Boolean existsFile(String fileName) {
        validConnect()

        def path = fullPath(null, fileName)
        def res = client.exists(path)
        if (res)
            res = client.getFileStatus(path).file

        return res
    }

    @Override
    Long getLastModified(String fileName) {
        validConnect()

        def s = client.getFileStatus(fullPath(_currentPath, fileName))
        return s.modificationTime
    }

    @Override
    void setLastModified(String fileName, Long time) {
        validConnect()

        if (saveOriginalDate)
            client.setTimes(fullPath(_currentPath, fileName), time, -1)
    }

    @Override
    String getObjectName() {
        if (server == null)
            return 'hdfs'

        String res
        if (rootPath == null || rootPath.length() == 0)
            res = "hdfs://$server"
        else if (currentRootPath[0] == '/')
            res = "hdfs://$server$currentRootPath"
        else
            res = "hdfs://$server/$currentRootPath"

        return res
    }

    @Override
    void noop () {
        super.noop()
        client.getStatus()
    }

    /** Logins manager */
    private LoginManager loginManager

    @Override
    void useLogin(String user) {
        loginManager.useLogin(user)
    }

    @Override
    void switchToNewLogin(String user) {
        loginManager.switchToNewLogin(user)
    }

    @Override
    void switchToPreviousLogin() {
        loginManager.switchToPreviousLogin()
    }

    @Override
    void useDslCreator(Getl value) {
        def passwords = loginManager.decryptObject()
        super.useDslCreator(value)
        loginManager.encryptObject(passwords)
    }

    @Override
    protected List<String> ignoreCloneClasses() { [StorageLogins.name] }

    @Override
    protected void afterClone(Manager original) {
        super.afterClone(original)
        def o = original as HDFSManager
        def passwords = o.loginManager.decryptObject()
        loginManager.encryptObject(passwords)
    }
}