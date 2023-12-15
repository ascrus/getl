//file:noinspection unused
//file:noinspection DuplicatedCode
package getl.files

import com.fasterxml.jackson.annotation.JsonIgnore
import getl.exception.FilemanagerError
import getl.exception.IOFilesError
import getl.exception.IncorrectParameterError
import getl.exception.RequiredParameterError
import getl.files.sub.FileManagerList
import getl.lang.Getl
import getl.lang.sub.UserLogins
import getl.utils.*
import getl.lang.sub.LoginManager
import getl.lang.sub.StorageLogins
import groovy.transform.CompileStatic
import groovy.transform.InheritConstructors
import groovy.transform.Synchronized
import groovy.transform.stc.ClosureParams
import groovy.transform.stc.SimpleType
import it.sauronsoftware.ftp4j.*

/**
 * FTP Manager
 * @author Alexsey Konstantinov
 *
 */
@InheritConstructors
class FTPManager extends Manager implements UserLogins {
	/** FTP driver */
	private final FTPClient client = new FTPClient()

	@Override
	void initParams() {
		super.initParams()
		loginManager = new LoginManager(this)
		params.storedLogins = new StorageLogins(loginManager)
	}
	
	@Override
	protected void registerParameters() {
		super.registerParameters()
		methodParams.register('super', ['server', 'port', 'login', 'password', 'passive', 'isHardDisconnect',
										'autoNoopTimeout', 'closeTimeout', 'connectionTimeout', 'readTimeout',
                                        'timeZone', 'storedLogins'])
	}
	
	@Override
	protected void onLoadConfig(Map configSection) {
		super.onLoadConfig(configSection)
		loginManager.encryptObject()
		if (rootPath != null && rootPath.substring(0, 1) != '/')
			rootPath = '/' + rootPath
	}

	@Override
	void setRootPath(String value) {
		if (value != null && value.length() > 1 && value.substring(0, 1) != '/') value = '/' + value
		super.setRootPath(value)
	}
	
	/** Server address */
	String getServer() { params.server }
	/** Server address */
	void setServer(String value) { params.server = value }
	/** Current server address */
	String server() { StringUtils.EvalMacroString(server, dslVars, false) }
	
	/** Server port */
	Integer getPort() { (params.port != null)?(params.port as Integer):21 }
	/** Server port */
	void setPort(Integer value) { params.port = value }
	
	@Override
	String getLogin() { params.login }
	@Override
	void setLogin(String value) { params.login = value }
	/** Current login */
	String login() { StringUtils.EvalMacroString(login, dslVars, false) }
	
	@Override
	String getPassword() { params.password }
	@Override
	void setPassword(String value) { params.password = loginManager.encryptPassword(value) }

	@Override
	Map<String, String> getStoredLogins() { params.storedLogins as Map<String, String> }
	@Override
	void setStoredLogins(Map<String, String> value) {
		storedLogins.clear()
		if (value != null)
			storedLogins.putAll(value)
	}
	
	/** Passive mode */
	Boolean getPassive() { (params.passive != null)?params.passive:true }
	/** Passive mode */
	void setPassive(Boolean value) {
		if (client.connected)
			client.setPassive(value)

		params.passive = value 
	}
	
	/** Hard disconnect */
	Boolean getIsHardDisconnect() { (params.isHardDisconnect != null)?params.isHardDisconnect:false }
	/** Hard disconnect */
	void setIsHardDisconnect(Boolean value) { params.isHardDisconnect = value }
	
	/** Auto noop timeout in seconds */
	Integer getAutoNoopTimeout() { params.autoNoopTimeout as Integer }
	/** Auto noop timeout in seconds */
	void setAutoNoopTimeout(Integer value) {
		if (value != null && value < 1)
			throw new IncorrectParameterError(this, '#params.great_zero', 'autoNoopTimeout')

		if (client.connected)
			client.setAutoNoopTimeout(value * 1000)

		params.autoNoopTimeout = value
	}
	
	/** Close timeout */
	Integer getCloseTimeout() { params.closeTimeout as Integer }
	/** Close timeout */
	void setCloseTimeout(Integer value) {
		if (value != null && value < 1)
			throw new IncorrectParameterError(this, '#params.great_zero', 'closeTimeout')

		if (client.connected)
			client.connector.closeTimeout = value

		params.closeTimeout = value
	}
	
	/** Connection timeout in seconds */
	Integer getConnectionTimeout() { params.connectionTimeout as Integer }
	/** Connection timeout in seconds */
	void setConnectionTimeout(Integer value) {
		if (value != null && value < 1)
			throw new IncorrectParameterError(this, '#params.great_zero', 'connectionTimeout')

		if (client.connected)
			client.connector.connectionTimeout = value

		params.connectionTimeout = value
	}
	
	/** Read timeout */
	Integer getReadTimeout() { params.readTimeout as Integer }
	/** Read timeout */
	void setReadTimeout(Integer value) {
		if (value != null && value < 1)
			throw new IncorrectParameterError(this, '#params.great_zero', 'readTimeout')

		if (client.connected)
			client.connector.readTimeout = value

		params.readTimeout = value
	}

    /** FTP server time zone */
    Integer getTimeZone() { (params.timeZone as Integer)?:0 }
	/** FTP server time zone */
	void setTimeZone(Integer value) { params.timeZone = value }
	
	@Override
	@JsonIgnore
	Boolean isCaseSensitiveName() { true }

	@Override
	@JsonIgnore
	String getHostOS() {
		return unixOS
	}

    private final supportCommands = [] as List<String>
    /**
     * Valid support command for FTP server
     * @param cmd
     * @return
     */
    Boolean supportCommand(String cmd) { (cmd.toUpperCase() in supportCommands) }

	@Override
	@JsonIgnore
	Boolean isConnected() { BoolUtils.IsValue(client?.connected) }
	
	@Override
	@Synchronized
	protected void doConnect() {
		if (server == null)
			throw new RequiredParameterError(this, 'server', 'connect')
		if (port == null)
			throw new RequiredParameterError(this, 'port', 'connect')
		if (login == null)
			throw new RequiredParameterError(this, 'login', 'connect')

		def lPassword = StringUtils.EvalMacroString(loginManager.currentDecryptPassword(), dslVars, false)

		writeScriptHistoryFile("Connect to ftp ${server()}:$port with login ${login()} from session $sessionID")
		
		if (connectionTimeout != null)
			client.connector.connectionTimeout = connectionTimeout
		if (closeTimeout != null)
			client.connector.closeTimeout = closeTimeout
		if (readTimeout != null)
			client.connector.readTimeout = readTimeout
		client.connect(server(), port)
		client.login(login(), lPassword)
        if (autoNoopTimeout != null)
			client.setAutoNoopTimeout(autoNoopTimeout * 1000)
		client.setType(FTPClient.TYPE_BINARY)
		client.setPassive(passive)

        supportCommands.clear()
        client.sendCustomCommand('FEAT').messages.each { String cmd ->
            supportCommands << cmd.trim().toUpperCase()
        }

		currentPath = currentRootPath
	}
	
	@Override
	@Synchronized
	protected void doDisconnect() {
		try {
			if (client.connected) {
				def numRetry = 0
				def sleepTime = ((client.autoNoopTimeout > 0) ? client.autoNoopTimeout : 100) as Integer
				client.autoNoopTimeout = 0

				client.disconnect(isHardDisconnect)
				while (client.connected) {
					numRetry++
					sleep(sleepTime)

					if (numRetry > 4)
						throw new FilemanagerError(this, '#fileman.fail_disconnect')
				}
			}
		}
		finally {
			_currentPath = null
		}
	}

	@InheritConstructors
	class FTPList extends FileManagerList {
		FTPList(FTPManager owner) {
			super()
			this.owner = owner
		}

		private FTPManager owner

		public FTPFile[] listFiles
		
		@CompileStatic
		@Override
		Integer size() {
			(listFiles != null)?listFiles.length:0
		}
		
		@CompileStatic
		@Override
		Map item(Integer index) {
			FTPFile f = listFiles[index]

			Map<String, Object> m = new HashMap<String, Object>()
			m.filename = f.name
			m.filedate = f.modifiedDate
			m.filesize = f.size
			m.link = f.link

			switch (f.type) {
				case FTPFile.TYPE_DIRECTORY:
					m.type = Manager.TypeFile.DIRECTORY
					break
				case FTPFile.TYPE_FILE:
					m.type = Manager.TypeFile.FILE
					break
				case FTPFile.TYPE_LINK:
					m.type = Manager.TypeFile.LINK
					break
				default:
					throw new FilemanagerError(owner, '#fileman.ftp.invalid_list_type', [type: f.type])
			  }
		  
			return m
		}

		@CompileStatic
		@Override
		void clear() {
			listFiles = null
		}
	}
	
	@CompileStatic
	@Override
	protected FileManagerList doListDir(String mask) {
		validConnect()

		FTPFile[] listFiles = (mask != null)?client.list(mask):client.list()

		FTPList res = new FTPList(this)
		res.listFiles = listFiles
		
		return res
	}
	
	@Override
	@JsonIgnore
	String getCurrentPath() {
		validConnect()

		return client.currentDirectory()
	}
	
	@Override
	void setCurrentPath(String path) {
		validConnect()

		try {
			client.changeDirectory(path)
			_currentPath = client.currentDirectory()
		}
		catch (Exception e) {
			if (writeErrorsToLog)
				logger.severe("Invalid directory \"$path\" on source \"$this\"", e)
			throw e
		}
	}
	
	@Override
	void changeDirectoryUp() {
		validConnect()

		try {
			client.changeDirectoryUp()
			_currentPath = currentPath
		}
		catch (Exception e) {
			if (writeErrorsToLog)
				logger.severe("Can not change directory to up on source \"$this\"", e)
			throw e
		}
	}
	
	@Override
	protected File doDownload(String filePath, String localPath, String localFileName) {
		validConnect()

		File res
		def fn = ((localPath != null)?localPath + '/':'') + localFileName
		try {
            res = new File(fn)
			client.download(filePath, res)
            setLocalLastModified(res, getLastModified(filePath))

		}
		catch (Exception e) {
			if (writeErrorsToLog)
				logger.severe("Can not download file \"$filePath\" to \"$fn\" on source \"$this\"", e)
			throw e
		}

		return res
	}

	@SuppressWarnings('SpellCheckingInspection')
	@Override
	protected void doUpload(String path, String fileName) {
		validConnect()
		validWrite()

		def fn = ((path != null)?path + '/':'') + fileName
		try {
            def f = new File(fn)
			client.upload(f)
            if (supportCommand('MFMT')) {
                def d = new Date(f.lastModified())
                if (timeZone != null) {
                    def tz = DateUtils.PartOfDate('TIMEZONE', d)
                    if (timeZone != tz) d = DateUtils.AddDate('HH', timeZone - tz, d)
                }
                setLastModified(fileName, d.time)
            }
		}
		catch (Exception e) {
			if (writeErrorsToLog)
				logger.severe("Can not upload file \"$fileName\" from \"$fn\" on source \"$this\"", e)
			throw e
		}
	}
	
	@Override
	void removeFile(String fileName) {
		validConnect()
		validWrite()

		try {
			client.deleteFile(fileName)
		}
		catch (Exception e) {
			if (writeErrorsToLog)
				logger.severe("Can not remove file \"$fileName\" on source \"$this\"", e)
			throw e
		}
	}
	
	@Override
	void createDir(String dirName) {
		validConnect()
		validWrite()

		def curDir = client.currentDirectory()
		String cdDir = null
		try {
			//noinspection RegExpSimplifiable
			def dirs = dirName.split('[/]')

			dirs.each { String dir ->
				if (cdDir == null) cdDir = dir else cdDir = "$cdDir/$dir"
				def isExists = true
				try {
					client.changeDirectory(dir)
				}
				catch (Exception ignored) {
					isExists = false
				}
				if (!isExists) {
					client.createDirectory(dir)
					client.changeDirectory(dir)
				}
			}
		}
		catch (Exception e) {
			if (writeErrorsToLog)
				logger.severe("Can not create directory \"$cdDir\" on source \"$this\"", e)
			throw e
		}
		finally {
			client.changeDirectory(curDir)
		}
	}
	
	@Override
	void removeDir(String dirName, Boolean recursive,
				   @ClosureParams(value = SimpleType, options = ['java.lang.String']) Closure onDelete = null) {
		validConnect()
		validWrite()

		try {
			if (recursive) {
                def l = client.list()
                def d = l.find { FTPFile f -> f.name == dirName && f.type == FTPFile.TYPE_DIRECTORY}
                if (d == null)
					throw new IOFilesError(this, '#io.dir.not_found', [path: dirName], writeErrorsToLog)

                doDeleteDirectory(d, onDelete)
            }
            else {
                client.deleteDirectory(dirName)
				if (onDelete != null)
					onDelete.call(dirName)
            }
		}
		catch (Exception e) {
			if (writeErrorsToLog)
				Logs.Severe(this, '#io.dir.fail_delete', [path: dirName], e)

			throw e
		}
	}

    /**
     * Recursive remove dir
     * @param objName
     */
    private void doDeleteDirectory(FTPFile obj,
								   @ClosureParams(value = SimpleType, options = ['java.lang.String']) Closure onDelete) {
        if (obj.type == FTPFile.TYPE_DIRECTORY) {
            client.changeDirectory(obj.name)
            try {
                def fl = client.list()
                fl.each { FTPFile f ->
                    doDeleteDirectory(f, onDelete)
                }
            }
            finally {
                client.changeDirectoryUp()
            }
            client.deleteDirectory(obj.name)
        }
        else  {
            client.deleteFile(obj.name)
        }
		if (onDelete != null)
			onDelete.call(obj.name)
    }
	
	@Override
	void rename(String fileName, String path) {
		validConnect()
		validWrite()

		try {
			client.rename(fileName, path)
		}
		catch (Exception e) {
			if (writeErrorsToLog)
				logger.severe("Can not rename file \"$fileName\" to \"$path\" on source \"$this\"", e)
			throw e
		}
	}

	@Override
	void noop() {
		super.noop()
		client.noop()
	}

    @Override
    Long getLastModified(String fileName) {
		validConnect()

        def fl = client.list(fileName)
        if (fl.length != 1)
			throw new IOFilesError(this, '#io.file.not_found', [path: fileName, type: 'FTP'], writeErrorsToLog)

        return fl[0].modifiedDate.time
    }

    @Override
    void setLastModified(String fileName, Long time) {
		validConnect()
		validWrite()

        if (!saveOriginalDate)
			return

        if (supportCommand('MFMT')) {
            def d = new Date(time)
			//noinspection SpellCheckingInspection
			def df = DateUtils.FormatDate('yyyyMMddHHmmss', d)
            client.sendCustomCommand("MFMT $df $fileName")
        }
    }

	@Override
	String getObjectName() {
		if (server == null)
			return 'ftp'

		String res
		def loginStr = (login != null)?"${login()}@":''
		if (rootPath == null || rootPath.length() == 0)
			res = "ftp://$loginStr${server()}"
		else if (rootPath[0] == '/')
			res = "ftp://$loginStr${server()}${rootPath()}"
		else
			res = "ftp://$loginStr${server()}/${rootPath()}"

		return res
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
		def o = original as FTPManager
		def passwords = o.loginManager.decryptObject()
		loginManager.encryptObject(passwords)
	}
}