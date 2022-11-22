//file:noinspection unused
package getl.files

import com.fasterxml.jackson.annotation.JsonIgnore
import getl.exception.FilemanagerError
import getl.exception.IOFilesError
import getl.exception.RequiredParameterError
import getl.files.sub.FileManagerList
import getl.lang.Getl
import getl.lang.sub.UserLogins
import getl.lang.sub.LoginManager
import getl.lang.sub.StorageLogins
import groovy.transform.CompileStatic
import groovy.transform.InheritConstructors
import com.jcraft.jsch.*
import getl.utils.*
import groovy.transform.Synchronized
import groovy.transform.stc.ClosureParams
import groovy.transform.stc.SimpleType

/**
 * SFTP file manager
 * @author Alexsey Konstantinov
 * For get ssh key run: ssh-keyscan -t rsa <IP>
 */

@InheritConstructors
class SFTPManager extends Manager implements UserLogins {
	@Override
	void initParams() {
		super.initParams()
		loginManager = new LoginManager(this)
		params.storedLogins = new StorageLogins(loginManager)
	}

	/** SSH driver */
	private final JSch client = new JSch()
	/* SSH driver */
	//private JSch getClient() { client }
	
	/** SSH session */
	private Session clientSession
	/* SSH session */
	//private Session getClientSession() { clientSession }
	
	/** STP channel */
	private ChannelSftp channelFtp
	/* STP channel */
	//private ChannelSftp getChannelFtp() { channelFtp }
	
	@Override
	protected void registerParameters() {
		super.registerParameters()
		methodParams.register('super', ['server', 'port', 'login', 'password', 'knownHostsFile',
										'identityFile', 'codePage', 'aliveInterval', 'aliveCountMax', 'hostKey',
										'hostOS', 'storedLogins', 'strictHostKeyChecking', 'passphrase'])
	}
	
	@Override
	protected void onLoadConfig(Map configSection) {
		super.onLoadConfig(configSection)
		loginManager.encryptObject()
		if (rootPath != null && rootPath.substring(0, 1) != "/")
			rootPath = "/" + rootPath
	}
	
	@Override
	void setRootPath(String value) {
		if (value != null && value.length() > 1 && value.substring(0, 1) != "/") value = "/" + value
		super.setRootPath(value)
	}
	
	/** Server address */
	String getServer() { params.server as String }
	/** Server address */
	void setServer(String value) { params.server = value }
	
	/** Server port */
	Integer getPort() { (params.port != null)?(params.port as Integer):22 }
	/** Server port */
	void setPort(Integer value) { params.port = value }
	
	@Override
	String getLogin() { params.login as String }
	@Override
	void setLogin(String value) { params.login = value }

	@Override
	String getPassword() { params.password as String }
	@Override
	void setPassword(String value) { params.password = loginManager.encryptPassword(value) }

	@Override
	Map<String, String> getStoredLogins() { params.storedLogins as Map<String, String> }
	@Override
	void setStoredLogins(Map<String, String> value) {
		storedLogins.clear()
		if (value != null) storedLogins.putAll(value)
	}
	
	/**
	 * Known hosts file name
	 * <br>use "ssh-keyscan -t rsa <IP address>" for get key
	 */
	String getKnownHostsFile() { params.knownHostsFile as String }
	/**
	 * Known hosts file name
	 * <br>use "ssh-keyscan -t rsa <IP address>" for get key
	 */
	void setKnownHostsFile(String value) { params.knownHostsFile = value }

	/**
	 * Host key
	 * <br>use "ssh-keyscan -t rsa <IP address>" for get key
	 */
	String getHostKey() { params.hostKey as String }
	/**
	 * Host key
	 * <br>use "ssh-keyscan -t rsa <IP address>" for get key
	 */
	void setHostKey(String value) { params.hostKey = value }

	/** Need checking RSA host key (default true) */
	Boolean getStrictHostKeyChecking() { BoolUtils.IsValue(params.strictHostKeyChecking, true) }
	/** Need checking RSA host key (default true) */
	void setStrictHostKeyChecking(Boolean value) { params.strictHostKeyChecking = value }
	
	/** Identity file name */
	String getIdentityFile() { params.identityFile as String }
	/** Identity file name */
	void setIdentityFile(String value) { params.identityFile = value }

	/** Password for identity file */
	String getPassphrase() { params.passphrase as String }
	/** Password for identity file */
	void setPassphrase(String value) { params.passphrase = value }

	/** Code page on command console */
	String getCodePage() { (params.codePage as String)?:"utf-8" }
	/** Code page on command console */
	void setCodePage(String value) { params.codePage = value }
	
	/** Alive interval (in seconds) */
	Integer getAliveInterval() { params."aliveInterval" as Integer }
	/** Alive interval (in seconds) */
	void setAliveInterval(Integer value) { params."aliveInterval" = value }
	
	/** Alive retry count max */
	Integer getAliveCountMax() { params."aliveCountMax" as Integer }
	/** Alive retry count max */
	void setAliveCountMax(Integer value) { params."aliveCountMax" = value }
	
	@Override
	Boolean isCaseSensitiveName() { true }

	/** Create new session manager */
	private Session newSession() {
		String h = "Open session $sessionID: host $server:$port, login $login"
		client.identityRepository.removeAll()
		if (identityFile != null) {
			def f = new File(FileUtils.TransformFilePath(identityFile, dslCreator))
			if (!f.exists())
				throw new IOFilesError(this, '#io.file.not_found', [path: f.path, type: 'RSA'], writeErrorsToLog)

			if (passphrase == null) {
				client.addIdentity(f.canonicalPath)
				h += ", identity file \"$identityFile\""
			}
			else {
				client.addIdentity(f.canonicalPath, passphrase.bytes)
				h += ", identity file \"$identityFile\" with \"${StringUtils.Replicate('*', passphrase.length())}\""
			}
		}

		Session res = client.getSession(login, server, port)
		try {
			res.setConfig('PreferredAuthentications', 'publickey,password,keyboard-interactive')
			def pass = loginManager.currentDecryptPassword()
			if (pass != null && identityFile == null) {
				res.setPassword(pass)
				h += ", used password \"${StringUtils.Replicate('*', pass.length())}\""
			}

			if (!strictHostKeyChecking) {
				res.setConfig('StrictHostKeyChecking', 'no')
				h += ', disable strict host key checking'
			}
			else {
				if (hostKey != null) {
					byte[] key = Base64.decoder.decode(hostKey)
					client.getHostKeyRepository().add(new HostKey(server, key), null)
					h += ", used host key \"${StringUtils.LeftStr(hostKey, 16)}\""
				}
				else if (knownHostsFile != null) {
					h += ", hosts file \"$knownHostsFile\""
					client.setKnownHosts(knownHostsFile)
				}
			}

			writeScriptHistoryFile(h)
			res.connect()

			if (aliveInterval != null) res.setServerAliveInterval(aliveInterval * 1000)
			if (aliveCountMax != null) res.setServerAliveCountMax(aliveCountMax)
		}
		catch (Exception e) {
			if (res.connected)
				clientSession.disconnect()

			throw e
		}
		
		return res
	}

	@Override
	Boolean isConnected() {
		return (clientSession != null && clientSession.connected)
	}

	@Override
	@Synchronized
	protected void doConnect() {
		if (server == null)
			throw new RequiredParameterError(this, 'server', 'connect')
		if (port == null)
			throw new RequiredParameterError(this, 'port', 'connect')
		if (login == null || (loginManager.currentPassword() == null && identityFile == null))
			throw new RequiredParameterError(this, 'login', 'connect')
		
		clientSession = newSession()
		try {
			channelFtp = clientSession.openChannel("sftp") as ChannelSftp
			writeScriptHistoryFile("Open stp channel from session $sessionID")
			channelFtp.connect()
			currentPath = currentRootPath
			if (channelFtp.serverVersion > 5)
				channelFtp.filenameEncoding = codePage.toUpperCase()
		}
		catch (Exception e) {
			if (channelFtp != null && channelFtp.connected) channelFtp.disconnect()
			if (clientSession.connected) clientSession.disconnect()
			throw e
		}
	}

	@Override
	@Synchronized
	protected void doDisconnect() {
		try {
			if (clientSession != null && clientSession.connected) {
				if (channelFtp != null && channelFtp.connected)
					channelFtp.disconnect()
				if (clientSession.connected)
					clientSession.disconnect()
				channelFtp = null
				clientSession == null
			}
		}
		finally {
			_currentPath = null
		}
	}
	
	class SFTPList extends FileManagerList {
		Vector<ChannelSftp.LsEntry> listFiles
		
		@CompileStatic
		@Override
		Integer size () {
			(listFiles != null)?listFiles.size():0
		}
		
		@CompileStatic
		@Override
		Map item (Integer index) {
			ChannelSftp.LsEntry item = listFiles.get(index)

			Map<String, Object> file = new HashMap<String, Object>()
			
			file."filename" = item.filename
			file."filedate" = new Date(item.attrs.MTime * 1000L)
			file."filesize" = item.attrs.size
			file."link" = item.attrs.link
			
			if (item.attrs.isDir()) {
				file."type" = Manager.TypeFile.DIRECTORY
			} else if (file."link") {
				file."type" = Manager.TypeFile.LINK
			}
			else {
				file."type" = Manager.TypeFile.FILE
			}
		  
			file
		}

		@CompileStatic
		@Override
		void clear () {
			listFiles = null
		}
	}

	@CompileStatic
	@Override
	protected FileManagerList doListDir(String maskFiles) {
		validConnect()

		if (maskFiles == null) maskFiles = "*"
		Vector< ChannelSftp.LsEntry> listFiles = channelFtp.ls(maskFiles)
		
		SFTPList res = new SFTPList()
		res.listFiles = listFiles
		
		return res
	}

	@Override
	@JsonIgnore
	String getCurrentPath() {
		validConnect()
		return channelFtp.pwd()
	}

	@Override
	void setCurrentPath(String path) {
		validConnect()

		try {
			channelFtp.cd(path)
		}
		catch (Exception e) {
			if (writeErrorsToLog)
				logger.severe("Invalid directory \"$path\" on source \"$this\"", e)
			throw e
		}
		_currentPath = channelFtp.pwd()
	}

	@Override
	void changeDirectoryUp() {
		validConnect()
		channelFtp.cd("..")
		_currentPath = currentPath
	}

	@Override
	protected File doDownload(String filePath, String localPath, String localFileName) {
		validConnect()

		def fn = ((localPath != null)?localPath + "/":"") + localFileName
        def res = new File(fn)
		OutputStream s = res.newOutputStream()
		try {
			channelFtp.get(filePath, s)
		}
		catch (Exception e) {
			if (writeErrorsToLog)
				logger.severe("Can not download file \"$filePath\" to \"$fn\" on source \"$this\"", e)
			throw e
		}
		finally {
			s.close()
		}

        setLocalLastModified(res, getLastModified(filePath))

		return res
	}

	@Override
	protected void doUpload(String path, String fileName) {
		validConnect()
		validWrite()

		def fn = ((path != null)?path + "/":"") + fileName
        def f = new File(fn)
		InputStream s = f.newInputStream()
		try {
			channelFtp.put(s, fileName)
            setLastModified(fileName, f.lastModified())
		}
		catch (Exception e) {
			if (writeErrorsToLog)
				logger.severe("Can not upload file \"$fileName\" from \"$fn\" on source \"$this\"", e)
			throw e
		}
		finally {
			s.close()
		}
	}

	@Override
	void removeFile(String fileName) {
		validConnect()
		validWrite()

		try {
			channelFtp.rm(fileName)
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

		def curDir = channelFtp.pwd()
		String cdDir = null
		try {
			//noinspection RegExpSimplifiable
			def dirs = dirName.split("[/]")

			dirs.each { String dir ->
				if (cdDir == null) cdDir = dir else cdDir = "$cdDir/$dir"
				def isExists = true
				try {
					channelFtp.cd(dir)
				}
				catch (Exception ignored) {
					isExists = false
				}
				if (!isExists) {
					channelFtp.mkdir(dir)
					channelFtp.cd(dir)
				}
			}
		}
		catch (Exception e) {
			if (writeErrorsToLog)
				logger.severe("Can not create directory \"$cdDir\" on source \"$this\"", e)
			throw e
		}
		finally {
			channelFtp.cd(curDir)
		}
	}

	@Override
	void removeDir(String dirName, Boolean recursive,
				   @ClosureParams(value = SimpleType, options = ['java.lang.String']) Closure onDelete = null) {
		validConnect()
		validWrite()

        if (!channelFtp.stat(dirName).isDir()) {
			if (writeErrorsToLog)
				logger.severe("\"$dirName\" is not directory on source \"$this\"!")
			throw new IOFilesError(this, '#io.dir.not_found', [path: dirName], writeErrorsToLog)
		}
		try {
            if (recursive) {
                doDeleteDirectory(dirName, onDelete)
            }
            else {
                channelFtp.rmdir(dirName)
				if (onDelete != null)
					onDelete.call(dirName)
            }
		}
		catch (Exception e) {
			if (writeErrorsToLog)
				logger.severe("Can not remove directory \"$dirName\" on source \"$this\"", e)
			throw e
		}
	}

    /**
     * Recursive remove dir
     * @param objName
     */
    private void doDeleteDirectory(String objName,
								   @ClosureParams(value = SimpleType, options = ['java.lang.String']) Closure onDelete) {
        if (objName in ['.', '..'])
			return

        if (channelFtp.stat(objName).isDir()) {
            channelFtp.cd(objName)
            try {
                def entries = channelFtp.ls(".") as Vector<ChannelSftp.LsEntry>
                for (ChannelSftp.LsEntry entry : entries) {
                    doDeleteDirectory(entry.getFilename(), onDelete)
                }
            }
            finally {
                channelFtp.cd("..")
            }
            channelFtp.rmdir(objName)
        } else {
            channelFtp.rm(objName)
        }

		if (onDelete != null)
			onDelete.call(objName)
    }

	@Override
	void rename(String fileName, String path) {
		validConnect()
		validWrite()

		try {
			channelFtp.rename(fileName, path)
		}
		catch (Exception e) {
			if (writeErrorsToLog)
				logger.severe("Can not rename file \"$fileName\" to \"$path\" on source \"$this\"", e)
			throw e
		}
	}

	@Override
	Boolean isAllowCommand() { true }
	
	@Override
	protected Integer doCommand(String command, StringBuilder out, StringBuilder err) {
		Integer res = null

		def channelCmd = clientSession.openChannel("exec") as ChannelExec

		String psFile
		if (_currentPath != null) {
			if (hostOS == winOS) {
				def curPath = _currentPath.substring(1)
				psFile = FileUtils.UniqueFileName() + '.ps1'
				new File(currentLocalDir() + '/' + psFile).text = """
Set-Location "$curPath"
cmd /c "${command.replace('`', '``').replace('"', '`"').replace('\'', '`\'').replace('$', '`$')}"
exit \$LastExitCode
"""
				upload(psFile)
				command = "powershell -NoProfile -NonInteractive -ExecutionPolicy unrestricted -Command \"$curPath/$psFile\""
			}
			else
				command = "cd \"$_currentPath\" && $command"
		}

		try {
			channelCmd.setCommand(command)
			
			channelCmd.setInputStream(null)
			def is=channelCmd.inputStream
			
			def os = new ByteArrayOutputStream()
			channelCmd.setErrStream(os)
			
			channelCmd.connect()
			
			BufferedReader reader = new BufferedReader(new InputStreamReader(is, codePage))
			String line
			
			while (!channelCmd.isClosed()) sleep 100

			while ((line = reader.readLine()) != null) {
				out.append(line)
				out.append('\n')
			}
			
			if (os.size() > 0) {
				err.append(os.toString(codePage))
			}

			res = channelCmd.getExitStatus()
		}
		finally {
			try {
				channelCmd.setInputStream(null)
				channelCmd.setErrStream(null)
				channelCmd.disconnect()
			}
			finally {
				if (psFile != null) {
					removeLocalFile(psFile)
					removeFile(psFile)
				}
			}
		}

		return res
	}

	/** Detect host OS */
	String detectOS() {
		StringBuilder out = new StringBuilder(), err = new StringBuilder()
		def res = doCommand('echo %OS%', out, err)
		if (res != 0)
			return null

		def line = out.toString().readLines()[0]
		if (line.matches('Windows.*'))
			return winOS

		if (line == '%OS%')
			return unixOS

		return null
	}

	@Override
	String getHostOS() { (params.hostOS as String)?:unixOS }
	void setHostOS(String value) { params.hostOS = value }

	@Override
	void noop () {
		super.noop()
		clientSession.sendKeepAliveMsg()
	}

	@Override
	Long getLastModified(String fileName) {
		validConnect()

		def a = channelFtp.stat(fileName)
		return new Date(a.MTime * 1000L).time
	}

	@Override
	void setLastModified(String fileName, Long time) {
		validConnect()
		validWrite()

		if (saveOriginalDate)
			channelFtp.setMtime(fileName, (time / 1000L).intValue())
	}

	@Override
	String getObjectName() {
		if (server == null)
			return 'sftp'

		String res
		def loginStr = (login != null)?"$login@":''
		if (rootPath == null || rootPath.length() == 0)
			res = "sftp $loginStr$server"
		else if (rootPath[0] == '/')
			res = "sftp $loginStr$server:$rootPath"
		else
			res = "sftp $loginStr$server:/$rootPath"

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
		def o = original as SFTPManager
		def passwords = o.loginManager.decryptObject()
		loginManager.encryptObject(passwords)
	}
}