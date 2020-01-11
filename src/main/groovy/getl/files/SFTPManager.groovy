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

import getl.files.sub.FileManagerList
import groovy.transform.CompileStatic
import groovy.transform.InheritConstructors
import com.jcraft.jsch.*
import getl.exception.ExceptionGETL
import getl.utils.*
import groovy.transform.Synchronized

/**
 * SFTP file manager
 * @author Alexsey Konstantinov
 * For get ssh key run: ssh-keyscan -t rsa <IP>
 */

@InheritConstructors
class SFTPManager extends Manager {
	/**
	 * SSH client
	 */
	public static final JSch client = new JSch()
	
	/**
	 * SSH session
	 */
	public Session clientSession
	
	/**
	 * STP channel
	 */
	public ChannelSftp channelFtp
	
	@Override
	protected void initMethods () {
		super.initMethods()
		methodParams.register('super', ['server', 'port', 'login', 'password', 'knownHostsFile',
										'identityFile', 'codePage', 'aliveInterval', 'aliveCountMax', 'hostKey'])
	}
	
	@Override
	protected void onLoadConfig (Map configSection) {
		super.onLoadConfig(configSection)
		if (rootPath != null && rootPath.substring(0, 1) != "/") rootPath = "/" + rootPath
	}
	
	@Override
	void setRootPath (String value) {
		if (value != null && value.substring(0, 1) != "/") value = "/" + value
		super.setRootPath(value)
	}
	
	/** Server address */
	String getServer () { params.server }
	/** Server address */
	void setServer (String value) { params.server = value }
	
	/** Server port */
	Integer getPort () { (params.port != null)?(params.port as Integer):22 }
	/** Server port */
	void setPort (Integer value) { params.port = value }
	
	/** Login user */
	String getLogin () { params.login }
	/** Login user */
	void setLogin (String value) { params.login = value }
	
	/** Password user */
	String getPassword () { params.password }
	/** Password user */
	void setPassword (String value) { params.password = value }
	
	/**
	 * Known hosts file name
	 * <br>use "ssh-keyscan -t rsa <IP address>" for get key
	 */
	String getKnownHostsFile () { params.knownHostsFile }
	/**
	 * Known hosts file name
	 * <br>use "ssh-keyscan -t rsa <IP address>" for get key
	 */
	void setKnownHostsFile (String value) { params.knownHostsFile = value }

	/**
	 * Host key
	 * <br>use "ssh-keyscan -t rsa <IP address>" for get key
	 */
	String getHostKey() { params.hostKey }
	/**
	 * Host key
	 * <br>use "ssh-keyscan -t rsa <IP address>" for get key
	 */
	void setHostKey(String value) { params.hostKey = value }
	
	/** Identity file name */
	String getIdentityFile () { params.identityFile }
	/** Identity file name */
	void setIdentityFile (String value) { params.identityFile = value }
	
	/** Code page on command console */
	String getCodePage () { params.codePage?:"utf-8" }
	/** Code page on command console */
	void setCodePage (String value) { params.codePage = value }
	
	/** Alive interval (in seconds) */
	Integer getAliveInterval () { params."aliveInterval" as Integer }
	/** Alive interval (in seconds) */
	void setAliveInterval (Integer value) { params."aliveInterval" = value }
	
	/** Alive retry count max */
	Integer getAliveCountMax () { params."aliveCountMax" as Integer }
	/** Alive retry count max */
	void setAliveCountMax (Integer value) { params."aliveCountMax" = value }
	
	@Override
	boolean isCaseSensitiveName () { true }

	/** Create new session manager */
	private Session newSession () {
		Session s = client.getSession(login, server, port)
		try {
			s.setPassword(password)
			String h = "CREATE SESSION: host $server:$port, login $login"
			if (knownHostsFile != null) {
				h += ", hosts file \"$knownHostsFile\""
				client.setKnownHosts(knownHostsFile)
			}

			if (hostKey != null) {
				byte[] key = Base64.decoder.decode(hostKey)
				client.getHostKeyRepository().add(new HostKey(server, key ), null)
			}
			
			if (identityFile != null) {
				h += ", identity file \"$identityFile\""
				client.addIdentity(identityFile)
			}
			
			writeScriptHistoryFile(h)
			
			try {
				s.connect()
			}
			catch (Exception e) {
				if (writeErrorsToLog) Logs.Severe("Can not connect to $server:$port or invalid login/password")
				throw e
			}
			
			if (aliveInterval != null) s.setServerAliveInterval(aliveInterval * 1000)
			if (aliveCountMax != null) s.setServerAliveCountMax(aliveCountMax)
		}
		catch (Exception e) {
			if (s.connected) clientSession.disconnect()
			throw e
		}
		
		s
	}

	@Override
	boolean isConnected() {
		return (clientSession != null && clientSession.connected)
	}

	@Override
	@Synchronized
	void connect() {
		if (clientSession != null && clientSession.connected) throw new ExceptionGETL("SFTP already connect to server")
		if (server == null || port == null) throw new ExceptionGETL("Required server host and port for connect")
		if (login == null || password == null) throw new ExceptionGETL("Required login and password for connect")
		
		clientSession = newSession()
		try {
			channelFtp = clientSession.openChannel("sftp") as ChannelSftp
			writeScriptHistoryFile("OPEN CHANNEL: sftp")
			channelFtp.connect()
			if (rootPath != null) currentPath = rootPath
		}
		catch (Exception e) {
			if (channelFtp != null && channelFtp.connected) channelFtp.disconnect()
			if (clientSession.connected) clientSession.disconnect()
			throw e
		}
	}

	@Override
	void disconnect() {
		try {
			if (clientSession != null && clientSession.connected) {
				writeScriptHistoryFile("CLOSE SESSION")
				if (channelFtp != null && channelFtp.connected) channelFtp.disconnect()
				if (clientSession.connected) clientSession.disconnect()
				channelFtp = null
				clientSession == null
			}
		}
		catch (Exception e) {
			if (writeErrorsToLog) Logs.Severe("Can not disconnect from $server:$port")
			throw e
		}
	}
	
	class SFTPList extends FileManagerList {
		Vector<ChannelSftp.LsEntry> listFiles
		
		@CompileStatic
		@Override
		Integer size () {
			listFiles.size()
		}
		
		@CompileStatic
		@Override
		Map item (int index) {
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
			listFiles.clear()
		}
	}

	@CompileStatic
	@Override
	FileManagerList listDir(String maskFiles) {
		if (maskFiles == null) maskFiles = "*"
		writeScriptHistoryFile("COMMAND: list \"$maskFiles\"")
		Vector< ChannelSftp.LsEntry> listFiles = channelFtp.ls(maskFiles)
		
		SFTPList res = new SFTPList()
		res.listFiles = listFiles
		
		return res
	}

	@Override
	String getCurrentPath() {
		writeScriptHistoryFile("COMMAND: pwd")
		def res = channelFtp.pwd()
		writeScriptHistoryFile("PWD: \"$res\"")
		
		return res
	}

	@Override
	void setCurrentPath(String path) {
		writeScriptHistoryFile("COMMAND: cd \"$path\"")
		channelFtp.cd(path)
	}

	@Override
	void changeDirectoryUp() {
		writeScriptHistoryFile("COMMAND: cd ..")
		channelFtp.cd("..")
	}

	@Override
	void download(String fileName, String path, String localFileName) {
		def fn = ((path != null)?path + "/":"") + localFileName
		writeScriptHistoryFile("COMMAND: get \"$fileName\" to \"$fn\"")
        def f = new File(fn)
		OutputStream s = f.newOutputStream()
		try {
			channelFtp.get(fileName, s)
		}
		catch (Exception e) {
			if (writeErrorsToLog) Logs.Severe("Can not download file \"$fileName\" to \"$fn\"")
			throw e
		}
		finally {
			s.close()
		}

        setLocalLastModified(f, getLastModified(fileName))
	}

	@Override
	void upload(String path, String fileName) {
		def fn = ((path != null)?path + "/":"") + fileName
		writeScriptHistoryFile("COMMAND: put \"$fn\" to \"$fileName\"")
        def f = new File(fn)
		InputStream s = f.newInputStream()
		try {
			channelFtp.put(s, fileName)
            setLastModified(fileName, f.lastModified())
		}
		catch (Exception e) {
			if (writeErrorsToLog) Logs.Severe("Can not upload file \"$fileName\" from \"$fn\"")
			throw e
		}
		finally {
			s.close()
		}
	}

	@Override
	void removeFile(String fileName) {
		writeScriptHistoryFile("COMMAND: remove \"$fileName\"")
		try {
			channelFtp.rm(fileName)
		}
		catch (Exception e) {
			if (writeErrorsToLog) Logs.Severe("Can not remove file \"$fileName\"")
			throw e
		}
	}

	@Override
	void createDir(String dirName) {
		writeScriptHistoryFile("COMMAND: pwd")
		def curDir = channelFtp.pwd()
		writeScriptHistoryFile("PWD: \"$curDir\"")
		String cdDir = null
		try {
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
					writeScriptHistoryFile("COMMAND: mkdir \"$dir\"")
					channelFtp.mkdir(dir)
					writeScriptHistoryFile("COMMAND: cd \"$dir\"")
					channelFtp.cd(dir)
				}
			}
		}
		catch (Exception e) {
			if (writeErrorsToLog) Logs.Severe("Can not create directory \"$cdDir\"")
			throw e
		}
		finally {
			channelFtp.cd(curDir)
			writeScriptHistoryFile("COMMAND: cd \"$curDir\"")
		}
	}

	@Override
	void removeDir(String dirName, Boolean recursive) {
		writeScriptHistoryFile("COMMAND: rmdir \"$dirName\"")
        if (!channelFtp.stat(dirName).isDir()) throw new ExceptionGETL("$dirName is not directory")
		try {
            if (recursive) {
                doDeleteDirectory(dirName)
            }
            else {
                channelFtp.rmdir(dirName)
            }
		}
		catch (Exception e) {
			if (writeErrorsToLog) Logs.Severe("Can not remove directory \"$dirName\"")
			throw e
		}
	}

    /**
     * Recursive remove dir
     * @param objName
     */
    private void doDeleteDirectory(String objName) {
        if (objName in ['.', '..']) return

        if (channelFtp.stat(objName).isDir()) {
            channelFtp.cd(objName)
            try {
                def entries = channelFtp.ls(".") as Vector<ChannelSftp.LsEntry>
                for (ChannelSftp.LsEntry entry : entries) {
                    doDeleteDirectory(entry.getFilename())
                }
            }
            finally {
                channelFtp.cd("..")
            }
            channelFtp.rmdir(objName)
        } else {
            channelFtp.rm(objName)
        }
    }

	@Override
	void rename(String fileName, String path) {
		writeScriptHistoryFile("COMMAND: rename \"$fileName\" to \"$path\"")
		try {
			channelFtp.rename(fileName, path)
		}
		catch (Exception e) {
			if (writeErrorsToLog) Logs.Severe("Can not rename file \"$fileName\" to \"$path\"")
			throw e
		}
	}

	@Override
	boolean existsDirectory(String dirName) {
		writeScriptHistoryFile("COMMAND: pwd \"$dirName\"")
		def cur = channelFtp.pwd()
		writeScriptHistoryFile("PWD: \"$cur\"")
		def isExists = true
		try {
			writeScriptHistoryFile("COMMAND: cd \"$dirName\"")
			channelFtp.cd(dirName)
		}
		catch (Exception ignored) {
			isExists = false
		}
		writeScriptHistoryFile("COMMAND: cd \"$cur\"")
		channelFtp.cd(cur)
		
		isExists
	}
	
	@Override
	boolean isAllowCommand() { true }
	
	@Override
	protected Integer doCommand(String command, StringBuilder out, StringBuilder err) {
		Integer res = null
		command = "cd \"$currentPath\" && $command"
		def channelCmd = clientSession.openChannel("exec") as ChannelExec
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
			res = channelCmd.getExitStatus()
			
			while ((line = reader.readLine()) != null) {
				out.append(line)
				out.append('\n')
			}
			
			if (os.count > 0) {
				err.append(os.toString(codePage))
			}
		}
		finally {
			channelCmd.setErrStream(null)
			channelCmd.disconnect()
		}
		
		return res
	}

	@Override
	void noop () {
		super.noop()
		clientSession.sendKeepAliveMsg()
		writeScriptHistoryFile("NOOP")
	}

	@Override
	long getLastModified(String fileName) {
		def a = channelFtp.stat(fileName)
		return new Date(a.MTime * 1000L).time
	}

	@Override
	void setLastModified(String fileName, long time) {
		if (saveOriginalDate) channelFtp.setMtime(fileName, (time / 1000L).intValue())
	}

	@Override
	String toString() {
		if (server == null) return 'sftp'
		String res
		def loginStr = (login != null)?"$login@":''
		if (rootPath == null || rootPath.length() == 0)
			res = "sftp $loginStr$server"
		else if (rootPath[0] == '/')
			res = "sftp $loginStr$server$rootPath"
		else
			res = "sftp $loginStr$server/$rootPath"

		return res
	}
}