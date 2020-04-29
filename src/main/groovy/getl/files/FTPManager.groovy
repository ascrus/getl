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
import getl.utils.*
import groovy.transform.CompileStatic
import groovy.transform.InheritConstructors
import it.sauronsoftware.ftp4j.*

/**
 * FTP Manager
 * @author Alexsey Konstantinov
 *
 */
@InheritConstructors
class FTPManager extends Manager {
	public FTPClient client = new FTPClient()
	
	@Override
	protected void initMethods () {
		super.initMethods()
		methodParams.register('super', ['server', 'port', 'login', 'password', 'passive', 'isHardDisconnect',
										'autoNoopTimeout', 'closeTimeout', 'connectionTimeout', 'readTimeout',
                                        'timeZone'])
	}
	
	@Override
	protected void onLoadConfig (Map configSection) {
		super.onLoadConfig(configSection)
		if (rootPath != null && rootPath.substring(0, 1) != '/') rootPath = '/' + rootPath
	}
	
	@Override
	void setRootPath (String value) {
		if (value != null && value.length() > 1 && value.substring(0, 1) != '/') value = '/' + value
		super.setRootPath(value)
	}
	
	/** Server address */
	String getServer () { params.server }
	/** Server address */
	void setServer (String value) { params.server = value }
	
	/** Server port */
	Integer getPort () { (params.port != null)?(params.port as Integer):21 }
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
	
	/** Passive mode */
	boolean getPassive () { (params.passive != null)?params.passive:true }
	/** Passive mode */
	void setPassive (boolean value) {
		if (client.connected) client.setPassive(value) 
		params.passive = value 
	}
	
	/** Hard disconnect */
	boolean getIsHardDisconnect () { (params.isHardDisconnect != null)?params.isHardDisconnect:false }
	/** Hard disconnect */
	void setIsHardDisconnect (boolean value) { params.isHardDisconnect = value }
	
	/** Auto noop timeout in seconds */
	Integer getAutoNoopTimeout () { params.autoNoopTimeout as Integer }
	/** Auto noop timeout in seconds */
	void setAutoNoopTimeout (Integer value) {
		if (client.connected) client.setAutoNoopTimeout(value * 1000)
		params.autoNoopTimeout = value
	}
	
	/** Close timeout */
	Integer getCloseTimeout () { params.closeTimeout as Integer }
	/** Close timeout */
	void setCloseTimeout (Integer value) {
		if (client.connected) client.connector.closeTimeout = value
		params.closeTimeout = value
	}
	
	/** Connection timeout */
	Integer getConnectionmTimeout () { params.connectionTimeout as Integer }
	/** Connection timeout */
	void setConnectionTimeout (Integer value) {
		if (client.connected) client.connector.connectionTimeout = value
		params.connectionTimeout = value
	}
	
	/** Read timeout */
	Integer getReadTimeout () { params.readTimeout as Integer }
	/** Read timeout */
	void setReadTimeout (Integer value) {
		if (client.connected) client.connector.readTimeout = value
		params.readTimeout = value
	}

    /** FTP server time zone */
    Integer getTimeZone () { (params.timeZone as Integer)?:0 }
	/** FTP server time zone */
	void setTimeZone (Integer value) { params.timeZone = value }
	
	@Override
	boolean isCaseSensitiveName () { true }

    final private supportCommands = []
    /**
     * Valid support command for FTP server
     * @param cmd
     * @return
     */
    boolean supportCommand(String cmd) { (cmd.toUpperCase() in supportCommands) }

	@Override
	boolean isConnected() { BoolUtils.IsValue(client?.connected) }
	
	@Override
	void connect () {
		if (connected)
			throw new ExceptionGETL('Manager already connected!')

		if (server == null || port == null) throw new ExceptionGETL('Required server host and port for connect')
		if (login == null) throw new ExceptionGETL('Required login for connect')
		
		if (connectionmTimeout != null) client.connector.connectionTimeout = connectionmTimeout
		if (closeTimeout != null) client.connector.closeTimeout = closeTimeout
		if (readTimeout != null) client.connector.readTimeout = readTimeout
		try {
			client.connect(server, port)
		}
		catch (Exception e) {
			if (writeErrorsToLog) Logs.Severe("Can not connect to $server:$port")
			throw e
		}
		try {
			client.login(login, password)
		}
		catch (Exception e) {
			if (writeErrorsToLog) Logs.Severe("Invalid login or password for $server:$port")
			throw e
		}
        if (autoNoopTimeout != null) client.setAutoNoopTimeout(autoNoopTimeout * 1000)
		client.setType(FTPClient.TYPE_BINARY)
		client.setPassive(passive)

        supportCommands.clear()
        client.sendCustomCommand('FEAT').messages.each { String cmd ->
            supportCommands << cmd.trim().toUpperCase()
        }

		if (rootPath != null) currentPath = rootPath
	}
	
	@Override
	void disconnect () {
		if (!connected)
			throw new ExceptionGETL('Manager already disconnected!')

		try {
			if (client.connected) {
				try {
					def numRetry = 0
					def sleepTime = ((client.autoNoopTimeout > 0) ? client.autoNoopTimeout : 100) as Integer
					client.autoNoopTimeout = 0

					client.disconnect(isHardDisconnect)
					while (client.connected) {
						numRetry++
						sleep(sleepTime)

						if (numRetry > 4) throw new ExceptionGETL('Can not disconnect from server')
					}
				}
				catch (Exception e) {
					if (writeErrorsToLog) Logs.Severe("Can not disconnect from $server:$port")
					throw e
				}
			}
		}
		finally {
			_currentPath = null
		}
	}

	@InheritConstructors
	class FTPList extends FileManagerList {
		public FTPFile[] listFiles
		
		@CompileStatic
		@Override
		Integer size () {
			listFiles.length
		}
		
		@CompileStatic
		@Override
		Map item (int index) {
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
					throw new ExceptionGETL("Unnknown type object ${f.type}")
			  }
		  
			m
		}

		@CompileStatic
		@Override
		void clear () {
			listFiles = []
		}
	}
	
	@CompileStatic
	@Override
	FileManagerList listDir(String mask) {
		validConnect()

		FTPFile[] listFiles 
		try {
			listFiles = (mask != null)?client.list(mask):client.list()
		}
		catch (Exception e) {
			if (writeErrorsToLog) Logs.Severe('Can not read ftp list')
			throw e
		}
		
		FTPList res = new FTPList()
		res.listFiles = listFiles
		
		return res
	}
	
	@Override
	String getCurrentPath () {
		validConnect()

		return client.currentDirectory()
	}
	
	@Override
	void setCurrentPath (String path) {
		validConnect()

		try {
			client.changeDirectory(path)
			_currentPath = client.currentDirectory()
		}
		catch (Exception e) {
			if (writeErrorsToLog) Logs.Severe("Can not change directory to \"$path\"")
			throw e
		}
	}
	
	@Override
	void changeDirectoryUp () {
		validConnect()

		try {
			client.changeDirectoryUp()
			_currentPath = currentPath
		}
		catch (Exception e) {
			if (writeErrorsToLog) Logs.Severe('Can not change directory to up')
			throw e
		}
	}
	
	@Override
	void download (String fileName, String path, String localFileName) {
		validConnect()

		def fn = ((path != null)?path + '/':'') + localFileName
		try {
            def f = new File(fn)
			client.download(fileName, f)
            setLocalLastModified(f, getLastModified(fileName))

		}
		catch (Exception e) {
			if (writeErrorsToLog) Logs.Severe("Can not download file \"$fileName\" to \"$fn\"")
			throw e
		}
	}
	
	@Override
	void upload (String path, String fileName) {
		validConnect()

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
			if (writeErrorsToLog) Logs.Severe("Can not upload file \"$fileName\" from \"$fn\"")
			throw e
		}
	}
	
	@Override
	void removeFile(String fileName) {
		validConnect()

		try {
			client.deleteFile(fileName)
		}
		catch (Exception e) {
			if (writeErrorsToLog) Logs.Severe("Can not remove file \"$fileName\"")
			throw e
		}
	}
	
	@Override
	void createDir (String dirName) {
		validConnect()

		def curDir = client.currentDirectory()
		String cdDir = null
		try {
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
			if (writeErrorsToLog) Logs.Severe("Can not create directory \"$cdDir\"")
			throw e
		}
		finally {
			client.changeDirectory(curDir)
		}
	}
	
	@Override
	void removeDir (String dirName, Boolean recursive) {
		validConnect()

		try {
			if (recursive) {
                def l = client.list()
                def d = l.find { FTPFile f -> f.name == dirName && f.type == FTPFile.TYPE_DIRECTORY}
                if (d == null) throw new ExceptionGETL("Cannot get attribute by directory \"$dirName\"")
                doDeleteDirectory(d)
            }
            else {
                client.deleteDirectory(dirName)
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
    private void doDeleteDirectory(FTPFile obj) {
        if (obj.type == FTPFile.TYPE_DIRECTORY) {
            client.changeDirectory(obj.name)
            try {
                def fl = client.list()
                fl.each { FTPFile f ->
                    doDeleteDirectory(f)
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
    }
	
	@Override
	void rename(String fileName, String path) {
		validConnect()

		try {
			client.rename(fileName, path)
		}
		catch (Exception e) {
			if (writeErrorsToLog) Logs.Severe("Can not rename file \"$fileName\" to \"$path\"")
			throw e
		}
	}

	@Override
	void noop () {
		super.noop()
		client.noop()
	}

    @Override
    long getLastModified(String fileName) {
		validConnect()

        def fl = client.list(fileName)
        if (fl.length != 1) throw new ExceptionGETL('File $fileName not found!')
        return fl[0].modifiedDate.time
    }

    @Override
    void setLastModified(String fileName, long time) {
		validConnect()

        if (!saveOriginalDate) return

        if (supportCommand('MFMT')) {
            def d = new Date(time)
            def df = DateUtils.FormatDate('yyyyMMddHHmmss', d)
            client.sendCustomCommand("MFMT $df $fileName")
        }
    }

	@Override
	String toString() {
		if (server == null) return 'ftp'
		String res
		def loginStr = (login != null)?"$login@":''
		if (rootPath == null || rootPath.length() == 0)
			res = "ftp://$loginStr$server"
		else if (rootPath[0] == '/')
			res = "ftp://$loginStr$server$rootPath"
		else
			res = "ftp://$loginStr$server/$rootPath"

		return res
	}
}