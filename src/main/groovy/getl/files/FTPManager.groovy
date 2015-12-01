/*
 GETL - based package in Groovy, which automates the work of loading and transforming data. His name is an acronym for "Groovy ETL".

 GETL is a set of libraries of pre-built classes and objects that can be used to solve problems unpacking,
 transform and load data into programs written in Groovy, or Java, as well as from any software that supports
 the work with Java classes.
 
 Copyright (C) 2013-2015  Alexsey Konstantonov (ASCRUS)

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
import getl.utils.*
import groovy.transform.InheritConstructors
import it.sauronsoftware.ftp4j.FTPClient
import it.sauronsoftware.ftp4j.FTPFile

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
		methodParams.register("super", ["server", "port", "login", "password", "passive", "isHardDisconnect", "autoNoopTimeout", "closeTimeout", "connectionTimeout", "readTimeout"])
	}
	
	@Override
	protected void onLoadConfig (Map configSection) {
		super.onLoadConfig(configSection)
		if (rootPath != null && rootPath.substring(0, 1) != "/") rootPath = "/" + rootPath
	}
	
	@Override
	public void setRootPath (String value) {
		if (value != null && value.substring(0, 1) != "/") value = "/" + value
		super.setRootPath(value)
	}
	
	/**
	 * Server address
	 */
	public String getServer () { params.server }
	public void setServer (String value) { params.server = value }
	
	/**
	 * Server port
	 */
	public Integer getPort () { (params.port != null)?params.port:21 }
	public void setPort (Integer value) { params.port = value }
	
	/**
	 * Login user
	 */
	public String getLogin () { params.login }
	public void setLogin (String value) { params.login = value }
	
	/**
	 * Password user
	 */
	public String getPassword () { params.password }
	public void setPassword (String value) { params.password = value }
	
	/**
	 * Passive mode
	 * @return
	 */
	public boolean getPassive () { (params.passive != null)?params.passive:true }
	public void setPassive (boolean value) {
		if (client.connected) client.setPassive(value) 
		params.passive = value 
	}
	
	/**
	 * Hard disconnect
	 */
	public boolean getIsHardDisconnect () { (params.isHardDisconnect != null)?params.isHardDisconnect:false }
	public void setIsHardDisconnect (boolean value) { params.isHardDisconnect = value }
	
	/**
	 * Auto noop timeout in seconds 
	 */
	public Integer getAutoNoopTimeout () { params.autoNoopTimeout }
	public void setAutoNoopTimeout (Integer value) { 
		if (client.connected) client.setAutoNoopTimeout(value * 1000)
		params.autoNoopTimeout = value
	}
	
	/**
	 * Close timeout
	 * @return
	 */
	public Integer getCloseTimeout () { params.closeTimeout }
	public void setCloseTimeout (int value) {
		if (client.connected) client.connector.closeTimeout = value
		params.closeTimeout = value
	}
	
	/**
	 * Connection timeout
	 * @return
	 */
	public Integer getConnectionmTimeout () { params.connectionTimeout }
	public void setConnectionTimeout (int value) {
		if (client.connected) client.connector.connectionTimeout = value
		params.connectionTimeout = value
	}
	
	/**
	 * Read timeout
	 * @return
	 */
	public Integer getReadTimeout () { params.readTimeout }
	public void setReadTimeout (int value) {
		if (client.connected) client.connector.readTimeout = value
		params.readTimeout = value
	}
	
	@Override
	public boolean isCaseSensitiveName () { true }
	
	@Override
	public void connect () {
		if (client.connected) throw new ExceptionGETL("FTP already connect to server")
		if (server == null || port == null) throw new ExceptionGETL("Required server host and port for connect")
		if (login == null) throw new ExceptionGETL("Required login for connect")
		
		if (connectionmTimeout != null) client.connector.connectionTimeout = connectionmTimeout
		try {
			client.connect(server, port)
		}
		catch (Throwable e) {
			Logs.Severe("Can not connect to $server:$port")
			throw e
		}
		try {
			client.login(login, password)
		}
		catch (Throwable e) {
			Logs.Severe("Invalid login or password for $server:$port")
			throw e
		}
		
		client.setType(FTPClient.TYPE_BINARY)
		client.setPassive(passive)
		if (autoNoopTimeout != null) client.setAutoNoopTimeout(autoNoopTimeout * 1000)
		if (closeTimeout != null) client.connector.closeTimeout = closeTimeout
		if (readTimeout != null) client.connector.readTimeout = readTimeout
		
		if (rootPath != null) currentPath = rootPath
	}
	
	@Override
	public void disconnect () {
		client.setAutoNoopTimeout(0)
		if (client.connected) {
			try {
				client.disconnect(isHardDisconnect)
			}
			catch (Throwable e) {
				Logs.Severe("Can not disconnect from $server:$port")
				throw e
			}
		}
	}
	
	@groovy.transform.CompileStatic
	@Override
	public void list(String mask, Closure processCode) {
		if (processCode == null) throw new ExceptionGETL("Required processing file attributes code")
		
		FTPFile[] list 
		try {
			list = (mask != null)?client.list(mask):client.list()
		}
		catch (Throwable e) {
			Logs.Severe("Can not read ftp list")
			throw e
		}
		list.each { FTPFile f ->  
			def m = [:]
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
			processCode(m)
		}
	}
	
	@Override
	public String getCurrentPath () {
		client.currentDirectory()
	}
	
	@Override
	public void setCurrentPath (String path) {
		try {
			client.changeDirectory(path)
		}
		catch (Throwable e) {
			Logs.Severe("Can not change directory to \"$path\"")
			throw e
		}
	}
	
	@Override
	public void changeDirectoryUp () {
		try {
			client.changeDirectoryUp()
		}
		catch (Throwable e) {
			Logs.Severe("Can not change directory to up")
			throw e
		}
	}
	
	@Override
	public void download (String fileName, String path, String localFileName) {
		def fn = ((path != null)?path + "/":"") + localFileName
		try {
			client.download(fileName, new File(fn))
		}
		catch (Throwable e) {
			Logs.Severe("Can not download file \"$fileName\" to \"$fn\"")
			throw e
		}
	}
	
	@Override
	public void upload (String path, String fileName) {
		def fn = ((path != null)?path + "/":"") + fileName
		try {
			client.upload(new File(fn))
		}
		catch (Throwable e) {
			Logs.Severe("Can not upload file \"$fileName\" from \"$fn\"")
			throw e
		}
	}
	
	@Override
	public void removeFile(String fileName) {
		try {
			client.deleteFile(fileName)
		}
		catch (Throwable e) {
			Logs.Severe("Can not remove file \"$fileName\"")
			throw e
		}
	}
	
	@Override
	public void createDir (String dirName) {
		def curDir = client.currentDirectory()
		def cdDir
		try {
			def dirs = dirName.split("[/]")

			dirs.each { dir ->
				if (cdDir == null) cdDir = dir else cdDir = "$cdDir/$dir"
				def isExists = true
				try {
					client.changeDirectory(dir)
				}
				catch (Throwable e) {
					isExists = false
				}
				if (!isExists) {
					client.createDirectory(dir)
					client.changeDirectory(dir)
				}
			}
		}
		catch (Throwable e) {
			Logs.Severe("Can not create directory \"$cdDir\"")
			throw e
		}
		finally {
			client.changeDirectory(curDir)
		}
	}
	
	@Override
	public void removeDir (String dirName) {
		try {
			client.deleteDirectory(dirName)
		}
		catch (Throwable e) {
			Logs.Severe("Can not remove directory \"$dirName\"")
			throw e
		}
	}
	
	@Override
	public void rename(String fileName, String path) {
		try {
			client.rename(fileName, path)
		}
		catch (Throwable e) {
			Logs.Severe("Can not rename file \"$fileName\" to \"$path\"")
			throw e
		}
	}
	
	@Override
	public boolean existsDirectory (String dirName) {
		def cur = client.currentDirectory()
		def isExists = true
		try {
			client.changeDirectory(dirName)
		}
		catch (Throwable e) {
			isExists = false
		}
		client.changeDirectory(cur)
		
		isExists
	}
}
