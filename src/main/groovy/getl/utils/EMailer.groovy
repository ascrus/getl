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

package getl.utils

import javax.mail.*
import javax.mail.internet.*

import getl.exception.ExceptionGETL
 
class EMailer {
	/** 
	 * Parameters
	 */
	private final Map params = [:]
	public Map getParams () { params }
	public void setParams(Map value) {
		params.clear()
		params.putAll(value)
	}
	
	/**
	 * Name in config from section "emailers"
	 */
	private String config
	public String getConfig () { config }
	public void setConfig (String value) {
		config = value
		if (config != null) {
			if (Config.ContainsSection("emailers.${this.config}")) {
				doInitConfig()
			}
			else {
				Config.RegisterOnInit(doInitConfig)
			}
		}
	}
	
	/**
	 * Host server
	 */
	public String getHost () { params."host" } 
	public void setHost (String value) { params."host" = value }
	
	/**
	 * Port server
	 */
	public int getPort () { params."port"?:25 }
	public void setPort(int value) { params.port = value }
	
	/**
	 * User name
	 */
    
	public String getUser () { params."user" }
	public void setUser(String value) { params."user" = value }
	
	/**
	 * Password
	 */
	public String getPassword () { params."password" }
	public void setPassword (String value) { params."password" = value }
	
	/**
	 * Required authorization
	 */
	public boolean getAuth () { ListUtils.NotNullValue([params."auth", false]) }
	public void setAuth (boolean value) { params."auth" = value }
	
	/**
	 * From address 
	 */
	public String getFromAddress () { params."fromAddress"?:"anonimus@getl" }
	public void setFromAddress (String value) { params."fromAddress" = value }
	
	/**
	 * To address
	 */
	public String getToAddress () { params."toAddress" }
	public void setToAddress (String value) { params."toAddress" = value }
	
	/**
	 * Active emailer
	 */
	public boolean getActive () { ListUtils.NotNullValue([params."active", true]) }
	public void setActive (boolean value) { params."active" = value }
	
	/**
	 * Call init configuraion
	 */
	private final Closure doInitConfig = {
		if (config == null) return
		Map cp = Config.FindSection("emailers.${config}")
		if (cp.isEmpty()) throw new ExceptionGETL("Config section \"emailers.${config}\" not found")
		onLoadConfig(cp)
		Logs.Config("Load config \"emailers\".\"${config}\" for object \"${this.getClass().name}\"")
	}
	
	/**
	 * Init configuration
	 */
	protected void onLoadConfig (Map configSection) {
		MapUtils.MergeMap(params, configSection)
	}
	
    public void sendMail(String toAddress, String subject, String message) {
		if (!active) return
		
		if (this.toAddress != null) {
			if (toAddress == null) toAddress = this.toAddress else toAddress = this.toAddress + "," + toAddress
		}
		Properties mprops = new Properties()
		mprops.setProperty("mail.transport.protocol", "smtp")
		mprops.setProperty("mail.host", host)
		mprops.setProperty("mail.smtp.port", port.toString())
		mprops.setProperty("mail.smtp.auth", auth.toString())
		
		def u = user
		def p = password
		
		def authenticator = new javax.mail.Authenticator() {
			protected PasswordAuthentication getPasswordAuthentication() {
				return new PasswordAuthentication(u, p);
			}
		  }
		
		Session lSession = Session.getInstance(mprops, authenticator)
		MimeMessage msg = new MimeMessage(lSession)
	  
		msg.setFrom(new InternetAddress(fromAddress))
		msg.setRecipients(Message.RecipientType.TO, InternetAddress.parse(toAddress, true))
		msg.setSubject(subject, "utf-8")
		msg.setText(message, "utf-8")
	  
		Transport transporter = lSession.getTransport("smtp")
		transporter.connect()
		 
		transporter.send(msg)
	}
}