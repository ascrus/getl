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

import javax.activation.DataHandler
import javax.activation.FileDataSource
import javax.mail.*
import javax.mail.internet.*

import getl.exception.ExceptionGETL

class EMailer {
	private final Map params = [:]
	/** Parameters */
	Map getParams () { params }
	/** Parameters */
	void setParams(Map value) {
		params.clear()
		params.putAll(value)
	}

	private String config
	/** Name in config from section "emailers" */
	public String getConfig () { config }
	/** Name in config from section "emailers" */
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

	/** Host server */
	String getHost () { params.host }
	/** Host server */
	void setHost (String value) { params.host = value }

	/** Port server */
	Integer getPort () {
		if (params.port != null) return params.port as Integer
		if (tls) return 587
		if (ssl) return 465

		return 25
	}
	/** Port server */
	void setPort(Integer value) { params.port = value }

	/** User name */
	String getUser () { params.user as String }
	/** User name */
	void setUser(String value) { params.user = value }

	/** Password */
	String getPassword () { params.password as String }
	/** Password */
	void setPassword (String value) { params.password = value }

	/** Use debug mode */
	Boolean getDebug () { BoolUtils.IsValue(params.debug) }
	/** Use debug mode */
	void setDebug (Boolean value) { params.debug = value }

	/** Required authorization */
	Boolean getAuth () { (user != null) }

	/** Use ssl */
	Boolean getSsl () { BoolUtils.IsValue(params.ssl) }
	/** Use ssl */
	void setSsl (Boolean value) { params.ssl = value }

	/** Use tls for authentication */
	Boolean getTls () { BoolUtils.IsValue(params.tls) }
	/** Use tls for authentication */
	void setTls (Boolean value) { params.tls = value }

	/** Socket factory fallback */
	Boolean getSocketFactoryFallback () { BoolUtils.IsValue(params.socketFactoryFallback) }
	/** Socket factory fallback */
	void setSocketFactoryFallback (Boolean value) { params.socketFactoryFallback = value }

	/**
	 * Use SSL socket factory
	 * Default: javax.net.ssl.SSLSocketFactory
	 */
	String getSocketFactoryClass () { (params.socketFactoryClass as String)?:'javax.net.ssl.SSLSocketFactory' }
	/**
	 * Use SSL socket factory
	 * Default: javax.net.ssl.SSLSocketFactory
	 */
	void setSocketFactoryClass (String value) { params.socketFactoryClass = value }

	/** From address */
	String getFromAddress () { (params.fromAddress as String)?:'anonimus@getl.com' }
	/** From address */
	void setFromAddress (String value) { params.fromAddress = value }

	/** To address */
	String getToAddress () { params.toAddress as String }
	/** To address */
	void setToAddress (String value) { params.toAddress = value }

	/** Emailer enabled */
	Boolean getActive () { BoolUtils.IsValue(params.active, true) }
	/** Emailer enabled */
	void setActive (boolean value) { params.active = value }

	/** Subject for mail */
	String getSubject() { params.subject as String }
	/** Subject for mail */
	void setSubject(String value) { params.subject = value }

	/** Body for mail */
	String getMessage() { params.message as String }
	/** Body for mail */
	void setMessage(String value) { params.message = value }

	/** Body for mail has html format */
	Boolean getIsHtml() { BoolUtils.IsValue(params.isHtml) }
	/** Body for mail has html format */
	void setIsHtml(Boolean value) { params.isHtml = value }

	/** Attachment for mail */
	Object getAttachment() { params.attachment }
	/** Attachment for mail */
	void setAttachment(Object value) { params.attachment = value }

	/** Call init configuraion */
	private final Closure doInitConfig = {
		if (config == null) return
		Map cp = Config.FindSection("emailers.${config}")
		if (cp.isEmpty()) throw new ExceptionGETL("Config section \"emailers.${config}\" not found")
		onLoadConfig(cp)
		Logs.Config("Load config \"emailers\".\"${config}\" for object \"${this.getClass().name}\"")
	}

	/** Init configuration */
	protected void onLoadConfig (Map configSection) {
		MapUtils.MergeMap(params, configSection)
	}

	/** Send mail message */
	void send() {
		sendMail(toAddress, subject, message, isHtml, attachment)
	}

    /** Send mail message with specified parameters*/
	void sendMail(String toAddress, String subject, String message, boolean isHtml = false, Object attachment = null) {
		if (!active) return

		if (this.toAddress != null) {
			if (toAddress == null) toAddress = this.toAddress else toAddress = this.toAddress + "," + toAddress
		}
		Properties mprops = new Properties()
		mprops.put('mail.transport.protocol', 'smtp')
		mprops.put('mail.smtp.host', host)
		mprops.put('mail.smtp.port', String.valueOf(port))
		if (ssl) {
			mprops.put('mail.smtp.socketFactory.port', String.valueOf(port))
			mprops.put('mail.smtp.ssl.trust', '*')
			mprops.put('mail.smtp.ssl.enable', 'true')
			mprops.put("mail.smtp.socketFactory.class", socketFactoryClass);
			if (socketFactoryFallback) mprops.put('mail.smtp.socketFactory.fallback', 'true')
		}
		if (tls) {
			mprops.put('mail.smtp.starttls.enable', 'true')
			mprops.put('mail.smtp.starttls.required', 'true')
		}
		mprops.put('mail.debug', debug.toString())

		Session lSession
		if (auth) {
			if (user == null || user.trim().length() == 0 || password == null || password.trim().length() == 0)
				throw new ExceptionGETL("User and password cannot be null for authorisation mail!")
			mprops.put('mail.smtp.auth', 'true')
			lSession = Session.getDefaultInstance(mprops, new EMailerAuth(user: user, password: password))
		}
		else {
			lSession = Session.getDefaultInstance(mprops)
		}
		MimeMessage msg = new MimeMessage(lSession)

		msg.setFrom(new InternetAddress(fromAddress))
		msg.setRecipients(Message.RecipientType.TO, InternetAddress.parse(toAddress, true))
		msg.setSubject(subject, 'utf-8')
		def mimeTextType = (isHtml)?'html':'plain'

		if (attachment != null) {
			def multipart = new MimeMultipart()

			def messageBodyPart = new MimeBodyPart()
			messageBodyPart.setText(message, 'UTF-8', mimeTextType)
			multipart.addBodyPart(messageBodyPart)

			def mimeBodyPart = new MimeBodyPart()
			if (attachment instanceof List<String>) {
				(attachment as List<String>).each { String att ->
					mimeBodyPart.attachFile(att)
					multipart.addBodyPart(mimeBodyPart)
				}
			} else if (attachment instanceof List<File>) {
				(attachment as List<File>).each { File att ->
					mimeBodyPart.attachFile(att)
					multipart.addBodyPart(mimeBodyPart)
				}
			} else if (attachment instanceof String) {
				mimeBodyPart.attachFile(attachment as String)
				multipart.addBodyPart(mimeBodyPart)
			} else if (attachment instanceof File) {
				mimeBodyPart.attachFile(attachment as File)
				multipart.addBodyPart(mimeBodyPart)
			}
			else {
				throw new ExceptionGETL("Unsupported attachment type \"${attachment.getClass().name}\"!")
			}

			msg.setContent(multipart)
		} else {
			msg.setContent(message, "text/$mimeTextType; charset=UTF-8")
		}

		try {
			Transport.send(msg)
		}
		catch (javax.mail.MessagingException e) {
			Logs.Severe("emailer: failed send message for param $mprops")
			throw e
		}
	}
}