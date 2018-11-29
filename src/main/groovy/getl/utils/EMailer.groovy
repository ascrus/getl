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
			Config.RegisterOnInit(doInitConfig)
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
	public int getPort () {
		if (params."port" != null) return params."port"
		if (starttls != null && starttls) return 587
		if (ssl != null && ssl) return 465

		25
	}
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
	public Boolean getAuth () { params."auth" }
	public void setAuth (Boolean value) { params."auth" = value }

	/**
	 * Use ssl
	 */
	public Boolean getSsl () { params."ssl" }
	public void setSsl (Boolean value) { params."ssl" = value }

	/**
	 * Use starttls for auth
	 */
	public Boolean getStarttls () { params."starttls" }
	public void setStarttls (Boolean value) { params."starttls" = value }

	/**
	 * Socket factory fallback
	 */
	public Boolean getSocketFactoryFallback () { params."socketFactoryFallback" }
	public void setSocketFactoryFallback (Boolean value) { params."socketFactoryFallback" = value }

	/**
	 * Use SSL socket factory
	 * Example: javax.net.ssl.SSLSocketFactory
	 */
	public String getSocketFactoryClass () { params."socketFactoryClass" }
	public void setSocketFactoryClass (String value) { params."socketFactoryClass" = value }

	/**
	 * From address
	 */
	public String getFromAddress () { params."fromAddress"?:"anonimus@getl.com" }
	public void setFromAddress (String value) { params."fromAddress" = value }

	/**
	 * To address
	 */
	public String getToAddress () { params."toAddress" }
	public void setToAddress (String value) { params."toAddress" = value }

	/**
	 * Active emailer
	 */
	public Boolean getActive () { BoolUtils.IsValue(params."active", true) }
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

    public void sendMail(String toAddress, String subject, String message, boolean isHtml = false, Object attachment = null) {
		if (!active) return

		if (this.toAddress != null) {
			if (toAddress == null) toAddress = this.toAddress else toAddress = this.toAddress + "," + toAddress
		}
		Properties mprops = new Properties()
		mprops.setProperty("mail.transport.protocol", "smtp")
		mprops.setProperty('mail.host', host)
		mprops.setProperty("mail.smtp.port", String.valueOf(port))
		if (auth != null) mprops.setProperty('mail.smtp.auth', auth.toString())
		if (starttls != null) mprops.setProperty('mail.smtp.starttls.enable', starttls.toString())
		if (ssl != null) mprops.setProperty('mail.smtp.ssl', ssl.toString())
		if (socketFactoryClass != null) mprops.setProperty("mail.smtp.socketFactory.class", socketFactoryClass)
		if (socketFactoryFallback != null) mprops.setProperty("mail.smtp.socketFactory.fallback", socketFactoryFallback.toString())

		def u = user
		def p = password

		def authenticator = new javax.mail.Authenticator() {
			protected PasswordAuthentication getPasswordAuthentication() {
				return new PasswordAuthentication(u, p)
			}
		}

		Session lSession = Session.getInstance(mprops, authenticator)
		MimeMessage msg = new MimeMessage(lSession)

		msg.setFrom(new InternetAddress(fromAddress))
		msg.setRecipients(Message.RecipientType.TO, InternetAddress.parse(toAddress, true))
		msg.setSubject(subject, "utf-8")
//		msg.setText(message, "utf-8")

		BodyPart messageBodyPart = new MimeBodyPart()

		if (attachment) {
			messageBodyPart.setText(message,"text/html")

			Multipart multipart = new MimeMultipart()
			multipart.addBodyPart(messageBodyPart)

			if (attachment instanceof List<String>) {
				(attachment as List<String>).each { String att ->
					messageBodyPart = new MimeBodyPart()
					messageBodyPart.attachFile(att)
					multipart.addBodyPart(messageBodyPart)
				}
			} else if (attachment instanceof List<File>) {
				(attachment as List<File>).each { File att ->
					messageBodyPart = new MimeBodyPart()
					messageBodyPart.attachFile(att)
					multipart.addBodyPart(messageBodyPart)
				}
			} else if (attachment instanceof String) {
				messageBodyPart = new MimeBodyPart()
				messageBodyPart.attachFile(attachment as String)
				multipart.addBodyPart(messageBodyPart)
			} else if (attachment instanceof File) {
				messageBodyPart = new MimeBodyPart()
				messageBodyPart.attachFile(attachment as File)
				multipart.addBodyPart(messageBodyPart)
			}

			msg.setContent(multipart)
		} else if (isHtml) msg.setContent(message, "text/html")
		else msg.setContent(message, "text/plain; charset=UTF-8")

		try {
			Transport transporter = lSession.getTransport("smtp")
			transporter.connect()
			try {
				transporter.send(msg)
			}
			finally {
				transporter.close()
			}
		}
		catch (javax.mail.MessagingException e) {
			Logs.Severe("emailer: failed send message for param $mprops")
			throw e
		}
	}
}
