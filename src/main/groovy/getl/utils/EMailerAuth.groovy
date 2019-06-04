package getl.utils

import groovy.transform.InheritConstructors

@InheritConstructors
class EMailerAuth extends javax.mail.Authenticator {
    String user
    String password

    @Override
    protected javax.mail.PasswordAuthentication getPasswordAuthentication() {
        if (user == null || password == null) return null
        return new javax.mail.PasswordAuthentication(user, password)
    }
}