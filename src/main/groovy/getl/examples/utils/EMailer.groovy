@BaseScript getl.lang.Getl getl

import groovy.transform.BaseScript

/*
Configuration options

Create config file in ./tests/emailer/vertica.dsl with syntax:
vars {
    host = 'smtp host server'
    port = <smtp port (default smtp: 25, ssl: 465, tsl:587)>
    ssl = <ssl protocol true/false>
    tls = <tls authentication true/false>
    user = '<user name>'
    password = '<password>'
    fromAddress = '<from address by mail>'
    toAddress = '<comma separated destination addresses example a@google.com, b@yandex.ru>
}
*/
config {
    // Directory of configuration file
    path = configVars.configPath?:'tests/emailer'

    // Load configuration file
    load'emailer.dsl'

    // Print message to log file and console
    logConfig "Load configuration emailer.dsl complete. Use smtp server \"${configVars.host}\"."
}

mail {
    host = configVars.host
    port = configVars.port
    ssl = configVars.ssl
    tls = configVars.tls
    user = configVars.user
    password = configVars.password
    fromAddress = configVars.fromAddress
    toAddress = configVars.toAddress
    subject = 'Test mail send'
    isHtml = true
    message = '<HTML><BODY><H1>Message</H1>This is test send from getl lang</BODY></HTML>'
    attachment = new File(config().path + '/emailer.dsl')
    send()

    logInfo('Send message complete!')
}