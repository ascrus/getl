package getl.utils

class EMailerTest extends getl.test.GetlTest {
    def configName = 'tests/emailer/emailer.conf'
    def mail_ssl = new EMailer(config: 'test_ssl')

    void testSendSsl() {
        if (!(new File(configName).exists())) return
        Config.LoadConfig(fileName: configName)
        mail_ssl.sendMail(null, 'Test smtp ssl getl mail', 'Test complete!', true)
    }
}

