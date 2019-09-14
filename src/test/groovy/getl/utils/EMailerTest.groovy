package getl.utils

import org.junit.Test

class EMailerTest extends getl.test.GetlTest {
    def configName = 'tests/emailer/emailer.conf'
    def mail_ssl = new EMailer(config: 'test_ssl')

    @Test
    void testSendSsl() {
        if (!(new File(configName).exists())) return
        Config.LoadConfig(fileName: configName)
        mail_ssl.sendMail(null, 'Test smtp ssl getl mail', 'Test complete!', true)
    }
}

