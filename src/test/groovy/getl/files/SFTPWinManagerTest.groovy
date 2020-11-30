package getl.files


import getl.utils.Config
import getl.utils.FileUtils
import org.junit.Ignore

/**
 * @author Alexsey Konstantinov
 */
@Ignore
class SFTPWinManagerTest extends ManagerTest {
    static final def confName = 'tests/filemanager/sftp.conf'

    @Override
    Manager newManager() {
        if (!FileUtils.ExistsFile(confName)) return null
        Config.LoadConfig(fileName: confName)
        return new SFTPManager(config: 'test_sftp_win_filemanager')
    }
}