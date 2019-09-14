package getl.files

import getl.utils.Config
import getl.utils.FileUtils

/**
 * @author Alexsey Konstantinov
 */
class FTPManagerTest extends ManagerTest {
    static final def confName = 'tests/filemanager/ftp.conf'

    @Override
    Manager newManager() {
        if (!FileUtils.ExistsFile(confName)) return null
        Config.LoadConfig(fileName: confName)
        return new FTPManager(config: 'test_ftp_filemanager')
    }
}