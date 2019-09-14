package getl.files

import getl.tfs.TFS
import getl.utils.Config
import getl.utils.FileUtils

/**
 * @author Alexsey Konstantinov
 */
class SFTPManagerTest extends ManagerTest {
    static final def confName = 'tests/filemanager/sftp.conf'

    @Override
    Manager newManager() {
        if (!FileUtils.ExistsFile(confName)) return null
        Config.LoadConfig(fileName: confName)
        return new SFTPManager(config: 'test_sftp_filemanager')
    }
}