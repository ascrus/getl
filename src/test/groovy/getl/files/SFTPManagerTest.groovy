package getl.files

import getl.tfs.TFS
import getl.utils.Config
import getl.utils.FileUtils

/**
 * @author Alexsey Konstantinov
 */
class SFTPManagerTest extends ManagerTest {
    SFTPManager manager
    protected Manager getManager() {
        if (manager == null) {
            def confName = 'tests/filemanager/sftp.conf'
            if (FileUtils.ExistsFile(confName)) {
                Config.LoadConfig(confName)
                if (Config.ContainsSection('files.test_sftp_filemanager')) {
                    manager = new SFTPManager(config: 'test_sftp_filemanager')
                }
            }
        }

        return manager
    }
}
