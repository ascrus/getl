package getl.files

import getl.utils.Config
import getl.utils.FileUtils

/**
 * @author Alexsey Konstantinov
 */
class FTPManagerTest extends ManagerTest {
    FTPManager manager
    protected Manager getManager() {
        if (manager == null) {
            def confName = 'tests/filemanager/ftp.conf'
            if (FileUtils.ExistsFile(confName)) {
                Config.LoadConfig(confName)
                if (Config.ContainsSection('files.test_ftp_filemanager')) {
                    manager = new FTPManager(config: 'test_ftp_filemanager')
                }
            }
        }

        return manager
    }
}
