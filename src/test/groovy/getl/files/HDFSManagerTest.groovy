package getl.files

import getl.utils.Config
import getl.utils.FileUtils

/**
 * @author Alexsey Konstantinov
 */
class HDFSManagerTest extends ManagerTest {
    HDFSManager manager
    protected Manager getManager() {
        if (manager == null) {
            def confName = 'tests/filemanager/hdfs.conf'
            if (FileUtils.ExistsFile(confName)) {
                Config.LoadConfig(confName)
                if (Config.ContainsSection('files.test_hdfs_filemanager')) {
                    manager = new HDFSManager(config: 'test_hdfs_filemanager')
                }
            }
        }

        return manager
    }
}
