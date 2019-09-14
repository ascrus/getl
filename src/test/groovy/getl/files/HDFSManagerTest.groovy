package getl.files

import getl.utils.Config
import getl.utils.FileUtils

/**
 * @author Alexsey Konstantinov
 */
class HDFSManagerTest extends ManagerTest {
    static final def confName = 'tests/filemanager/hdfs.conf'

    @Override
    Manager newManager() {
        if (!FileUtils.ExistsFile(confName)) return null
        Config.LoadConfig(fileName: confName)
        return new HDFSManager(config: 'test_hdfs_filemanager')
    }
}