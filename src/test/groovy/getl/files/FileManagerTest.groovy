package getl.files

import getl.tfs.TFS
import getl.utils.FileUtils

/**
 * @author Alexsey Konstantinov
 */
class FileManagerTest extends ManagerTest {
    @Override
    Manager newManager() {
        def rootPath = "${TFS.systemPath}"
        def f = new File(rootPath)
        FileUtils.ValidPath(f)
        f.deleteOnExit()
        return new FileManager(rootPath: rootPath)
    }
}