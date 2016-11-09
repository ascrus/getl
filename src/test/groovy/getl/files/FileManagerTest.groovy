package getl.files

import getl.tfs.TFS
import getl.utils.FileUtils

/**
 * @author Alexsey Konstantinov
 */
class FileManagerTest extends ManagerTest {
    FileManager manager
    protected Manager getManager() {
        if (manager == null) {
            def rootPath = "${TFS.systemPath}"
            def f = new File(rootPath)
            FileUtils.ValidPath(f)
            f.deleteOnExit()

            manager = new FileManager(rootPath: rootPath)
        }

        return manager
    }
}
