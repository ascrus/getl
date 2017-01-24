package getl.vertica

import getl.jdbc.JDBCConnection
import getl.jdbc.JDBCDriverProto
import getl.utils.Config
import getl.utils.FileUtils
import sun.misc.ClassLoaderUtil

/**
 * Created by ascru on 13.01.2017.
 */
class VerticaDriverTest extends JDBCDriverProto {
    @Override
    protected JDBCConnection newCon() {
        if (!FileUtils.ExistsFile('tests/vertica/vertica.conf')) return null
        Config.LoadConfig('tests/vertica/vertica.conf')
        return new VerticaConnection(config: 'vertica')
    }
}
