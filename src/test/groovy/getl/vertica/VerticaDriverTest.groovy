package getl.vertica

import getl.data.*
import getl.jdbc.*
import getl.utils.*

/**
 * Created by ascru on 13.01.2017.
 */
class VerticaDriverTest extends JDBCDriverProto {
    static final def configName = 'tests/vertica/vertica.conf'
    @Override
    protected JDBCConnection newCon() {
        if (!FileUtils.ExistsFile(configName)) return null
        Config.LoadConfig(configName)
        return new VerticaConnection(config: 'vertica')
    }
}