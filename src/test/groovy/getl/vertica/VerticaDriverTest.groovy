package getl.vertica

import getl.data.*
import getl.jdbc.*
import getl.utils.*

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
