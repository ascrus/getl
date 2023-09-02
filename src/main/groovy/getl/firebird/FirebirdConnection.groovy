package getl.firebird

import com.fasterxml.jackson.annotation.JsonIgnore
import getl.driver.Driver
import getl.jdbc.JDBCConnection
import getl.jdbc.TableDataset
import getl.utils.FileUtils
import groovy.transform.InheritConstructors

/**
 * Firebird connection class
 * @author Alexsey Konstantinov
 *
 */
@InheritConstructors
class FirebirdConnection extends JDBCConnection {
    @Override
    protected Class<Driver> driverClass() { FirebirdDriver }

    /** Current Firebird connection driver */
    @JsonIgnore
    FirebirdDriver getCurrentFirebirdDriver() { driver as FirebirdDriver }

    @Override
    protected void doInitConnection () {
        super.doInitConnection()
        driverName = 'org.firebirdsql.jdbc.FBDriver'
    }

    @Override
    protected Class<TableDataset> getTableClass() { FirebirdTable }

    @Override
    String connectDatabase() { FileUtils.TransformFilePath(super.connectDatabase(), false, dslCreator) }
}