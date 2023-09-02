package getl.sqlite

import com.fasterxml.jackson.annotation.JsonIgnore
import getl.driver.Driver
import getl.jdbc.JDBCConnection
import getl.jdbc.TableDataset
import getl.utils.FileUtils
import groovy.transform.InheritConstructors

/**
 * SQLite connection class
 * @author Alexsey Konstantinov
 */
@InheritConstructors
class SQLiteConnection extends JDBCConnection {
    @Override
    protected Class<Driver> driverClass() { SQLiteDriver }

    /** Current SQLite driver */
    @JsonIgnore
    SQLiteDriver getCurrentSQLiteDriver() { driver as SQLiteDriver }

    @Override
    protected void doInitConnection () {
        super.doInitConnection()
        driverName = 'org.sqlite.JDBC'
    }

    @Override
    protected Class<TableDataset> getTableClass() { SQLiteTable }

    @Override
    String connectDatabase() {
        def res = FileUtils.TransformFilePath(super.connectDatabase(), false, dslCreator)
        if (res != null)
            FileUtils.ValidFilePath(res)
        return res
    }
}