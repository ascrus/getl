package getl.hive

import com.fasterxml.jackson.annotation.JsonIgnore
import getl.driver.Driver
import getl.exception.ConnectionError
import getl.jdbc.JDBCConnection
import getl.jdbc.TableDataset
import groovy.transform.InheritConstructors

/**
 * Hive connection
 * @author Aleksey Konstantinov
 */
@InheritConstructors
class HiveConnection extends JDBCConnection {
    @Override
    protected Class<Driver> driverClass() { HiveDriver }

    /** Current Hive connection driver */
    @JsonIgnore
    HiveDriver getCurrentHiveDriver() { driver as HiveDriver }

    @Override
    protected void registerParameters () {
        super.registerParameters()
        methodParams.register('Super', ['vendor', 'version', 'hdfsHost', 'hdfsLogin', 'hdfsDir'])
    }

    @Override
    protected void onLoadConfig (Map configSection) {
        super.onLoadConfig(configSection)
        if (params.vendor != null)
            setVendor(params.vendor as String)
    }

    @Override
    protected void doBeforeConnect () {
        super.doBeforeConnect()
        switch (vendor?.toLowerCase()) {
            case 'apache':
                driverName = 'org.apache.hive.jdbc.HiveDriver'
                break
            case 'hortonworks':
                driverName = 'org.apache.hive.jdbc.HiveDriver'
                break
            case 'cloudera':
                def ver = versionDriver?:4
                driverName = "com.cloudera.hive.jdbc${ver}.HS2Driver"
                break
            default:
                if (driverName == null)
                    throw new ConnectionError(this, 'not set "vendor" and "driverName"')
        }
    }

    /** Vendor driver name */
    String getVendor() { params.vendor as String }
    /** Vendor driver name */
    void setVendor(String value) {
        if (value != null) {
            switch (value?.toLowerCase()) {
                case 'apache':
                    break
                case 'hortonworks':
                    break
                case 'cloudera':
                    break
                default:
                    throw new ConnectionError(this, 'invalid vendor "{vendor}"', [vendor: value])

            }
        }
        params.vendor = value
    }

    /** Version JDBC driver */
    Integer getVersionDriver() { params.versionDriver as Integer }
    /** Version JDBC driver */
    void setVersionDriver(Integer value) { params.versionDriver = value }

    /** HDFS host */
    String getHdfsHost() { params.hdfsHost as String }
    /** HDFS host */
    void setHdfsHost (String value) { params.hdfsHost = value }
    /** HDFS host */
    String hdfsHost() { hdfsHost?:connectHostName }

    /** HDFS port */
    Integer getHdfsPort() { params.hdfsPort as Integer }
    /** HDFS port */
    void setHdfsPort(Integer value) { params.hdfsPort = value }
    /** HDFS port */
    Integer hdfsPort() { hdfsPort?:8022 }

    /** HDFS login */
    String getHdfsLogin() { params.hdfsLogin as String }
    /** HDFS login */
    void setHdfsLogin (String value) { params.hdfsLogin = value }
    /** HDFS login */
    String hdfsLogin() { hdfsLogin?:login }

    /** HDFS directory for bulkload files */
    String getHdfsDir() { params.hdfsDir as String }
    /** HDFS directory for bulkload files */
    void setHdfsDir (String value) { params.hdfsDir = value }
    /** HDFS directory for bulkload files */
    String hdfsDir() {
        def hl = hdfsLogin()
        return hdfsDir?:((hl != null)?"/user/$hl":null)
    }

    @Override
    protected Class<TableDataset> getTableClass() { HiveTable }
}