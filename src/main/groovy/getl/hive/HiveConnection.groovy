package getl.hive

import com.fasterxml.jackson.annotation.JsonIgnore
import getl.exception.ExceptionGETL
import getl.jdbc.JDBCConnection
import getl.jdbc.TableDataset

/**
 * Hive connection
 * @author Aleksey Konstantinov
 */
class HiveConnection extends JDBCConnection {
    HiveConnection() {
        super(driver: HiveDriver)
    }

    HiveConnection(Map params) {
        super(new HashMap([driver: HiveDriver]) + params?:[:])
        if (this.getClass().name == 'getl.hive.HiveConnection') methodParams.validation('Super', params?:[:])
    }

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
        if (this.getClass().name == 'getl.hive.HiveConnection') methodParams.validation('Super', params)
        if (params.vendor != null) setVendor(params.vendor as String)
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
                throw new ExceptionGETL('Need set vendor name from Hive connection')
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
                    throw new ExceptionGETL("Unknown Hive vendor \"$value\"")

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
    String hdfsHost() { hdfsHost?:connectHost }

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
    String hdfsDir() { hdfsDir?:((hdfsLogin() != null)?"/user/${hdfsLogin()}":null) }

    @Override
    protected Class<TableDataset> getTableClass() { HiveTable }
}