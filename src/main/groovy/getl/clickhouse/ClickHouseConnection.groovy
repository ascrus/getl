package getl.clickhouse

import com.fasterxml.jackson.annotation.JsonIgnore
import getl.driver.Driver
import getl.exception.ExceptionGETL
import getl.jdbc.JDBCConnection
import getl.jdbc.TableDataset
import getl.utils.FileUtils
import groovy.transform.InheritConstructors

/**
 * ClickHouse connection class
 * @author Alexsey Konstantinov
 *
 */
@InheritConstructors
class ClickHouseConnection extends JDBCConnection {
    @Override
    protected Class<Driver> driverClass() { ClickHouseDriver }

    /** Current ClickHouse driver */
    @JsonIgnore
    ClickHouseDriver getCurrentClickHouseDriver() { driver as ClickHouseDriver }

    @Override
    protected void registerParameters () {
        super.registerParameters()
        methodParams.register('Super', ['useSsl', 'sslMode', 'sslRootCert', 'sslCert', 'sslKey'])
    }

    @Override
    protected void doInitConnection () {
        super.doInitConnection()
        driverName = 'com.clickhouse.jdbc.ClickHouseDriver'
    }

    @Override
    protected Class<TableDataset> getTableClass() { ClickHouseTable }

    /** Use ssl protocol */
    Boolean getUseSsl() { params.useSsl as Boolean }
    /** Use ssl protocol */
    void setUseSsl(Boolean value) { params.useSsl = value }

    /** Verify or not certificate: none (don't verify), strict (verify) */
    String getSslMode() { params.sslMode as String }
    /** Verify or not certificate: none (don't verify), strict (verify) */
    void setSslMode(String value) {
        if (value != null && value.toUpperCase() in ['NONE', 'STRICT'])
            throw new ExceptionGETL("Invalid ssl mode \"$value\", allowed NONE or STRICT")

        params.sslMode = value
    }

    /** Path to SSL/TLS root certificates */
    String getSslRootCert() { params.sslRootCert as String }
    /** Path to SSL/TLS root certificates */
    void setSslRootCert(String value) { params.sslRootCert = value }
    /** Absolute path to SSL/TLS root certificates */
    String sslRootCert() {
        FileUtils.ConvertToUnixPath(FileUtils.TransformFilePath(sslRootCert, dslCreator))
    }

    /** SSL/TLS certificate */
    String getSslCert() { params.sslCert as String }
    /** SSL/TLS certificate */
    void setSslCert(String value) { params.sslCert = value }

    /** RSA key in PKCS#8 format */
    String getSslKey() { params.sslKey as String }
    /** RSA key in PKCS#8 format */
    void setSslKey(String value) { params.sslKey = value }
}