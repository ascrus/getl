package getl.impala

import com.fasterxml.jackson.annotation.JsonIgnore
import getl.driver.Driver
import getl.jdbc.JDBCConnection
import getl.jdbc.TableDataset
import groovy.transform.InheritConstructors

/**
 * Impala connection
 * @author Aleksey Konstantinov
 */
@InheritConstructors
class ImpalaConnection extends JDBCConnection {
    @Override
    protected Class<Driver> driverClass() { ImpalaDriver }

    /** Current Impala connection driver */
    @JsonIgnore
    ImpalaDriver getCurrentImpalaDriver() { driver as ImpalaDriver }

    @Override
    protected void registerParameters () {
        super.registerParameters()
        methodParams.register('Super', ['hdfsHost', 'hdfsPort', 'hdfsLogin', 'hdfsDir', 'dualTable'])
    }

    @Override
    protected void doInitConnection () {
        super.doInitConnection()
        driverName = 'com.cloudera.impala.jdbc.Driver'
    }

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

    /** Pseudo dual table name*/
    String getDualTable() { params.dualTable }
    /** Pseudo dual table name*/
    void setDualTable(String value) { params.dualTable = value }

    @Override
    protected Class<TableDataset> getTableClass() { ImpalaTable }
}