/*
 GETL - based package in Groovy, which automates the work of loading and transforming data. His name is an acronym for "Groovy ETL".

 GETL is a set of libraries of pre-built classes and objects that can be used to solve problems unpacking,
 transform and load data into programs written in Groovy, or Java, as well as from any software that supports
 the work with Java classes.

 Copyright (C) EasyData Company LTD

 This program is free software: you can redistribute it and/or modify
 it under the terms of the GNU Lesser General Public License as published by
 the Free Software Foundation, either version 3 of the License, or
 (at your option) any later version.

 This program is distributed in the hope that it will be useful,
 but WITHOUT ANY WARRANTY; without even the implied warranty of
 MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 GNU General Public License for more details.

 You should have received a copy of the GNU General Public License and
 GNU Lesser General Public License along with this program.
 If not, see <http://www.gnu.org/licenses/>.
*/

package getl.hive

import getl.exception.ExceptionGETL
import getl.jdbc.JDBCConnection

/**
 * Created by ascru on 15.03.2017.
 */
class HiveConnection extends JDBCConnection {
    HiveConnection() {
        super(driver: HiveDriver)
    }

    HiveConnection(Map params) {
        super(new HashMap([driver: HiveDriver]) + params)
    }

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
    protected void doInitConnection () {
        super.doInitConnection()
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

    /**
     * Vendor driver name
     */
    public String getVendor() { return params.vendor }
    /**
     * Vendor driver name
     */
    public void setVendor(String value) {
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
        params.vendor = value
    }

    /**
     * Version JDBC driver
     */
    public Integer getVersionDriver() { return params.versionDriver }
    /**
     * Version JDBC driver
     */
    public void setVersionDriver(Integer value) { params.versionDriver = value }

    /**
     * HDFS host
     */
    public String getHdfsHost () { return params.hdfsHost }
    public void setHdfsHost (String value) { params.hdfsHost = value }

    /**
     * HDFS port
     */
    public Integer getHdfsPort() { return params.hdfsPort }
    public void setHdfsPort(Integer value) { params.hdfsPort = value }

    /**
     * HDFS login
     */
    public String getHdfsLogin () { return params.hdfsLogin }
    public void setHdfsLogin (String value) { params.hdfsLogin = value }

    /**
     * HDFS directory for bulkload files
     */
    public String getHdfsDir () { return params.hdfsDir }
    public void setHdfsDir (String value) { params.hdfsDir = value }
}