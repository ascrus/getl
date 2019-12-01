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
package getl.impala

import getl.jdbc.JDBCConnection
import groovy.transform.InheritConstructors

/**
 * Impala connection
 * @author Aleksey Konstantinov
 */
@InheritConstructors
class ImpalaConnection extends JDBCConnection {
    ImpalaConnection() {
        super(driver: ImpalaDriver)
    }

    ImpalaConnection(Map params) {
        super(new HashMap([driver: ImpalaDriver]) + params?:[:])
        if (this.getClass().name == 'getl.impala.ImpalaConnection') methodParams.validation('Super', params?:[:])
    }

    /** Current Impala connection driver */
    ImpalaDriver getCurrentImpalaDriver() { driver as ImpalaDriver }

    @Override
    protected void registerParameters () {
        super.registerParameters()
        methodParams.register('Super', ['hdfsHost', 'hdfsLogin', 'hdfsDir'])
    }

    @Override
    protected void doInitConnection () {
        super.doInitConnection()
        driverName = 'com.cloudera.impala.jdbc.Driver'
    }

    @Override
    protected void onLoadConfig (Map configSection) {
        super.onLoadConfig(configSection)
        if (this.getClass().name == 'getl.impala.ImpalaConnection') methodParams.validation('Super', params)
    }

    /** HDFS host */
    String getHdfsHost () { params.hdfsHost as String }
    /** HDFS host */
    void setHdfsHost (String value) { params.hdfsHost = value }

    /** HDFS port */
    Integer getHdfsPort() { params.hdfsPort as Integer }
    /** HDFS port */
    void setHdfsPort(Integer value) { params.hdfsPort = value }

    /** HDFS login */
    String getHdfsLogin () { params.hdfsLogin as String }
    /** HDFS login */
    void setHdfsLogin (String value) { params.hdfsLogin = value }

    /** HDFS directory for bulkload files */
    String getHdfsDir () { params.hdfsDir as String }
    /** HDFS directory for bulkload files */
    void setHdfsDir (String value) { params.hdfsDir = value }

    /** Pseudo dual table name*/
    String getDualTable() { params.dualTable }
    /** Pseudo dual table name*/
    void setDualTable(String value) { params.dualTable = value }
}