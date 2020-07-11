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
package getl.models.opts

import getl.exception.ExceptionModel
import getl.jdbc.QueryDataset
import getl.models.MonitorRules
import groovy.time.Duration

/**
 * Table tracking options
 * @author Alexsey Konstantinov
 */
class MonitorRuleSpec extends BaseSpec { /*TODO: added ignore property */
    MonitorRuleSpec(MonitorRules owner, String queryName) {
        super(owner)
        setQueryName(queryName)
    }

    MonitorRuleSpec(MonitorRules model, Map importParams) {
        super(model, importParams)
    }

    /** Owner monitor */
    MonitorRules getOwnerMonitorModel() { ownerModel as MonitorRules }

    /** Repository query name*/
    String getQueryName() { params.queryName as String }
    /** Repository query name*/
    protected void setQueryName(String value) { params.queryName = value }

    QueryDataset getQuery() {
        return ownerModel.dslCreator.query(queryName)
    }

    /** Check frequency */
    Duration getCheckFrequency() { params.checkFrequency as Duration }
    /** Check frequency */
    void setCheckFrequency(Duration value) {
        if (value != null && value.toMilliseconds() <= 0)
            throw new ExceptionModel('The value must be greater than zero!')

        params.checkFrequency = value
    }

    /** Error notification retry time */
    Duration getNotificationTime() { params.notificationTime as Duration }
    /** Error correction threshold */
    void setNotificationTime(Duration value) {
        if (value != null && value.toMilliseconds() <= 0)
            throw new ExceptionModel('The value must be greater than zero!')

        params.notificationTime = value
    }

    /** Allowable time lag */
    Duration getLagTime() { params.lagTime as Duration }
    /** Allowable time lag */
    void setLagTime(Duration value) {
        if (value != null && value.toMilliseconds() <= 0)
            throw new ExceptionModel('The value must be greater than zero!')

        params.lagTime = value
    }

    /** Rule description */
    String getDescription() { params.description as String }
    /** Rule description */
    void setDescription(String value) { params.description = value }

    @Override
    String toString() {
        return "$queryName:$lagTime"
    }
}