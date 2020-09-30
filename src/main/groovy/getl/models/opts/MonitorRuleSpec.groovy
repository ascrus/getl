package getl.models.opts

import com.fasterxml.jackson.annotation.JsonIgnore
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
    protected MonitorRules getOwnerMonitorModel() { ownerModel as MonitorRules }

    /** Query name */
    String getQueryName() { params.queryName as String }
    /** Query name */
    void setQueryName(String value) { params.queryName = value }

    /** Query */
    @JsonIgnore
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