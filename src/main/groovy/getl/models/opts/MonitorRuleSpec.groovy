package getl.models.opts

import com.fasterxml.jackson.annotation.JsonIgnore
import getl.exception.ExceptionModel
import getl.jdbc.QueryDataset
import getl.models.MonitorRules
import getl.models.sub.BaseSpec
import getl.utils.BoolUtils
import groovy.time.Duration
import groovy.transform.InheritConstructors

/**
 * Table tracking options
 * @author Alexsey Konstantinov
 */
@InheritConstructors
class MonitorRuleSpec extends BaseSpec {
    MonitorRuleSpec(MonitorRules owner, String queryName) {
        super(owner)
        setQueryName(queryName)
    }

    /** Owner monitor */
    protected MonitorRules getOwnerMonitorModel() { ownerModel as MonitorRules }

    /** Query name */
    String getQueryName() { params.queryName as String }
    /** Query name */
    void setQueryName(String value) { saveParamValue('queryName', value) }

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

        saveParamValue('checkFrequency', value)
    }

    /** Error notification retry time */
    Duration getNotificationTime() { params.notificationTime as Duration }
    /** Error correction threshold */
    void setNotificationTime(Duration value) {
        if (value != null && value.toMilliseconds() <= 0)
            throw new ExceptionModel('The value must be greater than zero!')

        saveParamValue('notificationTime', value)
    }

    /** Allowable time lag */
    Duration getLagTime() { params.lagTime as Duration }
    /** Allowable time lag */
    void setLagTime(Duration value) {
        if (value != null && value.toMilliseconds() <= 0)
            throw new ExceptionModel('The value must be greater than zero!')

        saveParamValue('lagTime', value)
    }

    /** Rule description */
    String getDescription() { params.description as String }
    /** Rule description */
    void setDescription(String value) { saveParamValue('description', value) }

    /** Allow rule */
    Boolean getEnabled() { params.enabled as Boolean }
    /** Allow rule */
    void setEnabled(Boolean value) { saveParamValue('enabled', value) }

    @Override
    String toString() {
        return "$queryName:$lagTime (" + ((BoolUtils.IsValue(enabled, true))?'enabled':'disabled') + ')'
    }
}