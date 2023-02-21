package getl.kafka

import com.fasterxml.jackson.annotation.JsonIgnore
import getl.data.Connection
import getl.data.Dataset
import getl.data.Field
import getl.exception.DatasetError
import getl.kafka.opts.KafkaReadSpec
import getl.utils.BoolUtils
import getl.utils.ConvertUtils
import groovy.transform.CompileStatic
import groovy.transform.InheritConstructors
import groovy.transform.stc.ClosureParams
import groovy.transform.stc.SimpleType

@InheritConstructors
class KafkaDataset extends Dataset {
    static public String offsetForRegisterLatest = 'latest'
    static public String offsetForRegisterEarliest = 'earliest'
    static public String offsetForRegisterNone = 'none'

    @Override
    protected void initParams() {
        super.initParams()

        _driver_params = new HashMap<String, Object>()
    }

    @Override
    void setConnection(Connection value) {
        if (value != null && !(value instanceof KafkaConnection))
            throw new DatasetError(this, '#dataset.invalid_connection', [className: KafkaConnection.name])

        super.setConnection(value)
    }

    /** Use specified connection */
    KafkaConnection useConnection(KafkaConnection value) {
        setConnection(value)
        return value
    }

    /** Current Kafka connection*/
    @JsonIgnore
    KafkaConnection getCurrentKafkaConnection() { connection as KafkaConnection }

    /** Topic name in kafka */
    String getKafkaTopic() { params.topic as String }
    /** Topic name in kafka */
    void setKafkaTopic(String value) { params.topic = value }

    /** Topic key name (default null to get all keys) */
    String getKeyName() { params.keyName as String }
    /** Topic key name (default null to get all keys) */
    void setKeyName(String value) { params.keyName = value }

    /** Automatically create a topic on request if it doesn't exist in Kafka */
    Boolean getAutoCreateTopic() { ConvertUtils.Object2Boolean(params.autoCreateTopic) }
    /** Automatically create a topic on request if it doesn't exist in Kafka */
    void setAutoCreateTopic(Boolean value) { params.autoCreateTopic = value}
    /** Automatically create a topic on request if it doesn't exist in Kafka */
    Boolean autoCreateTopic() { BoolUtils.IsValue(autoCreateTopic, currentKafkaConnection.autoCreateTopic()) }

    /** Format for date fields */
    String getFormatDate() { params.formatDate as String }
    /** Format for date fields */
    void setFormatDate(String value) { params.formatDate = value }
    /** Format for date fields */
    String formatDate() { formatDate?:currentKafkaConnection?.formatDate() }

    /** Format for datetime fields */
    String getFormatDateTime() { params.formatDateTime as String }
    /** Format for datetime fields */
    void setFormatDateTime(String value) { params.formatDateTime = value }
    /** Format for datetime fields */
    String formatDateTime(Boolean formatValue = false) { formatDateTime?:currentKafkaConnection?.formatDateTime(formatValue) }

    /** Format for timestamp with timezone fields */
    String getFormatTimestampWithTz() { params.formatTimestampWithTz as String }
    /** Format for timestamp with timezone fields */
    void setFormatTimestampWithTz(String value) { params.formatTimestampWithTz = value }
    /** Format for timestamp with timezone fields */
    String formatTimestampWithTz(Boolean formatValue = false) { formatTimestampWithTz?:currentKafkaConnection?.formatTimestampWithTz(formatValue) }

    /** Format for time fields */
    String getFormatTime() { params.formatTime as String }
    /** Format for time fields */
    void setFormatTime(String value) { params.formatTime = value }
    /** Format for time fields */
    String formatTime() { formatTime?:currentKafkaConnection?.formatTime() }

    /** Use the same date and time format */
    String getUniFormatDateTime() { params.uniFormatDateTime as String }
    /** Use the same date and time format */
    void setUniFormatDateTime(String value) { params.uniFormatDateTime = value }
    /** Use the same date and time format */
    String uniFormatDateTime() { uniFormatDateTime?:currentKafkaConnection?.uniFormatDateTime }

    /** Format for boolean fields */
    String getFormatBoolean() { params.formatBoolean as String }
    /** Format for boolean fields */
    void setFormatBoolean(String value) { params.formatBoolean = value }
    String formatBoolean() { formatBoolean?:currentKafkaConnection?.formatBoolean() }

    /** Decimal separator for number fields */
    String getDecimalSeparator() { params.decimalSeparator as String }
    /** Decimal separator for number fields */
    void setDecimalSeparator(String value) { params.decimalSeparator = value }
    /** Decimal separator for number fields */
    String decimalSeparator() { decimalSeparator?:currentKafkaConnection?.decimalSeparator() }

    /** Group separator for number fields */
    String getGroupSeparator() { params.groupSeparator as String }
    /** Group separator for number fields */
    void setGroupSeparator(String value) { params.groupSeparator = value }
    /** Group separator for number fields */
    String groupSeparator() { groupSeparator?:currentKafkaConnection?.groupSeparator() }

    /**
     * Return the format of the specified field
     * @param field dataset field
     * @return format
     */
    @CompileStatic
    String fieldFormat(Field field, Boolean formatValue = false) {
        if (field.format != null)
            return field.format
        if (uniFormatDateTime != null)
            return uniFormatDateTime

        String res = null
        switch (field.type) {
            case Field.dateFieldType:
                res = formatDate()
                break
            case Field.datetimeFieldType:
                res = formatDateTime(formatValue)
                break
            case Field.timestamp_with_timezoneFieldType:
                res = formatTimestampWithTz(formatValue)
                break
            case Field.timeFieldType:
                res = formatTime()
        }

        return res
    }

    /** Consumer options */
    KafkaReadSpec getReadOpts() { new KafkaReadSpec(this, true, directives('read')) }

    /** Consumer options */
    KafkaReadSpec readOpts(@DelegatesTo(KafkaReadSpec) @ClosureParams(value = SimpleType, options = ['getl.kafka.opts.KafkaReadSpec']) Closure cl) {
        def parent = readOpts
        parent.runClosure(cl)

        return parent
    }

    /** Name of data map node */
    String getDataNode() { params.dataNode as String }
    /** Name of data map node */
    void setDataNode(String value) { params.dataNode = value }

    /** Dataset name */
    @JsonIgnore
    @Override
    String getObjectName() {
        def objName = (kafkaTopic != null)?":$kafkaTopic":''
        return (connection != null)?"${currentKafkaConnection?.bootstrapServers}:[$objName]":'Kafka'
    }
    /** Full dataset name */
    @JsonIgnore
    String getObjectFullName() { objectName }
}