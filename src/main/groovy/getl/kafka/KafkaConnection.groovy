package getl.kafka

import com.fasterxml.jackson.annotation.JsonIgnore
import getl.data.Connection
import getl.data.Dataset
import getl.driver.Driver
import getl.exception.RequiredParameterError
import getl.utils.BoolUtils
import getl.utils.ConvertUtils
import getl.utils.DateUtils
import groovy.transform.InheritConstructors

/**
 * Kafka connection class
 * @author Alexsey Konstantinov
 */
@InheritConstructors
class KafkaConnection extends Connection {
    @Override
    protected Class<Driver> driverClass() { KafkaDriver }

    /** Current Kafka connection driver */
    @JsonIgnore
    KafkaDriver getCurrentKafkaDriver() { driver as KafkaDriver }

    @Override
    void initParams() {
        super.initParams()
        params.connectProperties = new HashMap<String, Object>()
    }

    @Override
    protected void registerParameters() {
        super.registerParameters()
        methodParams.register('Super', ['bootstrapServers', 'connectProperties', 'groupId', 'autoCreateTopic', 'groupSeparator', 'uniFormatDateTime',
                                        'decimalSeparator', 'formatBoolean', 'formatDate', 'formatDateTime', 'formatTime', 'formatTimestampWithTz'])
    }

    @Override
    protected Class<Dataset> getDatasetClass() { KafkaDataset }

    /** Bootstrap servers */
    String getBootstrapServers() { params.bootstrapServers as String }
    /** Bootstrap servers */
    void setBootstrapServers(String value) { params.bootstrapServers = value }

    /** Consumer group id */
    String getGroupId() { params.groupId as String }
    /** Consumer group id */
    void setGroupId(String value) { params.groupId = value }

    /** Connection properties */
    Map<String, Object> getConnectProperties() { params.connectProperties as Map<String, Object>}
    /** Connection properties */
    void setConnectProperties(Map<String, Object> value) {
        connectProperties.clear()
        if (value != null)
            connectProperties.putAll(value)
    }

    /** Automatically create a topic on request if it doesn't exist in Kafka */
    Boolean getAutoCreateTopic() { ConvertUtils.Object2Boolean(params.autoCreateTopic) }
    /** Automatically create a topic on request if it doesn't exist in Kafka */
    void setAutoCreateTopic(Boolean value) { params.autoCreateTopic = value}
    /** Automatically create a topic on request if it doesn't exist in Kafka */
    Boolean autoCreateTopic() { BoolUtils.IsValue(autoCreateTopic) }

    /** Format for date fields */
    String getFormatDate() { params.formatDate as String }
    /** Format for date fields */
    void setFormatDate(String value) { params.formatDate = value }
    /** Format for date fields */
    String formatDate() { formatDate?:DateUtils.defaultDateMask }

    /** Format for datetime fields */
    String getFormatDateTime() { params.formatDateTime as String }
    /** Format for datetime fields */
    void setFormatDateTime(String value) { params.formatDateTime = value }
    /** Format for datetime fields */
    String formatDateTime() { formatDateTime?:DateUtils.defaultDateTimeMask }

    /** Format for timestamp with timezone fields */
    String getFormatTimestampWithTz() { params.formatTimestampWithTz as String }
    /** Format for timestamp with timezone fields */
    void setFormatTimestampWithTz(String value) { params.formatTimestampWithTz = value }
    /** Format for timestamp with timezone fields */
    String formatTimestampWithTz() { formatTimestampWithTz?:DateUtils.defaultTimestampWithTzFullMask }

    /** Format for time fields */
    String getFormatTime() { params.formatTime as String }
    /** Format for time fields */
    void setFormatTime(String value) { params.formatTime = value }
    /** Format for time fields */
    String formatTime() { formatTime?:DateUtils.defaultTimeMask }

    /** Use the same date and time format */
    String getUniFormatDateTime() { params.uniFormatDateTime as String }
    /** Use the same date and time format */
    void setUniFormatDateTime(String value) { params.uniFormatDateTime = value }

    /** Format for boolean fields */
    String getFormatBoolean() { params.formatBoolean as String }
    /** Format for boolean fields */
    void setFormatBoolean(String value) { params.formatBoolean = value }
    String formatBoolean() { formatBoolean?:'true|false' }

    /** Decimal separator for number fields */
    String getDecimalSeparator() { params.decimalSeparator as String }
    /** Decimal separator for number fields */
    void setDecimalSeparator(String value) { params.decimalSeparator = value }
    /** Decimal separator for number fields */
    String decimalSeparator() { decimalSeparator?:'.' }

    /** Group separator for number fields */
    String getGroupSeparator() { params.groupSeparator as String }
    /** Group separator for number fields */
    void setGroupSeparator(String value) { params.groupSeparator = value }
    /** Group separator for number fields */
    String groupSeparator() { groupSeparator }

    /**
     * Check topic existing in Kafka
     * @param topicName topic name
     * @return check result
     */
    Boolean topicExists(String topicName) {
        if (topicName == null)
            throw new RequiredParameterError(this, 'topicName', 'topicExists')

        return currentKafkaDriver.existsTopic(topicName)
    }

    /**
     * Create topic in Kafka
     * @param topicName topic name
     * @param numPartitions number partitions
     * @param replicationFactor replication factor
     * @param ifNotExists create topic if not exists in Kafka
     */
    void createTopic(String topicName, Integer numPartitions = 1, Integer replicationFactor = 1, Boolean ifNotExists = false) {
        currentKafkaDriver.createTopic(topicName, numPartitions, replicationFactor.shortValue(), ifNotExists)
    }

    /**
     * Drop topic in Kafka
     * @param topicName topic name
     * @param ifExists drop topic if exists in Kafka
     */
    void dropTopic(String topicName, Boolean ifExists = false) {
        currentKafkaDriver.dropTopic(topicName, ifExists)
    }

    /**
     * Read list of topic in Kafka
     * @return list of topic
     */
    List<String> listTopics() {
        return currentKafkaDriver.listTopics()
    }

    @Override
    String getObjectName() { (bootstrapServers != null)?"Kafka:[$bootstrapServers]":'Kafka' }
}