package getl.kafka

import com.fasterxml.jackson.annotation.JsonIgnore
import getl.data.Connection
import getl.data.Dataset
import getl.driver.Driver
import getl.exception.ExceptionGETL
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
        params.connectProperties = [:] as Map<String, Object>
    }

    @Override
    protected void registerParameters() {
        super.registerParameters()
        methodParams.register('Super', ['bootstrapServers', 'connectProperties', 'groupId'])
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
}