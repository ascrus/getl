package getl.kafka

import com.fasterxml.jackson.annotation.JsonIgnore
import getl.data.Connection
import getl.data.Dataset
import getl.exception.ExceptionGETL

/**
 * Kafka connection class
 * @author Alexsey Konstantinov
 */
class KafkaConnection extends Connection {
    KafkaConnection() {
        super(driver: KafkaDriver)
    }

    KafkaConnection(Map params) {
        super(new HashMap([driver: KafkaDriver]) + params?:[:])
        if (this.getClass().name == 'getl.kafka.KafkaConnection') {
            methodParams.validation("Super", params?:[:])
        }
    }

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
    protected void onLoadConfig(Map configSection) {
        super.onLoadConfig(configSection)

        if (this.getClass().name == 'getl.kafka.KafkaConnection') {
            methodParams.validation('Super', params)
        }
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