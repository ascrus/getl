package getl.kafka

import com.fasterxml.jackson.annotation.JsonIgnore
import getl.data.Connection
import getl.data.Dataset
import getl.exception.ExceptionGETL
import getl.kafka.opts.KafkaReadSpec
import groovy.transform.stc.ClosureParams
import groovy.transform.stc.SimpleType

class KafkaDataset extends Dataset {
    KafkaDataset() {
        super()
        _driver_params = [:] as Map<String, Object>
    }

    @Override
    void setConnection(Connection value) {
        if (value != null && !(value instanceof KafkaConnection))
            throw new ExceptionGETL('Only class KafkaConnection connections are permitted!')

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
}