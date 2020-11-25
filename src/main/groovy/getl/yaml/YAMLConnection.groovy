package getl.yaml

import com.fasterxml.jackson.annotation.JsonIgnore
import getl.data.Dataset
import getl.data.FileConnection

/**
 * JSON connection class
 * @author Alexsey Konstantinov
 *
 */
class YAMLConnection extends FileConnection {
    YAMLConnection() {
        super([driver: YAMLDriver])
    }

    YAMLConnection(Map params) {
        super(new HashMap([driver: YAMLDriver]) + (params?:[:]))

        if (this.getClass().name == 'getl.yaml.YAMLConnection') methodParams.validation("Super", params?:[:])
    }

    /** Current JSON connection driver */
    @JsonIgnore
    YAMLDriver getCurrentYAMLDriver() { driver as YAMLDriver }

    @Override
    protected Class<Dataset> getDatasetClass() { YAMLDataset }
}
