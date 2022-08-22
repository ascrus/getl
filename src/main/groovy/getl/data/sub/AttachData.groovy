package getl.data.sub

/**
 * Interface for allowed attach data to dataset
 * @author Alexsey Konstantinov
 */
interface AttachData {
    Object getLocalDatasetData()
    void setLocalDatasetData(Object value)
}