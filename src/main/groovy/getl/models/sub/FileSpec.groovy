package getl.models.sub

import groovy.transform.InheritConstructors

/**
 * Base model file specification
 * @author Alexsey Konstantinov
 */
@InheritConstructors
class FileSpec extends BaseSpec {
    FileSpec(FilesModel model, String fileName) {
        super(model)
        setFilePath(fileName)
    }

    /** Owner processing model */
    protected FilesModel getOwnerFilesModel() { ownerModel as FilesModel }

    @Override
    protected String objectNameInModel() { filePath }

    /** Path to file */
    String getFilePath() { params.filePath as String }
    /** Path to file */
    void setFilePath(String value) { saveParamValue('filePath', value) }

    @Override
    String toString() {
        return filePath
    }
}