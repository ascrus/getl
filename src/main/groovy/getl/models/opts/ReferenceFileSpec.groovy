package getl.models.opts

import getl.models.ReferenceFiles
import groovy.transform.InheritConstructors

/**
 * Model file specification
 * @author Alexsey Konstantinov
 */
@InheritConstructors
class ReferenceFileSpec extends FileSpec {
    /** Owner reference model */
    protected ReferenceFiles getOwnerReferenceFilesModel() { ownerModel as ReferenceFiles }

    /** Destination file storage path */
    String getDestinationPath() { params.destinationPath as String }
    /** Destination file storage path */
    void setDestinationPath(String value) { saveParamValue('destinationPath', value) }
}