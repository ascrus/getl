package getl.data

import com.fasterxml.jackson.annotation.JsonIgnore
import groovy.transform.InheritConstructors
import getl.utils.*

/**
 * Base virtual dataset class 
 * @author Alexsey Konstantinov
 *
 */
@InheritConstructors
class VirtualDataset extends Dataset {
	/** Destinition dataset */
    Dataset getDest () { params.dest as Dataset }
	/** Destinition dataset */
    void setDest (Dataset value) { params.dest = value }
	
	@Override
    List<String> excludeSaveParams () {
		super.excludeSaveParams() + ["dest"]
	}
	
	@Override
    @JsonIgnore
    String getObjectName() { dest?.objectName }
	
	@Override
    @JsonIgnore
    String getObjectFullName() { dest?.objectFullName }
}