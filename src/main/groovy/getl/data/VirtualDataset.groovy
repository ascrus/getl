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
	/** Destination dataset */
    Dataset getDest () { params.dest as Dataset }
	/** Destination dataset */
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