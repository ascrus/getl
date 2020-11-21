package getl.models.opts

import getl.models.sub.BaseModel

import java.util.concurrent.ConcurrentHashMap

class BaseSpec extends getl.lang.opts.BaseSpec {
    BaseSpec(BaseModel model) {
        super(model)
    }

    BaseSpec(BaseModel model, Map importParams) {
        super(model, false, importParams)
    }

    @Override
    protected void initSpec() {
        super.initSpec()
        if (params.objectVars == null)
            params.objectVars = new ConcurrentHashMap<String, Object>()
    }

    /** Owner model */
    protected BaseModel getOwnerModel() { ownerObject as BaseModel }

    /** Model object variables */
    Map<String, Object> getObjectVars() { params.objectVars as Map<String, Object> }
    /** Model object variables */
    void setObjectVars(Map<String, Object> value) {
        objectVars.clear()
        if (value != null)
            objectVars.putAll(value)
    }
}