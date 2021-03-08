package getl.models.sub

import getl.exception.ExceptionDSL
import getl.utils.MapUtils

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
        if (params.attrs == null)
            params.attrs = new ConcurrentHashMap<String, Object>()
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

    /** Object attributes */
    Map<String, Object> getAttrs() { params.attrs as Map<String, Object> }
    /** Object attributes */
    void setAttrs(Map<String, Object> value) {
        attrs.clear()
        if (value != null)
            attrs.putAll(value)
    }
    /**
     * Get the value of the specified attribute
     * @param name attribute name
     * @return attribute value
     */
    Object attribute(String name) { attrs.get(name)?:ownerModel.modelAttrs.get(name) }

    /**
     * Check attribute naming and generate an unknown error
     * @param allowAttrs list of allowed attribute names
     */
    void checkAttrs(List<String> allowAttrs) {
        if (allowAttrs == null)
            throw new ExceptionDSL('The list of attribute names in parameter "allowAttrs" is not specified!')

        def unknownKeys = MapUtils.Unknown(attrs, allowAttrs)
        if (!unknownKeys.isEmpty())
            throw new ExceptionDSL("Unknown attributes were detected in model dataset " +
                    "\"$ownerModel.dslNameObject\".\"$this\": $unknownKeys, " +
                    "allow attributes: $allowAttrs")
    }
}