package getl.models.sub

import getl.exception.ExceptionDSL
import getl.utils.ListUtils
import getl.utils.MapUtils
import groovy.transform.Synchronized

class BaseSpec extends getl.lang.opts.BaseSpec {
    BaseSpec(BaseModel model) {
        super(model)
    }

    BaseSpec(BaseModel model, BaseSpec spec) {
        super(model, false, spec.params)
    }

    BaseSpec(BaseModel model, Map importParams) {
        super(model, false, importParams)
    }

    BaseSpec(BaseModel model, Boolean useExternalParams, Map importParams) {
        super(model, useExternalParams, importParams)
    }

    @Override
    protected void initSpec() {
        super.initSpec()
        if (params.objectVars == null)
            params.objectVars = [:] as Map<String, Object>
        if (params.attrs == null)
            params.attrs = [:] as Map<String, Object>
    }

    /** Owner model */
    protected BaseModel getOwnerModel() { ownerObject as BaseModel }

    /** Model object variables */
    @Synchronized
    Map<String, Object> getObjectVars() { params.objectVars as Map<String, Object> }
    /** Model object variables */
    @Synchronized
    void setObjectVars(Map<String, Object> value) {
        objectVars.clear()
        if (value != null)
            objectVars.putAll(value)
    }
    /**
     * Get the value of the specified variable
     * @param name variable name
     * @return variable value
     */
    Object variable(String name) { ListUtils.NotNullValue(objectVars.get(name), ownerModel.modelVars.get(name)) }

    /** Object attributes */
    @Synchronized
    Map<String, Object> getAttrs() { params.attrs as Map<String, Object> }
    /** Object attributes */
    @Synchronized
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
    Object attribute(String name) { ListUtils.NotNullValue(attrs.get(name), ownerModel.modelAttrs.get(name)) }

    /** Return all attributes defined for a model element and which are additionally present in the model */
    Map<String, Object> attributes() { ownerModel.modelAttrs + attrs }

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