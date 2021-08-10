//file:noinspection unused
package getl.models.sub

import getl.exception.ExceptionDSL
import getl.utils.ListUtils
import getl.utils.MapUtils
import getl.utils.Path
import groovy.transform.Synchronized

abstract class BaseSpec extends getl.lang.opts.BaseSpec {
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

    /** Object dsl name */
    abstract protected String objectNameInModel()

    private final Object synchVars = new Object()

    /** Model object variables */
    @Synchronized('synchVars')
    Map<String, Object> getObjectVars() { params.objectVars as Map<String, Object> }
    /** Model object variables */
    @Synchronized('synchVars')
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
    @Synchronized('synchVars')
    Object variable(String name) { ListUtils.NotNullValue(objectVars.get(name), ownerModel.modelVars.get(name)) }

    /**
     * Return node variables
     * @param mask mask name
     * @return variable set
     */
    @Synchronized('synchVars')
    Map<String, Object> variables(String mask = null) {
        return ownerModel.modelVariables(mask) + MapUtils.FindNodes(objectVars, mask)
    }

    private final Object synchAttrs = new Object()

    /** Object attributes */
    @Synchronized('synchAttrs')
    Map<String, Object> getAttrs() { params.attrs as Map<String, Object> }
    /** Object attributes */
    @Synchronized('synchAttrs')
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
    @Synchronized('synchAttrs')
    Object attribute(String name) { ListUtils.NotNullValue(attrs.get(name), ownerModel.modelAttrs.get(name)) }

    /**
     * Return node attributes
     * @param mask mask name
     * @return attribute set
     */
    @Synchronized('synchAttrs')
    Map<String, Object> attributes(String mask = null) {
        return ownerModel.modelAttributes(mask) + MapUtils.FindNodes(attrs, mask)
    }

    /**
     * Return node sub attributes
     * @param topName attribute group name for which all subordinate attributes should be returned
     * @return attribute set
     */
    @Synchronized('synchAttrs')
    Map<String, Object> subAttributes(String topName) {
        return ownerModel.modelSubAttributes(topName) + MapUtils.FindSubNodes(attrs, topName)
    }

    /**
     * Check attribute naming and generate an unknown error
     * @param allowAttrs list of allowed attribute names
     */
    void checkAttrs(List<String> allowAttrs) {
        if (allowAttrs == null)
            throw new ExceptionDSL('The list of attribute names in parameter "allowAttrs" is not specified!')

        def validation = Path.Masks2Paths(allowAttrs)
        checkAttrsInternal(validation, allowAttrs)
    }

    /**
     * Check attribute naming and generate an unknown error
     * @param validation list of mask validator
     */
    @Synchronized('synchAttrs')
    protected checkAttrsInternal(List<Path> validation, List<String> allowAttrs) {
        def unknownKeys = [] as List<String>
        attrs.each { k, v ->
            if (!Path.MatchList(k, validation))
                unknownKeys << k
        }

        if (!unknownKeys.isEmpty())
            throw new ExceptionDSL("Unknown attributes were detected in model dataset " +
                    "\"$ownerModel.dslNameObject\".\"$this\": $unknownKeys, " +
                    "allow attributes: $allowAttrs")
    }
}