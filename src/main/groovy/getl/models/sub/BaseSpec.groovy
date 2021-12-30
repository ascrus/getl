//file:noinspection unused
package getl.models.sub

import getl.exception.ExceptionDSL
import getl.utils.DateUtils
import getl.utils.ListUtils
import getl.utils.MapUtils
import getl.utils.Path
import getl.utils.StringUtils
import groovy.transform.Synchronized

import java.sql.Timestamp

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
    /** Save attribute value */
    @Synchronized('synchAttrs')
    void saveAttribute(String name, Object value) {
        attrs.put(name, value)
    }

    /**
     * Get the value of the specified attribute
     * @param name attribute name
     * @return attribute value
     */
    @Synchronized('synchAttrs')
    Object attribute(String name) { attributes().get(name) }

    /**
     * Get the value of the specified attribute with parsing variables
     * @param name attribute name
     * @param extVars extend variables
     * @return parsed attribute value
     */
    @Synchronized('synchAttrs')
    String attributeValue(String name, Map extVars = null) {
        def val = attributes().get(name)
        if (val == null)
            return null

        String res
        try {
            res = StringUtils.EvalMacroString(val.toString(), variables() + (extVars?:[:]))
        }
        catch (Exception e) {
            throw new ExceptionDSL("Error parsing the value of the \"$name\" attribute from \"${objectNameInModel()}\" node: ${e.message}")
        }

        return res
    }

    /**
     * Get the integer value of the specified attribute with parsing variables
     * @param name attribute name
     * @param extVars extend variables
     * @return parsed integer value
     */
    @Synchronized('synchAttrs')
    Integer attributeIntegerValue(String name, Map extVars = null) {
        def value = attributeValue(name, extVars)
        if (value == null || value.length() == 0)
            return null

        if (!value.integer)
            throw new ExceptionDSL("Error converting the value \"$value\" of the attribute \"$name\" into a " +
                    "number from \"${objectNameInModel()}\" node!")

        return value.toInteger()
    }

    /**
     * Get the long value of the specified attribute with parsing variables
     * @param name attribute name
     * @param extVars extend variables
     * @return parsed long value
     */
    @Synchronized('synchAttrs')
    Long attributeLongValue(String name, Map extVars = null) {
        def value = attributeValue(name, extVars)?.toString()
        if (value == null || value.length() == 0)
            return null

        if (!value.bigInteger)
            throw new ExceptionDSL("Error converting the value \"$value\" of the attribute \"$name\" into a " +
                    "number from \"${objectNameInModel()}\" node!")

        return value.toBigInteger().longValue()
    }

    /**
     * Get the timestamp value of the specified attribute with parsing variables
     * @param name attribute name
     * @param extVars extend variables
     * @param format parse format (default yyyy-MM-dd HH:mm:ss)
     * @return parsed timestamp value
     */
    @Synchronized('synchAttrs')
    Timestamp attributeTimestampValue(String name, Map extVars = null, String format = null) {
        def value = attributeValue(name, extVars)?.toString()
        if (value == null || value.length() == 0)
            return null

        Timestamp res
        try {
            res = DateUtils.ParseSQLTimestamp((format?:'yyyy-MM-dd HH:mm:ss'), value, false)
        }
        catch (Exception e) {
            throw new ExceptionDSL("Error converting the value \"$value\" of the attribute \"$name\" into a " +
                    "timestamp from \"${objectNameInModel()}\" node: ${e.message}!")
        }

        return res
    }

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
                unknownKeys.add(k)
        }

        if (!unknownKeys.isEmpty())
            throw new ExceptionDSL("Unknown attributes were detected in model dataset " +
                    "\"$ownerModel.dslNameObject\".\"$this\": $unknownKeys, " +
                    "allow attributes: $allowAttrs")
    }

    /** Check model owner */
    protected void checkGetlInstance() {
        ownerModel.checkGetlInstance()
    }
}