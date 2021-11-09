package getl.lang.sub

import com.fasterxml.jackson.annotation.JsonIgnore
import getl.lang.Getl

/**
 * Getl repository registration and operation interface
 * @author Alexsey Konstantinov
 */
interface GetlRepository {
    /** Name in Getl Dsl repository */
    @JsonIgnore
    String getDslNameObject()
    /** Name in Getl Dsl repository */
    void setDslNameObject(String value)

    /** Getl creator */
    @JsonIgnore
    Getl getDslCreator()
    /** Getl creator */
    void setDslCreator(Getl value)

    /** Getl registration time */
    @JsonIgnore
    Date getDslRegistrationTime()
    /** Getl registration time */
    void setDslRegistrationTime(Date value)

    /** Clean Dsl properties */
    void dslCleanProps()
}