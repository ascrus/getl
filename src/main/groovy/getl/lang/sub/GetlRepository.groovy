package getl.lang.sub

import com.fasterxml.jackson.annotation.JsonIgnore
import getl.lang.Getl

/**
 * Getl repository registration and operation interface
 * @author Alexsey Konstantinov
 */
interface GetlRepository {
    /** Name in Getl Dsl reposotory */
    @JsonIgnore
    String getDslNameObject()
    /** Name in Getl Dsl reposotory */
    void setDslNameObject(String value)

    /** Getl creator */
    @JsonIgnore
    Getl getDslCreator()
    /** Getl creator */
    void setDslCreator(Getl value)

    /** Clean Dsl properties */
    void dslCleanProps()
}