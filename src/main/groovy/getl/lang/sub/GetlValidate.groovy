package getl.lang.sub

import getl.exception.DslError

/**
 * Class of checks for work with Getl objects
 * @author Alexsey Konstantinov
 */
class GetlValidate {
    static void IsRegister(GetlRepository object, Boolean checkDslName = true) {
        if (object.dslCreator == null || (checkDslName && object.dslNameObject == null))
            throw new DslError(object, '#dsl.object.not_register')
    }
}