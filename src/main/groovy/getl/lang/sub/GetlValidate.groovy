package getl.lang.sub

import getl.exception.ExceptionDSL

/**
 * Class of checks for work with Getl objects
 * @author Alexsey Konstantinov
 */
class GetlValidate {
    static void IsRegister(GetlRepository object) {
        if (object.dslCreator == null || object.dslNameObject == null)
            throw new ExceptionDSL('The object is not registered in the repository!')
    }
}