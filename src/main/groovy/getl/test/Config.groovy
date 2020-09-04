package getl.test

import java.lang.annotation.ElementType
import java.lang.annotation.Retention
import java.lang.annotation.RetentionPolicy
import java.lang.annotation.Target

/**
 * Annotation of defining a configuration environment for running a test
 * @author Alexsey Konstantinov
 */
@Retention(RetentionPolicy.RUNTIME)
@Target(ElementType.METHOD)
@interface Config {
    /** Use specified configuration environment for test */
    String env()
}