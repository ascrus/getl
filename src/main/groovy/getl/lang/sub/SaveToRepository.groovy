package getl.lang.sub

import java.lang.annotation.ElementType
import java.lang.annotation.Retention
import java.lang.annotation.RetentionPolicy
import java.lang.annotation.Target

@Retention(RetentionPolicy.RUNTIME)
@Target(ElementType.METHOD)
@interface SaveToRepository {
    /** Use specified configuration environment */
    String env() default 'dev'

    /** Reread object state before saving */
    boolean retrieve() default false

    /** Type repositories */
    String type()
}