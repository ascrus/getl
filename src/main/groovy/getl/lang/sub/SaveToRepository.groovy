package getl.lang.sub

import java.lang.annotation.ElementType
import java.lang.annotation.Retention
import java.lang.annotation.RetentionPolicy
import java.lang.annotation.Target

@Retention(RetentionPolicy.RUNTIME)
@Target(ElementType.METHOD)
@interface SaveToRepository {
    /** Use specified configuration environments (use comma separator) */
    String env() default 'dev'

    /** Reread object structure before saving */
    boolean retrieve() default false

    /** Type repositories */
    String type()

    /** Name mask to save */
    String mask() default ''

    /** Save other types  (use comma separator) */
    String otherTypes() default ''
}