package com.rc.gds.annotation;

import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;

/**
 * 
 * Place on a field in a POJO to indicate that it is the ID field. Alternatively, if this annotation is missing and a field named 'id'
 * exists, it will be used instead. The field must be a String.
 * 
 */
@Retention(RetentionPolicy.RUNTIME)
@Target(ElementType.FIELD)
public @interface ID {
}
