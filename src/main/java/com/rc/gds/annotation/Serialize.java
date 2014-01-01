/**
 * 
 */
package com.rc.gds.annotation;

import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;

/**
 * If placed on a field, the object will not be stored as a unique object in the
 * datastore, but will be serialized to a Blob and stored within the enclosing
 * object. Cannot be filtered.
 * 
 */
@Retention(RetentionPolicy.RUNTIME)
@Target(ElementType.FIELD)
public @interface Serialize {
}
