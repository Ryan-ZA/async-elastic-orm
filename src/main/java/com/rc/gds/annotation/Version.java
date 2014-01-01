package com.rc.gds.annotation;

import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;

/**
 * Can be placed on an int or long field to specify that the version of the object must be stored inside there. This is optional - an object
 * without a version field will be unversioned and will always be updated. A versioned object will only update if it has not been updated in
 * some other way between fetching it and updating it. See http://www.elasticsearch.org/blog/versioning/ for more.
 * 
 */
@Retention(RetentionPolicy.RUNTIME)
@Target(ElementType.FIELD)
public @interface Version {
	
}
