package com.rc.gds;

import java.lang.reflect.AccessibleObject;
import java.lang.reflect.Field;
import java.lang.reflect.Modifier;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Date;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

import com.rc.gds.annotation.Embed;
import com.rc.gds.annotation.ID;
import com.rc.gds.annotation.Index;
import com.rc.gds.annotation.Version;

public class GDSField {

	public static final String GDS_ID_FIELD = "__GDS_ID_FIELD";
	public static final String GDS_VERSION_FIELD = "__GDS_VERSION_FIELD";

	/**
	 * https://developers.google.com/appengine/docs/java/datastore/entities?hl=
	 * iw#Properties_and_Value_Types
	 */
	static final Set<Class<?>> nonDSClasses = new HashSet<Class<?>>(Arrays.asList(
			new Class<?>[] {
					Object.class,
					String.class,
					Double.class,
					Integer.class,
					Character.class,
					Byte.class,
					Boolean.class,
					Short.class,
					Float.class,
					Long.class,
					Date.class,
			}
			));

	static final Map<Class<?>, Map<String, GDSField>> reflectionCache = new ConcurrentHashMap<Class<?>, Map<String, GDSField>>();

	/**
	 * Will return a new GDSField if field is an ID field (has @ID set or is
	 * named 'id'). If field is a Long or Integer, it may be null and will be
	 * generated. Otherwise, field cannot be null and will throw a null pointer
	 * exception. If field is not a Long or Integer, toString() will be called
	 * and this will be used as the key.
	 * 
	 * @param field
	 * @return
	 */
	static GDSField createIDField(Field field) {
		ID annotation = field.getAnnotation(ID.class);
		if (annotation == null && !field.getName().equals("id"))
			return null;

		GDSField gdsField = new GDSField();

		gdsField.field = field;
		gdsField.fieldName = GDS_ID_FIELD;

		Class<?> fieldType = field.getType();
		if (String.class == fieldType) {
			return gdsField;
		} else {
			throw new RuntimeException("Class " + field.getDeclaringClass().getName() + " has @ID on non-String field!");
		}
	}
	
	static GDSField createVersionField(Field field) {
		Version annotation = field.getAnnotation(Version.class);
		if (annotation == null)
			return null;
		
		GDSField gdsField = new GDSField();
		
		gdsField.field = field;
		gdsField.fieldName = GDS_VERSION_FIELD;
		
		Class<?> fieldType = field.getType();
		if (long.class == fieldType) {
			return gdsField;
		} else {
			throw new RuntimeException("Class " + field.getDeclaringClass().getName() + " has @Version on non-Long field!");
		}
	}

	/**
	 * 
	 * @param obj
	 * @param field
	 * @return
	 * @throws IllegalAccessException
	 * @throws IllegalArgumentException
	 */
	static GDSField createGDSField(Field field) throws IllegalArgumentException, IllegalAccessException {
		GDSField gdsField = new GDSField();
		gdsField.embedded = field.getAnnotation(Embed.class) != null;
		gdsField.indexed = field.getAnnotation(Index.class) != null;

		gdsField.field = field;
		gdsField.fieldName = field.getName();
		gdsField.nonDatastoreObject = false;

		Class<?> fieldType = field.getType();

		if (fieldType.isArray()) {
			gdsField.isArray = true;
			fieldType = fieldType.getComponentType();
		}
		if (!gdsField.embedded) {
			boolean hasIDField = GDSClass.hasIDField(fieldType);
			gdsField.embedded = !hasIDField;
		}

		if (fieldType.isPrimitive()) {
			gdsField.nonDatastoreObject = false;
		} else if (fieldType.isEnum()) {
			gdsField.isEnum = true;
			gdsField.nonDatastoreObject = false;
		} else if (nonDSClasses.contains(fieldType)) {
			gdsField.nonDatastoreObject = false;
		} else {
			gdsField.nonDatastoreObject = true;
		}

		return gdsField;
	}

	/**
	 * Creates a map of fieldnames to GDSFields. Used to store/load entities
	 * from the datastore.
	 * 
	 * @param obj
	 *            Any POJO object
	 * @return
	 * @throws IllegalArgumentException
	 * @throws IllegalAccessException
	 */
	public static Map<String, GDSField> createMapFromObject(Object obj) throws IllegalArgumentException, IllegalAccessException {
		Class<?> clazz = obj.getClass();
		if (reflectionCache.containsKey(clazz)) {
			return reflectionCache.get(clazz);
		}
		final Class<?> originalClazz = clazz;

		Map<String, GDSField> map = new HashMap<String, GDSField>();

		if (nonDSClasses.contains(clazz))
			throw new RuntimeException("Trying to get fields from a native datastore class!");

		ArrayList<Field> fields = new ArrayList<Field>();
		while (clazz != Object.class && clazz != null) {
			Field[] classfields = clazz.getDeclaredFields();
			try {
				AccessibleObject.setAccessible(classfields, true);
			} catch (Exception ex) {
				//System.out.println("Error trying to setAccessible for object: " + obj + " " + ex.toString());
			}

			for (Field field : classfields) {
				if (Modifier.isStatic(field.getModifiers()))
					continue;
				fields.add(field);
			}

			clazz = clazz.getSuperclass();
		}

		for (Field field : fields) {
			GDSField gdsField = createIDField(field);
			if (gdsField == null)
				gdsField = createVersionField(field);
			if (gdsField == null)
				gdsField = createGDSField(field);

			map.put(gdsField.fieldName, gdsField);
		}

		reflectionCache.put(originalClazz, map);

		return map;
	}

	public static String getID(Object pojo) throws IllegalArgumentException, IllegalAccessException {
		Map<String, GDSField> map = createMapFromObject(pojo);
		GDSField idfield = map.get(GDS_ID_FIELD);

		if (idfield == null)
			throw new RuntimeException("Class " + pojo.getClass().getName() + " does not have an ID field!");

		return (String) GDSField.getValue(idfield, pojo);
	}

	public static Object getValue(GDSField gdsField, Object pojo) throws IllegalArgumentException, IllegalAccessException {
		Class<?> fieldType = gdsField.field.getType();
		Object val = gdsField.field.get(pojo);

		if (val == null)
			return null;

		if (fieldType == Character.class || fieldType == char.class)
			val = val.toString();

		return val;
	}

	Field field;
	String fieldName;
	boolean nonDatastoreObject;
	boolean embedded;
	boolean indexed;
	boolean isArray;
	boolean isEnum;

}
