package com.rc.gds;

import java.lang.annotation.Annotation;
import java.lang.reflect.AccessibleObject;
import java.lang.reflect.Constructor;
import java.lang.reflect.Field;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

import com.rc.gds.annotation.PostSave;
import com.rc.gds.annotation.PreDelete;
import com.rc.gds.annotation.PreSave;
import com.rc.gds.interfaces.GDS;

public class GDSClass {

	/**
	 * Includes a list of all superclasses of the class, and the class itself. Used for filtering.
	 */
	public static final String GDS_FILTERCLASS_FIELD = "__GDS_FILTERCLASS_FIELD";
	/**
	 * Java class name for the object. Used to reconstruct the java pojo.
	 */
	public static final String GDS_CLASS_FIELD = "__GDS_CLASS_FIELD";

	/**
	 * If this field exists in an Entity, then the field is a Map. Maps are stored as K0=Key1,V0=Value1,K1=Key2,V1=Value2,...
	 */
	public static final String GDS_MAP_FIELD = "__GDS_MAP_FIELD";

	static final Map<Class<?>, Boolean> hasIdFieldMap = new ConcurrentHashMap<Class<?>, Boolean>();
	static final Map<Class<?>, Method> hasPreSaveMap = new ConcurrentHashMap<Class<?>, Method>();
	static final Map<Class<?>, Method> hasPostSaveMap = new ConcurrentHashMap<Class<?>, Method>();
	static final Map<Class<?>, Method> hasPreDeleteMap = new ConcurrentHashMap<Class<?>, Method>();
	
	static final Method nullMethod = GDSClass.class.getDeclaredMethods()[0];

	public static List<String> getKinds(Class<?> clazz) {
		ArrayList<String> list = new ArrayList<String>();
		while (clazz != null && clazz != Object.class) {
			list.add(fixName(clazz.getName()));
			clazz = clazz.getSuperclass();
		}
		return list;
	}
	
	public static String getKind(Class<?> clazz) {
		return GDSClass.fixName(GDSClass.getBaseClass(clazz).getName());
	}
	
	public static String getKind(Object o) {
		return GDSClass.fixName(GDSClass.getBaseClass(o.getClass()).getName());
	}

	public static String fixName(String classname) {
		return classname.replace("_", "##").replace(".", "_");
	}

	public static Class<?> getBaseClass(Class<?> clazz) {
		ArrayList<Class<?>> list = new ArrayList<Class<?>>();
		while (clazz != null && clazz != Object.class) {
			list.add(clazz);
			clazz = clazz.getSuperclass();
		}
		return list.get(list.size() - 1);
	}

	/**
	 * Checks if a class has a usable ID field.
	 * 
	 * @param clazz
	 * @return
	 */
	public static boolean hasIDField(Class<?> clazz) {
		if (hasIdFieldMap.containsKey(clazz)) {
			return hasIdFieldMap.get(clazz);
		}

		final Class<?> originalClazz = clazz;

		while (clazz != Object.class && clazz != null && !GDSField.nonDSClasses.contains(clazz) && !clazz.isPrimitive() && clazz != Class.class) {
			Field[] classfields = clazz.getDeclaredFields();
			try {
				AccessibleObject.setAccessible(classfields, true);
			} catch (Exception ex) {
				//System.out.println("Error trying to setAccessible for class: " + clazz + " " + ex.toString());
			}

			for (Field field : classfields) {
				if (GDSField.createIDField(field) != null) {
					hasIdFieldMap.put(clazz, true);
					return true;
				}
			}

			clazz = clazz.getSuperclass();
		}
		hasIdFieldMap.put(originalClazz, false);
		return false;
	}

	public static void makeConstructorsPublic(Class<?> clazz) {
		Constructor<?>[] cons = clazz.getDeclaredConstructors();
		try {
			AccessibleObject.setAccessible(cons, true);
		} catch (Exception ex) {
			//System.out.println("Error trying to makeConstructorsPublic for class: " + clazz + " " + ex.toString());
		}
	}
	
	private static void callAnnotatedMethod(GDS gds, Class<? extends Annotation> annotation, Map<Class<?>, Method> annotationMap, Object pojo) throws IllegalAccessException,
			IllegalArgumentException, InvocationTargetException {
		Method callMethod = annotationMap.get(pojo.getClass());
		
		if (callMethod == null) {
			for (Method method : pojo.getClass().getMethods()) {
				if (method.getAnnotation(annotation) != null) {
					callMethod = method;
					callMethod.setAccessible(true);
					break;
				}
			}
			
			if (callMethod == null)
				callMethod = nullMethod;
			
			annotationMap.put(pojo.getClass(), callMethod);
		}
		
		if (callMethod == nullMethod)
			return;
		
		if (callMethod.getParameterTypes().length == 0)
			callMethod.invoke(pojo);
		else
			callMethod.invoke(pojo, gds);
	}
	
	public static void onPreSave(GDS gds, Object pojo) throws IllegalAccessException, IllegalArgumentException, InvocationTargetException {
		callAnnotatedMethod(gds, PreSave.class, hasPreSaveMap, pojo);
	}
	
	public static void onPostSave(GDS gds, Object pojo) throws IllegalAccessException, IllegalArgumentException, InvocationTargetException {
		callAnnotatedMethod(gds, PostSave.class, hasPostSaveMap, pojo);
	}
	
	public static void onPreDelete(GDS gds, Object pojo) throws IllegalAccessException, IllegalArgumentException, InvocationTargetException {
		callAnnotatedMethod(gds, PreDelete.class, hasPreDeleteMap, pojo);
	}

}
