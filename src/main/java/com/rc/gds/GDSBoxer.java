package com.rc.gds;

import java.lang.reflect.Modifier;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.TreeMap;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentSkipListMap;

class GDSBoxer {

	public static Collection<Object> boxArray(Object array) {
		Collection<Object> collection = new ArrayList<Object>();
		Class<?> compClass = array.getClass().getComponentType();
		if (compClass == double.class) {
			double[] arr = (double[]) array;
			for (double i : arr) {
				collection.add(i);
			}
		} else if (compClass == int.class) {
			int[] arr = (int[]) array;
			for (int i : arr) {
				collection.add(i);
			}
		} else if (compClass == char.class) {
			char[] arr = (char[]) array;
			for (char i : arr) {
				collection.add(i);
			}
		} else if (compClass == byte.class) {
			byte[] arr = (byte[]) array;
			for (byte i : arr) {
				collection.add(i);
			}
		} else if (compClass == boolean.class) {
			boolean[] arr = (boolean[]) array;
			for (boolean i : arr) {
				collection.add(i);
			}
		} else if (compClass == short.class) {
			short[] arr = (short[]) array;
			for (short i : arr) {
				collection.add(i);
			}
		} else if (compClass == float.class) {
			float[] arr = (float[]) array;
			for (float i : arr) {
				collection.add(i);
			}
		} else if (compClass == long.class) {
			long[] arr = (long[]) array;
			for (long i : arr) {
				collection.add(i);
			}
		} else {
			Object[] arr = (Object[]) array;
			for (Object o : arr) {
				collection.add(o);
			}
		}
		return collection;
	}

	@SuppressWarnings("rawtypes")
	public static Object createBestFitCollection(Class<?> clazz) throws InstantiationException, IllegalAccessException {
		if (Modifier.isAbstract(clazz.getModifiers()) || clazz.isInterface()) {
			if (clazz.isAssignableFrom(ArrayList.class)) {
				return new ArrayList();
			} else if (clazz.isAssignableFrom(HashSet.class)) {
				return new HashSet();
			} else {
				throw new RuntimeException("Could not create a concrete collection for interface " + clazz.getName());
			}
		} else {
			return clazz.newInstance();
		}
	}
	
	@SuppressWarnings("rawtypes")
	public static Object createBestFitMap(Class<?> clazz) throws InstantiationException, IllegalAccessException {
		if (Modifier.isAbstract(clazz.getModifiers()) || clazz.isInterface()) {
			if (clazz.isAssignableFrom(HashMap.class)) {
				return new HashMap();
			} else if (clazz.isAssignableFrom(TreeMap.class)) {
				return new TreeMap();
			} else if (clazz.isAssignableFrom(ConcurrentHashMap.class)) {
				return new ConcurrentHashMap();
			} else if (clazz.isAssignableFrom(ConcurrentSkipListMap.class)) {
				return new ConcurrentSkipListMap();
			} else {
				throw new RuntimeException("Could not create a concrete map for interface " + clazz.getName());
			}
		} else {			
			return clazz.newInstance();
		}
	}

}
