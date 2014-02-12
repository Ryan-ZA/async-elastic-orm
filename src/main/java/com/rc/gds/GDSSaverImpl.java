package com.rc.gds;

import java.lang.reflect.InvocationTargetException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.UUID;

import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.index.IndexRequestBuilder;
import org.elasticsearch.action.index.IndexResponse;
import org.elasticsearch.action.update.UpdateRequestBuilder;
import org.elasticsearch.action.update.UpdateResponse;

import com.rc.gds.annotation.AlwaysPersist;
import com.rc.gds.interfaces.GDS;
import com.rc.gds.interfaces.GDSCallback;
import com.rc.gds.interfaces.GDSResult;
import com.rc.gds.interfaces.GDSSaver;

public class GDSSaverImpl implements GDSSaver {

	GDS gds;
	Object pojo;
	boolean recursiveUpdate;
	Map<String, GDSField> fieldMap;
	String specialID;
	List<Object> alreadyStoredObjects;
	boolean isUpdate = true;

	protected GDSSaverImpl(GDS gds) {
		this.gds = gds;
		alreadyStoredObjects = Collections.synchronizedList(new ArrayList<Object>());
	}

	protected GDSSaverImpl(GDSSaverImpl saver) {
		gds = saver.gds;
		recursiveUpdate = saver.recursiveUpdate;
		alreadyStoredObjects = saver.alreadyStoredObjects;
	}

	/* (non-Javadoc)
	 * @see com.rc.gds.GDSSaver#entity(java.lang.Object)
	 */
	@Override
	public GDSSaver entity(Object pojo) {
		this.pojo = pojo;
		return this;
	}

	/* (non-Javadoc)
	 * @see com.rc.gds.GDSSaver#withSpecialID(java.lang.String)
	 */
	@Override
	public GDSSaver withSpecialID(String specialID) {
		this.specialID = specialID;
		return this;
	}

	/* (non-Javadoc)
	 * @see com.rc.gds.GDSSaver#forceRecursiveUpdate(boolean)
	 */
	@Override
	public GDSSaver forceRecursiveUpdate(boolean recursiveUpdate) {
		this.recursiveUpdate = recursiveUpdate;
		return this;
	}
	
	private void createEntity(boolean isEmbedded, final GDSCallback<Entity> callback) throws IllegalArgumentException, IllegalAccessException, InvocationTargetException {
		if (fieldMap == null)
			fieldMap = GDSField.createMapFromObject(pojo);
		
		GDSClass.onPreSave(gds, pojo);

		String id = null;
		if (!isEmbedded && specialID == null) {
			GDSField idfield = fieldMap.get(GDSField.GDS_ID_FIELD);
			if (idfield == null)
				throw new RuntimeException("Class " + pojo.getClass().getName() + " does not have an ID field!");

			// Get the ID and create the low level entity
			id = (String) GDSField.getValue(idfield, pojo);
		}

		List<String> classKinds = GDSClass.getKinds(pojo.getClass());
		// classKind is the top most superclass for the pojo. All subclasses
		// will have the same kind to allow for querying across subclasses.
		String classKind = classKinds.get(classKinds.size() - 1);

		final Entity entity;
		if (specialID != null) {
			entity = new Entity(classKind, specialID);
		} else if (id == null) {
			entity = new Entity(classKind);
		} else {
			entity = new Entity(classKind, id);
		}
		
		// If the object is versioned we set it here
		GDSField verfield = fieldMap.get(GDSField.GDS_VERSION_FIELD);
		if (verfield != null) {
			entity.setVersion(verfield.field.getLong(pojo));
		}

		// Add indexed class and superclass information for easy polymorphic
		// querying
		entity.setProperty(GDSClass.GDS_FILTERCLASS_FIELD, classKinds);
		entity.setUnindexedProperty(GDSClass.GDS_CLASS_FIELD, classKinds.get(0));

		final List<GDSField> gdsFields = new ArrayList<>(fieldMap.values());
		GDSField blocker = new GDSField();
		gdsFields.add(blocker);

		// Add the field values
		for (final GDSField field : fieldMap.values()) {
			addFieldToEntity(field, entity, new GDSCallback<Void>() {
				
				@Override
				public void onSuccess(Void t, Throwable err) {
					testSuccess(callback, entity, gdsFields, field, err);
				}
			});
		}
		
		testSuccess(callback, entity, gdsFields, blocker, null);
	}
	
	private void addFieldToEntity(final GDSField field, final PropertyContainer entity, final GDSCallback<Void> callback) throws IllegalArgumentException, IllegalAccessException,
			InvocationTargetException {
		if (field.fieldName.equals(GDSField.GDS_ID_FIELD)) {
			callback.onSuccess(null, null);
			return;
		}

		Object fieldValue = GDSField.getValue(field, pojo);
		boolean hasInnerCallback = false;

		if (fieldValue == null) {
			// Only set nulls for indexed properties as nulls for un-indexed
			// properties do nothing.
			// Null must be set for indexed properties to ensure they appear in
			// the index and can be queried.
			if (field.indexed)
				setEntityProperty(entity, field, null);
			callback.onSuccess(null, null);
			return;
		}

		if (field.isArray) {
			// This will convert the array into a Collection while also boxing
			// all primitives to allow for insertion in a Collection.
			// GAE cannot deal with arrays - only with collections.
			fieldValue = GDSBoxer.boxArray(fieldValue);
		}

		if (field.isEnum) {
			Enum<?> e = (Enum<?>) fieldValue;
			setEntityProperty(entity, field, e.name());
		} else if (!field.nonDatastoreObject) {
			setEntityProperty(entity, field, fieldValue);
		} else if (fieldValue instanceof Collection<?>) {
			hasInnerCallback = true;
			storeCollectionOfPOJO(field, fieldValue, new GDSCallback<Collection<?>>() {
				
				@Override
				public void onSuccess(Collection<?> pojoOrKeyCollection, Throwable err) {
					setEntityProperty(entity, field, pojoOrKeyCollection);
					callback.onSuccess(null, err);
				}
			});
		} else if (fieldValue instanceof Map<?, ?>) {
			hasInnerCallback = true;
			storeMapOfPOJO(field, fieldValue, new GDSCallback<EmbeddedEntity>() {
				
				@Override
				public void onSuccess(EmbeddedEntity mapEntity, Throwable err) {
					setEntityProperty(entity, field, mapEntity);
					callback.onSuccess(null, err);
				}
			});
		} else if (field.embedded) {
			hasInnerCallback = true;
			createEmbeddedEntity(fieldValue, new GDSCallback<EmbeddedEntity>() {
				
				@Override
				public void onSuccess(EmbeddedEntity embeddedEntity, Throwable err) {
					entity.setUnindexedProperty(field.fieldName, embeddedEntity);
					callback.onSuccess(null, err);
				}
			});
		} else {
			hasInnerCallback = true;
			createKeyForRegularPOJO(fieldValue, new GDSCallback<Key>() {
				
				@Override
				public void onSuccess(Key key, Throwable err) {
					setEntityProperty(entity, field, key);
					callback.onSuccess(null, err);
				}
			});
		}
		
		if (!hasInnerCallback)
			callback.onSuccess(null, null);
	}
	
	private void storeCollectionOfPOJO(GDSField field, Object fieldValue, final GDSCallback<Collection<?>> callback) throws IllegalArgumentException, IllegalAccessException,
			InvocationTargetException {
		final Collection<Object> pojosOrKeys = new ArrayList<Object>();
		Collection<?> collection = (Collection<?>) fieldValue;
		final List<GDSCallback<?>> datastoreSaveCallbacks = new ArrayList<>();
		for (Object object : collection) {
			if (object == null) {
				continue;
			} else if (GDSField.nonDSClasses.contains(object.getClass())) {
				pojosOrKeys.add(object);
			} else if (GDSClass.hasIDField(object.getClass())) {
				GDSCallback<Key> inCallback = new GDSCallback<Key>() {
					
					@Override
					public void onSuccess(Key key, Throwable err) {
						testSuccess(this, callback, pojosOrKeys, datastoreSaveCallbacks, key == null ? null : key.toMap(), err);
					}
				};
				datastoreSaveCallbacks.add(inCallback);
				createKeyForRegularPOJO(object, inCallback);
			} else {
				GDSCallback<EmbeddedEntity> inCallback = new GDSCallback<EmbeddedEntity>() {
					
					@Override
					public void onSuccess(EmbeddedEntity embeddedEntity, Throwable err) {
						testSuccess(this, callback, pojosOrKeys, datastoreSaveCallbacks, embeddedEntity.dbObject, err);
					}
				};
				datastoreSaveCallbacks.add(inCallback);
				createEmbeddedEntity(object, inCallback);
			}
		}
		if (datastoreSaveCallbacks.isEmpty())
			callback.onSuccess(pojosOrKeys, null);
	}
	
	private void storeMapOfPOJO(GDSField field, Object fieldValue, final GDSCallback<EmbeddedEntity> callback) throws IllegalArgumentException, IllegalAccessException,
			InvocationTargetException {
		final EmbeddedEntity mapEntity = new EmbeddedEntity();
		Map<?, ?> map = (Map<?, ?>) fieldValue;
		final List<Object> datastoreSaveCallbacks = Collections.synchronizedList(new ArrayList<>());

		int counter = 1;
		for (Entry<?, ?> entry : map.entrySet()) {
			if (entry.getKey() == null || entry.getValue() == null) {
				continue;
			}

			Object input = entry.getKey();
			Class<?> inputClazz = input.getClass();
			if (GDSField.nonDSClasses.contains(inputClazz)) {
				mapEntity.setUnindexedProperty("K" + counter, input);
			} else if (GDSClass.hasIDField(inputClazz)) {
				final int fCounter = counter;
				GDSCallback<Key> inCallback = new GDSCallback<Key>() {
					
					@Override
					public void onSuccess(Key key, Throwable err) {
						testSuccess(this, callback, mapEntity, datastoreSaveCallbacks, "K" + fCounter, key.toMap(), err);
					}
				};
				datastoreSaveCallbacks.add(inCallback);
				createKeyForRegularPOJO(input, inCallback);
			} else {
				final int fCounter = counter;
				GDSCallback<EmbeddedEntity> inCallback = new GDSCallback<EmbeddedEntity>() {
					
					@Override
					public void onSuccess(EmbeddedEntity embeddedEntity, Throwable err) {
						testSuccess(this, callback, mapEntity, datastoreSaveCallbacks, "K" + fCounter, embeddedEntity.getDBDbObject(), err);
					}
				};
				datastoreSaveCallbacks.add(inCallback);
				createEmbeddedEntity(input, inCallback);
			}

			input = entry.getValue();
			inputClazz = input.getClass();
			if (GDSField.nonDSClasses.contains(inputClazz)) {
				mapEntity.setUnindexedProperty("V" + counter, input);
			} else if (GDSClass.hasIDField(inputClazz)) {
				final int fCounter = counter;
				GDSCallback<Key> inCallback = new GDSCallback<Key>() {
					
					@Override
					public void onSuccess(Key key, Throwable err) {
						testSuccess(this, callback, mapEntity, datastoreSaveCallbacks, "V" + fCounter, key == null ? null : key.toMap(), err);
					}
				};
				datastoreSaveCallbacks.add(inCallback);
				createKeyForRegularPOJO(input, inCallback);
			} else {
				final int fCounter = counter;
				GDSCallback<EmbeddedEntity> inCallback = new GDSCallback<EmbeddedEntity>() {
					
					@Override
					public void onSuccess(EmbeddedEntity embeddedEntity, Throwable err) {
						testSuccess(this, callback, mapEntity, datastoreSaveCallbacks, "V" + fCounter, embeddedEntity.getDBDbObject(), err);
					}
				};
				datastoreSaveCallbacks.add(inCallback);
				createEmbeddedEntity(input, inCallback);
			}

			counter++;
		}
		if (datastoreSaveCallbacks.isEmpty())
			callback.onSuccess(mapEntity, null);
	}

	private void createKeyForRegularPOJO(final Object fieldValue, final GDSCallback<Key> callback) throws IllegalArgumentException, IllegalAccessException {
		final String fieldValueKind = GDSClass.getKind(fieldValue);
		Map<String, GDSField> map = GDSField.createMapFromObject(fieldValue);
		final GDSField idfield = map.get(GDSField.GDS_ID_FIELD);
		Object idFieldValue = GDSField.getValue(idfield, fieldValue);
		String id = idfield == null ? null : (String) idFieldValue;
		if (recursiveUpdate || id == null || fieldValue.getClass().isAnnotationPresent(AlwaysPersist.class)) {
			if (alreadyStoredObjects.contains(fieldValue)) {
				// We've already saved this object from this call, no need to save it again
				callback.onSuccess(new Key(fieldValueKind, id), null);
			} else {
				alreadyStoredObjects.add(fieldValue);
				GDSSaverImpl saver = new GDSSaverImpl(this);
				saver.fieldMap = map;
				saver.pojo = fieldValue;
				if (id == null) {
					generateIDInPlace(fieldValue, idfield);
					saver.isUpdate = false;
				}
				saver.later(new GDSCallback<Key>() {
					
					@Override
					public void onSuccess(Key key, Throwable err) {
						try {
							String id = (String) GDSField.getValue(idfield, fieldValue);
							callback.onSuccess(new Key(fieldValueKind, id), err);
						} catch (Throwable e) {
							callback.onSuccess(null, e);
						}
					}
				});
			}
		} else {
			callback.onSuccess(new Key(fieldValueKind, id), null);
		}
	}

	private void generateIDInPlace(Object fieldValue, GDSField idfield) throws IllegalArgumentException, IllegalAccessException {
		String newid = UUID.randomUUID().toString();
		idfield.field.set(fieldValue, newid);
	}

	private void setEntityProperty(PropertyContainer entity, GDSField field, Object value) {
		if (field.indexed) {
			entity.setProperty(field.fieldName, value);
		} else {
			entity.setUnindexedProperty(field.fieldName, value);
		}
	}
	
	/* (non-Javadoc)
	 * @see com.rc.gds.GDSSaver#now()
	 */
	@Override
	public Key now() {
		final List<Key> lock = new LinkedList<>();
		final List<Throwable> errList = new LinkedList<>();
		
		later(new GDSCallback<Key>() {
			
			@Override
			public void onSuccess(Key key, Throwable err) {
				synchronized (lock) {
					if (err != null)
						errList.add(err);
					lock.add(key);
					lock.notifyAll();
				}
			}
		});
		
		synchronized (lock) {
			try {
				if (!errList.isEmpty())
					throwRuntime(errList.get(0));
				if (!lock.isEmpty())
					return lock.get(0);
				lock.wait();
				if (!errList.isEmpty())
					throwRuntime(errList.get(0));
				return lock.get(0);
			} catch (InterruptedException e) {
				throwRuntime(e);
			}
		}
		// Never called
		return null;
	}

	private void throwRuntime(Throwable throwable) {
		if (throwable instanceof RuntimeException) {
			throw (RuntimeException) throwable;
		} else {
			throw new RuntimeException("Error saving object " + pojo, throwable);
		}
	}

	/* (non-Javadoc)
	 * @see com.rc.gds.GDSSaver#later(com.rc.gds.interfaces.GDSCallback)
	 */
	@Override
	public void later(final GDSCallback<Key> callback) {
		try {
			createEntity(false, new GDSCallback<Entity>() {
				
				@Override
				public void onSuccess(Entity entity, Throwable err) {
					if (err != null)
						callback.onSuccess(null, err);
					saveToDatastore(entity, new GDSCallback<Key>() {
						
						@Override
						public void onSuccess(Key key, Throwable err) {
							try {
								if (err != null)
									throw err;
								GDSField idfield = fieldMap.get(GDSField.GDS_ID_FIELD);
								idfield.field.set(pojo, key.getId());
								GDSField verField = fieldMap.get(GDSField.GDS_VERSION_FIELD);
								if (verField != null)
									verField.field.setLong(pojo, key.version);
								GDSClass.onPostSave(gds, pojo);
								callback.onSuccess(key, err);
							} catch (Throwable e) {
								callback.onSuccess(null, e);
							}
						}
					});
				}
			});
		} catch (Throwable e) {
			throw new RuntimeException("Error saving object " + pojo, e);
		}
	}
	
	/* (non-Javadoc)
	 * @see com.rc.gds.GDSSaver#result()
	 */
	@Override
	public GDSResult<Key> result() {
		GDSAsyncImpl<Key> result = new GDSAsyncImpl<>();
		later(result);
		return result;
	}

	private void saveToDatastore(final Entity entity, final GDSCallback<Key> callback) {
		try {
			// Save to datastore
			if (entity.id != null && isUpdate) {
				UpdateRequestBuilder builder = gds.getClient().prepareUpdate(gds.indexFor(entity.getKind()), entity.getKind(), entity.id);
				builder.setDoc(entity.getDBDbObject());
				Long ver = entity.getVersion();
				if (ver != null)
					builder.setVersion(ver);

				builder.execute(new ActionListener<UpdateResponse>() {
					
					@Override
					public void onResponse(UpdateResponse response) {
						Key key = new Key(entity.getKind(), response.getId(), response.getVersion());
						callback.onSuccess(key, null);
					}
					
					@Override
					public void onFailure(Throwable e) {
						callback.onSuccess(null, e);
					}
				});
			} else {
				IndexRequestBuilder builder = gds.getClient().prepareIndex(gds.indexFor(entity.getKind()), entity.getKind());
				builder.setSource(entity.getDBDbObject());
				if (entity.id != null)
					builder.setId(entity.id);
				builder.execute().addListener(new ActionListener<IndexResponse>() {
					
					@Override
					public void onResponse(IndexResponse indexResponse) {
						Key key = new Key(entity.getKind(), indexResponse.getId(), indexResponse.getVersion());
						callback.onSuccess(key, null);
					}
					
					@Override
					public void onFailure(Throwable e) {
						callback.onSuccess(null, e);
					}
				});
			}
		} catch (Throwable e) {
			callback.onSuccess(null, e);
		}
	}
	
	private void createEmbeddedEntity(Object object, final GDSCallback<EmbeddedEntity> callback) throws IllegalArgumentException, IllegalAccessException, InvocationTargetException {
		GDSSaverImpl saver = new GDSSaverImpl(this);
		saver.pojo = object;
		saver.createEntity(true, new GDSCallback<Entity>() {

			@Override
			public void onSuccess(Entity entity, Throwable err) {
				EmbeddedEntity embeddedEntity = new EmbeddedEntity();
				embeddedEntity.setPropertiesFrom(entity);
				callback.onSuccess(embeddedEntity, err);
			}
		});
	}

	private synchronized void testSuccess(GDSCallback<?> inCallback, GDSCallback<EmbeddedEntity> callback, EmbeddedEntity mapEntity, List<Object> datastoreSaveCallbacks,
			String key, Map<String, Object> val, Throwable err) {
		mapEntity.setUnindexedProperty(key, val);
		datastoreSaveCallbacks.remove(inCallback);
		if (datastoreSaveCallbacks.isEmpty())
			callback.onSuccess(mapEntity, err);
	}

	private synchronized void testSuccess(GDSCallback<?> inCallback, GDSCallback<Collection<?>> callback, Collection<Object> pojosOrKeys,
			List<GDSCallback<?>> datastoreSaveCallbacks, Map<String, Object> map, Throwable err) {
		if (map != null)
			pojosOrKeys.add(map);
		datastoreSaveCallbacks.remove(inCallback);
		if (datastoreSaveCallbacks.isEmpty())
			callback.onSuccess(pojosOrKeys, err);
	}

	private synchronized void testSuccess(GDSCallback<Entity> callback, Entity entity, List<GDSField> gdsFields, GDSField field, Throwable err) {
		gdsFields.remove(field);
		if (gdsFields.isEmpty())
			callback.onSuccess(entity, err);
	}

}
