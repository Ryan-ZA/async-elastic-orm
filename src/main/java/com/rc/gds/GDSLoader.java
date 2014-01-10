package com.rc.gds;

import java.lang.reflect.Array;
import java.lang.reflect.Field;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.concurrent.ExecutionException;

import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.get.GetResponse;
import org.elasticsearch.action.get.MultiGetItemResponse;
import org.elasticsearch.action.get.MultiGetRequestBuilder;
import org.elasticsearch.action.get.MultiGetResponse;

import com.rc.gds.interfaces.GDSCallback;
import com.rc.gds.interfaces.GDSResult;

public class GDSLoader {

	GDS gds;
	private Map<Key, Object> localCache = Collections.synchronizedMap(new HashMap<Key, Object>());
	private List<Key> alreadyFetching = new ArrayList<>();

	protected GDSLoader(GDS gds) {
		this.gds = gds;
	}

	/**
	 * This will fetch a pojo of type clazz with specific id. This method is prefered over fetch(Key)
	 * 
	 * @param clazz
	 *            Class of the object to fetch. Should be the class itself if possible, but superclass or subclass of the object will work
	 *            too.
	 * @param id
	 *            Long id returned from a previous saved pojo
	 * @return
	 */
	@SuppressWarnings("unchecked")
	public <T> GDSResult<T> fetch(final Class<T> clazz, final String id) {
		try {
			final GDSAsyncImpl<T> callback = new GDSAsyncImpl<>();

			final String kind = GDSClass.fixName(GDSClass.getBaseClass(clazz).getName());
			final Key key = new Key(kind, id);

			if (localCache.containsKey(key)) {
				callback.onSuccess((T) localCache.get(key), null);
				return callback;
			}

			gds.client.prepareGet(gds.indexFor(key.kind), key.kind, key.id)
					.execute(new ActionListener<GetResponse>() {
						
						@Override
						public void onResponse(GetResponse response) {
							Entity entity = new Entity(kind, response.getId(), response.getSourceAsMap());

							final List<GDSLink> linksToFetch = new ArrayList<GDSLink>();
							try {
								entityToPOJO(entity, entity.getKey().getId(), linksToFetch, new GDSCallback<Object>() {
									
									@Override
									public void onSuccess(final Object pojo, Throwable err) {
										try {
											if (err != null)
												throw err;
											localCache.put(key, pojo);
											fetchLinks(linksToFetch, new GDSCallback<List<GDSLink>>() {

												@Override
												public void onSuccess(List<GDSLink> t, Throwable err) {
													callback.onSuccess((T) pojo, err);
												}
											});
										} catch (Throwable e) {
											e.printStackTrace();
											callback.onSuccess(null, e);
										}
									}
								});
							} catch (Throwable e) {
								e.printStackTrace();
								callback.onSuccess(null, e);
							}
						}
						
						@Override
						public void onFailure(Throwable e) {
							e.printStackTrace();
							callback.onSuccess(null, e);
						}
					});

			return callback;
		} catch (RuntimeException ex) {
			throw ex;
		} catch (Exception ex) {
			throw new RuntimeException(ex);
		}
	}

	/**
	 * Will fetch the pojo matching the key.
	 * 
	 * @param key
	 * @return
	 */
	public void fetch(final Key key, final GDSCallback<Object> callback) {
		
		gds.client.prepareGet(gds.indexFor(key.kind), key.kind, key.id)
				.execute(new ActionListener<GetResponse>() {
					
					@Override
					public void onResponse(GetResponse getResponse) {
						Entity entity = new Entity(key.kind, getResponse.getId(), getResponse.getSourceAsMap());
						final List<GDSLink> linksToFetch = new ArrayList<GDSLink>();
						try {
							entityToPOJO(entity, entity.getKey().getId(), linksToFetch, new GDSCallback<Object>() {
								
								@Override
								public void onSuccess(final Object pojo, Throwable err) {
									try {
										if (err != null)
											throw err;
										localCache.put(key, pojo);
										fetchLinks(linksToFetch, new GDSCallback<List<GDSLink>>() {
											
											@Override
											public void onSuccess(List<GDSLink> t, Throwable err) {
												callback.onSuccess(pojo, err);
											}
										});
									} catch (Throwable e) {
										e.printStackTrace();
										callback.onSuccess(null, e);
									}
								}
							});
						} catch (Throwable e) {
							e.printStackTrace();
							callback.onSuccess(null, e);
						}
					}
					
					@Override
					public void onFailure(Throwable e) {
						e.printStackTrace();
						callback.onSuccess(null, e);
					}
				});
	}

	/**
	 * Will fetch all pojos for keys.
	 * 
	 * @param keys
	 * @return
	 * @throws ExecutionException
	 * @throws InterruptedException
	 * @throws IllegalAccessException
	 * @throws InstantiationException
	 * @throws ClassNotFoundException
	 */
	public void fetch(final Iterable<Key> keys, final GDSCallback<Map<Key, Object>> callback) throws InterruptedException, ExecutionException, ClassNotFoundException,
			InstantiationException, IllegalAccessException {
		final Map<Key, Object> fetched = Collections.synchronizedMap(new HashMap<Key, Object>());
		final Set<Key> stillToFetch = Collections.synchronizedSet(new HashSet<Key>());
		
		for (Key key : keys) {
			Object pojo = localCache.get(key);
			if (pojo == null)
				stillToFetch.add(key);
			else
				fetched.put(key, pojo);
		}
		
		if (stillToFetch.isEmpty()) {
			callback.onSuccess(fetched, null);
			return;
		}
		
		final List<GDSLink> linksToFetch = new ArrayList<GDSLink>();
		
		bulkFetchList(stillToFetch, new GDSCallback<Map<Key, Entity>>() {
			
			@Override
			public void onSuccess(final Map<Key, Entity> dsMap, Throwable err) {
				try {
					if (err != null)
						throw err;
					for (final Entry<Key, Entity> entry : dsMap.entrySet()) {
						Entity entity = entry.getValue();
						final Key key = entry.getKey();

						entityToPOJO(entity, key.getId(), linksToFetch, new GDSCallback<Object>() {
							
							@Override
							public void onSuccess(Object pojo, Throwable err) {
								try {
									if (err != null)
										throw err;
									fetched.put(key, pojo);
									dsMap.remove(key);
									
									if (alreadyFetching.contains(key)) {
										callback.onSuccess(fetched, err);
										return;
									} else {
										alreadyFetching.add(key);
									}
									
									if (dsMap.isEmpty()) {
										fetchLinks(linksToFetch, new GDSCallback<List<GDSLink>>() {
											
											@Override
											public void onSuccess(List<GDSLink> t, Throwable err) {
												localCache.putAll(fetched);
												callback.onSuccess(fetched, err);
											}
										});
									}
								} catch (Throwable e) {
									e.printStackTrace();
									callback.onSuccess(null, e);
								}
							}
						});

					}
					
				} catch (Throwable e) {
					e.printStackTrace();
					callback.onSuccess(null, e);
				}
			}

		});
	}

	private void bulkFetchList(Set<Key> stillToFetch, final GDSCallback<Map<Key, Entity>> callback) {
		MultiGetRequestBuilder requestBuilder = gds.client.prepareMultiGet();

		for (Key key : stillToFetch) {
			requestBuilder.add(gds.indexFor(key.kind), key.kind, key.id);
		}
		
		requestBuilder.execute(new ActionListener<MultiGetResponse>() {
			
			@Override
			public void onResponse(MultiGetResponse response) {
				Map<Key, Entity> resultMap = Collections.synchronizedMap(new HashMap<Key, Entity>());
				for (MultiGetItemResponse itemResponse : response.getResponses()) {
					Entity entity = new Entity(itemResponse.getType(), itemResponse.getResponse().getId(), itemResponse.getResponse().getSourceAsMap());
					resultMap.put(new Key(itemResponse.getType(), itemResponse.getId()), entity);
				}
				callback.onSuccess(resultMap, null);
			}
			
			@Override
			public void onFailure(Throwable e) {
				e.printStackTrace();
				callback.onSuccess(null, e);
			}
		});
	}

	public void fetchLinks(final List<GDSLink> linksToFetch, final GDSCallback<List<GDSLink>> callback) throws IllegalArgumentException, IllegalAccessException,
			ClassNotFoundException,
			InstantiationException, InterruptedException, ExecutionException {
		Set<Key> stillToFetch = Collections.synchronizedSet(new HashSet<Key>());

		for (GDSLink link : linksToFetch) {
			if (link.keyCollection != null)
				stillToFetch.addAll(link.keyCollection);
			if (link.key != null)
				stillToFetch.add(link.key);
		}

		if (stillToFetch.isEmpty()) {
			callback.onSuccess(linksToFetch, null);
			return;
		}

		fetch(stillToFetch, new GDSCallback<Map<Key, Object>>() {
			
			@Override
			public void onSuccess(Map<Key, Object> fetched, Throwable err) {
				try {
					for (GDSLink link : linksToFetch) {
						if (link.key != null) {
							Object newPojo = fetched.get(link.key);
							if (newPojo == null)
								continue;
							
							if (link.collection != null) {
								link.collection.add(newPojo);
							} else if (link.gdsField != null && link.pojo != null) {
								link.gdsField.field.set(link.pojo, newPojo);
							} else {
								throw new RuntimeException("Undefined behaviour");
							}
						}
						if (link.keyCollection != null) {
							if (link.collection != null) {
								for (Key key : link.keyCollection) {
									link.collection.add(fetched.get(key));
								}
							} else if (link.gdsField != null && link.gdsField.isArray) {
								Class<?> fieldType = link.gdsField.field.getType().getComponentType();
								Object newArray = Array.newInstance(fieldType, link.keyCollection.size());
								int counter = 0;
								for (Key key : link.keyCollection) {
									Object childPOJO = fetched.get(key);
									setArrayIndex(counter, fieldType, newArray, childPOJO);
									counter++;
								}
							} else {
								throw new RuntimeException("Undefined behaviour");
							}
						}
					}
					callback.onSuccess(linksToFetch, err);
				} catch (Throwable e) {
					err.printStackTrace();
					callback.onSuccess(linksToFetch, e);
				}
			}
		});
	}

	/**
	 * Real logic for loading pojos from the datastore. Can be given a Entity or EmbeddedEntity, and if the entity is in the correct format
	 * you will get back a POJO.
	 * 
	 * @param entity
	 * @param id
	 * @return
	 * @throws ClassNotFoundException
	 * @throws InstantiationException
	 * @throws IllegalAccessException
	 * @throws InterruptedException
	 * @throws ExecutionException
	 */
	@SuppressWarnings({ "unchecked", "rawtypes" })
	public void entityToPOJO(PropertyContainer entity, String id, final List<GDSLink> linksToFetch, final GDSCallback<Object> callback) throws ClassNotFoundException,
			InstantiationException,
			IllegalAccessException,
			InterruptedException,
			ExecutionException {
		
		String kind = (String) entity.getProperty(GDSClass.GDS_CLASS_FIELD);
		Class<?> clazz = Class.forName(kind.replace("_", ".").replace("##", "_"));
		GDSClass.makeConstructorsPublic(clazz);
		final Object pojo = clazz.newInstance();
		Map<String, GDSField> map = GDSField.createMapFromObject(pojo);
		
		final List<GDSCallback<Void>> asyncWorkList = new ArrayList<>();
		GDSCallback<Void> block = new GDSCallback<Void>() {
			
			@Override
			public void onSuccess(Void t, Throwable e) {
				// Never called
			}
		};
		asyncWorkList.add(block);

		for (final GDSField gdsField : map.values()) {
			if (gdsField.fieldName.equals(GDSField.GDS_ID_FIELD)) {
				// ID field is part of the key of the entity
				gdsField.field.set(pojo, id);
				continue;
			}

			if (entity.getProperty(gdsField.fieldName) == null) {
				// No value for this field was saved, leave it blank
				continue;
			}

			Class<?> fieldType = gdsField.field.getType();

			if (fieldType.isArray()) {
				fieldType = fieldType.getComponentType();
				// Field is an array - we need to take the list from GAE,
				// get the number of elements, create an array of that size,
				// then set each element to the correct value

				List<Object> dsCollection = (List<Object>) entity.getProperty(gdsField.fieldName);
				final Object newArray = Array.newInstance(fieldType, dsCollection.size());

				if (gdsField.nonDatastoreObject) {
					if (gdsField.embedded) {
						// In this case, all stored objects will be
						// EmbeddedEntities that we need to turn into POJOs
						GDSCallback<Void> inCallback = new GDSCallback<Void>() {
							
							@Override
							public void onSuccess(Void t, Throwable err) {
								checkComplete(callback, pojo, asyncWorkList, this, err);
							}
						};
						asyncWorkList.add(inCallback);
						
						fillArrayAsync(entity, linksToFetch, gdsField, fieldType, newArray, inCallback);
					} else {
						// In this case, all stored objects in dsCollection will
						// be keys that we need to fetch and turn into actual
						// POJOs.
						Collection<Key> keyCollection = (Collection<Key>) entity.getProperty(gdsField.fieldName);
						linksToFetch.add(new GDSLink(pojo, gdsField, keyCollection));
					}
				} else {
					for (int i = 0; i < dsCollection.size(); i++) {
						setArrayIndex(i, fieldType, newArray, dsCollection.get(i));
					}
				}
				setField(gdsField.field, pojo, newArray);
			} else if (Collection.class.isAssignableFrom(fieldType)) {
				// Field is some kind of collection - so we need to take the
				// collection we get from GAE and convert it into the field's
				// subclass of collection
				final Collection<Object> newCollection = (Collection<Object>) GDSBoxer.createBestFitCollection(fieldType);

				if (gdsField.nonDatastoreObject) {
					GDSCallback<Void> inCallback = new GDSCallback<Void>() {
						
						@Override
						public void onSuccess(Void t, Throwable err) {
							checkComplete(callback, pojo, asyncWorkList, this, err);
						}
					};
					asyncWorkList.add(inCallback);
					
					fillCollectionAsync(entity, linksToFetch, gdsField, newCollection, inCallback);
				} else {
					Collection<Object> dsCollection = (Collection<Object>) entity.getProperty(gdsField.fieldName);

					for (Object object : dsCollection) {
						newCollection.add(object);
					}
				}
				setField(gdsField.field, pojo, newCollection);
			} else if (Map.class.isAssignableFrom(fieldType)) {
				// Field is some kind of map - so we need to take the
				// collection we get from GAE and convert it into a map using
				// special K and V properties.
				final Map<Object, Object> newMap = (Map<Object, Object>) GDSBoxer.createBestFitMap(fieldType);
				
				GDSCallback<Void> inCallback = new GDSCallback<Void>() {
					
					@Override
					public void onSuccess(Void t, Throwable err) {
						checkComplete(callback, pojo, asyncWorkList, this, err);
					}
				};
				asyncWorkList.add(inCallback);
				
				fillMapAsync(entity, linksToFetch, gdsField, newMap, inCallback);

				setField(gdsField.field, pojo, newMap);
			} else if (gdsField.nonDatastoreObject) {
				if (gdsField.embedded) {
					final GDSCallback<Void> inCallback = new GDSCallback<Void>() {
						
						@Override
						public void onSuccess(Void t, Throwable err) {
							checkComplete(callback, pojo, asyncWorkList, this, err);
						}
					};
					asyncWorkList.add(inCallback);

					Map<String, Object> dbObject = (Map<String, Object>) entity.getProperty(gdsField.fieldName);
					EmbeddedEntity embeddedEntity = new EmbeddedEntity();
					embeddedEntity.dbObject = dbObject;
					
					entityToPOJO(embeddedEntity, null, linksToFetch, new GDSCallback<Object>() {
						
						@Override
						public void onSuccess(Object embeddedPOJO, Throwable err) {
							try {
								synchronized (pojo) {
									setField(gdsField.field, pojo, embeddedPOJO);
								}
								inCallback.onSuccess(null, err);
							} catch (Throwable e) {
								inCallback.onSuccess(null, e);
							}
						}
					});
				} else {
					// Get the datastore key, we will use it later to fetch all
					// children at once
					Key key = new Key((Map<String, Object>) entity.getProperty(gdsField.fieldName));
					linksToFetch.add(new GDSLink(pojo, gdsField, key));
				}
			} else if (gdsField.isEnum) {
				String enumName = entity.getProperty(gdsField.fieldName).toString();
				setField(gdsField.field, pojo, Enum.valueOf((Class<? extends Enum>) gdsField.field.getType(), enumName));
			} else {
				// Regular/primitive fields
				Object fieldPOJO = entity.getProperty(gdsField.fieldName);
				setField(gdsField.field, pojo, fieldPOJO);
			}
		}
		
		checkComplete(callback, pojo, asyncWorkList, block, null);
	}
	
	private void checkComplete(GDSCallback<Object> callback, final Object pojo, List<GDSCallback<Void>> asyncWorkList, GDSCallback<Void> block, Throwable err) {
		synchronized (asyncWorkList) {
			if (err != null) {
				callback.onSuccess(pojo, err);
			} else {
				asyncWorkList.remove(block);
				if (asyncWorkList.isEmpty()) {
					callback.onSuccess(pojo, null);
				}
			}
		}
	}

	@SuppressWarnings("unchecked")
	private void fillMapAsync(PropertyContainer entity, final List<GDSLink> linksToFetch, final GDSField gdsField, final Map<Object, Object> newMap,
			final GDSCallback<Void> callback)
			throws ClassNotFoundException, InstantiationException, IllegalAccessException, InterruptedException, ExecutionException {
		EmbeddedEntity embeddedEntity = new EmbeddedEntity();
		embeddedEntity.dbObject = (Map<String, Object>) entity.getProperty(gdsField.fieldName);

		final Collection<Object> keyCheck = new ArrayList<>();
		Object block = new Object();
		keyCheck.add(block);
		
		for (int counter = 1;; counter++) {
			if (!embeddedEntity.hasProperty("K" + counter))
				break;
			
			final Object keyFind = embeddedEntity.getProperty("K" + counter);
			final Object valFind = embeddedEntity.getProperty("V" + counter);
			
			synchronized (newMap) {
				keyCheck.add(keyFind);
			}
			
			mapValueToPOJO(keyFind, linksToFetch, new GDSCallback<Object>() {
				
				@Override
				public void onSuccess(final Object key, Throwable err) {
					try {
						if (err != null)
							throw err;
						mapValueToPOJO(valFind, linksToFetch, new GDSCallback<Object>() {
							
							@Override
							public void onSuccess(Object val, Throwable err) {
								synchronized (newMap) {
									if (err != null) {
										callback.onSuccess(null, err);
									} else {
										newMap.put(key, val);
										keyCheck.remove(keyFind);
										if (keyCheck.isEmpty())
											callback.onSuccess(null, null);
									}
								}
							}
						});
					} catch (Throwable e) {
						e.printStackTrace();
						callback.onSuccess(null, e);
					}
				}
			});
		}
		
		synchronized (newMap) {
			keyCheck.remove(block);
			if (keyCheck.isEmpty())
				callback.onSuccess(null, null);
		}
	}
	
	@SuppressWarnings("unchecked")
	private void fillCollectionAsync(PropertyContainer entity, final List<GDSLink> linksToFetch, final GDSField gdsField, final Collection<Object> newCollection,
			final GDSCallback<Void> callback) throws ClassNotFoundException, InstantiationException, IllegalAccessException, InterruptedException, ExecutionException {
		
		Collection<Object> dsCollection = (Collection<Object>) entity.getProperty(gdsField.fieldName);

		final Collection<Object> collectionCopy = new ArrayList<>(dsCollection);
		Object block = new Object();
		collectionCopy.add(block);

		for (final Object obj : dsCollection) {
			if (obj instanceof Key) {
				Key key = (Key) obj;
				linksToFetch.add(new GDSLink(newCollection, key));
				synchronized (newCollection) {
					collectionCopy.remove(obj);
				}
			} else {
				mapValueToPOJO(obj, linksToFetch, new GDSCallback<Object>() {
					
					@Override
					public void onSuccess(Object newobj, Throwable err) {
						synchronized (newCollection) {
							if (err != null) {
								callback.onSuccess(null, err);
							} else {
								newCollection.add(newobj);
								collectionCopy.remove(obj);
								if (collectionCopy.isEmpty()) {
									callback.onSuccess(null, null);
								}
							}
						}
					}
				});
			}
		}
		
		synchronized (newCollection) {
			collectionCopy.remove(block);
			if (collectionCopy.isEmpty()) {
				callback.onSuccess(null, null);
			}
		}
	}
	
	@SuppressWarnings("unchecked")
	private void fillArrayAsync(PropertyContainer entity, final List<GDSLink> linksToFetch, final GDSField gdsField, Class<?> fieldType, final Object newArray,
			final GDSCallback<Void> callback)
			throws ClassNotFoundException, InstantiationException, IllegalAccessException, InterruptedException, ExecutionException {
		Collection<EmbeddedEntity> collection = (Collection<EmbeddedEntity>) entity.getProperty(gdsField.fieldName);
		final Collection<EmbeddedEntity> collectionCopy = new ArrayList<>(collection);
		EmbeddedEntity block = new EmbeddedEntity();
		collectionCopy.add(block);
		int counter = 0;
		for (final EmbeddedEntity embeddedEntity : collection) {
			final int fCounter = counter++;
			final Class<?> fFieldType = fieldType;
			
			GDSCallback<Object> inCallback = new GDSCallback<Object>() {
				
				@Override
				public void onSuccess(Object embeddedPOJO, Throwable err) {
					synchronized (newArray) {
						if (err != null) {
							callback.onSuccess(null, err);
						} else {
							setArrayIndex(fCounter, fFieldType, newArray, embeddedPOJO);
							collectionCopy.remove(embeddedEntity);
							if (collectionCopy.isEmpty())
								callback.onSuccess(null, null);
						}
					}
				}
			};
			
			entityToPOJO(embeddedEntity, null, linksToFetch, inCallback);
		}
		synchronized (newArray) {
			collectionCopy.remove(block);
			if (collectionCopy.isEmpty())
				callback.onSuccess(null, null);
		}
	}
	
	@SuppressWarnings("unchecked")
	private void mapValueToPOJO(Object obj, List<GDSLink> linksToFetch, GDSCallback<Object> callback) throws ClassNotFoundException, InstantiationException,
			IllegalAccessException, InterruptedException,
			ExecutionException {
		
		if (obj instanceof Map<?, ?>) {
			Map<String, Object> map = (Map<String, Object>) obj;
			if (!map.containsKey(GDSClass.GDS_CLASS_FIELD)) {
				Key key = new Key(map);
				fetch(key, callback);
			} else {
				EmbeddedEntity entity = new EmbeddedEntity();
				entity.dbObject = map;
				entityToPOJO(entity, null, linksToFetch, callback);
			}
		} else {
			callback.onSuccess(obj, null);
		}
	}

	/**
	 * Nasty function to get around nasty java primitive arrays.
	 * 
	 * @param index
	 * @param type
	 * @param array
	 * @param fieldPOJO
	 */
	private void setArrayIndex(int index, Class<?> type, Object array, Object fieldPOJO) {
		if (type.isPrimitive()) {
			if (type == int.class) {
				Long l = (Long) fieldPOJO;
				Array.setInt(array, index, l.intValue());
			} else if (type == short.class) {
				Long l = (Long) fieldPOJO;
				Array.setShort(array, index, l.shortValue());
			} else if (type == byte.class) {
				Long l = (Long) fieldPOJO;
				Array.setByte(array, index, l.byteValue());
			} else if (type == float.class) {
				Double d = (Double) fieldPOJO;
				Array.setFloat(array, index, d.floatValue());
			} else if (type == char.class) {
				String s = (String) fieldPOJO;
				Array.setChar(array, index, s.charAt(0));
			} else {
				Array.set(array, index, fieldPOJO);
			}
		} else {
			if (type == Integer.class) {
				Long l = (Long) fieldPOJO;
				Array.set(array, index, l.intValue());
			} else if (type == Short.class) {
				Long l = (Long) fieldPOJO;
				Array.set(array, index, l.shortValue());
			} else if (type == Byte.class) {
				Long l = (Long) fieldPOJO;
				Array.set(array, index, l.byteValue());
			} else if (type == Float.class) {
				Double d = (Double) fieldPOJO;
				Array.set(array, index, d.floatValue());
			} else if (type == Character.class) {
				String s = (String) fieldPOJO;
				fieldPOJO = new Character(s.charAt(0));
				Array.set(array, index, fieldPOJO);
			} else {
				Array.set(array, index, fieldPOJO);
			}
		}
	}

	/**
	 * Nasty function to get around nasty java primitive fields.
	 * 
	 * @param field
	 * @param pojo
	 * @param fieldPOJO
	 * @throws IllegalArgumentException
	 * @throws IllegalAccessException
	 */
	private void setField(Field field, Object pojo, Object fieldPOJO) throws IllegalArgumentException, IllegalAccessException {
		Class<?> type = field.getType();
		if (type.isPrimitive()) {
			if (type == int.class) {
				Integer l = (Integer) fieldPOJO;
				field.setInt(pojo, l);
			} else if (type == short.class) {
				Integer l = (Integer) fieldPOJO;
				field.setShort(pojo, l.shortValue());
			} else if (type == byte.class) {
				String l = (String) fieldPOJO;
				field.setByte(pojo, Byte.valueOf(l));
			} else if (type == float.class) {
				Double d = (Double) fieldPOJO;
				field.setFloat(pojo, d.floatValue());
			} else if (type == char.class) {
				Character s = (Character) fieldPOJO;
				field.setChar(pojo, s);
			} else {
				field.set(pojo, fieldPOJO);
			}
		} else {
			if (Number.class.isAssignableFrom(type)) {
				Number number;
				if (fieldPOJO instanceof String) {
					number = Double.valueOf((String) fieldPOJO);
				} else {
					number = (Number) fieldPOJO;
				}
				if (type == Integer.class) {
					field.set(pojo, number.intValue());
				} else if (type == Long.class) {
					field.set(pojo, number.longValue());
				} else if (type == Short.class) {
					field.set(pojo, number.shortValue());
				} else if (type == Byte.class) {
					field.set(pojo, number.byteValue());
				} else if (type == Float.class) {
					field.set(pojo, number.floatValue());
				} else if (type == Double.class) {
					field.set(pojo, number.doubleValue());
				} else {
					field.set(pojo, number.doubleValue());
				}
			} else if (type == Character.class) {
				Character s = (Character) fieldPOJO;
				field.set(pojo, s);
			} else {
				field.set(pojo, fieldPOJO);
			}
		}
	}

}
