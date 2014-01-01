package com.rc.gds;

import static org.elasticsearch.node.NodeBuilder.nodeBuilder;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;

import org.elasticsearch.client.Client;
import org.elasticsearch.node.Node;

/**
 * 
 * Main class for GDS. A new instance of GDS should be created for every transaction and discarded afterwards.
 * 
 */
public class GDS {
	
	static Map<String, Client> clientMap = new HashMap<>();
	static List<Node> nodeList = new ArrayList<>();

	static synchronized Client getClient(String dbname) {
		if (clientMap.containsKey(dbname)) {
			return clientMap.get(dbname);
		} else {
			Node node = nodeBuilder().clusterName(dbname).node();
			node.start();
			nodeList.add(node);
			Client client = node.client();
			clientMap.put(dbname, client);
			return client;
		}
	}
	
	Client client;
	String cluster;

	public GDS() {
		client = getClient("gloopsh");
	}
	
	public GDS(String cluster) {
		client = getClient(cluster);
	}
	
	public String indexFor(String kind) {
		return kind.toLowerCase(Locale.US);
	}
	
	public String[] indexFor(String[] kinds) {
		String[] index = new String[kinds.length];
		for (int i = 0; i < kinds.length; i++)
			index[i] = kinds[i].toLowerCase(Locale.US);
		return index;
	}

	/**
	 * @return A new GDSLoader that can be used to load pojos IFF you have the ID or Key of the pojo.
	 */
	public GDSLoader load() {
		return new GDSLoader(this);
	}

	/**
	 * @return A new GDSSaver that can be used to save any collection of pojos.
	 */
	public GDSSaver save(Object o) {
		GDSSaver gdsSaver = new GDSSaver(this);
		return gdsSaver.entity(o);
	}
	
	/**
	 * @return A new GDSSaver that can be used to save any collection of pojos.
	 */
	public GDSSaver save() {
		return new GDSSaver(this);
	}
	
	/**
	 * 
	 * @return A new GDSDelete that can be used to delete pojos from the datastore
	 */
	public GDSDeleter delete() {
		return new GDSDeleter(this);
	}

	/**
	 * @param clazz
	 *            The class of pojos to search for. All subclasses of this type will also be searched for.
	 * @return A new parametrized GDSQuery that can be used to search for specific kinds of pojos. Filters and sorting are available.
	 */
	public <T> GDSQuery<T> query(Class<T> clazz) {
		return new GDSQuery<T>(this, clazz);
	}

	/**
	 * Begin a transaction that will last until commitTransaction() or rollbackTransaction() is called. You must call one of these when you
	 * have finished the transaction. The transaction will apply to all load() save() and query() calls from this GDS.
	 * 
	 * It is not required to call this to do simple operations - you only need to use this if you wish to commit/rollback all operations
	 * done by this GDS.
	 */
	public void beginTransaction() {
		//db.requestStart();
	}

	/**
	 * Must call beginTransaction before using this or you will receive a NullPointerException.
	 */
	public void commitTransaction() {
		//db.requestDone();
	}

	/**
	 * Must call beginTransaction before using this or you will receive a NullPointerException.
	 */
	public void rollbackTransaction() {
		//db.requestDone();
	}
	
	public Client getClient() {
		return client;
	}
	
	public static synchronized void shutdownAllNodes() {
		for (Client client : clientMap.values()) {
			client.close();
		}
		for (Node node : nodeList) {
			node.stop();
			node.close();
		}
		nodeList.clear();
		clientMap.clear();
	}

}
