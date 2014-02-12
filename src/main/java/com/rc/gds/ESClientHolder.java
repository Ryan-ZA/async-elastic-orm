package com.rc.gds;

import static org.elasticsearch.node.NodeBuilder.nodeBuilder;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.elasticsearch.client.Client;
import org.elasticsearch.node.Node;

public class ESClientHolder {
	
	public static Map<String, Client> clientMap = new HashMap<>();
	public static List<Node> nodeList = new ArrayList<>();
	
	public static synchronized Client getClient(boolean isclient, String dbname) {
		if (clientMap.containsKey(dbname)) {
			return clientMap.get(dbname);
		} else {
			Client client;
			Node node = nodeBuilder().clusterName(dbname).local(false).client(isclient).node();
			node.start();
			nodeList.add(node);
			client = node.client();
			clientMap.put(dbname, client);
			return client;
		}
	}

}
