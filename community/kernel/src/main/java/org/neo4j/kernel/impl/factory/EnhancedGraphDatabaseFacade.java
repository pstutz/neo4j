package org.neo4j.kernel.impl.factory;

import org.neo4j.graphdb.*;
import org.neo4j.graphdb.index.IndexManager;
import org.neo4j.kernel.impl.core.NodeProxy;

import java.util.Map;
import java.util.TreeMap;

/**
 * Created by Sascha Peukert on 03.08.2016.
 */
public class EnhancedGraphDatabaseFacade extends GraphDatabaseFacade {

    private Map<Integer, TreeMap<Integer, PropertyContainer>> virtualNodes; // TA-Hashcode -> Map ( Id -> Node)
    private Map<Integer,TreeMap<Integer,PropertyContainer>> virtualRelationships; // TA-Hashcode -> Map ( Id -> Relationship)

    private int getFreeVirtualId(TreeMap<Integer,PropertyContainer> map){
        if(map.size()==0){
            return -1;
        }
        return map.firstKey() -1; // because negative id -> first key is the lowest one
        // TODO: is this ordering/collection fast enough?
    }

    @Override
    public void init(SPI spi) {
        super.init(spi);
    }

    public Node createVirtualNode(){
        int transaction_hashcode = spi.currentTransaction().hashCode();

        // Ensure that there is a map
        TreeMap<Integer,PropertyContainer> current_map = virtualNodes.get(transaction_hashcode);
        if(current_map==null){
            current_map = new TreeMap<>();
            virtualNodes.put(transaction_hashcode,current_map);
        }

        int id = getFreeVirtualId(current_map);
        NodeProxy newNode = new NodeProxy(nodeActions,id);  // TODO: Exchange NodeProxy because of how relationships are created
        current_map.put(id,newNode);
        return newNode;
    }

    @Override
    public Node createNode() {
        return super.createNode();
    }

    @Override
    public Node createNode(Label... labels) {
        return super.createNode(labels);
    }

    @Override
    public Node getNodeById(long id) {
        return super.getNodeById(id);
    }

    @Override
    public Relationship getRelationshipById(long id) {
        return super.getRelationshipById(id);
    }

    @Override
    public IndexManager index() {
        return super.index();
    }

    @Override
    public ResourceIterable<Node> getAllNodes() {
        return super.getAllNodes();
    }

    @Override
    public ResourceIterable<Relationship> getAllRelationships() {
        return super.getAllRelationships();
    }

    @Override
    public ResourceIterable<Label> getAllLabelsInUse() {
        return super.getAllLabelsInUse();
    }

    @Override
    public ResourceIterable<RelationshipType> getAllRelationshipTypesInUse() {
        return super.getAllRelationshipTypesInUse();
    }

    @Override
    public ResourceIterable<Label> getAllLabels() {
        return super.getAllLabels();
    }

    @Override
    public ResourceIterable<RelationshipType> getAllRelationshipTypes() {
        return super.getAllRelationshipTypes();
    }

    @Override
    public ResourceIterable<String> getAllPropertyKeys() {
        return super.getAllPropertyKeys();
    }

    @Override
    public ResourceIterator<Node> findNodes(Label myLabel, String key, Object value) {
        return super.findNodes(myLabel, key, value);
    }

    @Override
    public Node findNode(Label myLabel, String key, Object value) {
        return super.findNode(myLabel, key, value);
    }

    @Override
    public ResourceIterator<Node> findNodes(Label myLabel) {
        return super.findNodes(myLabel);
    }
}
