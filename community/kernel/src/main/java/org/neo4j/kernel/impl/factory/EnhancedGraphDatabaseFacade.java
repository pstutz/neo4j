package org.neo4j.kernel.impl.factory;

import org.neo4j.graphdb.*;
import org.neo4j.graphdb.index.IndexManager;

import java.util.HashMap;

/**
 * Created by Sascha Peukert on 03.08.2016.
 */
public class EnhancedGraphDatabaseFacade extends GraphDatabaseFacade {

    private HashMap<Integer,Node> virtualNodes;

    @Override
    public void init(SPI spi) {
        super.init(spi);
    }

    @Override
    public Node createNode() {
        //spi.currentTransaction().hashCode();
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
