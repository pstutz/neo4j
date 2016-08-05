package org.neo4j.kernel.impl.factory;

import org.neo4j.graphdb.*;
import org.neo4j.graphdb.index.IndexManager;
import org.neo4j.kernel.api.Statement;
import org.neo4j.kernel.api.exceptions.EntityNotFoundException;
import org.neo4j.kernel.api.exceptions.InvalidTransactionTypeKernelException;
import org.neo4j.kernel.api.exceptions.schema.ConstraintValidationKernelException;
import org.neo4j.kernel.api.exceptions.schema.SchemaKernelException;
import org.neo4j.kernel.impl.core.NodeProxy;

import java.util.HashMap;
import java.util.Map;
import java.util.TreeMap;

/**
 * Created by Sascha Peukert on 03.08.2016.
 */
public class EnhancedGraphDatabaseFacade extends GraphDatabaseFacade {

    private Map<Integer, TreeMap<Integer, PropertyContainer>> virtualNodes; // TA-Hashcode -> Map ( Id -> Node)
    private Map<Integer,TreeMap<Integer,PropertyContainer>> virtualRelationships; // TA-Hashcode -> Map ( Id -> Relationship)
    private Map<Integer,TreeMap<Integer,Label>> virtualLabels; // TA-Hashcode -> Map (NodeId -> Label)

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

        virtualNodes = new HashMap<>();
        virtualRelationships = new HashMap<>();
        virtualLabels = new HashMap<>();
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

    public Node createVirtualNode(Label... labels){
        try
        {
            Node newNode = createVirtualNode();
            for ( Label label : labels )
            {
                // IF NOT YET HERE!
                int labelId = spi.currentStatement().tokenWriteOperations().labelGetOrCreateForName( label.name() );

                newNode.addLabel(label); // this will also call dataWriteOperations, so this is no option!
                // -> Handle everything yourself here
                // Remember that there could be constraints

                //statement.dataWriteOperations().nodeAddLabel( nodeId, labelId );
                //TODO: Finish this

            }
            return newNode;
        }
        catch ( ConstraintValidationKernelException e )
        {
            throw new ConstraintViolationException( "Unable to add label.", e );
        }
        catch ( SchemaKernelException e )
        {
            throw new IllegalArgumentException( e );
        }
        catch ( InvalidTransactionTypeKernelException e )
        {
            throw new ConstraintViolationException( e.getMessage(), e );
        }
    }

    @Override
    public Node createNode(Label... labels) {
        return super.createNode(labels);
    }

    @Override
    public Node getNodeById(long id) {
        if(id<0) {
            // virtual node
            int transaction_hashcode = spi.currentTransaction().hashCode();
            try {
                return (Node) virtualNodes.get(transaction_hashcode).get(id);
            } catch (NullPointerException e){
                throw new IllegalArgumentException("You looked for a virtual node that isn't there. How?");
            }
        }
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
