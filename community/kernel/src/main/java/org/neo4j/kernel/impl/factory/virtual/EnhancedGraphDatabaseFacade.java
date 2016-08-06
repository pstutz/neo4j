package org.neo4j.kernel.impl.factory.virtual;

import org.neo4j.collection.primitive.PrimitiveLongIterator;
import org.neo4j.graphdb.*;
import org.neo4j.graphdb.index.IndexManager;
import org.neo4j.helpers.collection.PrefetchingResourceIterator;
import org.neo4j.kernel.api.Statement;
import org.neo4j.kernel.api.exceptions.EntityNotFoundException;
import org.neo4j.kernel.impl.core.NodeProxy;
import org.neo4j.kernel.impl.core.RelationshipProxy;
import org.neo4j.kernel.impl.factory.GraphDatabaseFacade;
import org.neo4j.storageengine.api.EntityType;

import java.util.HashMap;
import java.util.Map;
import java.util.TreeMap;

import static java.lang.String.format;

/**
 * Created by Sascha Peukert on 03.08.2016.
 */
public class EnhancedGraphDatabaseFacade extends GraphDatabaseFacade {

    //TODO: Write tests!

    private Map<String, TreeMap<Integer, PropertyContainer>> virtualNodes; // TA -> Map ( Id -> Node)
    private Map<String,TreeMap<Integer,PropertyContainer>> virtualRelationships; // TA -> Map ( Id -> Relationship)
    private Map<String,TreeMap<Integer,Label>> virtualLabels; // TA -> Map (NodeId -> Label)

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
        String transaction_ident = spi.currentTransaction().toString();

        // Ensure that there is a map
        TreeMap<Integer,PropertyContainer> current_map = virtualNodes.get(transaction_ident);
        if(current_map==null){
            current_map = new TreeMap<>();
            virtualNodes.put(transaction_ident,current_map);
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
        /*try
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
        }*/

        return null;  // just for the moment
    }

    @Override
    public Node createNode(Label... labels) {
        return super.createNode(labels);
    }

    @Override
    public Node getNodeById(long id) {
        if(id<0) {
            // virtual node
            String transaction_ident = spi.currentTransaction().toString();
            try {
                Object n = virtualNodes.get(transaction_ident).get(id);
                if(n!=null){
                    return (Node) n;
                }
                throw new NotFoundException( format( "Node %d not found", id ),
                        new EntityNotFoundException( EntityType.NODE, id ) );
            } catch (NullPointerException e){
                throw new IllegalArgumentException("You looked for a virtual node that isn't there. How?");
            }
        }
        return super.getNodeById(id);
    }

    @Override
    public Relationship getRelationshipById(long id) {
        if(id<0) {
            // virtual relationship
            String transaction_ident = spi.currentTransaction().toString();
            try {
                Object r = virtualRelationships.get(transaction_ident).get(id);
                if(r!=null){
                    return (Relationship) r;
                }
                throw new NotFoundException( format( "Relationship %d not found", id ),
                        new EntityNotFoundException( EntityType.RELATIONSHIP, id ) );
            } catch (NullPointerException e){
                throw new IllegalArgumentException("You looked for a virtual relationship that isn't there. How?");
            }
        }
        return super.getRelationshipById(id);
    }

    @Override
    public IndexManager index() {
        // TODO need to add virtual stuff to index?
        return super.index();
    }

    @Override
    public ResourceIterable<Node> getAllNodes() {
        assertTransactionOpen();
        return () -> {
            Statement statement = spi.currentStatement();
            String transaction_ident = spi.currentTransaction().toString();
            Map<Integer,PropertyContainer> currentMap = virtualNodes.get(transaction_ident);
            PrimitiveLongIterator it = statement.readOperations().nodesGetAll();
            if(currentMap!=null){
                it = new MergePrimitiveLongIterator(it,virtualNodes.get(transaction_ident).values(),true);
            }
            return map2nodes( it, statement );
        };
    }

    @Override
    public ResourceIterable<Relationship> getAllRelationships() {
        assertTransactionOpen();
        return () -> {
            final Statement statement = spi.currentStatement();
            String transaction_ident = spi.currentTransaction().toString();
            Map<Integer,PropertyContainer> currentMap = virtualRelationships.get(transaction_ident);
            PrimitiveLongIterator ids = statement.readOperations().relationshipsGetAll();
            if(currentMap!=null){
                ids = new MergePrimitiveLongIterator(ids,virtualRelationships.get(transaction_ident).values(),false);
            }
            final PrimitiveLongIterator newOne = ids;
            return new PrefetchingResourceIterator<Relationship>()
            {
                @Override
                public void close()
                {
                    statement.close();
                }

                @Override
                protected Relationship fetchNextOrNull()
                {
                    return newOne.hasNext() ? new RelationshipProxy( relActions, newOne.next() ) : null;
                }
            };
        };
    }

    @Override
    public ResourceIterable<Label> getAllLabelsInUse() {
        // TODO need to add all used (only) virtual labels to this Iterable
        return super.getAllLabelsInUse();
    }

    @Override
    public ResourceIterable<RelationshipType> getAllRelationshipTypesInUse() {
        // TODO need to add all used (only) virtual relationship types to this Iterable
        return super.getAllRelationshipTypesInUse();
    }

    @Override
    public ResourceIterable<Label> getAllLabels() {
        // TODO need to add all (only) virtual labels to this Iterable
        return super.getAllLabels();
    }

    @Override
    public ResourceIterable<RelationshipType> getAllRelationshipTypes() {
        // TODO need to add all (only) virtual relationship types to this Iterable
        return super.getAllRelationshipTypes();
    }

    @Override
    public ResourceIterable<String> getAllPropertyKeys() {
        // TODO need to add all only virtual property keys to this Iterable
        return super.getAllPropertyKeys();
    }

    @Override
    public ResourceIterator<Node> findNodes(Label myLabel, String key, Object value) {
        // TODO search virtual nodes too
        return super.findNodes(myLabel, key, value);
    }

    @Override
    public Node findNode(Label myLabel, String key, Object value) {
        // TODO search virtual nodes too
        return super.findNode(myLabel, key, value);
    }

    @Override
    public ResourceIterator<Node> findNodes(Label myLabel) {
        // TODO search virtual nodes too
        return super.findNodes(myLabel);
    }
}
