/*
 * Copyright (c) 2002-2016 "Neo Technology,"
 * Network Engine for Objects in Lund AB [http://neotechnology.com]
 *
 * This file is part of Neo4j.
 *
 * Neo4j is free software: you can redistribute it and/or modify
 * it under the terms of the GNU General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with this program.  If not, see <http://www.gnu.org/licenses/>.
 */
package org.neo4j.kernel.impl.api;

import org.apache.commons.lang3.ArrayUtils;
import org.neo4j.collection.RawIterator;
import org.neo4j.collection.primitive.PrimitiveIntIterator;
import org.neo4j.collection.primitive.PrimitiveLongIterator;
import org.neo4j.cursor.Cursor;
import org.neo4j.graphdb.Direction;
import org.neo4j.kernel.api.KernelTransaction;
import org.neo4j.kernel.api.LegacyIndexHits;
import org.neo4j.kernel.api.exceptions.*;
import org.neo4j.kernel.api.exceptions.index.IndexNotFoundKernelException;
import org.neo4j.kernel.api.exceptions.legacyindex.AutoIndexingKernelException;
import org.neo4j.kernel.api.exceptions.legacyindex.LegacyIndexNotFoundKernelException;
import org.neo4j.kernel.api.exceptions.schema.ConstraintValidationKernelException;
import org.neo4j.kernel.api.exceptions.schema.IllegalTokenNameException;
import org.neo4j.kernel.api.exceptions.schema.IndexBrokenKernelException;
import org.neo4j.kernel.api.exceptions.schema.TooManyLabelsException;
import org.neo4j.kernel.api.index.IndexDescriptor;
import org.neo4j.kernel.api.proc.ProcedureSignature.ProcedureName;
import org.neo4j.kernel.api.properties.DefinedProperty;
import org.neo4j.kernel.api.properties.Property;
import org.neo4j.kernel.impl.api.store.RelationshipIterator;
import org.neo4j.kernel.impl.proc.Procedures;
import org.neo4j.kernel.impl.util.Cursors;
import org.neo4j.register.Register.DoubleLongRegister;
import org.neo4j.storageengine.api.*;

import java.util.*;

// TODO Sascha
public class VirtualOperationsFacade extends OperationsFacade
{

    class PropertyValueId {

        private long entityId;
        private int propKeyId;

        public PropertyValueId(long entity, int prop){
            this.entityId = entity;
            this.propKeyId = prop;
        }

        public long getEntityId(){
            return entityId;
        }

        public int getPropertyKeyId(){
            return propKeyId;
        }

        @Override
        public boolean equals(Object obj) {
            if(obj.getClass()==this.getClass()){
                PropertyValueId other = (PropertyValueId) obj;
                if((this.entityId==other.entityId) && (this.propKeyId == other.propKeyId)){
                    return true;
                }
                return false;
            } else{
                return false;
            }
        }
    }

    private TreeMap<Long,Integer> virtualRelationshipToTypeId; // actualData and ref to types
    private SortedSet<Long> virtualNodeIds;  // "actual data"
    private TreeMap<Integer,String> virtualPropertyKeyIdsToName;  // "actual data"
    private TreeMap<Integer,String> virtualLabels; // actual data
    private TreeMap<Integer,String> virtualRelationshipTypes; // actual data

    //entityId + propKeyId -> value
    private Map<PropertyValueId,Object> virtualPropertyIdToValueForNodes;
    private Map<PropertyValueId,Object> virtualPropertyIdToValueForRels;

    //private Map<Integer,Object> virtualPropertiyIdsToObjectForNodes; // actual data
    //private Map<Integer,Object> virtualPropertiyIdsToObjectForRels; // actual data

    private Map<Long,Set<Integer>> virtualNodeIdToPropertyIds;   // Natural ordering 1 2 3 10 12 ...  -> first one is the smallest with negative
    private Map<Long,Set<Long>> virtualNodeIdToConnectedRelationshipIds;
    private Map<Long,Set<Integer>> virtualRelationshipIdToPropertyIds;
    private Map<Long,Set<Integer>> virtualNodeIdToLabelIds;
    private Map<Long,Long[]> virtualRelationshipToVirtualNodeIds; // Node[0] = from, Node[1] = to

    VirtualOperationsFacade(KernelTransaction tx, KernelStatement statement,
                            StatementOperationParts operations, Procedures procedures )
    {
        super(tx,statement,operations,procedures);


        virtualRelationshipToTypeId = new TreeMap<>();
        virtualNodeIds = new TreeSet<>();
        virtualPropertyKeyIdsToName = new TreeMap<>();
        virtualLabels = new TreeMap<>();
        virtualRelationshipTypes = new TreeMap<>();

        virtualNodeIdToPropertyIds = new HashMap<>();
        virtualRelationshipIdToPropertyIds = new HashMap<>();
        virtualNodeIdToLabelIds = new HashMap<>();
        virtualRelationshipToVirtualNodeIds = new HashMap<>();
        virtualNodeIdToConnectedRelationshipIds = new HashMap<>();

        virtualPropertyIdToValueForNodes = new HashMap<>();
        virtualPropertyIdToValueForRels = new HashMap<>();

        //virtualPropertiyKeyIdsToObjectForNodes = new HashMap<>();
        //virtualPropertiyKeyIdsToObjectForRels = new HashMap<>();
    }

    // <DataRead>

    @Override
    public PrimitiveLongIterator nodesGetAll()
    {
        PrimitiveLongIterator allRealNodes = super.nodesGetAll();
        MergingPrimitiveLongIterator bothNodeIds = new MergingPrimitiveLongIterator(allRealNodes, virtualNodeIds);
        return bothNodeIds;
    }

    @Override
    public PrimitiveLongIterator relationshipsGetAll()
    {
        PrimitiveLongIterator allRealRels = super.relationshipsGetAll();
        MergingPrimitiveLongIterator bothRelIds =
                new MergingPrimitiveLongIterator(allRealRels,virtualRelationshipIds());
        return bothRelIds;
    }

    @Override
    public PrimitiveLongIterator nodesGetForLabel( int labelId )
    {
        ArrayList<Long> resultList = new ArrayList<>();

        // TODO: Test this
        // TODO: Improvements possible
        for(Long nodeId : virtualNodeIdToLabelIds.keySet()) {
            Set<Integer> labelIds = virtualNodeIdToLabelIds.get(nodeId);
            if(labelIds.contains(labelId)){
                resultList.add(nodeId);
            }
        }
        return new MergingPrimitiveLongIterator(null,resultList);
    }

    @Override
    public PrimitiveLongIterator nodesGetFromIndexSeek( IndexDescriptor index, Object value )
            throws IndexNotFoundKernelException
    {
        // TODO !
        return super.nodesGetFromIndexSeek(index,value);
    }

    @Override
    public PrimitiveLongIterator nodesGetFromIndexRangeSeekByNumber( IndexDescriptor index,
            Number lower,
            boolean includeLower,
            Number upper,
            boolean includeUpper )
            throws IndexNotFoundKernelException
    {
        // TODO !
        return super.nodesGetFromIndexRangeSeekByNumber(index,lower,includeLower,upper,includeUpper);
    }

    @Override
    public PrimitiveLongIterator nodesGetFromIndexRangeSeekByString( IndexDescriptor index,
            String lower,
            boolean includeLower,
            String upper,
            boolean includeUpper )
            throws IndexNotFoundKernelException
    {
        // TODO !
        return super.nodesGetFromIndexRangeSeekByString(index,lower,includeLower,upper,includeUpper);
    }

    @Override
    public PrimitiveLongIterator nodesGetFromIndexRangeSeekByPrefix( IndexDescriptor index, String prefix )
            throws IndexNotFoundKernelException
    {
        // TODO !
        return super.nodesGetFromIndexRangeSeekByPrefix(index,prefix);
    }

    @Override
    public PrimitiveLongIterator nodesGetFromIndexScan( IndexDescriptor index )
            throws IndexNotFoundKernelException
    {
        // TODO !
        return super.nodesGetFromIndexScan(index);
    }

    @Override
    public PrimitiveLongIterator nodesGetFromIndexContainsScan( IndexDescriptor index, String term )
            throws IndexNotFoundKernelException
    {
        // TODO !
        return super.nodesGetFromIndexContainsScan(index,term);
    }

    @Override
    public PrimitiveLongIterator nodesGetFromIndexEndsWithScan( IndexDescriptor index, String suffix )
            throws IndexNotFoundKernelException
    {
        // TODO !
        return super.nodesGetFromIndexEndsWithScan(index,suffix);
    }

    @Override
    public long nodeGetFromUniqueIndexSeek( IndexDescriptor index, Object value )
            throws IndexNotFoundKernelException, IndexBrokenKernelException
    {
        // TODO !
        return super.nodeGetFromUniqueIndexSeek(index,value);
    }

    @Override
    public boolean nodeExists( long nodeId )
    {
        if(isVirtual(nodeId)){
            return virtualNodeIds.contains(nodeId);
        } else {
            return super.nodeExists(nodeId);
        }
    }

    @Override
    public boolean relationshipExists( long relId )
    {
        if(isVirtual(relId)){
            return virtualRelationshipIds().contains(relId);
        } else {
            return super.relationshipExists(relId);
        }
    }

    @Override
    public boolean nodeHasLabel( long nodeId, int labelId ) throws EntityNotFoundException
    {
        if(isVirtual(nodeId)){
            if(nodeExists(nodeId)){
                Set<Integer> labelIds = virtualNodeIdToLabelIds.get(nodeId);
                if(labelIds==null){
                    return false;
                }
                return labelIds.contains(labelId);
            } else{
                throw new EntityNotFoundException(EntityType.NODE,nodeId);
            }
        } else {
            return super.nodeHasLabel(nodeId, labelId);
        }
    }

    @Override
    public PrimitiveIntIterator nodeGetLabels( long nodeId ) throws EntityNotFoundException
    {
        if(isVirtual(nodeId)){
            if(nodeExists(nodeId)){
                Set<Integer> labelIds = virtualNodeIdToLabelIds.get(nodeId);
                MergingPrimitiveIntIterator it = new MergingPrimitiveIntIterator(null,labelIds);
                return it;
            } else{
                throw new EntityNotFoundException(EntityType.NODE,nodeId);
            }
        } else {
            return super.nodeGetLabels(nodeId);
        }
    }

    @Override
    public boolean nodeHasProperty( long nodeId, int propertyKeyId ) throws EntityNotFoundException
    {
        if(isVirtual(nodeId)){
            if(nodeExists(nodeId)){
                Set<Integer> propIds = virtualNodeIdToPropertyIds.get(nodeId);
                if(propIds==null){
                    return false;
                }
                return propIds.contains(propertyKeyId);
            } else{
                throw new EntityNotFoundException(EntityType.NODE,nodeId);
            }
        } else {
            return super.nodeHasProperty(nodeId, propertyKeyId);
        }
    }

    @Override
    public Object nodeGetProperty( long nodeId, int propertyKeyId ) throws EntityNotFoundException
    {
        if(isVirtual(nodeId)){
            if(nodeExists(nodeId)){
                // assert that the property belongs to this node
                Set<Integer> props = virtualNodeIdToPropertyIds.get(nodeId);
                if(props.contains(propertyKeyId)){
                    return getPropertyValueForNodes(nodeId,propertyKeyId); //TODO: Test this! Might need some improvements
                }
                throw new EntityNotFoundException(EntityType.NODE,nodeId);
            } else{
                throw new EntityNotFoundException(EntityType.NODE,nodeId);
            }

        } else {
            return super.nodeGetProperty(nodeId, propertyKeyId);
        }
    }

    @Override
    public RelationshipIterator nodeGetRelationships( long nodeId, Direction direction, int... relTypes )
            throws EntityNotFoundException
    {
        // TODO !
        return super.nodeGetRelationships(nodeId,direction,relTypes);
    }

    @Override
    public RelationshipIterator nodeGetRelationships( long nodeId, Direction direction )
            throws EntityNotFoundException
    {
        // TODO !
        return super.nodeGetRelationships(nodeId,direction);
    }

    @Override
    public int nodeGetDegree( long nodeId, Direction direction, int relType ) throws EntityNotFoundException
    {
        // TODO !
        return super.nodeGetDegree(nodeId,direction,relType);
    }

    @Override
    public int nodeGetDegree( long nodeId, Direction direction ) throws EntityNotFoundException
    {
        // TODO !
        return super.nodeGetDegree(nodeId,direction);
    }

    @Override
    public boolean nodeIsDense( long nodeId ) throws EntityNotFoundException
    {
        // TODO !
        return super.nodeIsDense(nodeId);
    }

    @Override
    public PrimitiveIntIterator nodeGetRelationshipTypes( long nodeId ) throws EntityNotFoundException
    {
        if(isVirtual(nodeId)) {
            if(nodeExists(nodeId)){
                Set<Long> relIds = virtualNodeIdToConnectedRelationshipIds.get(nodeId);
                Set<Integer> resultSet = new HashSet<>();
                for(Long relId:relIds){
                    resultSet.add(virtualRelationshipToTypeId.get(relId)); // this should not be null! TODO: Tests to ensure that
                }
                return new MergingPrimitiveIntIterator(null,resultSet);
            } else{
                throw new EntityNotFoundException(EntityType.NODE,nodeId);
            }
        } else{
            return super.nodeGetRelationshipTypes(nodeId);
        }
    }

    @Override
    public boolean relationshipHasProperty( long relationshipId, int propertyKeyId ) throws EntityNotFoundException
    {
        if(isVirtual(relationshipId)){
            if(relationshipExists(relationshipId)){
                Set<Integer> propIds = virtualRelationshipIdToPropertyIds.get(relationshipId);
                if(propIds==null){
                    return false;
                }
                return propIds.contains(propertyKeyId);
            } else{
                throw new EntityNotFoundException(EntityType.RELATIONSHIP,relationshipId);
            }
        } else {
            return super.relationshipHasProperty(relationshipId,propertyKeyId);
        }
    }

    @Override
    public Object relationshipGetProperty( long relationshipId, int propertyKeyId ) throws EntityNotFoundException
    {
        //TODO: Ensure that virtual entities can only got virtual props
        if(isVirtual(relationshipId)){
            if(relationshipExists(relationshipId)){
                // assert that this rel has got that prop -> already covered by relationshipHasProperty but are they always chained?
                Set<Integer> propIds = virtualRelationshipIdToPropertyIds.get(relationshipId);
                if(propIds==null){
                    return false;
                }
                if(propIds.contains(propertyKeyId)){
                    return getPropertyValueForRels(relationshipId,propertyKeyId);
                }
                throw new EntityNotFoundException(EntityType.RELATIONSHIP,relationshipId); // hm.. not that good
            } else{
                throw new EntityNotFoundException(EntityType.RELATIONSHIP,relationshipId);
            }
        } else {
            return super.relationshipGetProperty(relationshipId,propertyKeyId);
        }

    }

    @Override
    public boolean graphHasProperty( int propertyKeyId )
    {
        if(isVirtual(propertyKeyId)){
            return virtualPropertyIds().contains(propertyKeyId);
        } else {
            return super.graphHasProperty(propertyKeyId);
        }
    }

    @Override
    public Object graphGetProperty( int propertyKeyId )
    {
        // TODO: TEST THIS!!!!!
        if(isVirtual(propertyKeyId)){
            Iterator<Long> it = virtualNodeIds.iterator();
            while(it.hasNext()){
                long current_nodeId = it.next();
                try{
                    return nodeGetProperty(current_nodeId,propertyKeyId);
                }catch (EntityNotFoundException e){
                }
            }
            it = virtualRelationshipIds().iterator();
            while(it.hasNext()){
                long current_relId = it.next();
                try{
                    return relationshipGetProperty(current_relId,propertyKeyId);
                }catch (EntityNotFoundException e){
                }
            }

            return null;
        } else {
            return super.graphGetProperty(propertyKeyId);
        }
    }

    @Override
    public PrimitiveIntIterator nodeGetPropertyKeys( long nodeId ) throws EntityNotFoundException
    {
        if(isVirtual(nodeId)){
            return new MergingPrimitiveIntIterator(null,virtualNodeIdToPropertyIds.get(nodeId));
        } else {
            return super.nodeGetPropertyKeys(nodeId);
        }
    }

    @Override
    public PrimitiveIntIterator relationshipGetPropertyKeys( long relationshipId ) throws EntityNotFoundException
    {
        if(isVirtual(relationshipId)){
            return new MergingPrimitiveIntIterator(null,virtualRelationshipIdToPropertyIds.get(relationshipId));
        } else {
            return super.relationshipGetPropertyKeys(relationshipId);
        }
    }

    @Override
    public PrimitiveIntIterator graphGetPropertyKeys()
    {
        return new MergingPrimitiveIntIterator(super.graphGetPropertyKeys(), virtualPropertyIds());
    }

    @Override
    public <EXCEPTION extends Exception> void relationshipVisit( long relId,
            RelationshipVisitor<EXCEPTION> visitor ) throws EntityNotFoundException, EXCEPTION
    {
        // TODO !
        super.relationshipVisit(relId,visitor);
    }

    @Override
    public long nodesGetCount()
    {
        return super.nodesGetCount() + virtualNodeIds.size();
    }

    @Override
    public long relationshipsGetCount()
    {
        return super.relationshipsGetCount() + virtualRelationshipIds().size();
    }

    // </DataRead>

    // <DataReadCursors>
    @Override
    public Cursor<NodeItem> nodeCursor( long nodeId )
    {
        // TODO: Test this
        if(isVirtual(nodeId)) {
            statement.assertOpen(); // from super
            return MyStubCursors.asNodeCursor(nodeId); // this might not be a good solution?
        }
        return super.nodeCursor(nodeId);
    }

    @Override
    public Cursor<RelationshipItem> relationshipCursor( long relId )
    {
        //TODO: Finish this
        if(isVirtual(relId)){
            return null; //MyStubCursors.asRelationship() ...
        }
        return super.relationshipCursor(relId);
    }

    @Override
    public Cursor<NodeItem> nodeCursorGetAll()
    {
        // take the normal cursor and add to it the virtual nodes
        statement.assertOpen(); // from super
        ArrayList<Long> list = new ArrayList<>();
        PrimitiveLongIterator it = nodesGetAll();
        while(it.hasNext()){
            long l = it.next();
            list.add(l);
        }
        long[] array = ArrayUtils.toPrimitive((Long[])list.toArray()); // TODO: Test this !!!
        return MyStubCursors.asNodeCursor(array);

        // this might not be a good solution
    }

    @Override
    public Cursor<RelationshipItem> relationshipCursorGetAll()
    {
        statement.assertOpen(); // from super
        ArrayList<Long> list = new ArrayList<>();
        PrimitiveLongIterator it = relationshipsGetAll();
        while(it.hasNext()){
            long l = it.next();
            list.add(l);
        }
        long[] array = ArrayUtils.toPrimitive((Long[])list.toArray()); // TODO: Test this !!!
        //return MyStubCursors.asRelationshipCursor(array); // won't work

        // TODO: finish this
        // Need an array of RelationshipItems
        Cursors.cursor();

        // TODO !
        return super.relationshipCursorGetAll();
    }

    @Override
    public Cursor<NodeItem> nodeCursorGetForLabel( int labelId )
    {
        // TODO !
        return super.nodeCursorGetForLabel(labelId);
    }

    @Override
    public Cursor<NodeItem> nodeCursorGetFromIndexSeek( IndexDescriptor index,
            Object value ) throws IndexNotFoundKernelException
    {
        // TODO !
        return super.nodeCursorGetFromIndexSeek(index,value);
    }

    @Override
    public Cursor<NodeItem> nodeCursorGetFromIndexScan( IndexDescriptor index ) throws IndexNotFoundKernelException
    {
        // TODO !
        return super.nodeCursorGetFromIndexScan(index);
    }

    @Override
    public Cursor<NodeItem> nodeCursorGetFromIndexRangeSeekByNumber( IndexDescriptor index,
            Number lower, boolean includeLower,
            Number upper, boolean includeUpper )
            throws IndexNotFoundKernelException
    {
        // TODO !
        return super.nodeCursorGetFromIndexRangeSeekByNumber(index,lower,includeLower,upper,includeUpper);
    }

    @Override
    public Cursor<NodeItem> nodeCursorGetFromIndexRangeSeekByString( IndexDescriptor index,
            String lower, boolean includeLower,
            String upper, boolean includeUpper )
            throws IndexNotFoundKernelException
    {
        // TODO !
        return super.nodeCursorGetFromIndexRangeSeekByString(index,lower,includeLower,upper,includeUpper);
    }

    @Override
    public Cursor<NodeItem> nodeCursorGetFromIndexRangeSeekByPrefix( IndexDescriptor index, String prefix )
            throws IndexNotFoundKernelException
    {
        // TODO !
        return super.nodeCursorGetFromIndexRangeSeekByPrefix(index,prefix);
    }

    @Override
    public Cursor<NodeItem> nodeCursorGetFromUniqueIndexSeek( IndexDescriptor index, Object value )
            throws IndexNotFoundKernelException, IndexBrokenKernelException
    {
        // TODO !
        return super.nodeCursorGetFromUniqueIndexSeek(index,value);
    }

    @Override
    public long nodesCountIndexed( IndexDescriptor index, long nodeId, Object value )
            throws IndexNotFoundKernelException, IndexBrokenKernelException
    {
        // TODO !
        return super.nodesCountIndexed(index,nodeId,value);
    }

    // </DataReadCursors>

    // <SchemaRead>
    // TODO: Are they needed?
    /*
    @Override
    public IndexDescriptor indexGetForLabelAndPropertyKey( int labelId, int propertyKeyId )
            throws SchemaRuleNotFoundException
    {
        statement.assertOpen();
        IndexDescriptor descriptor = schemaRead().indexGetForLabelAndPropertyKey( statement, labelId, propertyKeyId );
        if ( descriptor == null )
        {
            throw new IndexSchemaRuleNotFoundException( labelId, propertyKeyId );
        }
        return descriptor;
    }

    @Override
    public Iterator<IndexDescriptor> indexesGetForLabel( int labelId )
    {
        statement.assertOpen();
        return schemaRead().indexesGetForLabel( statement, labelId );
    }

    @Override
    public Iterator<IndexDescriptor> indexesGetAll()
    {
        statement.assertOpen();
        return schemaRead().indexesGetAll( statement );
    }

    @Override
    public IndexDescriptor uniqueIndexGetForLabelAndPropertyKey( int labelId, int propertyKeyId )
            throws SchemaRuleNotFoundException, DuplicateIndexSchemaRuleException

    {
        IndexDescriptor result = null;
        Iterator<IndexDescriptor> indexes = uniqueIndexesGetForLabel( labelId );
        while ( indexes.hasNext() )
        {
            IndexDescriptor index = indexes.next();
            if ( index.getPropertyKeyId() == propertyKeyId )
            {
                if ( null == result )
                {
                    result = index;
                }
                else
                {
                    throw new DuplicateIndexSchemaRuleException( labelId, propertyKeyId, true );
                }
            }
        }

        if ( null == result )
        {
            throw new IndexSchemaRuleNotFoundException( labelId, propertyKeyId, true );
        }

        return result;
    }

    @Override
    public Iterator<IndexDescriptor> uniqueIndexesGetForLabel( int labelId )
    {
        statement.assertOpen();
        return schemaRead().uniqueIndexesGetForLabel( statement, labelId );
    }

    @Override
    public Long indexGetOwningUniquenessConstraintId( IndexDescriptor index ) throws SchemaRuleNotFoundException
    {
        statement.assertOpen();
        return schemaRead().indexGetOwningUniquenessConstraintId( statement, index );
    }

    @Override
    public Iterator<IndexDescriptor> uniqueIndexesGetAll()
    {
        statement.assertOpen();
        return schemaRead().uniqueIndexesGetAll( statement );
    }

    @Override
    public InternalIndexState indexGetState( IndexDescriptor descriptor ) throws IndexNotFoundKernelException
    {
        statement.assertOpen();
        return schemaRead().indexGetState( statement, descriptor );
    }

    @Override
    public PopulationProgress indexGetPopulationProgress( IndexDescriptor descriptor ) throws IndexNotFoundKernelException
    {
        statement.assertOpen();
        return schemaRead().indexGetPopulationProgress( statement, descriptor );
    }

    @Override
    public long indexSize( IndexDescriptor descriptor ) throws IndexNotFoundKernelException
    {
        statement.assertOpen();
        return schemaRead().indexSize( statement, descriptor );
    }

    @Override
    public double indexUniqueValuesSelectivity( IndexDescriptor descriptor ) throws IndexNotFoundKernelException
    {
        statement.assertOpen();
        return schemaRead().indexUniqueValuesPercentage( statement, descriptor );
    }

    @Override
    public String indexGetFailure( IndexDescriptor descriptor ) throws IndexNotFoundKernelException
    {
        statement.assertOpen();
        return schemaRead().indexGetFailure( statement, descriptor );
    }

    @Override
    public Iterator<NodePropertyConstraint> constraintsGetForLabelAndPropertyKey( int labelId, int propertyKeyId )
    {
        statement.assertOpen();
        return schemaRead().constraintsGetForLabelAndPropertyKey( statement, labelId, propertyKeyId );
    }

    @Override
    public Iterator<NodePropertyConstraint> constraintsGetForLabel( int labelId )
    {
        statement.assertOpen();
        return schemaRead().constraintsGetForLabel( statement, labelId );
    }

    @Override
    public Iterator<RelationshipPropertyConstraint> constraintsGetForRelationshipType( int typeId )
    {
        statement.assertOpen();
        return schemaRead().constraintsGetForRelationshipType( statement, typeId );
    }

    @Override
    public Iterator<RelationshipPropertyConstraint> constraintsGetForRelationshipTypeAndPropertyKey( int typeId,
            int propertyKeyId )
    {
        statement.assertOpen();
        return schemaRead().constraintsGetForRelationshipTypeAndPropertyKey( statement, typeId, propertyKeyId );
    }

    @Override
    public Iterator<PropertyConstraint> constraintsGetAll()
    {
        statement.assertOpen();
        return schemaRead().constraintsGetAll( statement );
    }
    */

    // </SchemaRead>


    // <TokenRead>
    @Override
    public int labelGetForName( String labelName )
    {
        //TODO: Might be faster with contains?
        Iterator<Integer> it = virtualLabels.keySet().iterator();
        while(it.hasNext()){
            int key = it.next();
            if(virtualLabels.get(key).equals(labelName)){
                return key;
            }
        }

        return super.labelGetForName(labelName);
    }

    @Override
    public String labelGetName( int labelId ) throws LabelNotFoundKernelException
    {
        if(isVirtual(labelId)){
            if(virtualLabels.containsKey(labelId)){
                return virtualLabels.get(labelId);
            }
            throw new LabelNotFoundKernelException("No virtual Label found for id: "+labelId,new Exception());
        }

        return super.labelGetName(labelId);
    }

    @Override
    public int propertyKeyGetForName( String propertyKeyName )
    {
        Iterator<Integer> it = virtualPropertyIds().iterator();
        while(it.hasNext()){
            int key = it.next();
            if(virtualPropertyKeyIdsToName.get(key).equals(propertyKeyName)){
                return key;
            }
        }
        return super.propertyKeyGetForName(propertyKeyName);
    }

    @Override
    public String propertyKeyGetName( int propertyKeyId ) throws PropertyKeyIdNotFoundKernelException
    {
        if(isVirtual(propertyKeyId)){
            if(virtualPropertyIds().contains(propertyKeyId)){
                return virtualPropertyKeyIdsToName.get(propertyKeyId);
            }
            throw new PropertyKeyIdNotFoundKernelException(propertyKeyId,new Exception());
        }

        return super.propertyKeyGetName(propertyKeyId);
    }

    @Override
    public Iterator<Token> propertyKeyGetAllTokens()
    {
        Iterator<Token> realOnes = super.propertyKeyGetAllTokens();
        ArrayList<Token> virtualOnes = new ArrayList<>();

        for(int key:virtualPropertyIds()){
            String name = virtualPropertyKeyIdsToName.get(key);
            virtualOnes.add(new Token(name,key)); // TODO: Definitely need to test this
        }
        return new MergingTokenIterator(realOnes,virtualOnes);
    }

    @Override
    public Iterator<Token> labelsGetAllTokens()
    {
        Iterator<Token> realOnes = super.labelsGetAllTokens();

        ArrayList<Token> virtualOnes = new ArrayList<>();
        for(int key:virtualLabels()){
            String name = virtualLabels.get(key);
            virtualOnes.add(new Token(name,key));
        }
        return new MergingTokenIterator(realOnes,virtualOnes);
    }

    @Override
    public Iterator<Token> relationshipTypesGetAllTokens()
    {
        Iterator<Token> realOnes = super.relationshipTypesGetAllTokens();
        ArrayList<Token> virtualOnes = new ArrayList<>();
        for(int key:virtualRelationshipTypes.keySet()){
            String name = virtualRelationshipTypes.get(key);
            virtualOnes.add(new Token(name,key));
        }
        return new MergingTokenIterator(realOnes,virtualOnes);
    }

    @Override
    public int relationshipTypeGetForName( String relationshipTypeName )
    {
        // TODO: Improvements with contains?
        Iterator<Integer> it = virtualRelationshipTypes.keySet().iterator();
        while(it.hasNext()){
            int key = it.next();
            if(virtualRelationshipTypes.get(key).equals(relationshipTypeName)){
                return key;
            }
        }

        return super.relationshipTypeGetForName(relationshipTypeName);
    }

    @Override
    public String relationshipTypeGetName( int relationshipTypeId ) throws RelationshipTypeIdNotFoundKernelException
    {
        if(isVirtual(relationshipTypeId)){
            if(virtualRelationshipTypes.keySet().contains(relationshipTypeId)){
                return virtualRelationshipTypes.get(relationshipTypeId);
            }
            throw new RelationshipTypeIdNotFoundKernelException(relationshipTypeId,new Exception());
        }

        return super.relationshipTypeGetName(relationshipTypeId);
    }

    @Override
    public int labelCount()
    {
        // TODO: Solution without counting same type twice if in both collections
        return super.labelCount() + virtualLabels().size();
    }

    @Override
    public int propertyKeyCount()
    {
        // TODO: Solution without counting same type twice if in both collections
        return super.propertyKeyCount() + virtualPropertyIds().size();
    }

    @Override
    public int relationshipTypeCount()
    {
        // TODO: Solution without counting same type twice if in both collections
        return super.relationshipTypeCount() + virtualRelationshipTypes.keySet().size();
    }

    // </TokenRead>

    // <TokenWrite>
    @Override
    public int virtualLabelGetOrCreateForName( String labelName ) throws IllegalTokenNameException,
            TooManyLabelsException, NoSuchMethodException
    {
        // Try getting the labelId
        // TODO: might be faster with contains?
        Iterator<Integer> it = virtualLabels.keySet().iterator();
        while(it.hasNext()){
            int key = it.next();
            if(virtualLabels.get(key).equals(labelName)){
                return key;
            }
        }

        // not found, need to create
        int newId;
        if(virtualLabels().size()==0){
            newId = -1;
        } else{
            newId = virtualLabels.firstKey()-1;
        }
        virtualLabelCreateForName(labelName,newId);
        return newId;
    }

    @Override
    public int virtualPropertyKeyGetOrCreateForName( String propertyKeyName ) throws IllegalTokenNameException
    {
        // Try getting the proplId
        // TODO: might be faster with contains?
        Iterator<Integer> it = virtualPropertyIds().iterator();
        while(it.hasNext()){
            int key = it.next();
            if(virtualPropertyKeyIdsToName.get(key).equals(propertyKeyName)){
                return key;
            }
        }

        // not found, need to create
        int newId;
        if(virtualPropertyIds().size()==0){
            newId = -1;
        } else{
            newId = virtualPropertyKeyIdsToName.firstKey()-1;
        }
        virtualPropertyKeyCreateForName(propertyKeyName,newId);
        return newId;
    }

    @Override
    public int virtualRelationshipTypeGetOrCreateForName( String relationshipTypeName ) throws IllegalTokenNameException
    {
        // Try getting the relId
        // TODO: might be faster with contains?
        Iterator<Integer> it = virtualRelationshipTypes.keySet().iterator();
        while(it.hasNext()){
            int key = it.next();
            if(virtualRelationshipTypes.get(key).equals(relationshipTypeName)){
                return key;
            }
        }

        // not found, need to create
        int newId;
        if(virtualRelationshipTypes.size()==0){
            newId = -1;
        } else{
            newId = virtualRelationshipTypes.firstKey()-1;
        }
        virtualRelationshipTypeCreateForName(relationshipTypeName,newId);
        return newId;
    }

    @Override
    public void virtualLabelCreateForName( String labelName, int id ) throws
            IllegalTokenNameException, TooManyLabelsException
    {
        //TODO: Token name checking missing
        virtualLabels.put(id,labelName);
    }

    @Override
    public void virtualPropertyKeyCreateForName( String propertyKeyName,
            int id ) throws
            IllegalTokenNameException
    {
        //TODO: Token name checking missing
        virtualPropertyKeyIdsToName.put(id,propertyKeyName);
    }

    @Override
    public void virtualRelationshipTypeCreateForName( String relationshipTypeName,
            int id ) throws
            IllegalTokenNameException
    {
        //TODO: Token name checking missing
        virtualRelationshipTypes.put(id,relationshipTypeName);
    }

    // </TokenWrite>

    // <DataWrite>
    @Override
    public long virtualNodeCreate()
    {
        statement.assertOpen();
        long new_id;
        if(virtualNodeIds.size()==0){
            new_id = -1;
        } else {
            long smallest = virtualNodeIds.first();
            new_id = smallest - 1;
        }
        virtualNodeIds.add(new_id);
        virtualNodeIdToPropertyIds.put(new_id,new LinkedHashSet<>());
        virtualNodeIdToLabelIds.put(new_id,new LinkedHashSet<>());
        virtualNodeIdToConnectedRelationshipIds.put(new_id,new LinkedHashSet<>());

        return new_id;
    }

    @Override
    public void nodeDelete( long nodeId )
            throws EntityNotFoundException, InvalidTransactionTypeKernelException, AutoIndexingKernelException
    {
        if(isVirtual(nodeId)) {
            if (virtualNodeIds.contains(nodeId)) {
                //TODO: needs more checks!

                virtualNodeIds.remove(nodeId);

                virtualNodeIdToLabelIds.remove(nodeId);
                virtualNodeIdToPropertyIds.remove(nodeId);

                // TODO: Remove refs that are returned from those calls

                // AND rel prop

                virtualNodeIdToConnectedRelationshipIds.remove(nodeId);

            } else {
                throw new EntityNotFoundException(EntityType.NODE, nodeId);
            }
        } else{
            super.nodeDelete(nodeId);
        }
    }

    @Override
    public long relationshipCreate( int relationshipTypeId, long startNodeId, long endNodeId )
            throws RelationshipTypeIdNotFoundKernelException, EntityNotFoundException
    {
        // TODO !
        return super.relationshipCreate(relationshipTypeId,startNodeId,endNodeId);
    }

    @Override
    public void relationshipDelete( long relationshipId )
            throws EntityNotFoundException, InvalidTransactionTypeKernelException, AutoIndexingKernelException
    {
        // TODO !
        super.relationshipDelete(relationshipId);
    }

    @Override
    public boolean nodeAddLabel( long nodeId, int labelId )
            throws EntityNotFoundException, ConstraintValidationKernelException
    {
        // TODO !
        return super.nodeAddLabel(nodeId,labelId);
    }

    @Override
    public boolean nodeRemoveLabel( long nodeId, int labelId ) throws EntityNotFoundException
    {
        // TODO !
        return super.nodeRemoveLabel(nodeId,labelId);
    }

    @Override
    public Property nodeSetProperty( long nodeId, DefinedProperty property )
            throws EntityNotFoundException, ConstraintValidationKernelException, AutoIndexingKernelException, InvalidTransactionTypeKernelException
    {
        // TODO !
        return super.nodeSetProperty(nodeId,property);
    }

    @Override
    public Property relationshipSetProperty( long relationshipId, DefinedProperty property )
            throws EntityNotFoundException, AutoIndexingKernelException, InvalidTransactionTypeKernelException
    {
        // TODO !
        return super.relationshipSetProperty(relationshipId,property);
    }

    @Override
    public Property graphSetProperty( DefinedProperty property )
    {
        // TODO !
        return super.graphSetProperty(property);
    }

    @Override
    public Property nodeRemoveProperty( long nodeId, int propertyKeyId )
            throws EntityNotFoundException, AutoIndexingKernelException, InvalidTransactionTypeKernelException
    {
        // TODO !
        return super.nodeRemoveProperty(nodeId,propertyKeyId);
    }

    @Override
    public Property relationshipRemoveProperty( long relationshipId, int propertyKeyId )
            throws EntityNotFoundException, AutoIndexingKernelException, InvalidTransactionTypeKernelException
    {
        // TODO !
        return super.relationshipRemoveProperty(relationshipId,propertyKeyId);
    }

    @Override
    public Property graphRemoveProperty( int propertyKeyId )
    {
        // TODO !
        return super.graphRemoveProperty(propertyKeyId);
    }

    @Override
    public RawIterator<Object[], ProcedureException> procedureCallWrite( ProcedureName name, Object[] input ) throws ProcedureException
    {
        // TODO ?
        return super.procedureCallWrite(name,input);
    }
    // </DataWrite>

    // <SchemaWrite>
    // TODO: Are they needed?
    /*
    @Override
    public IndexDescriptor indexCreate( int labelId, int propertyKeyId )
            throws AlreadyIndexedException, AlreadyConstrainedException
    {
        statement.assertOpen();
        return schemaWrite().indexCreate( statement, labelId, propertyKeyId );
    }

    @Override
    public void indexDrop( IndexDescriptor descriptor ) throws DropIndexFailureException
    {
        statement.assertOpen();
        schemaWrite().indexDrop( statement, descriptor );
    }

    @Override
    public UniquenessConstraint uniquePropertyConstraintCreate( int labelId, int propertyKeyId )
            throws CreateConstraintFailureException, AlreadyConstrainedException, AlreadyIndexedException
    {
        statement.assertOpen();
        return schemaWrite().uniquePropertyConstraintCreate( statement, labelId, propertyKeyId );
    }

    @Override
    public NodePropertyExistenceConstraint nodePropertyExistenceConstraintCreate( int labelId, int propertyKeyId )
            throws CreateConstraintFailureException, AlreadyConstrainedException
    {
        statement.assertOpen();
        return schemaWrite().nodePropertyExistenceConstraintCreate( statement, labelId, propertyKeyId );
    }

    @Override
    public RelationshipPropertyExistenceConstraint relationshipPropertyExistenceConstraintCreate(
            int relTypeId, int propertyKeyId )
            throws CreateConstraintFailureException, AlreadyConstrainedException
    {
        statement.assertOpen();
        return schemaWrite().relationshipPropertyExistenceConstraintCreate( statement, relTypeId, propertyKeyId );
    }

    @Override
    public void constraintDrop( NodePropertyConstraint constraint ) throws DropConstraintFailureException
    {
        statement.assertOpen();
        schemaWrite().constraintDrop( statement, constraint );
    }

    @Override
    public void constraintDrop( RelationshipPropertyConstraint constraint ) throws DropConstraintFailureException
    {
        statement.assertOpen();
        schemaWrite().constraintDrop( statement, constraint );
    }

    @Override
    public void uniqueIndexDrop( IndexDescriptor descriptor ) throws DropIndexFailureException
    {
        statement.assertOpen();
        schemaWrite().uniqueIndexDrop( statement, descriptor );
    }
    */

    // </SchemaWrite>

    // There is no need for locking here

    // <Legacy index>
    @Override
    public LegacyIndexHits nodeLegacyIndexGet( String indexName, String key, Object value )
            throws LegacyIndexNotFoundKernelException
    {
        // TODO ?
        return super.nodeLegacyIndexGet(indexName,key,value);
    }

    @Override
    public LegacyIndexHits nodeLegacyIndexQuery( String indexName, String key, Object queryOrQueryObject )
            throws LegacyIndexNotFoundKernelException
    {
        // TODO ?
        return super.nodeLegacyIndexQuery(indexName,key,queryOrQueryObject);
    }

    @Override
    public LegacyIndexHits nodeLegacyIndexQuery( String indexName, Object queryOrQueryObject )
            throws LegacyIndexNotFoundKernelException
    {
        // TODO ?
        return super.nodeLegacyIndexQuery(indexName,queryOrQueryObject);
    }

    @Override
    public LegacyIndexHits relationshipLegacyIndexGet( String indexName, String key, Object value,
            long startNode, long endNode ) throws LegacyIndexNotFoundKernelException
    {
        // TODO ?
        return super.relationshipLegacyIndexGet(indexName,key,value,startNode,endNode);
    }

    @Override
    public LegacyIndexHits relationshipLegacyIndexQuery( String indexName, String key, Object queryOrQueryObject,
            long startNode, long endNode ) throws LegacyIndexNotFoundKernelException
    {
        // TODO ?
        return super.relationshipLegacyIndexQuery(indexName,key,queryOrQueryObject,startNode,endNode);
    }

    @Override
    public LegacyIndexHits relationshipLegacyIndexQuery( String indexName, Object queryOrQueryObject,
            long startNode, long endNode ) throws LegacyIndexNotFoundKernelException
    {
        // TODO ?
        return super.relationshipLegacyIndexQuery(indexName,queryOrQueryObject,startNode,endNode);
    }

    @Override
    public void nodeLegacyIndexCreateLazily( String indexName, Map<String,String> customConfig )
    {
        // TODO ?
        super.nodeLegacyIndexCreateLazily(indexName,customConfig);
    }

    @Override
    public void nodeLegacyIndexCreate( String indexName, Map<String,String> customConfig )
    {
        // TODO ?
        super.nodeLegacyIndexCreate(indexName,customConfig);
    }

    @Override
    public void relationshipLegacyIndexCreateLazily( String indexName, Map<String,String> customConfig )
    {
        // TODO ?
        super.relationshipLegacyIndexCreateLazily(indexName,customConfig);
    }

    @Override
    public void relationshipLegacyIndexCreate( String indexName, Map<String,String> customConfig )
    {
        // TODO ?
        super.relationshipLegacyIndexCreate(indexName,customConfig);
    }

    @Override
    public void nodeAddToLegacyIndex( String indexName, long node, String key, Object value )
            throws EntityNotFoundException, LegacyIndexNotFoundKernelException
    {
        // TODO ?
        super.nodeAddToLegacyIndex(indexName,node,key,value);
    }

    @Override
    public void nodeRemoveFromLegacyIndex( String indexName, long node, String key, Object value )
            throws LegacyIndexNotFoundKernelException
    {
        // TODO ?
        super.nodeRemoveFromLegacyIndex(indexName,node,key,value);
    }

    @Override
    public void nodeRemoveFromLegacyIndex( String indexName, long node, String key )
            throws LegacyIndexNotFoundKernelException
    {
        // TODO ?
        super.nodeRemoveFromLegacyIndex(indexName,node,key);
    }

    @Override
    public void nodeRemoveFromLegacyIndex( String indexName, long node ) throws LegacyIndexNotFoundKernelException
    {
        // TODO ?
        super.nodeRemoveFromLegacyIndex(indexName,node);
    }

    @Override
    public void relationshipAddToLegacyIndex( String indexName, long relationship, String key, Object value )
            throws EntityNotFoundException, LegacyIndexNotFoundKernelException
    {
        // TODO ?
        super.relationshipAddToLegacyIndex(indexName,relationship,key,value);
    }

    @Override
    public void relationshipRemoveFromLegacyIndex( String indexName, long relationship, String key, Object value )
            throws EntityNotFoundException, LegacyIndexNotFoundKernelException
    {
        // TODO ?
        super.relationshipRemoveFromLegacyIndex(indexName,relationship,key,value);
    }

    @Override
    public void relationshipRemoveFromLegacyIndex( String indexName, long relationship, String key )
            throws LegacyIndexNotFoundKernelException, EntityNotFoundException
    {
        // TODO ?
        super.relationshipRemoveFromLegacyIndex(indexName,relationship,key);
    }

    @Override
    public void relationshipRemoveFromLegacyIndex( String indexName, long relationship )
            throws LegacyIndexNotFoundKernelException, EntityNotFoundException
    {
        // TODO ?
        super.relationshipRemoveFromLegacyIndex(indexName,relationship);
    }

    @Override
    public void nodeLegacyIndexDrop( String indexName ) throws LegacyIndexNotFoundKernelException
    {
        // TODO ?
        super.nodeLegacyIndexDrop(indexName);
    }

    @Override
    public void relationshipLegacyIndexDrop( String indexName ) throws LegacyIndexNotFoundKernelException
    {
        // TODO ?
        super.relationshipLegacyIndexDrop(indexName);
    }

    @Override
    public Map<String,String> nodeLegacyIndexGetConfiguration( String indexName )
            throws LegacyIndexNotFoundKernelException
    {
        // TODO ?
        return super.nodeLegacyIndexGetConfiguration(indexName);
    }

    @Override
    public Map<String,String> relationshipLegacyIndexGetConfiguration( String indexName )
            throws LegacyIndexNotFoundKernelException
    {
        // TODO ?
        return super.relationshipLegacyIndexGetConfiguration(indexName);
    }

    @Override
    public String nodeLegacyIndexSetConfiguration( String indexName, String key, String value )
            throws LegacyIndexNotFoundKernelException
    {
        // TODO ?
        return super.nodeLegacyIndexSetConfiguration(indexName,key,value);
    }

    @Override
    public String relationshipLegacyIndexSetConfiguration( String indexName, String key, String value )
            throws LegacyIndexNotFoundKernelException
    {
        // TODO ?
        return super.relationshipLegacyIndexSetConfiguration(indexName,key,value);
    }

    @Override
    public String nodeLegacyIndexRemoveConfiguration( String indexName, String key )
            throws LegacyIndexNotFoundKernelException
    {
        // TODO ?
        return super.nodeLegacyIndexRemoveConfiguration(indexName,key);
    }

    @Override
    public String relationshipLegacyIndexRemoveConfiguration( String indexName, String key )
            throws LegacyIndexNotFoundKernelException
    {
        // TODO ?
        return super.relationshipLegacyIndexRemoveConfiguration(indexName,key);
    }

    @Override
    public String[] nodeLegacyIndexesGetAll()
    {
        // TODO ?
        return super.nodeLegacyIndexesGetAll();
    }

    @Override
    public String[] relationshipLegacyIndexesGetAll()
    {
        // TODO ?
        return super.relationshipLegacyIndexesGetAll();
    }
    // </Legacy index>

    // <Counts>

    @Override
    public long countsForNode( int labelId )
    {
        // TODO !!!
        return super.countsForNode(labelId);
    }

    @Override
    public long countsForNodeWithoutTxState( int labelId )
    {
        // TODO ?
        return super.countsForNodeWithoutTxState(labelId);
    }

    @Override
    public long countsForRelationship( int startLabelId, int typeId, int endLabelId )
    {
        // TODO !
        return super.countsForRelationship(startLabelId,typeId,endLabelId);
    }

    @Override
    public long countsForRelationshipWithoutTxState( int startLabelId, int typeId, int endLabelId )
    {
        // TODO ?
        return super.countsForRelationshipWithoutTxState(startLabelId,typeId,endLabelId);
    }

    @Override
    public DoubleLongRegister indexUpdatesAndSize( IndexDescriptor index, DoubleLongRegister target )
            throws IndexNotFoundKernelException
    {
        // TODO ?
        return super.indexUpdatesAndSize(index,target);
    }

    @Override
    public DoubleLongRegister indexSample( IndexDescriptor index, DoubleLongRegister target )
            throws IndexNotFoundKernelException
    {
        // TODO ?
        return super.indexSample(index,target);
    }

    // </Counts>
    
    private boolean isVirtual(long entityId){
        return entityId<0;
    }

    private Set<Long> virtualRelationshipIds(){
        return virtualRelationshipToTypeId.keySet();
    }

    private Set<Integer> virtualPropertyIds(){
        return virtualPropertyKeyIdsToName.keySet();
    }

    private Set<Integer> virtualLabels(){
        return virtualLabels.keySet();
    }

    private Object getPropertyValueForNodes(long nodeId, int propertykey){
        Iterator<PropertyValueId> it = virtualPropertyIdToValueForNodes.keySet().iterator();
        while(it.hasNext()){
            PropertyValueId pId = it.next();
            if(pId.getEntityId()==nodeId && pId.getPropertyKeyId()==propertykey){
                // success!
                return virtualPropertyIdToValueForNodes.get(pId);
            }
        }
        return null;
    }

    private Object getPropertyValueForRels(long relId, int propertykey){
        Iterator<PropertyValueId> it =virtualPropertyIdToValueForRels.keySet().iterator();
        while(it.hasNext()){
            PropertyValueId pId = it.next();
            if(pId.getEntityId()==relId && pId.getPropertyKeyId()==propertykey){
                // success!
                return virtualPropertyIdToValueForRels.get(pId);
            }
        }
        return null;
    }
}
