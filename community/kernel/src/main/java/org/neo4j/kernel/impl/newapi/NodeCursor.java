/*
 * Copyright (c) 2002-2017 "Neo Technology,"
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
package org.neo4j.kernel.impl.newapi;

import org.neo4j.internal.kernel.api.LabelSet;
import org.neo4j.internal.kernel.api.PropertyCursor;
import org.neo4j.internal.kernel.api.RelationshipGroupCursor;
import org.neo4j.internal.kernel.api.RelationshipTraversalCursor;
import org.neo4j.io.pagecache.PageCursor;
import org.neo4j.kernel.impl.store.NodeLabelsField;
import org.neo4j.kernel.impl.store.RecordCursor;
import org.neo4j.kernel.impl.store.record.DynamicRecord;
import org.neo4j.kernel.impl.store.record.NodeRecord;

import static org.neo4j.kernel.impl.newapi.References.setDirectFlag;
import static org.neo4j.kernel.impl.newapi.References.setGroupFlag;

class NodeCursor extends NodeRecord implements org.neo4j.internal.kernel.api.NodeCursor
{
    private Read read;
    private RecordCursor<DynamicRecord> labelCursor;
    private PageCursor pageCursor;
    private long next;
    private long highMark;

    NodeCursor()
    {
        super( NO_ID );
    }

    void scan( Read read )
    {
        if ( getId() != NO_ID )
        {
            reset();
        }
        if ( pageCursor == null )
        {
            pageCursor = read.nodePage( 0 );
        }
        this.next = 0;
        this.highMark = read.nodeHighMark();
        this.read = read;
        this.labelCursor = read.labelCursor();
    }

    void single( long reference, Read read )
    {
        if ( getId() != NO_ID )
        {
            reset();
        }
        if ( pageCursor == null )
        {
            pageCursor = read.nodePage( reference );
        }
        this.next = reference;
        this.highMark = NO_ID;
        this.read = read;
        this.labelCursor = read.labelCursor();
    }

    @Override
    public long nodeReference()
    {
        return getId();
    }

    @Override
    public LabelSet labels()
    {
        return new Labels( NodeLabelsField.get( this, labelCursor ) );
    }

    @Override
    public boolean hasProperties()
    {
        return nextProp != (long) NO_ID;
    }

    @Override
    public void relationships( RelationshipGroupCursor cursor )
    {
        read.relationshipGroups( nodeReference(), relationshipGroupReference(), cursor );
    }

    @Override
    public void allRelationships( RelationshipTraversalCursor cursor )
    {
        read.relationships( nodeReference(), allRelationshipsReference(), cursor );
    }

    @Override
    public void properties( PropertyCursor cursor )
    {
        read.nodeProperties( propertiesReference(), cursor );
    }

    @Override
    public long relationshipGroupReference()
    {
        return isDense() ? getNextRel() : setDirectFlag( getNextRel() );
    }

    @Override
    public long allRelationshipsReference()
    {
        return isDense() ? setGroupFlag( getNextRel() ) : getNextRel();
    }

    @Override
    public long propertiesReference()
    {
        return getNextProp();
    }

    @Override
    public boolean next()
    {
        if ( next == NO_ID )
        {
            reset();
            return false;
        }
        do
        {
            read.node( this, next++, pageCursor );
            if ( next > highMark )
            {
                if ( highMark == NO_ID )
                {
                    next = NO_ID;
                    return inUse();
                }
                else
                {
                    highMark = read.nodeHighMark();
                    if ( next > highMark )
                    {
                        next = NO_ID;
                        return inUse();
                    }
                }
            }
        }
        while ( !inUse() );
        return true;
    }

    @Override
    public boolean shouldRetry()
    {
        return false;
    }

    @Override
    public void close()
    {
        if ( pageCursor != null )
        {
            pageCursor.close();
            pageCursor = null;
        }

        if ( labelCursor != null )
        {
            labelCursor.close();
            labelCursor = null;
        }
        reset();
    }

    @Override
    public boolean isClosed()
    {
        return pageCursor == null;
    }

    private void reset()
    {
        next = NO_ID;
        setId( NO_ID );
    }

    @Override
    public String toString()
    {
        if ( isClosed() )
        {
            return "NodeCursor[closed state]";
        }
        else
        {
            return "NodeCursor[id=" + getId() + ", open state with: highMark=" + highMark + ", next=" + next + ", underlying record=" + super.toString() + " ]";
        }
    }
}
