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

import java.util.Arrays;

import org.neo4j.internal.kernel.api.LabelSet;

class Labels implements LabelSet
{
    /**
     * This really only needs to be {@code int[]}, but the underlying implementation uses {@code long[]} for some
     * reason.
     */
    private final long[] labels;

    Labels( long[] labels )
    {
        this.labels = labels;
    }

    @Override
    public int numberOfLabels()
    {
        return labels.length;
    }

    @Override
    public int label( int offset )
    {
        return (int) labels[offset];
    }

    @Override
    public boolean contains( int labelToken )
    {
        for ( long label : labels )
        {
            if ( label == labelToken )
            {
                return true;
            }
        }
        return false;
    }

    @Override
    public String toString()
    {
        return "Labels" + Arrays.toString( labels );
    }
}
