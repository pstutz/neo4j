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
package org.neo4j.cypher;

import org.junit.Test;
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Result;
import org.neo4j.graphdb.Transaction;
import org.neo4j.helpers.collection.Iterables;
import org.neo4j.test.TestGraphDatabaseFactory;

import static org.junit.Assert.assertEquals;

public class GraphDatabaseServiceExecuteTest
{
    @Test
    public void shouldExecuteCypher() throws Exception
    {
        // given
        GraphDatabaseService graphDb = new TestGraphDatabaseFactory().newImpermanentDatabase();
        final long before, after;
        try ( Transaction tx = graphDb.beginTx() )
        {
            before = Iterables.count( graphDb.getAllNodes() );
            tx.success();
        }

        // when
        Result r = graphDb.execute( "CREATE (n:Foo{bar:\"baz\"})" );

        // then
        try ( Transaction tx = graphDb.beginTx() )
        {
            after = Iterables.count( graphDb.getAllNodes() );
            tx.success();
        }
        assertEquals( before + 1, after );
    }

    @Test
    public void shouldExecuteCypherWithVirtualNode() throws Exception
    {
        // given
        GraphDatabaseService graphDb = new TestGraphDatabaseFactory().newImpermanentDatabase();
        final long before, after;
        try ( Transaction tx = graphDb.beginTx() )
        {
            before = Iterables.count( graphDb.getAllNodes() );
            tx.success();
        }

        // when


        try ( Transaction tx = graphDb.beginTx() )
        {
            Result r = graphDb.execute( "CREATE (n:Foo{virtual:\"baz\"}) RETURN n.virtual" );
            assertEquals("{n.virtual=baz}",r.next().toString());

            r = graphDb.execute("MATCH (n:Foo) RETURN n.virtual, COUNT(n)");
            System.out.println(r.next().toString());
        }

        // then
        try ( Transaction tx = graphDb.beginTx() )
        {
            after = Iterables.count( graphDb.getAllNodes() );
            tx.success();
        }
        assertEquals( before , after );
    }
}
