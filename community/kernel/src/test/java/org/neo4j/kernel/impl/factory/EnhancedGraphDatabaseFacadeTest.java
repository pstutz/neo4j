package org.neo4j.kernel.impl.factory;

import org.junit.Test;
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.Transaction;
import org.neo4j.helpers.collection.Iterables;
import org.neo4j.test.TestGraphDatabaseFactory;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

/**
 * Created by Sascha Peukert on 06.08.2016.
 */
public class EnhancedGraphDatabaseFacadeTest {

    @Test
    public void shouldCreateVirtualNodesThatOnlyExistForThisTransaction() throws Exception
    {
        GraphDatabaseService graphDb = new TestGraphDatabaseFactory().newImpermanentDatabase();

        long before, after;
        try ( Transaction tx = graphDb.beginTx() )
        {
            before = Iterables.count( graphDb.getAllNodes() );
            tx.success();
        }

        try ( Transaction tx = graphDb.beginTx() )
        {
            Node n = graphDb.createVirtualNode();
            assertTrue("Id of an virtual node should be negative",n.getId()<0);
            after = Iterables.count( graphDb.getAllNodes() );
            tx.success();
        }
        assertEquals("Virtual node should appear in transaction", before+1, after );

        try ( Transaction tx = graphDb.beginTx() )
        {
            after = Iterables.count( graphDb.getAllNodes() );
            tx.success();
        }
        assertEquals("Virtual node should only appear in transaction it was created in", before, after );

    }
}
