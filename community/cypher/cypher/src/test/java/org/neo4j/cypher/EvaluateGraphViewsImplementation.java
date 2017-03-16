package org.neo4j.cypher;

import org.apache.commons.lang3.time.StopWatch;
import org.apache.commons.math3.stat.descriptive.rank.Median;
import org.junit.Test;
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Result;
import org.neo4j.graphdb.Transaction;
import org.neo4j.test.TestGraphDatabaseFactory;

import java.util.HashMap;
import java.util.Map;

/**
 * Created by Sascha Peukert on 12.03.2017.
 */
public class EvaluateGraphViewsImplementation {
    // not the best place for measurements, but it works for now...
    private int size = 21; //  > 1  !
    private int max = 600000;

    @Test
    public void MeasureCreatingVirtualEntities() throws Exception
    {
        GraphDatabaseService graphDb;
        StopWatch watch = new StopWatch();
        Map<String,Object> params = new HashMap<>();

        System.out.println("Virtual Entities");
        for(int i=1;i<max;i=i*2) {
            params.put("1", i);

            Median median = new Median();
            double[] values = new double[size];
            int p = 0;
            for(int j=0;j<size;j++){
            // VE
                graphDb = new TestGraphDatabaseFactory().newImpermanentDatabase();
                try ( Transaction tx = graphDb.beginTx() )
                {
                    // preparing labels and types
                    Result r = graphDb.execute( "CREATE (a:User {id:0})-[:IS_SAME_AS]->(a)" );

                    tx.success();
                }

                try (Transaction tx = graphDb.beginTx()) {

                    watch.start();
                    graphDb.execute("FOREACH (r IN range(1,{1}) | \n" +
                        "  CREATE VIRTUAL (b:User {id:r})-[:IS_SAME_AS]->(b)\n" +
                        ");", params);
                    watch.stop();
                    values[p] = (double) watch.getTime();
                    p++;
                    watch.reset();
                    tx.success();  // implicit cleanup
                }

                graphDb.shutdown();
            }
            double middle = 0;
            for(int j=1;j<size;j++){
                // ignore the first run because of jvm stuff
                middle = middle +values[j];
            }
            middle = middle / size-1; // ignore first run

            System.out.println(i+" " +median.evaluate(values) + " " + middle );

        }

    }
}
