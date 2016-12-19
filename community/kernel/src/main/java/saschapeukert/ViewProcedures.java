package saschapeukert;

import org.neo4j.graphdb.DependencyResolver;
import org.neo4j.kernel.api.KernelTransaction;
import org.neo4j.kernel.api.exceptions.ProcedureException;
import org.neo4j.kernel.impl.api.VirtualOperationsFacade;
import org.neo4j.kernel.internal.GraphDatabaseAPI;
import org.neo4j.procedure.Context;
import org.neo4j.procedure.Description;
import org.neo4j.procedure.Name;
import org.neo4j.procedure.Procedure;

import java.util.ArrayList;
import java.util.List;
import java.util.Set;
import java.util.stream.Stream;

import static org.neo4j.procedure.Mode.READ;
import static org.neo4j.procedure.Mode.WRITE;

/**
 * Created by Sascha Peukert on 18.12.2016.
 */
public class ViewProcedures {


    @Context
    public KernelTransaction tx;

    @Context
    public DependencyResolver resolver;

    @Context
    public GraphDatabaseAPI graphDatabaseAPI;


    @Description( "THIS NEEDS SOME DESCRIPTION LATER" )
    @Procedure( name = "db.viewTest", mode = READ )
    public Stream<Output> viewTest( @Name( "index" ) String index)
            throws ProcedureException
    {
        ArrayList<Output> ar = new ArrayList<>();
        Output a = new Output();
        a.Message = "OK";
        ar.add(a);
        a = new Output();
        a.Message = index;
        ar.add(a);

        VirtualOperationsFacade v = (VirtualOperationsFacade) tx.acquireStatement().readOperations();

        a = new Output();
        a.Message = String.valueOf(v.nodeExists(0l));
        ar.add(a);

        ViewDefinition def = ViewController.getInstance().getView(index);

        //graphDatabaseAPI.execute()

        return ar.stream();
    }

    @Description( "THIS NEEDS SOME DESCRIPTION LATER" )
    @Procedure( name = "db.createView", mode = WRITE )
    public Stream<Output> createView(@Name( "name" ) String name,
                                             @Name( "query" ) String query,
                                             @Name("savedNodes") List<String> nodeBindings,
                                             @Name("savedRelationships") List<String> relBindings
                                    )
            throws ProcedureException
    {

        ViewDefinition result = new ViewDefinition();
        result.name = name;
        result.query = query;
        result.savedNodes = nodeBindings;
        result.savedRelationships = relBindings;

        // TODO: input validation

        ViewDefinition old = ViewController.getInstance().addView(result);
        Output o = new Output();
        if(old!=null){
            // overwrite
            o.Message = "Replaced old view definition for '" + old.name + "': (query: '" + old.query + "' , savedNodes: " +
                    old.savedNodes.toString() + ", savedRelationships: " + old.savedRelationships.toString() + " )";
        } else {
            o.Message = "Created view " + result.name;
        }


        ArrayList<Output> returnList = new ArrayList<>();
        returnList.add(o);
        //VirtualOperationsFacade f =
        //(graphDatabaseAPI).getDependencyResolver()
        //        .resolveDependency(VirtualOperationsFacade.class);
        // just testing this

        //System.Message.println(f.nodeExists(0l));


        return returnList.stream();
    }

    @Description( "THIS NEEDS SOME DESCRIPTION LATER" )
    @Procedure( name = "db.getAllViewDefinitions", mode = READ )
    public Stream<ViewDefinition> getAllViewDefinitions()
            throws ProcedureException
    {
        // better for debugging than actual use?
        ArrayList<ViewDefinition> returnList = new ArrayList<>();
        ViewController singleton = ViewController.getInstance();
        Set<String> names = ViewController.getInstance().getAllViewNames();

        for(String s:names) {
            returnList.add(singleton.getView(s));
        }
        return returnList.stream();
    }

    @Description( "THIS NEEDS SOME DESCRIPTION LATER" )
    @Procedure( name = "db.removeView", mode = WRITE )
    public Stream<Output> removeView(@Name( "name" ) String name)
            throws ProcedureException
    {
        ViewDefinition old = ViewController.getInstance().removeView(name);
        Output o = new Output();
        if(old!=null){
            // overwrite
            o.Message = "Removed view defintion for '" + old.name + "': (query: '" + old.query + "' , savedNodes: " +
                    old.savedNodes.toString() + ", savedRelationships: " + old.savedRelationships.toString() + " ).";
        } else {
            o.Message = "No views with name '"+ name + "' registered.";
        }

        ArrayList<Output> returnList = new ArrayList<>();
        returnList.add(o);

        return returnList.stream();
    }


    /*@Description( "Wait for an index to come online (for example: CALL db.awaitIndex(\":Person(name)\"))." )
    @Procedure( name = "db.awaitIndex", mode = READ )
    public void awaitIndex( @Name( "index" ) String index,
                            @Name( value = "timeOutSeconds", defaultValue = "300" ) long timeout )
            throws ProcedureException
    {
        try ( IndexProcedures indexProcedures = indexProcedures() )
        {
            indexProcedures.awaitIndex( index, timeout, TimeUnit.SECONDS );
        }
    }

    @Description( "List all indexes in the database." )
    @Procedure( name = "db.indexes", mode = READ )
    public Stream<BuiltInProcedures.IndexResult> listIndexes() throws ProcedureException
    {
        try ( Statement statement = tx.acquireStatement() )
        {
            ReadOperations operations = statement.readOperations();
            TokenNameLookup tokens = new StatementTokenNameLookup( operations );

            List<IndexDescriptor> indexes =
                    asList( operations.indexesGetAll() );

            Set<IndexDescriptor> uniqueIndexes = asSet( operations.uniqueIndexesGetAll() );
            indexes.addAll( uniqueIndexes );
            indexes.sort( ( a, b ) -> a.userDescription( tokens ).compareTo( b.userDescription( tokens ) ) );

            ArrayList<BuiltInProcedures.IndexResult> result = new ArrayList<>();
            for ( IndexDescriptor index : indexes )
            {
                try
                {
                    String type;
                    if ( uniqueIndexes.contains( index ) )
                    {
                        type = BuiltInProcedures.IndexType.NODE_UNIQUE_PROPERTY.typeName();
                    }
                    else
                    {
                        type = BuiltInProcedures.IndexType.NODE_LABEL_PROPERTY.typeName();
                    }

                    result.add( new BuiltInProcedures.IndexResult( "INDEX ON " + index.userDescription( tokens ),
                            operations.indexGetState( index ).toString(), type ) );
                }
                catch ( IndexNotFoundKernelException e )
                {
                    throw new ProcedureException( Status.Schema.IndexNotFound, e,
                            "No index on ", index.userDescription( tokens ) );
                }
            }
            return result.stream();
        }
    }

    */

    public class Output {
        public String Message;
    }
}
