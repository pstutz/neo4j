package saschapeukert;

import org.neo4j.graphdb.DependencyResolver;
import org.neo4j.graphdb.Result;
import org.neo4j.kernel.api.KernelTransaction;
import org.neo4j.kernel.api.exceptions.ProcedureException;
import org.neo4j.kernel.api.exceptions.schema.IllegalTokenNameException;
import org.neo4j.kernel.api.exceptions.schema.TooManyLabelsException;
import org.neo4j.kernel.impl.api.ViewOperationsFacade;
import org.neo4j.kernel.internal.GraphDatabaseAPI;
import org.neo4j.procedure.Context;
import org.neo4j.procedure.Description;
import org.neo4j.procedure.Name;
import org.neo4j.procedure.Procedure;

import java.util.*;
import java.util.stream.Stream;

import static java.lang.String.format;
import static java.lang.String.join;
import static java.util.stream.Collectors.toList;
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

    //@Context
    //public UserManager userManager;


    @Description( "THIS NEEDS SOME DESCRIPTION LATER" )
    @Procedure( name = "db.viewTest", mode = READ )
    public Stream<OutputStringResult> viewTest(@Name( "index" ) String index)
            throws ProcedureException
    {
        ArrayList<OutputStringResult> ar = new ArrayList<>();
        OutputStringResult a = new OutputStringResult();
        a.Message = index;
        ar.add(a);

        ViewOperationsFacade v = (ViewOperationsFacade) tx.acquireStatement().readOperations(); // not that elegant...

        a = new OutputStringResult();
        a.Message = String.valueOf(v.nodeExists(0l));
        ar.add(a);

        ViewDefinition def = ViewController.getInstance().getView(index);

        Result r = graphDatabaseAPI.execute(def.getIdQuery());
        List<Long> list = (List<Long>) r.next().values().iterator().next();
        Set<Long> set = new HashSet<Long>(list);

        a = new OutputStringResult();
        a.Message = "Result of IdQuery: " + set.toString();
        ar.add(a);

        Set<String> labelFilter = def.getLabels();
        Set<String> relFilter = def.getRelTypes();
        Iterator<String> labelIt =labelFilter.iterator();
        Iterator<String> relIt = relFilter.iterator();

        ArrayList<Integer> labelIds = new ArrayList<>();
        ArrayList<Integer> typeIds = new ArrayList<>();

        while(labelIt.hasNext()){
            String label = labelIt.next();
            int labelId = v.labelGetForName(label);  // == -1 if No such label (could be virtual!!!) //TODO
            try {
                v.labelGetOrCreateForName(label); // Alternative method!
                v.virtualLabelGetOrCreateForName(label);
            } catch (IllegalTokenNameException e) {
                e.printStackTrace();
            } catch (TooManyLabelsException e) {
                e.printStackTrace();
            } catch (NoSuchMethodException e) {
                e.printStackTrace();
            }
            labelIds.add(labelId);
        }

        //TODO: Test this
        a = new OutputStringResult();
        a.Message = "LabelIds: "+ labelIds.toString();
        ar.add(a);

        while(relIt.hasNext()){
            String relType = relIt.next();
            int relId = v.relationshipTypeGetForName(relType); // == -1 if No such label (could be virtual!!!) //TODO, alternatives like above

            typeIds.add(relId);
        }

        a = new OutputStringResult();
        a.Message = "RelTypeIds: " + typeIds.toString();
        ar.add(a);


        Result userResult = graphDatabaseAPI.execute("CALL dbms.security.showCurrentUser()");
        String name = (String)userResult.next().get("username");

        a = new OutputStringResult();
        a.Message = "Username: " +name;
        ar.add(a);



        return ar.stream();
    }

    @Procedure( name = "db.useView", mode = WRITE )
    @Description("THIS NEEDS SOME DESCRIPTION LATER")
    public void useView(@Name("views") List<String> views) {
        // check if all views exist

        List<ViewDefinition> viewDefs = new ArrayList<>();
        String error = "";

        for(String view :views){

            ViewDefinition v = ViewController.getInstance().getView(view);
            if(v==null){
                // no view with this name found
                error += "\n No View with the name '" + view + "' found. ";
            } else{
                viewDefs.add(v);
            }
        }
        if(error.length()>0){
            //  Error!
            error += "\n\nExecution aborted.";
            throw new NoSuchElementException(error);
            //return result.stream();
        }

        // Filter!

        ViewOperationsFacade facade = (ViewOperationsFacade) tx.acquireStatement().readOperations();

        Set<Long> nodeIdSet = new HashSet<>();
        Set<Long> relIdSet = new HashSet<>();

        String[] names = new String[viewDefs.size()];
        int i = 0;
        for(ViewDefinition v:viewDefs){

            Collection<Long> n_set;
            Collection<Long> colRel;

            // is this view cached?
            List<Collection<Long>> list = facade.getCachedView(v.name);
            if(list==null){
                // is not cached yet -> execute IdQuery
                String idqueryString = v.getIdQuery();
                Result resultIdQuery = graphDatabaseAPI.execute(idqueryString);

                Map<String, Object> idQueryMap = resultIdQuery.next();

                n_set = (Collection<Long>) idQueryMap.get("nodeIds");
                colRel = (Collection<Long>) idQueryMap.get("relIds");

                // saving this
                list = new ArrayList<>();
                list.add(n_set);
                list.add(colRel);
                facade.cacheView(v.name,list);

            }

            names[i] = v.name;
            i++;

        }
        facade.clearNodeIdFilter();
        facade.clearRelationshipIdFilter();

        facade.enableViews(names);

    }

    @Procedure( name = "db.clearViews", mode = READ )
    @Description("THIS NEEDS SOME DESCRIPTION LATER")
    public void clearViews() {

        ViewOperationsFacade facade = (ViewOperationsFacade) tx.acquireStatement().readOperations();
        facade.clearNodeIdFilter();
        facade.clearRelationshipIdFilter();
    }

    /*
    @Procedure( name = "db.runOnView", mode = WRITE )
    @Description("THIS NEEDS SOME DESCRIPTION LATER")
    public Stream<MapResult> runOnView(@Name("view") String view,@Name("cypher") String statement, @Name("params") Map<String, Object> params) {
        // check if view exists
        ViewDefinition v = ViewController.getInstance().getView(view);
        if(v==null){
            // no view with this name found
            HashMap<String,Object> error = new HashMap<>();
            error.put("Error","No View with the name '" + view + "' found. Execution aborted");
            List<MapResult> result = new ArrayList<>();
            result.add(new MapResult(error));
            return result.stream();
        }

        // Filter!

        ViewOperationsFacade facade = (ViewOperationsFacade) tx.acquireStatement().readOperations();
        String idqueryString = v.getIdQuery();

        Result resultIdQuery = graphDatabaseAPI.execute(idqueryString);

        /*
        if(idqueryString.toLowerCase().startsWith("call db.runonview(")){

            while(resultIdQuery.hasNext()){
                Map<String, Object> idQueryMap = resultIdQuery.next();
            }

        } else {
        */ /*
            Map<String, Object> idQueryMap = resultIdQuery.next();

            Set<Long> nodeIdSet = new HashSet<>();
            nodeIdSet.addAll((Collection<Long>) idQueryMap.get("nodeIds"));
            // TODO: Problem: "CALL db.runOnView('First','MATCH (n) RETURN n',null)" returns value
            Set<Long> relIdSet = new HashSet<>();

            Collection<Long> colRel = (Collection<Long>) idQueryMap.get("relIds");
            if (colRel != null) {
                relIdSet.addAll(colRel);
            }
            facade.nodeIdFilter.addAll(nodeIdSet);
            facade.relIdFilter.addAll(relIdSet);
        //}

        // execute query

        if (params == null) params = Collections.emptyMap();
        Stream<MapResult> result = graphDatabaseAPI.execute(withParamMapping(statement, params.keySet()), params).stream().map(MapResult::new);

        // TODO: Test this behaviour in combination with nesting
        facade.nodeIdFilter.clear();
        facade.nodeIdFilter.clear();
        return result;
    }
    */

    @Description( "THIS NEEDS SOME DESCRIPTION LATER" )
    @Procedure( name = "db.createView", mode = WRITE )
    public Stream<OutputStringResult> createView(@Name( "name" ) String name,
                                                 @Name( "query" ) String query,
                                                 @Name("savedNodes") List<String> nodeBindings,
                                                 @Name("savedRelationships") List<String> relBindings
                                    )
            throws ProcedureException
    {

        ViewDefinition result = new ViewDefinition();
        result.name = name;
        result.setQuery( query);
        result.savedNodes = nodeBindings;
        result.savedRelationships = relBindings;

        // TODO: input validation

        ViewDefinition old = ViewController.getInstance().addView(result);
        OutputStringResult o = new OutputStringResult();
        if(old!=null){
            // overwrite
            o.Message = "Replaced old view definition for '" + old.name + "': (query: '" + old.query + "' , savedNodes: " +
                    old.savedNodes.toString() + ", savedRelationships: " + old.savedRelationships.toString() + " )";
        } else {
            o.Message = "Created view '" + result.name + "'";
        }


        ArrayList<OutputStringResult> returnList = new ArrayList<>();
        returnList.add(o);

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
    public Stream<OutputStringResult> removeView(@Name( "name" ) String name)
            throws ProcedureException
    {
        ViewDefinition old = ViewController.getInstance().removeView(name);
        OutputStringResult o = new OutputStringResult();
        if(old!=null){
            // overwrite
            o.Message = "Removed view defintion for '" + old.name + "': (query: '" + old.query + "' , savedNodes: " +
                    old.savedNodes.toString() + ", savedRelationships: " + old.savedRelationships.toString() + " ).";
        } else {
            o.Message = "No views with name '"+ name + "' registered.";
        }

        ArrayList<OutputStringResult> returnList = new ArrayList<>();
        returnList.add(o);

        return returnList.stream();
    }

    // Borrowed from apoc repo
    public static String withParamMapping(String fragment, Collection<String> keys) {
        if (keys.isEmpty()) return fragment;
        String declaration = " WITH " + join(", ", keys.stream().map(s -> format(" {`%s`} as `%s` ", s, s)).collect(toList()));
        return declaration + fragment;
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

    public class OutputStringResult {
        public String Message;
    }
}
