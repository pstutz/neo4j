package saschapeukert;

import java.util.List;

/**
 * Created by Sascha Peukert on 18.12.2016.
 */
public class ViewDefinition {

    public String name;
    public String query;
    public List<String> savedNodes;
    public List<String> savedRelationships;

    public String getIdQuery(){
        //TODO: This query might include duplicate ids

        query = query + " RETURN collect(id("; //+
        boolean first = true;
        for(String var:savedNodes){
            if(first){
                first = false;
                query = query + var +"))";
            } else{
                query = query + "+ collect(id("+ var +"))";
            }

        }
        query = query+ " AS nodeIds";
        return query;
    }
}
