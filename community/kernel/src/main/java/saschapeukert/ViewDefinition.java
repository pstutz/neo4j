package saschapeukert;

import java.util.HashSet;
import java.util.List;
import java.util.Set;

/**
 * Created by Sascha Peukert on 18.12.2016.
 */
public class ViewDefinition {

    public String name;
    public String query;
    public List<String> savedNodes;
    public List<String> savedRelationships;

    public void setQuery(String q){
        String remove = "  ";
        String instead = " ";

        int idx = q.toLowerCase().indexOf(remove.toLowerCase());
        while(idx!=-1){
            q = q.substring(0,idx) + instead +q.substring(idx+remove.length());
            idx = q.toLowerCase().indexOf(remove.toLowerCase());
        }

        query = q; // replace useless whitespace
    }

    public String getIdQuery(){
        //TODO: This query might include duplicate ids
        String result= query;

        if(query.toLowerCase().startsWith("call db.runonview(")){
            return result;
        }


        result = result + " RETURN collect(id("; //+
        boolean first = true;
        for(String var:savedNodes){
            if(first){
                first = false;
                result = result + var +"))";
            } else{
                result = result + "+ collect(id("+ var +"))";
            }

        }
        result = result+ " AS nodeIds";
        if(savedRelationships.size()>0){
            result = result+ " , collect(id(";
            first = true;
            for(String var:savedRelationships){
                if(first){
                    first = false;
                    result = result + var +"))";
                } else{
                    result = result + "+ collect(id("+ var +"))";
                }

            }
            result = result+ " AS relIds";

        }

        return result;
    }

    public Set<String> getLabels(){
        Set<String> list = new HashSet<>();

        //TODO: Sascha: Path Rewriting
        // MATCH path = (:Label)-->(x:Label)
        // RETURN path


        //TODO: Sascha: undefined Nodes -> ALL!
        // MATCH (n)
        // RETURN n

        //TODO: Sascha:  WITH Rewriting (inkl. Path?)
        // MATCH (z:LABEL)
        // WITH z as x      x -> z
        // WITH x AS y      y -> x
        // SAVE y

        for(String binding:savedNodes){

            int index;
            // try 1
            index = query.indexOf("("+binding+":");
            if(index!=-1) {
                index += "(".length() + binding.length() + ":".length();
            }

            if(index==-1) {
                // try 2
                index = query.indexOf("( " + binding + ":");
                if (index != -1) {
                    index += "( ".length() + binding.length() + ":".length();
                }
            }
            if(index==-1) {
                // try 3
                index = query.indexOf("( " + binding + " :");
                if(index!=-1) {
                    index += "( ".length() + binding.length() + " :".length();
                }
            }

            if(index==-1) {
                // try 4
                index = query.indexOf("(" + binding + " :");
                if (index != -1) {
                    index += "(".length() + binding.length() + " :".length();
                }
            }
            int end = index;
            int propStart=index;
            int endSearch = query.length()-1;

            end = query.indexOf(")",index+1);
            propStart = query.indexOf("{",index+1);

            if(propStart!=-1) {
                if (propStart < end) {
                    endSearch = propStart;
                } else {
                    endSearch = end;
                }
            } else{
                endSearch = end;
            }

            boolean next;
            do{
                next = false;
                String label;
                int nextIndex = query.indexOf(":",index+1);
                if(nextIndex < endSearch && nextIndex!=-1){
                    next = true;
                    label = query.substring(index,nextIndex);
                    label.replaceAll(" ","");
                    list.add(label);

                    index = nextIndex+1;

                } else {
                    label = query.substring(index,endSearch);
                    label.replaceAll(" ","");
                    list.add(label);
                }

            } while(next);
        }
        return list;
    }

    public Set<String> getRelTypes(){
        Set<String> list = new HashSet<>();

        //TODO: Sascha: Path Rewriting
        // MATCH path = (:Label)-->(x:Label)
        // RETURN path

        //TODO: Sascha: undefined types -> ALL!
        // MATCH (n)-[r]->(m)
        // RETURN n,r,m

        //TODO: Sascha:  WITH Rewriting
        // MATCH (z:LABEL)
        // WITH z as x      x -> z
        // WITH x AS y      y -> x
        // SAVE y

        for(String binding : savedRelationships){

            int index;
            // try 1
            index = query.indexOf("["+binding+":");
            if(index!=-1) {
                index += "[".length() + binding.length() + ":".length();
            }

            if(index==-1) {
                // try 2
                index = query.indexOf("[ " + binding + ":");
                if (index != -1) {
                    index += "[ ".length() + binding.length() + ":".length();
                }
            }
            if(index==-1) {
                // try 3
                index = query.indexOf("[ " + binding + " :");
                if (index != -1) {
                    index += "[ ".length() + binding.length() + " :".length();
                }
            }
            if(index==-1) {
                // try 4
                index = query.indexOf("[" + binding + " :");
                if (index != -1) {
                    index += "[".length() + binding.length() + " :".length();
                }
            }

            int end = index;
            int propStart=index;
            int endSearch = query.length()-1;

            end = query.indexOf("]",index+1);
            propStart = query.indexOf("{",index+1);

            if(propStart!=-1) {
                if (propStart < end) {
                    endSearch = propStart;
                } else {
                    endSearch = end;
                }
            } else{
                endSearch = end;
            }

            boolean next;
            do{
                next = false;
                String label;
                int nextIndex = query.indexOf(":",index+1);
                if(nextIndex < endSearch && nextIndex!=-1){
                    next = true;
                    label = query.substring(index,nextIndex);
                    label.replaceAll(" ","");
                    list.add(label);

                    index = nextIndex+1;

                } else {
                    label = query.substring(index,endSearch);
                    label.replaceAll(" ","");
                    list.add(label);
                }

            } while(next);
        }
        return list;
    }
}