package saschapeukert;

import java.util.Collection;
import java.util.HashSet;
import java.util.Set;

/**
 * Created by Sascha Peukert on 04.03.2017.
 */
public class IdFilter{

    private Set<Long> ids;
    private boolean unused; // previously: empty

    public IdFilter(){
        ids = new HashSet<>();
        unused = true;
    }

    public void addAll(Collection<Long> set){
        if(set!=null) {
            ids.addAll(set);
            unused = false;
        }

    }

    //TODO: Write test that checks view with no actual elements does not have access to anything!
    public void clear(){
        ids.clear();
        unused = true;
    }

    public boolean isUnused(){
        return unused;
    }

    public boolean idIsInFilter(long id){
        return ids.contains(id);
    }
}
