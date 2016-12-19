package saschapeukert;

import java.util.HashMap;
import java.util.Map;
import java.util.Set;

/**
 * Created by Sascha Peukert on 19.12.2016.
 */
public class ViewController {

    private static ViewController instance;
    private Map<String,ViewDefinition> views;

    public static ViewController getInstance(){
        // Singleton because of transactional boundaries
        // TODO: Better solution
        if(instance==null){
            instance = new ViewController();
        }
        return instance;
    }

    private ViewController(){
        views = new HashMap<String,ViewDefinition>();
    }

    public ViewDefinition addView(ViewDefinition def){
        // TODO: Check if view is valid?

        return views.put(def.name,def);
    }

    public void clearViews(){
        views.clear();
    }

    public ViewDefinition getView(String name){
        return views.get(name);
    }

    public Set<String> getAllViewNames(){
        return views.keySet();
    }

    public ViewDefinition removeView(String name){
        return views.remove(name);
    }
}
