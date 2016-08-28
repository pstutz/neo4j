package org.neo4j.kernel.impl.api;

import org.neo4j.storageengine.api.LabelItem;

/**
 * Created by Sascha Peukert on 28.08.2016.
 */
public class VirtualLabelItem implements LabelItem {
    private int id;

    public VirtualLabelItem(int id){
        this.id = id;
    }

    @Override
    public int getAsInt() {
        return id;
    }
}
