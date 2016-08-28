package org.neo4j.kernel.impl.api;

import org.neo4j.storageengine.api.PropertyItem;

/**
 * Created by Sascha Peukert on 28.08.2016.
 */
public class VirtualPropertyItem implements PropertyItem {
    private int keyId;
    private Object value;

    public VirtualPropertyItem(int propertyKeyId, Object value){
        this.keyId = propertyKeyId;
        this.value = value;
    }

    @Override
    public int propertyKeyId() {
        return keyId;
    }

    @Override
    public Object value() {
        return value;
    }
}
