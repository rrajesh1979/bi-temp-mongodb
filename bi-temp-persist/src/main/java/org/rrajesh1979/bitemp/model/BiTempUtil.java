package org.rrajesh1979.bitemp.model;

import org.bson.Document;

public class BiTempUtil {
    public static Document toBiTempObjectToDocument(BiTempObject biTempObject) {
        return new Document()
                .append("key", biTempObject.key())
                .append("data", biTempObject.data())
                .append("recordMeta", biTempObject.recordMeta())
                .append("effectiveMeta", biTempObject.effectiveMeta())
                .append("isActive", biTempObject.isActive());
    }
}
