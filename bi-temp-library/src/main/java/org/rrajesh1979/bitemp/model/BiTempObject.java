package org.rrajesh1979.bitemp.model;

import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import org.bson.types.ObjectId;
import org.rrajesh1979.bitemp.utils.DateTimeSerializer;

import java.time.OffsetDateTime;

public record BiTempObject(
        Object key,
        Object data,
        RecordMeta recordMeta,
        EffectiveMeta effectiveMeta,
        Object previousId,
        Object nextId,
        ObjectId _id) {
    //Override toString() method to print the object in JSON format
    @Override
    public String toString() {
        GsonBuilder gsonBuilder = new GsonBuilder();
        gsonBuilder.registerTypeAdapter(OffsetDateTime.class, new DateTimeSerializer());
        Gson gson = gsonBuilder.create();
        return gson.toJson(this);
    }

}
