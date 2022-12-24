package org.rrajesh1979.bitemp.model;

import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import lombok.AllArgsConstructor;
import lombok.Getter;
import lombok.Setter;
import org.rrajesh1979.bitemp.utils.DateTimeSerializer;

import java.time.OffsetDateTime;

public record BiTempObject(Object id, Object data, RecordMeta recordMeta, EffectiveMeta effectiveMeta,
                           Object previousId, Object nextId) {
    //Override toString() method to print the object in JSON format
    @Override
    public String toString() {
        GsonBuilder gsonBuilder = new GsonBuilder();
        gsonBuilder.registerTypeAdapter(OffsetDateTime.class, new DateTimeSerializer());
        Gson gson = gsonBuilder.create();
        return gson.toJson(this);
    }

}
