package org.rrajesh1979.bitemp.model;

import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import lombok.AllArgsConstructor;
import lombok.Setter;
import org.bson.Document;
import org.rrajesh1979.bitemp.utils.DateTimeSerializer;

import java.time.OffsetDateTime;

@Setter
@AllArgsConstructor
public class BiTempDocument {
    private BiTempObject biTempObject;

    public Document toDocument() {
        return new Document()
                .append("_id", biTempObject.id())
                .append("data", biTempObject.data())
                .append("recordMeta", biTempObject.recordMeta())
                .append("effectiveMeta", biTempObject.effectiveMeta());
    }
}
