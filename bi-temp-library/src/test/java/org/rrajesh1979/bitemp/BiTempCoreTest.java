package org.rrajesh1979.bitemp;

//Test case for Bi-temporal library

import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import lombok.extern.log4j.Log4j2;
import lombok.extern.slf4j.Slf4j;
import org.junit.jupiter.api.Test;
import org.rrajesh1979.bitemp.model.BiTempObject;
import org.rrajesh1979.bitemp.model.EffectiveMeta;
import org.rrajesh1979.bitemp.model.RecordMeta;
import org.rrajesh1979.bitemp.utils.DateTimeSerializer;

import java.time.LocalDateTime;
import java.time.OffsetDateTime;
import java.time.ZoneOffset;

@Slf4j
public class BiTempCoreTest {

    //Test Record
    record TestRecord(String name, int age) {
    }

    @Test
    public void testBiTempModel() {
//        OffsetDateTime validFrom = OffsetDateTime.of(LocalDateTime.of(2021, 1, 1, 0, 0, 0), ZoneOffset.UTC);
//        OffsetDateTime validTo = OffsetDateTime.of(LocalDateTime.MAX, ZoneOffset.UTC);
//        EffectiveMeta effectiveMeta = new EffectiveMeta(validFrom, validTo);
//        log.debug("LocalDateTime.MAX: {}", LocalDateTime.MAX);
//        log.debug("Effective Meta: {}", effectiveMeta);
//
//        //Create RecordMeta
//        String createdBy = "Rajesh";
//        OffsetDateTime createdAt = OffsetDateTime.now();
//        String updatedBy = "Rajesh";
//        OffsetDateTime updatedAt = OffsetDateTime.now();
//        var recordMeta = new RecordMeta(createdBy, createdAt, updatedBy, updatedAt);
//        log.debug("Record Meta: {}", recordMeta);
//
//        //Create JSON of TestRecord
//        Gson gson = new Gson();
//        TestRecord testRecord = new TestRecord("Rajesh", 40);
//        String json = gson.toJson(testRecord);
//        log.debug("JSON: {}", json);
//
//        //Create BiTempObject
//        BiTempObject biTempObject = new BiTempObject(
//                10,
//                testRecord,
//                recordMeta,
//                effectiveMeta,
//                null,
//                null);
//        log.debug("BiTempObject: {}", biTempObject);

    }
}
