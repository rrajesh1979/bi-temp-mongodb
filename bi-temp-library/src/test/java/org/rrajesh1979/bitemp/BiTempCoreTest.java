package org.rrajesh1979.bitemp;

//Test case for Bi-temporal library

import com.google.gson.Gson;
import lombok.extern.log4j.Log4j2;
import lombok.extern.slf4j.Slf4j;
import org.junit.jupiter.api.Test;
import org.rrajesh1979.bitemp.model.BiTempObject;
import org.rrajesh1979.bitemp.model.EffectiveMeta;
import org.rrajesh1979.bitemp.model.RecordMeta;

import java.time.LocalDateTime;
import java.time.ZoneOffset;

@Slf4j
public class BiTempCoreTest {

    //Test Record
    record TestRecord(String name, int age) {
    }

    @Test
    public void testBiTempModel() {
        long validFrom = LocalDateTime.of(2021, 1, 1, 0, 0).toEpochSecond(ZoneOffset.UTC);
        long validTo = LocalDateTime.of(2021, 12, 31, 23, 59).toEpochSecond(ZoneOffset.UTC);
        EffectiveMeta effectiveMeta = new EffectiveMeta(validFrom, validTo);
        log.info("Effective Meta: {}", effectiveMeta);

        //Create RecordMeta
        String createdBy = "Rajesh";
        long createdAt = LocalDateTime.now().toEpochSecond(ZoneOffset.UTC);
        String updatedBy = "Rajesh";
        long updatedAt = LocalDateTime.now().toEpochSecond(ZoneOffset.UTC);
        var recordMeta = new RecordMeta(createdBy, createdAt, updatedBy, updatedAt);
        log.info("Record Meta: {}", recordMeta);

        //Create JSON of TestRecord
        Gson gson = new Gson();
        TestRecord testRecord = new TestRecord("Rajesh", 40);
        String json = gson.toJson(testRecord);
        log.info("JSON: {}", json);

        //Create BiTempObject
        BiTempObject biTempObject = new BiTempObject(
                json,
                recordMeta,
                effectiveMeta);
        log.info("BiTempObject: {}", biTempObject);

    }
}
