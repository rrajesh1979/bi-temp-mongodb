package org.rrajesh1979.bitemp.resource;

import lombok.extern.log4j.Log4j2;
import org.rrajesh1979.bitemp.model.BiTempObject;
import org.rrajesh1979.bitemp.model.CreateRequest;
import org.rrajesh1979.bitemp.model.EffectiveMeta;
import org.rrajesh1979.bitemp.model.RecordMeta;
import org.rrajesh1979.bitemp.service.BiTempService;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;

import java.time.OffsetDateTime;
import java.time.ZoneOffset;

@RestController
@RequestMapping( "${api.default.path}" )
@Log4j2
public class BiTempResource {
    private static final String ACCEPT_APPLICATION_JSON = "Accept=application/json";

    private final BiTempService biTempService;

    @Autowired
    public BiTempResource(BiTempService biTempService) {
        this.biTempService = biTempService;
    }

    @PostMapping( value = "/create", headers = ACCEPT_APPLICATION_JSON )
    public ResponseEntity<String> createBiTempData(@RequestBody CreateRequest createRequest) {
        log.info("Create BiTemp Data: {}", createRequest);
        EffectiveMeta effectiveMeta = new EffectiveMeta(
                createRequest.effectiveFrom().atOffset(ZoneOffset.UTC),
                createRequest.effectiveTo().atOffset(ZoneOffset.UTC));
        RecordMeta recordMeta = new RecordMeta(
                createRequest.createdBy(),
                OffsetDateTime.now(),
                createRequest.createdBy(),
                OffsetDateTime.now());
        BiTempObject biTempObject = new BiTempObject(
                createRequest.id(),
                createRequest.data(),
                recordMeta,
                effectiveMeta);
        log.info("BiTemp Object: {}", biTempObject);

        String id = biTempService.createBiTempData(biTempObject);
        return ResponseEntity.ok(id);

    }

    @GetMapping( value = "/get", headers = ACCEPT_APPLICATION_JSON )
    public ResponseEntity<String> getBiTempData() {
        return ResponseEntity.ok( "Hello World" );
    }
}
