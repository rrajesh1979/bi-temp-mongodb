package org.rrajesh1979.bitemp.resource;

import lombok.extern.log4j.Log4j2;
import org.bson.Document;
import org.rrajesh1979.bitemp.model.*;
import org.rrajesh1979.bitemp.service.BiTempService;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;

import java.time.OffsetDateTime;
import java.time.ZoneOffset;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

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
    public ResponseEntity<Map<String, Object>> getBiTempData(@RequestBody GetRequest getRequest) {
        log.info("Get BiTemp Data for key: {}", getRequest.id());
        log.info("Get BiTemp Data for effectiveFrom: {}", getRequest.effectiveFrom().atOffset(ZoneOffset.UTC));
        log.info("Get BiTemp Data for effectiveTo: {}", getRequest.effectiveTo().atOffset(ZoneOffset.UTC));

        List<Document> result = biTempService.getBiTempData(
                getRequest.id(),
                getRequest.effectiveFrom(),
                getRequest.effectiveTo()
        );

        Map<String, Object> response = new HashMap<>();
        response.put("result", result);
        response.put("count", result.size());
        response.put("status", "success");

        return new ResponseEntity<>(response, HttpStatus.OK);
    }
}
