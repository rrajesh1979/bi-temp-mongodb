package org.rrajesh1979.bitemp.resource;

import lombok.extern.log4j.Log4j2;
import org.bson.Document;
import org.rrajesh1979.bitemp.model.*;
import org.rrajesh1979.bitemp.service.BiTempService;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;

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
    public ResponseEntity<Map<String, Object>> createBiTempData(@RequestBody CreateRequest createRequest) {
        log.debug("Create BiTemp Data: {}", createRequest);

        String result = biTempService.createBiTempData(createRequest);

        Map<String, Object> response = new HashMap<>();
        response.put("result", result);
        response.put("status", "success");

        return new ResponseEntity<>(response, HttpStatus.OK);

    }

    @GetMapping( value = "/get", headers = ACCEPT_APPLICATION_JSON )
    public ResponseEntity<Map<String, Object>> getBiTempData(@RequestBody GetRequest getRequest) {
        log.debug("Get BiTemp Request: {}", getRequest);

        List<Document> result = biTempService.getBiTempData(getRequest);

        Map<String, Object> response = new HashMap<>();
        response.put("result", result);
        response.put("count", result.size());
        response.put("status", "success");

        return new ResponseEntity<>(response, HttpStatus.OK);
    }

    @DeleteMapping( value = "/delete", headers = ACCEPT_APPLICATION_JSON )
    public ResponseEntity<Map<String, Object>> deleteBiTempData(@RequestBody DeleteRequest deleteRequest) {
        log.debug("Delete BiTemp Request: {}", deleteRequest);

        String result = biTempService.deleteBiTempData(deleteRequest);

        Map<String, Object> response = new HashMap<>();
        response.put("result", result);
        response.put("status", "success");

        return new ResponseEntity<>(response, HttpStatus.OK);
    }
}
