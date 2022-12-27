package org.rrajesh1979.bitemp.model;

import java.time.LocalDateTime;
import java.util.List;

public record CreateResponse(
        List<Object> insertResult,
        List<Object> updateResult) {

}
