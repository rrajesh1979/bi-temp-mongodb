package org.rrajesh1979.bitemp.model;

import java.time.LocalDateTime;
import java.util.List;

public record CreateResponse(
        List<String> insertResult,
        List<String> updateResult) {

}
