package org.rrajesh1979.bitemp.model;

import java.time.LocalDateTime;

public record GetRequest(
        Object id,
        LocalDateTime effectiveFrom,
        LocalDateTime effectiveTo) {

}
