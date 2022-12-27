package org.rrajesh1979.bitemp.model;

import java.time.LocalDateTime;

public record GetRequest(
        Object key,
        LocalDateTime effectiveFrom,
        LocalDateTime effectiveTo) {

}
