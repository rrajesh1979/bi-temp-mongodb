package org.rrajesh1979.bitemp.model;

import java.time.LocalDateTime;

public record DeleteRequest(
        Object key,
        LocalDateTime effectiveFrom,
        LocalDateTime effectiveTo) {

}
