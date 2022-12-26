package org.rrajesh1979.bitemp.model;

import java.time.LocalDateTime;

public record DeleteRequest(
        Object key,
        String deletedBy,
        LocalDateTime effectiveFrom,
        LocalDateTime effectiveTo) {

}
