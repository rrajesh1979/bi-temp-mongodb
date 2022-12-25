package org.rrajesh1979.bitemp.model;

import java.time.LocalDateTime;

public record CreateRequest(
        Object key,
        Object data,
        String createdBy,
        LocalDateTime effectiveFrom,
        LocalDateTime effectiveTo) {

}
