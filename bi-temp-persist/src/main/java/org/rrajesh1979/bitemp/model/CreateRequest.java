package org.rrajesh1979.bitemp.model;

import java.time.LocalDateTime;

public record CreateRequest(
        Object id,
        Object data,
        String createdBy,
        LocalDateTime effectiveFrom,
        LocalDateTime effectiveTo) {

}
