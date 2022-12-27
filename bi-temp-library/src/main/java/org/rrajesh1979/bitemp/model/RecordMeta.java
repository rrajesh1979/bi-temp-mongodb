package org.rrajesh1979.bitemp.model;

import java.time.OffsetDateTime;

public record RecordMeta(String createdBy, OffsetDateTime createdAt, String updatedBy, OffsetDateTime updatedAt) {
}

