package org.rrajesh1979.bitemp.model;

import lombok.AllArgsConstructor;
import lombok.Getter;
import lombok.Setter;

import java.time.OffsetDateTime;

public record RecordMeta(String createdBy, OffsetDateTime createdAt, String updatedBy, OffsetDateTime updatedAt) {
}

