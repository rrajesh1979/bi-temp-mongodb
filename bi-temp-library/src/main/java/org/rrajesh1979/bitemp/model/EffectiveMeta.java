package org.rrajesh1979.bitemp.model;

import lombok.AllArgsConstructor;
import lombok.Getter;
import lombok.Setter;

import java.time.OffsetDateTime;

public record EffectiveMeta(OffsetDateTime validFrom, OffsetDateTime validTo) {
}
