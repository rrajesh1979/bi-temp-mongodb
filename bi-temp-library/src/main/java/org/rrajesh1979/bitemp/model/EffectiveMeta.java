package org.rrajesh1979.bitemp.model;

import java.time.OffsetDateTime;

public record EffectiveMeta(
        OffsetDateTime validFrom,
        OffsetDateTime validTo) {
}
