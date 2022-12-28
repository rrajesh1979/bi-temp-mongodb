package org.rrajesh1979.bitemp.model;

import java.time.LocalDateTime;

public record GetAsOfRequest(
        Object key,
        LocalDateTime asOf,
        LocalDateTime effectiveFrom,
        LocalDateTime effectiveTo) {

}
