package com.andesstay.ms_andesstay_audit.web;

import com.andesstay.ms_andesstay_audit.web.dto.AuditEventResponse;
import org.springframework.format.annotation.DateTimeFormat;
import org.springframework.security.access.prepost.PreAuthorize;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

import java.time.Instant;
import java.util.List;

/** API de solo lectura: no expone POST/PUT/DELETE, la escritura la hace el listener de Kafka. */
@RestController
@RequestMapping("/api/audit")
@PreAuthorize("hasAnyRole('Admin', 'Auditor')")
public class AuditController {

    private final AuditQueryService service;

    public AuditController(AuditQueryService service) {
        this.service = service;
    }

    @GetMapping("/events")
    public List<AuditEventResponse> search(@RequestParam(required = false) String reservationId,
                                            @RequestParam(required = false) String actorId,
                                            @RequestParam(required = false) String type,
                                            @RequestParam(required = false) @DateTimeFormat(iso = DateTimeFormat.ISO.DATE_TIME) Instant from,
                                            @RequestParam(required = false) @DateTimeFormat(iso = DateTimeFormat.ISO.DATE_TIME) Instant to) {
        return service.search(reservationId, actorId, type, from, to).stream()
                .map(AuditEventResponse::from)
                .toList();
    }

    @GetMapping("/events/reservation/{reservationId}")
    public List<AuditEventResponse> timelineOf(@PathVariable String reservationId) {
        return service.search(reservationId, null, null, null, null).stream()
                .map(AuditEventResponse::from)
                .toList();
    }
}
