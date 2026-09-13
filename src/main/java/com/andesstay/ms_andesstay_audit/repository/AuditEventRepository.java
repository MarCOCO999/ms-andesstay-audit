package com.andesstay.ms_andesstay_audit.repository;

import com.andesstay.ms_andesstay_audit.domain.AuditEvent;
import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.data.jpa.repository.JpaSpecificationExecutor;

public interface AuditEventRepository extends JpaRepository<AuditEvent, Long>, JpaSpecificationExecutor<AuditEvent> {

    boolean existsByEventId(String eventId);
}
