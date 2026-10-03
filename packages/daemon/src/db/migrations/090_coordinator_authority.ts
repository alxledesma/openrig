import type { Migration } from "../migrate.js";
export const coordinatorAuthoritySchema: Migration = {
  name: "090_coordinator_authority.sql",
  sql: `
CREATE TABLE coordinator_authority (
 rig_id TEXT PRIMARY KEY REFERENCES rigs(id), baton_id TEXT NOT NULL UNIQUE REFERENCES queue_items(qitem_id),
 owner_session TEXT NOT NULL, owner_generation TEXT NOT NULL, epoch INTEGER NOT NULL CHECK(epoch>0),
 lease_until INTEGER NOT NULL, state TEXT NOT NULL CHECK(state IN ('reconciling','active','recovery')),
 operation_id TEXT NOT NULL, coordinators TEXT NOT NULL, recovery_queue_id TEXT
);
CREATE TABLE coordinator_operations (
 rig_id TEXT NOT NULL REFERENCES coordinator_authority(rig_id), operation_id TEXT NOT NULL,
 kind TEXT NOT NULL, receipt TEXT NOT NULL, request_hash TEXT NOT NULL, PRIMARY KEY(rig_id,operation_id)
);
CREATE TABLE coordinator_packages (
 rig_id TEXT NOT NULL REFERENCES coordinator_authority(rig_id), package_key TEXT NOT NULL,
 contract TEXT NOT NULL, contract_hash TEXT NOT NULL, admitted_by TEXT NOT NULL,
 PRIMARY KEY(rig_id,package_key)
);
CREATE TABLE coordinator_assignments (
 rig_id TEXT NOT NULL, package_key TEXT NOT NULL, queue_id TEXT NOT NULL UNIQUE,
 destination TEXT NOT NULL, body_hash TEXT NOT NULL, owner_session TEXT NOT NULL,
 owner_generation TEXT NOT NULL, epoch INTEGER NOT NULL, disposition_id TEXT, source_queue_id TEXT,
 FOREIGN KEY(rig_id,package_key) REFERENCES coordinator_packages(rig_id,package_key),
 PRIMARY KEY(rig_id,package_key)
);
CREATE TABLE coordinator_stage_assignments (
 rig_id TEXT NOT NULL, package_key TEXT NOT NULL, source TEXT NOT NULL, destination TEXT NOT NULL,
 body_hash TEXT NOT NULL, queue_id TEXT NOT NULL UNIQUE, source_generation TEXT NOT NULL, source_queue_id TEXT,
 PRIMARY KEY(rig_id,package_key,source,destination,body_hash),
 FOREIGN KEY(rig_id,package_key) REFERENCES coordinator_packages(rig_id,package_key)
);
CREATE TABLE coordinator_resources (
 rig_id TEXT NOT NULL, resource_key TEXT NOT NULL, package_key TEXT NOT NULL,
 PRIMARY KEY(rig_id,resource_key),
 FOREIGN KEY(rig_id,package_key) REFERENCES coordinator_packages(rig_id,package_key)
);`,
};
