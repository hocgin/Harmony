//
//  Harmonic+CRUD.swift
//  Harmony
//
//  Created by Aaron Pearce on 11/06/23.
//

import CloudKit
import Foundation
import GRDB
import os.log

public extension Harmonic {
    func read<T>(_ block: (Database) throws -> T) throws -> T {
        try reader.read(block)
    }

    func read<T>(_ block: @Sendable @escaping (Database) throws -> T) async throws -> T {
        try await reader.read { db in
            try block(db)
        }
    }

    func create<T: HRecord>(record: T) async throws {
        try await database.write { db in
            try record.insert(db)
        }

        guard iCloudSyncEnabled else { return }
        queueSaves(for: [record])
    }

    func create<T: HRecord>(records: [T]) async throws {
        try await database.write { db in
            try records.forEach {
                try $0.insert(db)
            }
        }
        
        guard iCloudSyncEnabled else { return }
        queueSaves(for: records)
    }

    func save<T: HRecord>(record: T) async throws {
        try await database.write { db in
            try record.save(db)
        }

        guard iCloudSyncEnabled else { return }
        queueSaves(for: [record])
    }

    func save<T: HRecord>(records: [T]) async throws {
        _ = try await database.write { db in
            try records.forEach {
                try $0.save(db)
            }
        }

        guard iCloudSyncEnabled else { return }
        queueSaves(for: records)
    }

    func delete<T: HRecord>(record: T) async throws {
        _ = try await database.write { db in
            try record.delete(db)
        }

        guard iCloudSyncEnabled else { return }
        queueDeletions(for: [record])
    }

    func delete<T: HRecord>(records: [T]) async throws {
        _ = try await database.write { db in
            try records.forEach {
                try $0.delete(db)
            }
        }

        guard iCloudSyncEnabled else { return }
        queueDeletions(for: records)
    }

    /// Pushes all of the given record type to CloudKit
    /// This occurs regardless of changes.
    /// Sometimes used during migration for schema changes.
    func pushAll<T: HRecord>(for recordType: T.Type) throws {
        let records = try read { db in
            try recordType.fetchAll(db)
        }

        guard iCloudSyncEnabled else { return }
        queueSaves(for: records)
    }

    private func queueSaves(for records: [any HRecord]) {
        Logger.database.info("Queuing saves")
        let pendingSaves: [CKSyncEngine.PendingRecordZoneChange] = records.map {
            .saveRecord($0.recordID)
        }

        syncEngine.state.add(pendingRecordZoneChanges: pendingSaves)
    }

    private func queueDeletions(for records: [any HRecord]) {
        Logger.database.info("Queuing deletions")
        let pendingDeletions: [CKSyncEngine.PendingRecordZoneChange] = records.map {
            .deleteRecord($0.recordID)
        }

        syncEngine.state.add(pendingRecordZoneChanges: pendingDeletions)
    }

    func sendChanges() async throws {
        try await syncEngine.sendChanges()
    }

    func fetchChanges() async throws {
        try await syncEngine.fetchChanges()
    }
}
