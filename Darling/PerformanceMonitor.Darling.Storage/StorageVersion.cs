/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

namespace PerformanceMonitor.Darling.Storage;

/// <summary>
/// The latest schema version this build knows — always the highest version in
/// <see cref="PgMigrations.Scripts"/> (pinned by test). The service applies pending versioned
/// SQL migrations on startup (headless plan: "plain versioned SQL scripts the service applies
/// on startup"); a store at this version is fully migrated.
/// </summary>
public static class StorageVersion
{
    public const int SchemaVersion = 106;
}
