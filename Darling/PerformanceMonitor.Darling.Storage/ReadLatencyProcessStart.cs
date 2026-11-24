/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

namespace PerformanceMonitor.Darling.Storage;

/// <summary>
/// The service process's start instant on each <c>collect.read_latency</c> bucket (#9434), so <c>get_read_latency</c> can split a
/// route's timings before and after a new build started without comparing by hand.
///
/// <para><b>Why the key changes.</b> The table keeps one row per hour, route, outcome and window band, and a flush adds into an
/// existing row. The hour a new build started therefore held both processes' calls in one row, and a split on the start time alone
/// could not tell them apart. With <c>process_started_at</c> in the key, a restart inside an hour writes a second row for that
/// hour and the split is exact.</para>
///
/// <para><b>The key.</b> V148's primary key (rewritten by V182) cannot name a nullable column, and rows written before this rung
/// have no start. So the primary key is dropped and a unique index with <c>NULLS NOT DISTINCT</c> takes its place: old rows keep
/// NULL, two NULL rows of one key are still one key, and the flush's <c>ON CONFLICT</c> names the same six columns. <c>NULLS NOT DISTINCT</c> needs PostgreSQL 15, and the store
/// minimum is 16. The table is a plain table that holds at most 90 days of hourly rows, so the index build is short and the
/// column is catalog-only. An older build's flush names the old five-column conflict key, which no unique index matches after this
/// rung, so it fails (42P10) until the rollback statement in docs/darling/admin.md restores the primary key. <b>No Lite twin:</b> Lite records no read latency.</para>
///
/// <para>Idempotent: the column is IF NOT EXISTS, the constraint drop is IF EXISTS, and the index is IF NOT EXISTS, so a second run
/// does nothing.</para>
/// </summary>
public static class ReadLatencyProcessStart
{
    /// <summary>The store migration that adds the column and moves the key. One constant so a renumber is mechanical.</summary>
    public const int Version = 315;

    /// <summary>The unique index that is the bucket key from this rung on. The probe finds the rung by this name.</summary>
    public const string KeyIndexName = "uq_read_latency_key";

    /// <summary>The rung's statement.</summary>
    public const string Sql = @"
ALTER TABLE collect.read_latency
    ADD COLUMN IF NOT EXISTS process_started_at timestamptz;

ALTER TABLE collect.read_latency DROP CONSTRAINT IF EXISTS read_latency_pkey;

CREATE UNIQUE INDEX IF NOT EXISTS uq_read_latency_key
    ON collect.read_latency (metric_time, surface, route, outcome, window_band, process_started_at)
    NULLS NOT DISTINCT;";
}
