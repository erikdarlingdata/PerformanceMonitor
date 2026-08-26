// Copyright (c) Erik Darling Data. All rights reserved.
// Licensed under the terms in the LICENSE file in the repository root.

using System;
using System.Globalization;
using System.Linq;
using PerformanceMonitor.Darling.Service;
using PerformanceMonitor.Darling.Service.Mcp;
using PerformanceMonitor.Darling.Storage;
using Xunit;

namespace Darling.Tests;

/// <summary>
/// V100 (#2653) — the PostgreSQL major on the registry, and the reason it has to be there.
///
/// <para>Seven PostgreSQL collectors branch on <c>postgresMajorVersion</c> and emit <c>NULL::bigint</c> for
/// columns the target's version does not have. They get the version from the live probe and are correct.
/// The READS have no connection, so before this rung a structurally-absent column arrived as a naked NULL,
/// indistinguishable from a measurement that did not happen — the failure #2511 and #2623 exist to prevent,
/// on the version axis. <c>get_pg_write_stats</c> was the visible case: on PostgreSQL 17 it returned
/// <c>buffers_backend: null</c> under a note that explained the column as a live backpressure signal.</para>
/// </summary>
public sealed class PostgresMajorVersionRegistryTests
{
    /* ---------------- the rung ---------------- */

    [Fact]
    public void TheRungIsRegisteredAtTheTopOfADenseLadder()
    {
        var versions = PgMigrations.Scripts.Select(s => s.Version).ToList();

        Assert.Equal("pg-major-version", PgMigrations.Scripts.Single(s => s.Version == 100).Name);

        /* The top-of-ladder pins live with whichever rung is newest, so they sit here until one displaces
           them — same convention as V82's test, which gave them up when V83 landed. */
        Assert.Equal(StorageVersion.SchemaVersion, PgMigrations.Scripts[^1].Version);
        Assert.Equal(StorageVersion.SchemaVersion, versions.Max());

        Assert.Equal(versions.Distinct().OrderBy(v => v), versions);
        var above = versions.Where(v => v > 45).OrderBy(v => v).ToList();
        Assert.Equal(Enumerable.Range(above[0], above.Count), above);
    }

    /// <summary>
    /// Nullable, no DEFAULT, no backfill — as V82. A default would have every server assert a version it was
    /// never probed at, and on a SQL Server target there is no correct value to default TO: the column stays
    /// NULL there forever because it is not a fact about that server.
    /// </summary>
    [Fact]
    public void TheRungAddsTheColumn_Idempotently_WithoutADefaultOrABackfill()
    {
        var sql = PgMigrations.Scripts.Single(s => s.Version == 100).Sql;

        Assert.Contains("ALTER TABLE collect.servers", sql, StringComparison.Ordinal);
        Assert.Contains("ADD COLUMN IF NOT EXISTS postgres_major_version integer", sql, StringComparison.Ordinal);

        Assert.DoesNotContain("DEFAULT", sql, StringComparison.Ordinal);
        Assert.DoesNotContain("UPDATE ", sql, StringComparison.Ordinal);
        Assert.DoesNotContain("NOT NULL", sql, StringComparison.Ordinal);
    }

    /* ---------------- the upsert ---------------- */

    /// <summary>
    /// The column list, the VALUES list and the placeholder count have to move together. This is the defect
    /// this shape actually invites: the parameters are POSITIONAL and are bound in the order they are added,
    /// so a new column whose placeholder is not the highest, or whose <c>AddWithValue</c> is not last, binds
    /// silently to the wrong value rather than failing.
    /// </summary>
    [Fact]
    public void TheUpsertCarriesTheColumnOnBothArms_AndItsPlaceholdersAgree()
    {
        var sql = DarlingObservability.UpsertServerSql;

        Assert.Contains("postgres_major_version)", sql, StringComparison.Ordinal);
        Assert.Contains("$9)", sql, StringComparison.Ordinal);

        /* The re-connect arm matters as much as the insert: a version is a probed fact about the target, so
           a server upgraded from 16 to 17 has to correct it on the next connect, exactly as engine_kind does.
           Without this the column would be right only for servers first registered after the rung landed. */
        Assert.Contains("postgres_major_version = EXCLUDED.postgres_major_version", sql, StringComparison.Ordinal);

        var columns = sql[(sql.IndexOf("INSERT INTO servers (", StringComparison.Ordinal) + "INSERT INTO servers (".Length)..];
        columns = columns[..columns.IndexOf(')')];
        var columnCount = columns.Split(',').Length;

        var values = sql[(sql.IndexOf("VALUES (", StringComparison.Ordinal) + "VALUES (".Length)..];
        values = values[..values.IndexOf(')')];
        var slots = values.Split(',').Select(v => v.Trim()).ToList();

        Assert.Equal(columnCount, slots.Count);

        /* The two invariants that actually keep a parameter bound to its column, neither of which hardcodes
           a count: $7 is deliberately reused for created_date and modified_date and one slot is the literal
           TRUE, so the highest placeholder is NOT derivable from the slot count.

           Contiguity first — a gap in $1..$N means some parameter is added but never referenced, or
           referenced but never added, and Npgsql binds POSITIONALLY in add order, so that misaligns every
           parameter after the gap rather than failing. */
        var placeholders = slots
            .Where(v => v.StartsWith('$'))
            .Select(v => int.Parse(v[1..], CultureInfo.InvariantCulture))
            .Distinct()
            .OrderBy(n => n)
            .ToList();

        Assert.Equal(Enumerable.Range(1, placeholders[^1]), placeholders);

        /* And the new column binds LAST. Its AddWithValue is appended after every existing one, so its
           placeholder has to be the highest and has to sit in the final slot; anything else silently swaps
           two values of the same type. */
        Assert.Equal("$" + placeholders[^1].ToString(CultureInfo.InvariantCulture), slots[^1]);
    }

    /* ---------------- what the reads may say ---------------- */

    /// <summary>
    /// The lookup reads the registry column and nothing else. It must not fall back to
    /// <c>sql_major_version</c>: 17 is a real major in both engines, so a reader joining them has no way to
    /// tell which vocabulary a number belongs to.
    /// </summary>
    [Fact]
    public void TheLookupReadsOnlyThePostgresColumn()
    {
        var sql = DarlingEngineCapability.PostgresMajorVersionSql;

        Assert.Contains("SELECT postgres_major_version", sql, StringComparison.Ordinal);
        Assert.Contains("FROM servers", sql, StringComparison.Ordinal);
        Assert.Contains("WHERE server_id = $1", sql, StringComparison.Ordinal);
        Assert.DoesNotContain("sql_major_version", sql, StringComparison.Ordinal);
    }
}
