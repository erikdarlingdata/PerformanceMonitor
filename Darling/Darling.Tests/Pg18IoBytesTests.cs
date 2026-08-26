// Copyright (c) Erik Darling Data. All rights reserved.
// Licensed under the terms in the LICENSE file in the repository root.

using System;
using System.Linq;
using PerformanceMonitor.Darling.Storage;
using Xunit;

namespace Darling.Tests;

/// <summary>
/// V101 (#2655) — PostgreSQL 18's measured I/O byte totals.
///
/// <para>18 removed <c>op_bytes</c> from <c>pg_stat_io</c>, and both byte figures the I/O read serves were
/// derived from it, so on 18 they came back null with nothing saying why. The three columns 18 replaced it
/// with are a DIFFERENT quantity, not a rename: <c>op_bytes</c> was the per-operation block size that a
/// reader multiplies by a count to ESTIMATE volume, while these are measured totals. 18 also introduced
/// vectored reads, so one entry in <c>reads</c> can cover several blocks — measured on a real 18.6 target
/// through the running service, <c>client backend/bulkread</c> reported 4,742 reads against 448,724,992
/// bytes, which the old estimate would have called 38,846,464. An 11.6x undercount, not a rounding
/// difference. That is why the two must never be served under the same name without saying which is
/// which.</para>
/// </summary>
public sealed class Pg18IoBytesTests
{
    /* ---------------- the rung ---------------- */

    [Fact]
    public void TheRungIsRegisteredAtTheTopOfADenseLadder()
    {
        var versions = PgMigrations.Scripts.Select(s => s.Version).ToList();

        Assert.Equal("pg18-io-bytes", PgMigrations.Scripts.Single(s => s.Version == 101).Name);

        Assert.Equal(StorageVersion.SchemaVersion, PgMigrations.Scripts[^1].Version);
        Assert.Equal(StorageVersion.SchemaVersion, versions.Max());

        Assert.Equal(versions.Distinct().OrderBy(v => v), versions);
        var above = versions.Where(v => v > 45).OrderBy(v => v).ToList();
        Assert.Equal(Enumerable.Range(above[0], above.Count), above);
    }

    /// <summary>
    /// <c>numeric</c>, because that is what PostgreSQL declares them — verified on 18.6, where
    /// <c>reads</c> is <c>bigint</c> and <c>read_bytes</c> beside it is <c>numeric</c>. Storing a byte total
    /// as bigint is a narrowing the catalog never promised.
    /// </summary>
    [Fact]
    public void TheRungAddsThreeNumericColumns_Idempotently_WithoutADefaultOrABackfill()
    {
        var sql = PgMigrations.Scripts.Single(s => s.Version == 101).Sql;

        Assert.Contains("ALTER TABLE collect.pg_io_stats", sql, StringComparison.Ordinal);
        Assert.Contains("ADD COLUMN IF NOT EXISTS read_bytes numeric", sql, StringComparison.Ordinal);
        Assert.Contains("ADD COLUMN IF NOT EXISTS write_bytes numeric", sql, StringComparison.Ordinal);
        Assert.Contains("ADD COLUMN IF NOT EXISTS extend_bytes numeric", sql, StringComparison.Ordinal);

        /* bigint would compile and silently narrow; pin the absence so a later edit cannot "tidy" it. */
        Assert.DoesNotContain("bigint", sql, StringComparison.Ordinal);

        Assert.DoesNotContain("DEFAULT", sql, StringComparison.Ordinal);
        Assert.DoesNotContain("UPDATE ", sql, StringComparison.Ordinal);
        Assert.DoesNotContain("NOT NULL", sql, StringComparison.Ordinal);
    }

    /* ---------------- the read ---------------- */

    /// <summary>
    /// The byte columns are cumulative counters and must be differenced like every other counter in this
    /// read. A plain SUM would report the running total since <c>stats_reset</c> rather than the window,
    /// which on a long-lived server is off by the entire uptime.
    /// </summary>
    [Fact]
    public void TheReadDifferencesTheByteCounters_RatherThanSummingLevels()
    {
        var sql = DarlingPgIoReader.PgIoSql;

        foreach (var column in new[] { "read_bytes", "write_bytes", "extend_bytes" })
        {
            Assert.Contains($"GREATEST({column}", sql, StringComparison.Ordinal);
            Assert.Contains($"LAG({column})", sql, StringComparison.Ordinal);
            Assert.Contains($"SUM(d_{column})", sql, StringComparison.Ordinal);
        }
    }

    /// <summary>
    /// <c>GREATEST(NULL, 0)</c> returns 0 and the outer <c>coalesce</c> would anyway, so below 18 — where
    /// these columns do not exist and the collector writes NULL — the sums come back as a perfectly credible
    /// ZERO BYTES. The tracked flag is the only thing that separates "read nothing" from "bytes are not
    /// measured on this version", exactly as <c>write_counters_tracked</c> does for Aurora's write side.
    /// </summary>
    [Fact]
    public void TheReadCarriesAByteTrackedFlag_SoZeroCannotMasqueradeAsAMeasurement()
    {
        var sql = DarlingPgIoReader.PgIoSql;

        Assert.Contains("byte_counters_tracked", sql, StringComparison.Ordinal);
        Assert.Contains("bool_or(byte_counters_tracked)", sql, StringComparison.Ordinal);

        /* read_bytes is the probe for all three, and which column is probed is the whole correctness
           question. WAL rows legitimately report no extend_bytes — verified on 18.6 — so probing
           extend_bytes would answer "bytes are not measured here" for a row that simply does not extend,
           and the read would then serve a measured server as an unmeasured one. */
        Assert.Contains("(read_bytes IS NOT NULL) AS byte_counters_tracked", sql, StringComparison.Ordinal);
        Assert.DoesNotContain("(extend_bytes IS NOT NULL) AS byte_counters_tracked", sql, StringComparison.Ordinal);
    }

    /// <summary>
    /// The SELECT list and the positional reader have to agree; a mismatch here shifts every column after
    /// the gap and reports one counter's value under another's name.
    /// </summary>
    [Fact]
    public void TheReadsSelectListMatchesThePositionalReader()
    {
        var sql = DarlingPgIoReader.PgIoSql;

        /* Anchored WITHOUT leading whitespace: PgIoSql is a raw string literal, so the compiler strips the
           common indentation and the runtime string is not indented the way the source file is. */
        var fromIdx = sql.IndexOf("FROM differenced", StringComparison.Ordinal);
        var outer = sql[sql.LastIndexOf("SELECT", fromIdx, StringComparison.Ordinal)..fromIdx];

        var items = outer
            .Split('\n')
            .Select(l => l.Trim())
            .Where(l => l.Length > 0 && !l.StartsWith("/*", StringComparison.Ordinal)
                        && !l.StartsWith("*", StringComparison.Ordinal)
                        && !l.StartsWith("SELECT", StringComparison.Ordinal))
            .Count(l => l.Contains(" AS ", StringComparison.Ordinal)
                        || l.TrimEnd(',') is "backend_type" or "object_type" or "context");

        /* 15 before this rung, plus read_bytes, write_bytes, extend_bytes and byte_counters_tracked. */
        Assert.Equal(19, items);
    }
}
