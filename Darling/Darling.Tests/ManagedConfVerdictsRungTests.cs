/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using System.Globalization;
using System.Linq;
using System.Reflection;
using System.Text.RegularExpressions;
using PerformanceMonitor.Darling.Storage;
using PerformanceMonitor.Darling.Viewer;
using Xunit;

namespace Darling.Tests;

/// <summary>
/// Pins Darling rung V146 (#4215): <c>collect.managed_conf_verdicts</c>, the per-key verdict a managed store's
/// owner connection computes once at every service-owned start. This file is the RUNG: the ladder, the DDL, and
/// the viewer probe; the writer is pinned where it lives.
///
/// <para>This file took over the "I am the top rung" claim that moved off
/// <see cref="QueryStoreIntervalWideRungTests"/> (V145) when this rung landed, and hands it on to
/// <c>ComposeStatementTimeoutV147MigrationLiveTests</c> (V147, #4442) when that one did.</para>
/// </summary>
public sealed class ManagedConfVerdictsRungTests
{
    private const int RungVersion = 146;
    private const int PreviousVersion = 145;

    /// <summary>This rung's sentinel ordinal in the viewer probe. No longer the last argument — V147
    /// appended its own — so the invariant that outlives the handoff is that the ordinal is FIXED: a later
    /// rung appends after it and never shifts it.</summary>
    private const int ProbeOrdinal = 121;

    private const string Table = "managed_conf_verdicts";

    private static PgMigrations.Migration V146 => PgMigrations.Scripts.Single(m => m.Version == RungVersion);

    [Fact]
    public void TheRungIsRegisteredInADenseLadder()
    {
        var versions = PgMigrations.Scripts.Select(s => s.Version).ToList();

        Assert.Equal("managed-conf-verdicts", V146.Name);
        Assert.Equal(StorageVersion.SchemaVersion, PgMigrations.Scripts[^1].Version);
        Assert.Equal(StorageVersion.SchemaVersion, versions.Max());
        /* One below the top since V147 landed; the "RungVersion == StorageVersion.SchemaVersion" half of
           the top-arm claim moved to ComposeStatementTimeoutV147MigrationLiveTests with the top. */
        Assert.True(RungVersion < StorageVersion.SchemaVersion, "V146 is expected to sit below the ladder's top now that V147 has landed");
        Assert.Equal(versions.Distinct().OrderBy(v => v), versions);
    }

    /// <summary>One idempotent CREATE, engine-plain, no ALTER of anything that already exists, keyed on
    /// <c>setting_name</c> alone (there is no <c>server_id</c> — this is the store's own settings, not a
    /// monitored target's).</summary>
    [Fact]
    public void TheRungCreatesOneTable_EnginePlain_AndNothingElse()
    {
        var sql = V146.Sql.Replace("\r\n", "\n", StringComparison.Ordinal);

        Assert.Contains($"CREATE TABLE IF NOT EXISTS collect.{Table}", sql, StringComparison.Ordinal);
        Assert.Single(Regex.Matches(sql, "CREATE TABLE"));
        Assert.DoesNotContain("CREATE INDEX", sql, StringComparison.Ordinal);

        var body = Regex.Replace(sql, @"/\*.*?\*/", string.Empty, RegexOptions.Singleline);
        foreach (var absent in new[] { "GRANT", "VIEW", "DROP ", "ALTER TABLE", "CONCURRENTLY", "create_hypertable", "timescaledb" })
        {
            Assert.DoesNotContain(absent, body, StringComparison.OrdinalIgnoreCase);
        }

        /* Naive-UTC timestamp columns, never timestamptz. */
        Assert.DoesNotContain("timestamptz", sql, StringComparison.OrdinalIgnoreCase);
        Assert.Contains("computed_at timestamp NOT NULL", sql, StringComparison.Ordinal);
        Assert.Contains("postmaster_start_time timestamp NOT NULL", sql, StringComparison.Ordinal);

        /* Keyed on setting_name alone — no server_id, this is the store's own settings. */
        Assert.Contains("CONSTRAINT pk_managed_conf_verdicts PRIMARY KEY (setting_name)", sql, StringComparison.Ordinal);
        Assert.DoesNotContain("server_id", sql, StringComparison.Ordinal);

        /* No serial id. */
        Assert.DoesNotContain("serial", sql, StringComparison.OrdinalIgnoreCase);
    }

    /// <summary>
    /// The viewer's schema map, given every sentinel up to this rung's ordinal, returns this rung's version, and with
    /// this rung's sentinel clear, the previous rung's: a sentinel at the wrong ordinal maps a store one rung off.
    /// </summary>
    [Fact]
    public void TheProbeMapsAStoreStoppedHereToThisRung()
    {
        var method = typeof(ViewerDataService).GetMethod("MapProbedSchemaVersion", BindingFlags.NonPublic | BindingFlags.Static)!;
        var arity = method.GetParameters().Length;
        Assert.True(ProbeOrdinal < arity - 1);
        var atThisRung = Enumerable.Range(0, arity).Select(i => (object)(i <= ProbeOrdinal)).ToArray();
        Assert.Equal(RungVersion, (int)method.Invoke(null, atThisRung)!);
        var behind = (object[])atThisRung.Clone();
        behind[ProbeOrdinal] = false;
        Assert.Equal(PreviousVersion, (int)method.Invoke(null, behind)!);
    }
}
