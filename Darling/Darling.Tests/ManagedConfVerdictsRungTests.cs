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
    /// The viewer probe's sentinel carries this rung, and the map's arm for it returns 146 — a position
    /// within the signature, not its end, now that V147 has appended its own.
    /// </summary>
    [Fact]
    public void TheProbeMapsAStoreStoppedHereToThisRung()
    {
        Assert.Contains(
            $"table_name = '{Table}'",
            ViewerDataService.StoreSchemaProbeSql.Replace("\r\n", "\n", StringComparison.Ordinal), StringComparison.Ordinal);

        var viewer = RepoFile.ReadRepoFile("Darling", "PerformanceMonitor.Darling.Viewer", "ViewerDataService.cs");
        Assert.Contains($"reader.GetBoolean({ProbeOrdinal})", viewer, StringComparison.Ordinal);

        Assert.Equal(StorageVersion.SchemaVersion, ViewerDataService.RequiredStoreSchemaVersion);

        var method = typeof(ViewerDataService).GetMethod("MapProbedSchemaVersion", BindingFlags.NonPublic | BindingFlags.Static)!;
        var arity = method.GetParameters().Length;

        /* A position within the signature, not its end: "ProbeOrdinal == arity - 1" asserted this rung is
           the NEWEST sentinel, which stopped being true the moment V147 appended its own. */
        Assert.True(ProbeOrdinal < arity - 1);
        Assert.Equal("hasManagedConfVerdicts", method.GetParameters()[ProbeOrdinal].Name);

        var atThisRung = Enumerable.Range(0, arity).Select(i => (object)(i <= ProbeOrdinal)).ToArray();
        Assert.Equal(RungVersion, (int)method.Invoke(null, atThisRung)!);

        var behind = (object[])atThisRung.Clone();
        behind[ProbeOrdinal] = false;
        Assert.Equal(PreviousVersion, (int)method.Invoke(null, behind)!);

        /* In the source, this rung's arm sits ABOVE the previous rung's and BELOW V147's, and returns this
           rung's own version. */
        var thisArm = viewer.IndexOf("if (hasManagedConfVerdicts)", StringComparison.Ordinal);
        var previousArm = viewer.IndexOf("if (hasQueryStoreIntervalWide)", StringComparison.Ordinal);
        var nextArm = viewer.IndexOf("if (hasComposeTimeoutSixty)", StringComparison.Ordinal);
        Assert.True(thisArm >= 0, "the viewer has no V146 sentinel arm — a fully-migrated store would map one rung low");
        Assert.True(previousArm >= 0, "the previous rung's arm is gone, so this pin is comparing against nothing");
        Assert.True(nextArm >= 0, "the V147 arm is gone, so the handoff this file claims never happened");
        Assert.True(thisArm < previousArm, "the V146 arm sits below V145's, so a current store maps one rung low");
        Assert.True(nextArm < thisArm, "the V147 arm sits below the V146 arm, so a current V147 store maps one rung low");
        Assert.Contains(
            "return " + RungVersion.ToString(CultureInfo.InvariantCulture) + ";",
            viewer[thisArm..previousArm], StringComparison.Ordinal);

        /* The V71 finding: the table is named in the probe line and nowhere in the arm's prose. */
        var armProseStart = viewer.LastIndexOf("/* V146 (#4215)", thisArm, StringComparison.Ordinal);
        Assert.True(armProseStart >= 0, "the V146 arm has no comment block saying why it exists");
        Assert.DoesNotContain(Table, viewer[armProseStart..thisArm], StringComparison.Ordinal);
    }
}
