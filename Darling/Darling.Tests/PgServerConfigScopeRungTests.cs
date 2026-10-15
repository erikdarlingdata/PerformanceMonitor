/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using System.Collections.Generic;
using System.Globalization;
using System.IO;
using System.Linq;
using System.Reflection;
using System.Text.Json;
using System.Text.RegularExpressions;
using System.Threading.Tasks;
using Npgsql;
using PerformanceMonitor.Collectors;
using PerformanceMonitor.Darling.Service.Mcp;
using PerformanceMonitor.Darling.Storage;
using PerformanceMonitor.Darling.Viewer;
using Xunit;

namespace Darling.Tests;

/// <summary>
/// Pins Darling rung V138 (partial #3691, the "per-database settings" collector line): TWO nullable columns
/// on the existing <c>collect.pg_server_config</c> — <c>database_name</c> and <c>role_name</c> — so a stored
/// setting row says WHOSE setting it is. No new table, no new hypertable
/// (<see cref="TimescaleSupport.HypertableCount"/> stays 72), no DEFAULT, no backfill, no passthrough (the
/// table has no <c>v_</c> view), no Lite twin (Lite has no PostgreSQL collectors at all). The shape is
/// <see cref="QsCaptureModeRouteKnobToastRungTests"/>' (V137) column half, one table instead of three.
///
/// <para><b>The lie it ends.</b> <c>pg_settings</c> is the RESOLVED view for the collector's own backend, so
/// a cluster where one database carries <c>ALTER DATABASE … SET work_mem</c> stored nothing about it and the
/// <c>CONFIG_PG_*</c> facts graded a value that database's sessions never use. The collector now
/// <c>UNION ALL</c>s <c>pg_db_role_setting</c> onto <c>pg_settings</c>, and these two columns are what tell
/// the two populations apart in the stored rows.</para>
///
/// <para><b>The correctness edge is the READS, which is why the census below is the centre of this file.</b>
/// Every shipped read of this table is a latest-snapshot, one-row-per-setting-NAME shape — several loading a
/// dictionary keyed by name, one taking a single row by <c>ORDER BY … LIMIT 1</c>, one partitioning a
/// <c>LAG</c> by name — so an override row arriving with a duplicate name would shadow the server-wide value,
/// throw on a duplicate key, or manufacture a configuration change. Nine reads therefore carry
/// <c>database_name IS NULL AND role_name IS NULL</c> and exactly ONE selects the overrides. The census
/// asserts that partition over the tree rather than over a list, so the NEXT reader has to choose an arm
/// deliberately.</para>
///
/// <para>This file carried the "I am the top rung" claims that moved off
/// <see cref="QsCaptureModeRouteKnobToastRungTests"/> (V137) when this rung landed, and handed them on to
/// <see cref="PostmasterStartTimeRungTests"/> (V139, #3955) when that one did. What stays is everything true of
/// this rung wherever it sits: its name, its DDL, its probe sentinel at its own ordinal, and that a store which
/// stopped here maps to exactly 138.</para>
///
/// <para>The collector's <c>pg_settings</c> behaviour and its two original reads are
/// <see cref="PgServerConfigTests"/> (V102); the page contract and the snapshot counts are
/// <c>McpPageContractTests</c>. This file is the RUNG: the ladder, the DDL, the V102 restatement, the
/// collector's payload tail and UNION, the probe, and the live round trip through
/// <c>get_pg_server_config</c>.</para>
/// </summary>
[Trait("Stage", "Guard")] // #5798: needs no store, so the guard stage runs it on every pull request
public sealed class PgServerConfigScopeRungTests
{
    private const int RungVersion = 138;
    private const int PreviousVersion = 137;

    /// <summary>This rung's sentinel ordinal in the viewer probe. No longer the last argument — V139 (#3955, the
    /// postmaster start time) appended its own — so this is a position within the signature rather than its end,
    /// the handoff <see cref="QsCaptureModeRouteKnobToastRungTests"/> made to this file one rung ago.</summary>
    private const int ProbeOrdinal = 113;

    private static PgMigrations.Migration V138 => PgMigrations.Scripts.Single(m => m.Version == RungVersion);

    /* ---- the rung ------------------------------------------------------------------------------------ */

    [Fact]
    public void TheRungIsRegisteredAtTheTopOfADenseLadder()
    {
        var versions = PgMigrations.Scripts.Select(s => s.Version).ToList();

        Assert.Equal("pg-server-config-database-role-overrides", V138.Name);
        Assert.Equal(StorageVersion.SchemaVersion, PgMigrations.Scripts[^1].Version);
        Assert.Equal(StorageVersion.SchemaVersion, versions.Max());
        /* Not `RungVersion == SchemaVersion` any more: that asserted this rung is the newest, which stopped
           being true when V139 landed. The invariant that outlives the handoff is that the LADDER's top and
           the declared version agree, which the two lines above already say. */
        Assert.True(RungVersion < StorageVersion.SchemaVersion);
        Assert.Equal(versions.Distinct().OrderBy(v => v), versions);
    }

    /* ---- the collector ------------------------------------------------------------------------------- */

    /* ---- the probe (top arm) -------------------------------------------------------------------------- */

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
        var all = Enumerable.Repeat((object)true, arity).ToArray();
        Assert.Equal(StorageVersion.SchemaVersion, (int)method.Invoke(null, all)!);
        var atThisRung = Enumerable.Range(0, arity).Select(i => (object)(i <= ProbeOrdinal)).ToArray();
        Assert.Equal(RungVersion, (int)method.Invoke(null, atThisRung)!);
        var behind = (object[])atThisRung.Clone();
        behind[ProbeOrdinal] = false;
        Assert.Equal(PreviousVersion, (int)method.Invoke(null, behind)!);
    }

    /* ---- the tool's section --------------------------------------------------------------------------- */

    /* ---- the live round trip -------------------------------------------------------------------------- */

    /// <summary>
    /// #4251 round-1 review, L1: nothing offline kept <see cref="PgServerConfigCollector"/>'s two query texts
    /// (plain, and the <c>pg_file_settings</c> route) in step — only a live run with a granted role ever
    /// exercised the second one. This pins that the two BuildQuery outputs differ in exactly one span: the
    /// <c>pending_restart</c> column. A column added to one text alone, or a file-settings predicate that
    /// drifted from what <c>PayloadColumns</c> expects, fails this without needing a live database.
    /// </summary>
    [Fact]
    public void TheFileSettingsQuery_IsTheDefaultQuery_WithOnlyThePendingRestartColumnChanged()
    {
        static string Text(bool readable) => PgServerConfigCollector.Instance.BuildQuery(new CollectorContext
        {
            ServerId = -4251,
            ServerName = "pin-4251",
            CollectionTime = DateTime.UtcNow,
            Deltas = NoDeltas.Instance,
            PgFileSettingsReadable = readable,
        }).Text;

        var plain = Text(false);
        var withFile = Text(true);
        const string plainColumn = "    s.pending_restart                       AS pending_restart,";
        Assert.Single(Regex.Matches(plain, Regex.Escape(plainColumn)));

        var start = withFile.IndexOf("    (s.pending_restart", StringComparison.Ordinal);
        Assert.True(start >= 0);
        const string tail = "AS pending_restart,";
        var end = withFile.IndexOf(tail, start, StringComparison.Ordinal) + tail.Length;
        Assert.Equal(plain, withFile[..start] + plainColumn + withFile[end..]);
    }

    private sealed class NoDeltas : ICollectorDeltaCalculator
    {
        public static readonly NoDeltas Instance = new();

        public long CalculateDelta(int serverId, string key, string metric, long current, DateTime? at = null, int i = 0) => 0;

        public long CalculateDeltaWithInterval(int serverId, string key, string metric, long current, out int seconds, DateTime? at = null, int i = 0)
        {
            seconds = 60;
            return 0;
        }
    }
}
