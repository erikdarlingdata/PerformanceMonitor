/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor Lite.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;
using DuckDB.NET.Data;
using PerformanceMonitor.Analysis;
using PerformanceMonitorLite.Analysis;
using PerformanceMonitorLite.Database;
using PerformanceMonitorLite.Tests;
using Xunit;

namespace Lite.Tests;

/// <summary>
/// Real-DuckDB pin for #2991, the Lite twin of Darling's
/// <c>ParameterSensitivityClockFrameLiveTests</c>: the PARAMETER_SENSITIVITY detector's
/// compiled-before-the-window predicate must select the same plan population no matter what the
/// monitored server's UTC offset is.
///
/// <para><c>query_stats.creation_time</c> is the monitored server's LOCAL wall clock —
/// <c>QueryStatsCollector</c> ships the <c>sys.dm_exec_query_stats</c> value verbatim, and says so about
/// this very column: "creation_time is in the monitored server's local time while collection times are
/// UTC". The window bound it is compared against is naive UTC off <c>DateTime.UtcNow</c>. Comparing them
/// untranslated is a different question on every server, and the answer is wrong in both directions.</para>
///
/// <para><b>What the predicate is for.</b> It means "this plan was compiled before the analysis window
/// opened", which is what makes a wide min/max worker-time spread evidence of PARAMETER SENSITIVITY
/// rather than an artefact of a plan too young to have seen varied parameters yet. Loosening it or
/// removing it destroys the signal, so the fix is to make it mean what it says.</para>
///
/// <para><b>The invariance, not a timestamp.</b> This asserts a FRAME RELATIONSHIP: the same five-plan
/// fixture, re-expressed in each server's local clock the way the collector would really have stored it,
/// must yield the identical offender set. Pinning one expected <c>creation_time</c> instead would pass
/// for the wrong reason the moment anything shifted. Membership is asserted alongside invariance on
/// purpose — a constant empty result is also invariant, so invariance alone is not a test.</para>
///
/// <para><b>Both signs and zero.</b> UTC-4 is the live production fleet and the FALSE-POSITIVE
/// direction: a plan compiled at UTC instant T is stored as T-4h, so the untranslated predicate admits
/// T &lt;= W+4h and on the default four-hour window that is every plan compiled inside the window —
/// precisely the population the predicate exists to exclude. A POSITIVE offset is the suppression
/// direction: at UTC+10 it admits only plans compiled at least ten hours before the window, so the plans
/// that legitimately predate it are discarded and the finding class is sharply thinned. A fixture at
/// -240 alone would never see that half.</para>
///
/// <para><b>Watched RED before the fix</b> at 5 / 3 / 1 offenders for -240 / 0 / +600 against a true 3,
/// on this exact fixture. Zero was the only offset that was ever right, which is why every store anyone
/// develops against agreed with the bug.</para>
///
/// <para>Real DuckDB rather than a string pin for two reasons this dialect makes specific. Lite's copy
/// of the query is an inline <c>CommandText</c>, so there is no constant for a text assertion to name;
/// and the thing under test is the ENGINE's answer to <c>&lt;signed int&gt; * INTERVAL '1' MINUTE</c>
/// against a naive <c>TIMESTAMP</c>. DuckDB has no <c>make_interval(mins =&gt; ...)</c>, so this dialect
/// cannot share Darling's spelling, and <c>AT TIME ZONE</c> would drag in ICU — the multiplication form
/// is the one already used against the sibling column in this same table
/// (<c>LocalDataService.QueryStats</c>), and it needs no extension.</para>
/// </summary>
public sealed class ParameterSensitivityClockFrameTests : IClassFixture<SharedDuckDbFixture>, IDisposable
{
    private const int ServerId = 29910;
    private const string ServerName = "SynthSrv";
    private const string Db = "SynthDb";

    /* The three offsets the fix has to hold at. -240 is the live fleet and the false-positive
       direction, +600 is the suppression direction, 0 is the dev box and the no-offset fallback. */
    private const int EasternOffsetMinutes = -240;
    private const int UtcOffsetMinutes = 0;
    private const int FarEastOffsetMinutes = 600;

    /* Minutes of the plan's compile instant relative to the window START, in UTC, and whether it
       therefore belongs in the offender set. Two plans straddle the bound by a single minute in each
       direction: that is what makes a wrong frame change the ANSWER rather than just the arithmetic. */
    private static readonly (string Label, int MinutesFromWindowStart, bool Expected)[] Plans =
    [
        ("old_3d", -3 * 24 * 60, true),
        ("pre_5h", -5 * 60,      true),
        ("pre_1m", -1,           true),
        ("in_1m",  1,            false),
        ("in_3h",  3 * 60,       false),
    ];

    private static int ExpectedOffenders => Plans.Count(p => p.Expected);

    private static readonly DateTime WindowEnd =
        DateTime.SpecifyKind(new DateTime(DateTime.UtcNow.Ticks - (DateTime.UtcNow.Ticks % TimeSpan.TicksPerSecond)), DateTimeKind.Unspecified);
    private static readonly DateTime WindowStart = WindowEnd.AddHours(-4);

    private readonly DuckDbInitializer _duckDb;
    private DuckDBConnection? _seedConn;
    private long _nextId = 1;

    public ParameterSensitivityClockFrameTests(SharedDuckDbFixture fixture)
    {
        fixture.ResetData();
        _duckDb = fixture.DuckDb;
    }

    public void Dispose() => _seedConn?.Dispose();

    private static AnalysisContext Context() => new()
    {
        ServerId = ServerId,
        ServerName = ServerName,
        TimeRangeStart = WindowStart,
        TimeRangeEnd = WindowEnd,
    };

    [Fact]
    public async Task TheCompiledBeforeTheWindowPredicate_SelectsTheSamePlans_AtEveryServerUtcOffset()
    {
        var observed = new Dictionary<string, double>(StringComparer.Ordinal);
        var drilled = new Dictionary<string, double>(StringComparer.Ordinal);

        /* Each case re-expresses the SAME five compile instants in a different server-local clock,
           exactly as the collector would have stored them, and writes the matching collected offset.
           The last case writes NO offset at all. */
        foreach (var (name, offsetMinutes) in new (string, int?)[]
        {
            ("utc-4 (the live fleet)", EasternOffsetMinutes),
            ("utc (the dev box)",      UtcOffsetMinutes),
            ("utc+10 (suppression)",   FarEastOffsetMinutes),
            ("no collected offset",    null),
        })
        {
            await ClearAsync();
            if (offsetMinutes.HasValue)
            {
                await SeedServerPropertiesAsync(offsetMinutes.Value);
            }
            await SeedPlansAsync(offsetMinutes ?? 0);

            var facts = await new DuckDbFactCollector(_duckDb).CollectFactsAsync(Context());
            var fact = facts.FirstOrDefault(f => f.Key == "PARAMETER_SENSITIVITY");

            Assert.NotNull(fact);
            observed[name] = fact!.Metadata["offender_count"];

            /* The drill-down re-runs the detector's own signature to list the offenders, so it carries
               a SECOND copy of the same predicate. A fix applied only to the detector would leave the
               operator reading a list assembled in the wrong frame. */
            drilled[name] = await CountParameterSensitiveDrillDownAsync();
        }

        /* The invariance. The offset is a property of the SERVER's clock, not of its workload, so it
           must not be able to change which plans the detector counts. */
        Assert.True(
            observed.Values.Distinct().Count() == 1,
            "the compiled-before-the-window predicate selected a DIFFERENT plan population per server "
            + "UTC offset, so creation_time is still being compared across frames: "
            + string.Join(", ", observed.Select(kv => $"{kv.Key} => {kv.Value}")));

        /* And membership, because a constant empty answer would satisfy the invariance above while
           having destroyed the signal. The two plans that straddle the window bound by one minute are
           what make this assertion load-bearing. */
        foreach (var (name, count) in observed)
        {
            Assert.True(
                count == ExpectedOffenders,
                $"at {name} the detector counted {count} offenders against the {ExpectedOffenders} plans "
                + "whose UTC compile instant really precedes the window. Admitting the in-window plans "
                + "is the UTC-4 failure (5 here); dropping the ones that just predate the window is the "
                + "UTC+10 failure (1 here).");
        }

        /* Same two assertions again for the drill-down's own copy of the predicate. */
        Assert.True(
            drilled.Values.Distinct().Count() == 1,
            "the drill-down's copy of the compiled-before-the-window predicate selected a DIFFERENT "
            + "plan population per server UTC offset: "
            + string.Join(", ", drilled.Select(kv => $"{kv.Key} => {kv.Value}")));

        foreach (var (name, count) in drilled)
        {
            Assert.True(
                count == ExpectedOffenders,
                $"at {name} the drill-down listed {count} offenders against the {ExpectedOffenders} "
                + "plans whose UTC compile instant really precedes the window.");
        }
    }

    /// <summary>
    /// Drives the drill-down's own copy of the detection through the real enrich seam. Severity is set
    /// past the display gate, below which the expensive drill-downs are skipped wholesale and this
    /// collector never runs at all.
    /// </summary>
    private async Task<double> CountParameterSensitiveDrillDownAsync()
    {
        var finding = new AnalysisFinding
        {
            RootFactKey = "PARAMETER_SENSITIVITY",
            StoryPath = "PARAMETER_SENSITIVITY",
            Severity = 1.0,
        };

        await new DrillDownCollector(_duckDb).EnrichFindingsAsync([finding], Context());

        if (finding.DrillDown is null
            || !finding.DrillDown.TryGetValue("parameter_sensitive_queries", out var raw))
        {
            return 0;
        }

        return System.Text.Json.JsonSerializer.SerializeToElement(raw).GetArrayLength();
    }

    private async Task<DuckDBConnection> SeedConnectionAsync()
    {
        if (_seedConn is null)
        {
            _seedConn = _duckDb.CreateConnection();
            await _seedConn.OpenAsync();
        }
        return _seedConn;
    }

    private async Task ClearAsync()
    {
        using var readLock = _duckDb.AcquireReadLock();
        var connection = await SeedConnectionAsync();
        foreach (var table in new[] { "query_stats", "server_properties" })
        {
            using var cmd = connection.CreateCommand();
            cmd.CommandText = $"DELETE FROM {table} WHERE server_id = $1";
            cmd.Parameters.Add(new DuckDBParameter { Value = ServerId });
            await cmd.ExecuteNonQueryAsync();
        }
    }

    private async Task SeedServerPropertiesAsync(int offsetMinutes)
    {
        using var readLock = _duckDb.AcquireReadLock();
        var connection = await SeedConnectionAsync();
        using var cmd = connection.CreateCommand();
        cmd.CommandText = @"
INSERT INTO server_properties
    (collection_id, collection_time, server_id, server_name, edition, product_version, product_level,
     engine_edition, utc_offset_minutes)
VALUES ($1, $2, $3, $4, 'Enterprise Edition', '16.0.4085.2', 'RTM', 3, $5)";
        cmd.Parameters.Add(new DuckDBParameter { Value = _nextId++ });
        cmd.Parameters.Add(new DuckDBParameter { Value = WindowEnd });
        cmd.Parameters.Add(new DuckDBParameter { Value = ServerId });
        cmd.Parameters.Add(new DuckDBParameter { Value = ServerName });
        cmd.Parameters.Add(new DuckDBParameter { Value = offsetMinutes });
        await cmd.ExecuteNonQueryAsync();
    }

    /// <summary>
    /// Seeds the five plans. <paramref name="offsetMinutes"/> is applied to <c>creation_time</c> and
    /// ONLY to creation_time: server-local is UTC plus the offset, which is what the DMV would have read
    /// on a server at that offset. collection_time stays UTC because the collector stamps it from the
    /// monitoring host's clock. Every row clears the detector's other floors with room to spare, so the
    /// only thing that can move the offender count is the compile-time predicate.
    /// </summary>
    private async Task SeedPlansAsync(int offsetMinutes)
    {
        using var readLock = _duckDb.AcquireReadLock();
        var connection = await SeedConnectionAsync();

        for (var i = 0; i < Plans.Length; i++)
        {
            var plan = Plans[i];
            var compiledUtc = WindowStart.AddMinutes(plan.MinutesFromWindowStart);

            using var cmd = connection.CreateCommand();
            cmd.CommandText = @"
INSERT INTO query_stats
    (collection_id, collection_time, server_id, server_name, database_name, query_hash, query_plan_hash,
     creation_time, execution_count, min_worker_time, max_worker_time, min_grant_kb, max_grant_kb,
     min_spills, max_spills, query_text, delta_execution_count)
VALUES ($1, $2, $3, $4, $5, $6, $7, $8, 5000, 20000, 20000000, 1024, 1048576, 0, 50, $9, 500)";
            cmd.Parameters.Add(new DuckDBParameter { Value = _nextId++ });
            cmd.Parameters.Add(new DuckDBParameter { Value = WindowStart.AddMinutes(30 + i) });
            cmd.Parameters.Add(new DuckDBParameter { Value = ServerId });
            cmd.Parameters.Add(new DuckDBParameter { Value = ServerName });
            cmd.Parameters.Add(new DuckDBParameter { Value = Db });
            cmd.Parameters.Add(new DuckDBParameter { Value = "0xQH_" + plan.Label });
            cmd.Parameters.Add(new DuckDBParameter { Value = "0xPH_" + plan.Label });
            cmd.Parameters.Add(new DuckDBParameter { Value = compiledUtc.AddMinutes(offsetMinutes) });
            cmd.Parameters.Add(new DuckDBParameter { Value = "SELECT * FROM dbo.Synth_" + plan.Label });
            await cmd.ExecuteNonQueryAsync();
        }
    }
}
