/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using System.Text.Json;
using System.Threading;
using System.Threading.Tasks;
using Npgsql;
using PerformanceMonitor.Analysis;
using PerformanceMonitor.Collectors;
using PerformanceMonitor.Common;
using PerformanceMonitor.Darling.Analysis;
using PerformanceMonitor.Darling.Service.Mcp;
using PerformanceMonitor.Darling.Storage;
using Xunit;

namespace Darling.Tests;

/// <summary>
/// #2506: the ANALYSIS family's window anchor. #2495 anchored the read surface and deliberately left
/// these four out, because their window is built inside the analysis engine rather than in the tool and
/// because <c>analyze_server</c> PERSISTS what it finds.
///
/// <para>These are the ungated halves: the engine's own rule about when a pass may write, and the shape
/// of the refusal. The live half — seed a past window, analyze it anchored, analyze it unanchored, and
/// prove the two disagree — is <see cref="AnalysisAsOfAnchorLivePostgresTests"/>.</para>
/// </summary>
public sealed class AnalysisAsOfAnchorTests
{
    /// <summary>
    /// The whole persistence decision, stated as one derivable rule rather than as a flag a caller has to
    /// remember to set. A settable flag would let "anchored AND persist" be expressed, and there is no
    /// caller that should be able to express it.
    /// </summary>
    [Fact]
    public void AnAnchoredPass_DoesNotPersist_AndAnUnanchoredOneStillDoes()
    {
        Assert.True(new AnalysisContext().PersistFindings);

        Assert.True(new AnalysisContext
        {
            TimeRangeStart = new DateTime(2026, 8, 19, 1, 0, 0, DateTimeKind.Utc),
            TimeRangeEnd = new DateTime(2026, 8, 19, 5, 0, 0, DateTimeKind.Utc)
        }.PersistFindings);

        Assert.False(new AnalysisContext
        {
            TimeRangeStart = new DateTime(2026, 8, 19, 1, 0, 0, DateTimeKind.Utc),
            TimeRangeEnd = new DateTime(2026, 8, 19, 5, 0, 0, DateTimeKind.Utc),
            AsOfUtc = new DateTime(2026, 8, 19, 5, 0, 0, DateTimeKind.Utc)
        }.PersistFindings);
    }

    /// <summary>
    /// The rule is on the ANCHOR, not on how old the window happens to be. An anchor a few seconds in the
    /// past — what an agent sends when it computes "now" from its own clock — is still an anchor and still
    /// exploratory, because the alternative is a persistence decision that depends on how long the pass
    /// took to start, which is not a decision anybody could reason about.
    /// </summary>
    [Fact]
    public void AnAnchorAtEssentiallyNow_IsStillAnAnchor()
    {
        Assert.False(new AnalysisContext { AsOfUtc = DateTime.UtcNow }.PersistFindings);
    }

    /// <summary>
    /// The window builder honours the anchor: <c>hours_back</c> stays the LENGTH and the anchor becomes the
    /// END. Asserted on the engine's own entry point rather than on a tool, because that is the seam #2495
    /// could not reach and the whole reason this was a separate issue.
    /// </summary>
    [Fact]
    public void TheEngineEntryPoint_BuildsTheWindowEndingAtTheAnchor()
    {
        var anchor = new DateTime(2026, 8, 19, 5, 0, 0, DateTimeKind.Utc);

        /* Built the same way DarlingAnalysisService.AnalyzeAsync builds it — the arithmetic is the
           contract, and it is one line in both SKUs. */
        var context = new AnalysisContext
        {
            TimeRangeEnd = anchor,
            TimeRangeStart = anchor.AddHours(-4),
            AsOfUtc = anchor
        };

        Assert.Equal(new DateTime(2026, 8, 19, 1, 0, 0, DateTimeKind.Utc), context.TimeRangeStart);
        Assert.Equal(4 * 60 * 60 * 1000d, context.PeriodDurationMs);
        Assert.False(context.PersistFindings);
    }
}

/// <summary>
/// Gated (DARLING_TEST_PG) proof of the point: an incident planted in a PAST window is analyzed when the
/// call is anchored there and is invisible on the default anchor — and the anchored run leaves the
/// findings table exactly as it found it, while the SAME window analyzed unanchored writes to it.
/// </summary>
[Collection("live-postgres")]
public sealed class AnalysisAsOfAnchorLivePostgresTests
{
    private const string ServerName = "darling-analysis-asof-e2e";
    private const string WaitType = "AN2506_TEST_WAIT";
    private const string HistoricStoryHash = "an2506-historic-hash";
    private const string AheadStoryHash = "an2506-ahead-of-now-hash";
    private static readonly int ServerId = ServerIdHelper.GetDeterministicHashCode(ServerName);

    private static string? ConnectionString => Environment.GetEnvironmentVariable("DARLING_TEST_PG");

    [Fact]
    public async Task TheAnalysisFamilyAnswersAboutAPastWindow_AndTheAnchoredRunPersistsNothing()
    {
        var cs = ConnectionString;
        Assert.SkipWhen(string.IsNullOrEmpty(cs), "Set DARLING_TEST_PG to a Postgres connection string to run the live analysis as_of test.");

        var ct = TestContext.Current.CancellationToken;
        using var connection = new NpgsqlConnection(cs);
        await connection.OpenAsync(ct);
        await PgMigrations.MigrateAsync(connection, ct);
        await DeleteRowsAsync(connection, ct);

        await using var postgres = NpgsqlDataSource.Create(cs!);

        var bodySucceeded = false;
        try
        {
            await RegisterServerAsync(connection, ct);
            var service = new DarlingAnalysisService(postgres);

            /* ── the fixture, DarlingAnalysisPipelineTests' recipe verbatim ──
               >24h of wait_stats history in ONE hour-of-day x day-of-week bucket (a past Monday 10:00
               UTC, 8-14 days back), then three heavy collections on the FOLLOWING Monday at the same
               hour. The span carries the pipeline's 24h data-span gate on real MIN/MAX arithmetic, and
               the heavy window rates at ~666 ms/sec against a 250 ms/sec fallback bar, which is one
               ANOMALY_WAIT_PROFILE fact — enough to root a story. */
            var day = DateTime.UtcNow.Date.AddDays(-8);
            while (day.DayOfWeek != DayOfWeek.Monday) day = day.AddDays(-1);
            var historyStart = Naive(day.AddHours(10));

            for (var i = 0; i < 12; i++)
            {
                await PlantWaitAsync(connection, i + 1, historyStart.AddMinutes(5 * i), (i == 5 || i == 6) ? 0L : 60_000L, ct);
            }

            var incidentStart = historyStart.AddDays(7);
            await PlantWaitAsync(connection, 100, incidentStart.AddMinutes(5), 200_000L, ct);
            await PlantWaitAsync(connection, 101, incidentStart.AddMinutes(10), 200_000L, ct);
            await PlantWaitAsync(connection, 102, incidentStart.AddMinutes(15), 200_000L, ct);

            /* The anchor: one hour AFTER the incident's first collection, so `hours_back = 1` names
               exactly the window the pipeline fixture was built for. Deliberately not "the incident plus
               a bit either side": the baseline is keyed on the window's START hour, and a window starting
               at 09:30 would be scored against a different hour-of-day bucket than the one the history
               fills — a real behaviour of this engine, and one an anchored test should not stumble into
               by accident. */
            var incidentEnd = incidentStart.AddHours(1);
            var anchor = Utc(incidentEnd).ToString("o");

            /* ── 1. get_analysis_facts: the incident's window scores facts; the default window has none.
                  Both branches are reachable — the planted rows are >= 7 days old, so no legal default
                  window (the widest on this tool is its own hours_back) can reach them. */
            var factsAnchored = await DarlingMcpTools.GetAnalysisFacts(service, postgres, ServerName, 1, as_of: anchor);
            Assert.Contains(WaitType, factsAnchored, StringComparison.Ordinal);

            var factsNow = await DarlingMcpTools.GetAnalysisFacts(service, postgres, ServerName);
            Assert.Equal("unavailable", StatusOf(factsNow));

            /* ── 2. analyze_server anchored: the anomaly is found, and the result says out loud that it
                  was not written down. */
            var analysisAnchored = await DarlingMcpTools.AnalyzeServer(service, postgres, ServerName, 1, anchor);
            using (var doc = JsonDocument.Parse(analysisAnchored))
            {
                Assert.Equal("findings", doc.RootElement.GetProperty("status").GetString());
                Assert.False(doc.RootElement.GetProperty("persisted").GetBoolean());
                Assert.Contains(
                    "NOT written to the store",
                    doc.RootElement.GetProperty("persistence_note").GetString()!,
                    StringComparison.Ordinal);
            }

            Assert.Contains("ANOMALY_WAIT_PROFILE", analysisAnchored, StringComparison.Ordinal);

            /* ── 3. …and it really did not write. This is the assertion the whole persistence argument
                  rests on: a note in the payload that the row was withheld is worth nothing if the row
                  is there anyway. */
            Assert.Equal(0L, await CountFindingsAsync(connection, ct));

            /* ── 4. The SAME window, unanchored, DOES persist — so step 3 is the anchor's doing and not
                  a fixture that could never produce a finding in the first place. Without this the
                  previous assertion passes for a server that simply has nothing to say. */
            var persistedFindings = await service.AnalyzeAsync(new AnalysisContext
            {
                ServerId = ServerId,
                ServerName = ServerName,
                TimeRangeStart = incidentStart,
                TimeRangeEnd = incidentEnd,
                ServerUtcOffset = TimeSpan.Zero
            });

            Assert.NotEmpty(persistedFindings);
            Assert.True(await CountFindingsAsync(connection, ct) > 0);

            /* ── 5. get_analysis_findings windows on ANALYSIS TIME. The rows step 4 just wrote are
                  stamped NOW, and a row planted as if a scheduled pass had run 30 hours ago is outside
                  every default window (the tool's default is 24). Anchored at that pass, the read
                  returns it and NOT the rows from now — which is what proves the upper bound is doing
                  work rather than the read simply starting earlier. */
            var historicRun = Naive(DateTime.UtcNow.AddHours(-30));
            await PlantFindingAsync(connection, historicRun, ct);

            var findingsNow = await DarlingMcpTools.GetAnalysisFindings(service, postgres, ServerName);

            /* The default read must actually be RETURNING something — step 4's rows — or the
               DoesNotContain below would pass on a read that came back empty for an unrelated reason,
               which is an assertion that cannot fail and therefore proves nothing. */
            Assert.True(
                JsonDocument.Parse(findingsNow).RootElement.GetProperty("finding_count").GetInt32() >= 1,
                "the default findings read returned nothing, so the exclusion below would prove nothing");
            Assert.DoesNotContain(HistoricStoryHash, findingsNow, StringComparison.Ordinal);

            var findingsAnchored = await DarlingMcpTools.GetAnalysisFindings(
                service, postgres, ServerName, 4, false, Utc(historicRun.AddMinutes(30)).ToString("o"));
            Assert.Contains(HistoricStoryHash, findingsAnchored, StringComparison.Ordinal);

            /* Exactly one: the anchored window's UPPER bound is what keeps step 4's now-stamped rows out.
               A read that only moved its START earlier would return them too, and would look like a
               working anchor right up until someone counted. */
            Assert.Equal(1, JsonDocument.Parse(findingsAnchored).RootElement.GetProperty("finding_count").GetInt32());

            /* ── 6. compare_analysis hangs BOTH windows off the anchor — the peak-vs-a-week-ago
                  comparison the tool exists for, which was unreachable while both windows were pinned
                  to now. 168 and not 169: baseline_hours_back is a span like any other and
                  ValidateHoursBack caps it at MaxHoursBack, so the baseline lands an hour past the
                  history rows rather than on them. That does not weaken the claim — the claim is that
                  both bounds MOVED WITH THE ANCHOR, and it is asserted on the rendered instants rather
                  than on a field path, so it holds for the data-bearing shape and for the "neither
                  window had facts" status envelope alike. */
            var compared = await DarlingMcpTools.CompareAnalysis(service, postgres, ServerName, 1, 168, anchor);
            Assert.Contains(Utc(incidentEnd).ToString("o"), compared, StringComparison.Ordinal);
            Assert.Contains(Utc(incidentEnd.AddHours(-168)).ToString("o"), compared, StringComparison.Ordinal);

            /* The control: unanchored, the same two instants are nowhere in the payload, because both
               windows hang off now. Without this the assertions above would pass on a tool that echoed
               the anchor into the payload and windowed on the clock. */
            var comparedNow = await DarlingMcpTools.CompareAnalysis(service, postgres, ServerName, 1, 168);
            Assert.DoesNotContain(Utc(incidentEnd).ToString("o"), comparedNow, StringComparison.Ordinal);

            /* ── 7. The UNANCHORED findings read keeps its half-open window, which is not a detail:
                  bounding it at "now" broke a live test on the first attempt, and the mechanism behind
                  that is real. analysis_time is stamped by the WRITER and filtered by the READER, so a
                  default read bounded at the reader's clock drops a run written a moment earlier the
                  day those two clocks stop being the same one. A row stamped ahead of now is the cheap,
                  deterministic stand-in for that skew. */
            await PlantFindingAsync(connection, Naive(DateTime.UtcNow.AddMinutes(30)), ct, AheadStoryHash);
            Assert.Contains(
                AheadStoryHash,
                await DarlingMcpTools.GetAnalysisFindings(service, postgres, ServerName),
                StringComparison.Ordinal);

            /* …and the ANCHORED read still excludes it, so the bound is present exactly where it is
                  needed and absent exactly where it would do harm. */
            Assert.DoesNotContain(
                AheadStoryHash,
                await DarlingMcpTools.GetAnalysisFindings(
                    service, postgres, ServerName, 4, false, Utc(historicRun.AddMinutes(30)).ToString("o")),
                StringComparison.Ordinal);

            /* ── 8. Refusals reach the caller as the tool's own message, on the persisting tool too — a
                  bad anchor must never fall back to "now" and then run a real, persisting analysis. */
            Assert.StartsWith(
                "Invalid as_of",
                await DarlingMcpTools.AnalyzeServer(service, postgres, ServerName, 1, "last tuesday"),
                StringComparison.Ordinal);
            Assert.Contains(
                "future",
                await DarlingMcpTools.AnalyzeServer(service, postgres, ServerName, 1, DateTime.UtcNow.AddDays(1).ToString("o")),
                StringComparison.Ordinal);

            bodySucceeded = true;
        }
        finally
        {
            await LiveStoreCleanup.RunAsync(cs!, bodySucceeded, async (cleanup, cleanupCt) =>
                await DeleteRowsAsync(cleanup, cleanupCt));
        }
    }

    private static string StatusOf(string json)
    {
        using var doc = JsonDocument.Parse(json);
        return doc.RootElement.GetProperty("status").GetString()!;
    }

    /// <summary>Naive-UTC, the Kind every timestamp column in this store is bound with.</summary>
    private static DateTime Naive(DateTime value) => DateTime.SpecifyKind(value, DateTimeKind.Unspecified);

    /// <summary>The same instant re-labelled UTC, so <c>ToString("o")</c> renders the Z form as_of parses.</summary>
    private static DateTime Utc(DateTime value) => DateTime.SpecifyKind(value, DateTimeKind.Utc);

    private static async Task<long> CountFindingsAsync(NpgsqlConnection connection, CancellationToken ct)
    {
        using var command = new NpgsqlCommand("SELECT COUNT(*) FROM analysis_findings WHERE server_id = $1", connection);
        command.Parameters.AddWithValue(ServerId);
        return Convert.ToInt64(await command.ExecuteScalarAsync(ct) ?? 0L);
    }

    private static async Task RegisterServerAsync(NpgsqlConnection connection, CancellationToken ct)
    {
        using var command = new NpgsqlCommand(@"
INSERT INTO servers (server_id, server_name, display_name, is_enabled, sql_major_version, created_date, modified_date)
VALUES ($1, $2, $3, TRUE, 15, $4, $4)
ON CONFLICT (server_id) DO UPDATE SET is_enabled = TRUE, sql_major_version = 15;", connection);
        command.Parameters.AddWithValue(ServerId);
        command.Parameters.AddWithValue(ServerName);
        command.Parameters.AddWithValue(ServerName);
        command.Parameters.AddWithValue(Naive(DateTime.UtcNow));
        await command.ExecuteNonQueryAsync(ct);
    }

    private static async Task PlantWaitAsync(
        NpgsqlConnection connection, long collectionId, DateTime at, long waitMs, CancellationToken ct)
    {
        using var command = new NpgsqlCommand(@"
INSERT INTO wait_stats
    (collection_id, collection_time, server_id, server_name, wait_type, delta_waiting_tasks, delta_wait_time_ms)
VALUES ($1, $2, $3, $4, $5, $6, $7)", connection);
        command.Parameters.AddWithValue(collectionId);
        command.Parameters.AddWithValue(at);
        command.Parameters.AddWithValue(ServerId);
        command.Parameters.AddWithValue(ServerName);
        command.Parameters.AddWithValue(WaitType);
        command.Parameters.AddWithValue(50L);
        command.Parameters.AddWithValue(waitMs);
        await command.ExecuteNonQueryAsync(ct);
    }

    /// <summary>
    /// One finding row as a scheduled pass would have left it 30 hours ago. Planted rather than produced,
    /// because producing it would mean an analysis whose <c>analysis_time</c> is stamped NOW — the very
    /// thing the anchored run refuses to do.
    /// </summary>
    private static async Task PlantFindingAsync(
        NpgsqlConnection connection, DateTime analysisTime, CancellationToken ct, string? storyPathHash = null)
    {
        using var command = new NpgsqlCommand(@"
INSERT INTO analysis_findings
    (finding_id, analysis_time, server_id, server_name, database_name,
     time_range_start, time_range_end, severity, confidence, category,
     story_path, story_path_hash, story_text,
     root_fact_key, root_fact_value, leaf_fact_key, leaf_fact_value, fact_count,
     incident_id, remediation_action_json, drill_down_json)
VALUES ($1, $2, $3, $4, NULL, $5, $6, 0.9, 0.8, 'waits',
        'AN2506_HISTORIC', $7, 'planted for the #2506 anchored findings read',
        'AN2506_HISTORIC', 1, NULL, NULL, 1, NULL, NULL, NULL)", connection);
        command.Parameters.AddWithValue(CollectionIdGenerator.Next());
        command.Parameters.AddWithValue(analysisTime);
        command.Parameters.AddWithValue(ServerId);
        command.Parameters.AddWithValue(ServerName);
        command.Parameters.AddWithValue(analysisTime.AddHours(-4));
        command.Parameters.AddWithValue(analysisTime);
        command.Parameters.AddWithValue(storyPathHash ?? HistoricStoryHash);
        await command.ExecuteNonQueryAsync(ct);
    }

    private static async Task DeleteRowsAsync(NpgsqlConnection connection, CancellationToken ct)
    {
        using var cleanup = new NpgsqlCommand(
            $"DELETE FROM wait_stats WHERE server_id = {ServerId}; " +
            $"DELETE FROM analysis_findings WHERE server_id = {ServerId}; " +
            $"DELETE FROM analysis_muted WHERE server_id = {ServerId}; " +
            $"DELETE FROM servers WHERE server_id = {ServerId};", connection);
        await cleanup.ExecuteNonQueryAsync(ct);
    }
}
