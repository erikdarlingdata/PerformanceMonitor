/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Threading.Tasks;
using PerformanceMonitor.Common;
using PerformanceMonitor.Darling.Service;
using PerformanceMonitor.Darling.Service.Mcp;
using Xunit;
using static Darling.Tests.RepoFile;

namespace Darling.Tests;

/// <summary>
/// #3399: the oversized-plan backlog sweep's DURABLE run-record — one <c>collection_log</c> row per tick
/// under the fleet sentinel, and the read that can name it.
///
/// <para><b>The empty tick is the whole point, so it is pinned separately from the busy one.</b> The sweep
/// recorded its per-tick outcome through <c>ILogger</c> alone, and a tick that claimed nothing wrote nothing
/// anywhere — so "has the sweep run in the last N hours" had the same answer, none, for a healthy drained
/// backlog and for a pass that died. A pin that only proved a row appears when plans were captured would
/// have passed against that exact defect, which is why the zero-count record has its own facts here and why
/// the MECHANISM that produces it (the write sits in a <c>finally</c>, and the claim is tallied before the
/// empty-backlog return) is asserted as structure rather than assumed from the busy path.</para>
///
/// <para><b>The expiry arm is covered by fixture, not by field evidence.</b> <c>expired_at</c> is zero on
/// both live stores, so no tick has ever taken that branch: the encoding's handling of an expiry is
/// therefore asserted from a constructed tally rather than inferred from a row somebody has seen.</para>
///
/// <para>Every fact here was verified by mutating the shipped behaviour or the instrument it is read
/// through, not merely by being written after the change.</para>
/// </summary>
public sealed class OversizedPlanSweepRunRecordTests
{
    private const string SweepSource = "Darling/PerformanceMonitor.Darling.Service/OversizedPlanBacklogSweep.cs";
    private const string ObservabilitySource = "Darling/PerformanceMonitor.Darling.Service/DarlingObservability.cs";
    private const string FleetReaderSource = "Darling/PerformanceMonitor.Darling.Service/Mcp/DarlingFleetReader.cs";
    private const string DataToolsSource = "Darling/PerformanceMonitor.Darling.Service/Mcp/DarlingMcpDataTools.cs";
    private const string ResolverSource = "Darling/PerformanceMonitor.Darling.Service/Mcp/DarlingServerResolver.cs";

    /* ---- what a tick's record SAYS ------------------------------------------------------------------ */

    /// <summary>
    /// The empty tick — no servers reached, nothing claimed — still produces a SUCCESS record that names all
    /// four counts. Asserted as the whole string rather than by <c>Contains</c>, because the defect this
    /// closes is an ABSENT statement: a message that quietly dropped its zero clauses would leave a reader
    /// unable to tell "claimed nothing" from "did not look".
    /// </summary>
    [Fact]
    public void AnEmptyTickStillProducesARecord_AndItNamesEveryCountAtZero()
    {
        var (status, message) = OversizedPlanBacklogSweep.BuildRunRecordSummary(
            new OversizedPlanBacklogSweep.SweepTally());

        Assert.Equal("SUCCESS", status);
        Assert.Equal(
            "Swept 0 server(s): 0 plan(s) claimed, 0 captured, 0 expired, 0 fetch failure(s)",
            message);
    }

    /// <summary>
    /// The status branch. A fetch failure, a per-server fault and an interrupted pass each demote the record
    /// to WARNING; captures and EXPIRIES do not, because an expiry is the benign end state the sweep's own
    /// doc calls an expected outcome and a record that cried WARNING over one would make the status useless
    /// for spotting the two that matter.
    /// </summary>
    [Theory]
    [InlineData(0, 0, 0, 0, false, "SUCCESS")]
    [InlineData(3, 3, 0, 0, false, "SUCCESS")]
    [InlineData(3, 0, 3, 0, false, "SUCCESS")]
    [InlineData(3, 1, 1, 1, false, "WARNING")]
    [InlineData(0, 0, 0, 0, true, "WARNING")]
    public void TheStatusDemotesOnlyForAFailure_AFaultOrAnInterrupt(
        int claimed, int captured, int expired, int failures, bool interrupted, string expected)
    {
        var tally = new OversizedPlanBacklogSweep.SweepTally
        {
            ServersSwept = 1,
            PlansClaimed = claimed,
            PlansCaptured = captured,
            PlansExpired = expired,
            FetchFailures = failures,
            Interrupted = interrupted,
        };

        Assert.Equal(expected, OversizedPlanBacklogSweep.BuildRunRecordSummary(tally).Status);
    }

    /// <summary>
    /// A per-server fault demotes the status too, and says so in its own clause rather than being folded
    /// into the fetch-failure count — the two are different events with different responses, and the fetch
    /// count is the one whose rows stay claimable.
    /// </summary>
    [Fact]
    public void APerServerFaultDemotesTheStatus_AndAppendsItsOwnClause()
    {
        var tally = new OversizedPlanBacklogSweep.SweepTally { ServersSwept = 4, ServersFaulted = 2 };
        var (status, message) = OversizedPlanBacklogSweep.BuildRunRecordSummary(tally);

        Assert.Equal("WARNING", status);
        Assert.Equal(
            "Swept 4 server(s): 0 plan(s) claimed, 0 captured, 0 expired, 0 fetch failure(s), "
            + "2 server(s) faulted (see prior warnings)",
            message);
    }

    /// <summary>
    /// The expiry arm, by FIXTURE: <c>expired_at</c> is zero on both live stores, so this branch has never
    /// run in the field and nothing about the encoding's treatment of it can be read off a real row. A tick
    /// whose whole claim expired is a clean tick that captured nothing, and its record has to distinguish
    /// itself from an empty tick — same <c>rows_collected</c>, different claim and expiry counts.
    /// </summary>
    [Fact]
    public void ATickWhoseWholeClaimExpired_ReadsAsACleanTickThatCapturedNothing()
    {
        var expiredOnly = new OversizedPlanBacklogSweep.SweepTally
        {
            ServersSwept = 2,
            PlansClaimed = 3,
            PlansExpired = 3,
        };

        var (status, message) = OversizedPlanBacklogSweep.BuildRunRecordSummary(expiredOnly);

        Assert.Equal("SUCCESS", status);
        Assert.Equal(
            "Swept 2 server(s): 3 plan(s) claimed, 0 captured, 3 expired, 0 fetch failure(s)",
            message);

        /* And it is NOT the empty tick's record, which is the whole reason the claim and expiry counts are
           on the row: rows_collected is 0 for both. */
        Assert.NotEqual(
            OversizedPlanBacklogSweep.BuildRunRecordSummary(
                new OversizedPlanBacklogSweep.SweepTally { ServersSwept = 2 }).Message,
            message);
    }

    /// <summary>
    /// An interrupted pass appends its own clause, after any fault clause, so a partial record cannot be
    /// mistaken for a complete one whose fleet happened to be small.
    /// </summary>
    [Fact]
    public void AnInterruptedPassSaysSo_AfterTheFaultClause()
    {
        var tally = new OversizedPlanBacklogSweep.SweepTally
        {
            ServersSwept = 5,
            ServersFaulted = 1,
            Interrupted = true,
        };

        Assert.Equal(
            "Swept 5 server(s): 0 plan(s) claimed, 0 captured, 0 expired, 0 fetch failure(s), "
            + "1 server(s) faulted (see prior warnings); interrupted by service shutdown",
            OversizedPlanBacklogSweep.BuildRunRecordSummary(tally).Message);
    }

    /* ---- the phase split ---------------------------------------------------------------------------- */

    /// <summary>
    /// The stored phases. The target time and the tick elapsed come off separate stopwatches, so the split
    /// clamps rather than trusts: a target above the parent would let a reader compute a share above 1, and
    /// a negative one would put a negative into the storage residual. The two terms sum to the total in every
    /// row, which is the identity a collector's own row carries.
    /// </summary>
    [Theory]
    [InlineData(0L, 0L, 0, 0, 0)]
    [InlineData(100L, 40L, 100, 40, 60)]
    [InlineData(100L, 0L, 100, 0, 100)]
    [InlineData(100L, 100L, 100, 100, 0)]
    [InlineData(100L, 140L, 100, 100, 0)]
    [InlineData(100L, -5L, 100, 0, 100)]
    [InlineData(-5L, 10L, 0, 0, 0)]
    public void ThePhaseSplitClampsToItsParent_AndAlwaysSumsToIt(
        long durationMs, long targetMs, int elapsed, int target, int storage)
    {
        var split = DarlingObservability.SplitSweepPhases(durationMs, targetMs);

        Assert.Equal(elapsed, split.Elapsed);
        Assert.Equal(target, split.TargetMs);
        Assert.Equal(storage, split.StorageMs);
        Assert.Equal(split.Elapsed, split.TargetMs + split.StorageMs);
    }

    /// <summary>A tick longer than <c>int.MaxValue</c> milliseconds saturates rather than wrapping negative.</summary>
    [Fact]
    public void AnAbsurdlyLongTickSaturatesRatherThanWrapping()
    {
        var split = DarlingObservability.SplitSweepPhases(long.MaxValue, long.MaxValue);

        Assert.Equal(int.MaxValue, split.Elapsed);
        Assert.Equal(int.MaxValue, split.TargetMs);
        Assert.Equal(0, split.StorageMs);
    }

    /* ---- the mechanism that makes the empty tick land ---------------------------------------------- */

    /// <summary>
    /// The record is written from a <c>finally</c>, and there is exactly ONE write. Structure rather than
    /// behaviour because the interesting paths are the early <c>return</c>s: the pass returns early on
    /// shutdown both before the first server and mid-loop, and a write placed after the loop would be
    /// skipped on precisely those ticks — leaving the absence of a row meaning either "the sweep is dead" or
    /// "the last pass was interrupted", which need opposite responses.
    /// </summary>
    [Fact]
    public void TheRunRecordIsWrittenFromAFinally_SoNoExitPathCanSkipIt()
    {
        var code = CSharpSourceWalker.StripCommentsAndStrings(ReadRepoFile(SweepSource));

        var writes = AllIndexesOf(code, "DarlingObservability.LogOversizedPlanSweepRunAsync");
        Assert.Single(writes);

        /* Scoped to RunAsync's own body: SweepServerAsync returns early too, and those returns are inside
           the try this finally guards, so a file-wide sweep for them would fail on correct code. */
        var runAt = code.IndexOf("internal static async Task RunAsync(", StringComparison.Ordinal);
        Assert.True(runAt > 0, "RunAsync is no longer declared the way this pin locates it.");

        var body = CSharpSourceWalker.BraceBalanced(code, code.IndexOf('{', code.IndexOf(')', runAt)));

        var finallyAt = body.IndexOf("finally", StringComparison.Ordinal);
        Assert.True(finallyAt > 0, "RunAsync no longer has a finally block at all.");

        var open = body.IndexOf('{', finallyAt);

        Assert.Contains(
            "LogOversizedPlanSweepRunAsync",
            CSharpSourceWalker.BraceBalanced(body, open),
            StringComparison.Ordinal);

        /* And every return RunAsync can take precedes that block, which is what makes "the finally runs
           last" more than a claim about the one path a reader happens to trace. Two today: the pre-loop
           cancellation check, and the per-server try's shutdown arm. */
        var returns = AllIndexesOf(body, "return;");
        Assert.Equal(2, returns.Count);

        foreach (var ret in returns)
        {
            Assert.True(ret < open,
                "A return sits after the run-record's finally block — the tick's record would be skipped.");
        }
    }

    /// <summary>
    /// The claim is tallied BEFORE the empty-backlog return, so a server whose backlog is empty contributes
    /// its zero to the tick rather than dropping out of the count. Swapping the two lines is the whole
    /// defect: the record would still be written, and it would silently under-report the servers it looked at.
    /// </summary>
    [Fact]
    public void TheClaimIsTalliedBeforeTheEmptyBacklogReturn()
    {
        var code = CSharpSourceWalker.StripCommentsAndStrings(ReadRepoFile(SweepSource));

        var tallied = code.IndexOf("tally.PlansClaimed += pending.Count", StringComparison.Ordinal);
        var bailed = code.IndexOf("if (pending.Count == 0)", StringComparison.Ordinal);

        Assert.True(tallied > 0, "The sweep no longer tallies the claim.");
        Assert.True(bailed > 0, "The sweep no longer returns early on an empty backlog.");
        Assert.True(tallied < bailed, "The empty-backlog return pre-empts the claim tally.");
    }

    /// <summary>
    /// The target stopwatch wraps the FETCH and nothing else. <c>sql_duration_ms</c> is documented as time
    /// spent querying the monitored server, and the two steps either side of the fetch are store writes — so
    /// an accumulate moved below <see cref="M:PerformanceMonitor.Darling.Service.OversizedPlanBacklogSweep.RecordOutcomeAsync"/>
    /// would bill the store's own write to the target's column, which is the misattribution #3192 had to
    /// correct on the collector path.
    /// </summary>
    [Fact]
    public void TheTargetStopwatchWrapsTheFetchAlone()
    {
        var code = CSharpSourceWalker.StripCommentsAndStrings(ReadRepoFile(SweepSource));

        var started = code.IndexOf("Stopwatch.GetTimestamp()", StringComparison.Ordinal);
        var fetched = code.IndexOf("FetchOnePlanAsync(server, plan", StringComparison.Ordinal);
        var accumulated = code.IndexOf("tally.TargetMs +=", StringComparison.Ordinal);
        var stored = code.IndexOf("RecordOutcomeAsync(postgres", StringComparison.Ordinal);

        Assert.True(started > 0 && fetched > 0 && accumulated > 0 && stored > 0,
            "The sweep no longer has the four steps this pin orders.");
        Assert.True(started < fetched, "The target stopwatch starts after the fetch it measures.");
        Assert.True(fetched < accumulated, "The target time is accumulated before the fetch completes.");
        Assert.True(accumulated < stored, "The outcome write is inside the target stopwatch.");
    }

    /* ---- the row's columns -------------------------------------------------------------------------- */

    /// <summary>
    /// Each of the sweep writer's figures lands in the column the shared INSERT names for that position.
    /// Derived from the statement's own column list rather than from a transcribed order, so two bindings
    /// swapped is a failure and a column list widened without a matching binding block is one too — the
    /// 08P01 this statement has produced three times, which fails SILENTLY because the writer is
    /// failure-isolated by design.
    ///
    /// <para>This is the pin that holds the issue's own acceptance: <c>rows_collected</c> carries the plans
    /// captured, <c>duration_ms</c> the tick duration, <c>collector_name</c> the exact name the read filters
    /// on, and <c>server_id</c> the sentinel. Every column the sweep has nothing to say about binds
    /// <c>DBNull</c>, so a figure cannot be quietly parked in a neighbour's column.</para>
    /// </summary>
    [Fact]
    public void TheSweepWriterLandsEachFigureInTheColumnTheStatementNames()
    {
        var source = ReadRepoFile(ObservabilitySource);

        var insertAt = source.IndexOf("INSERT INTO collection_log (", StringComparison.Ordinal);
        Assert.True(insertAt > 0, "The shared collection_log INSERT is no longer in this file.");

        var columns = source[(source.IndexOf('(', insertAt) + 1)..source.IndexOf(')', insertAt)]
            .Split(',')
            .Select(c => c.Trim())
            .ToList();

        var writerAt = source.IndexOf(
            "new NpgsqlCommand(InsertCollectionLogSql, connection) { CommandTimeout = ServiceCommandDeadlines.SerialLoopSeconds };",
            source.IndexOf("LogOversizedPlanSweepRunAsync", StringComparison.Ordinal),
            StringComparison.Ordinal);
        Assert.True(writerAt > 0, "The sweep's run-record writer no longer binds the shared INSERT.");

        var bindings = source[writerAt..source.IndexOf("ExecuteNonQueryAsync", writerAt, StringComparison.Ordinal)]
            .Split('\n')
            .Where(l => l.Contains("command.Parameters.Add", StringComparison.Ordinal))
            .ToList();

        Assert.Equal(columns.Count, bindings.Count);

        var expected = new Dictionary<string, string>(StringComparer.Ordinal)
        {
            ["log_id"] = "CollectionIdGenerator.Next()",
            ["server_id"] = "FleetServerId",
            ["server_name"] = "FleetServerName",
            ["collector_name"] = "OversizedPlanSweepCollectorName",
            ["collection_time"] = "DateTimeKind.Unspecified",
            ["duration_ms"] = "elapsed",
            ["status"] = "status",
            ["error_message"] = "text",
            ["rows_collected"] = "plansCaptured",
            ["sql_duration_ms"] = "target",
            ["duckdb_duration_ms"] = "storage",
        };

        for (var i = 0; i < columns.Count; i++)
        {
            var column = columns[i];
            var wanted = expected.TryGetValue(column, out var token) ? token : "DBNull.Value";

            Assert.True(bindings[i].Contains(wanted, StringComparison.Ordinal),
                $"collection_log.{column} is position {i} of the shared INSERT, and the sweep's writer binds "
                + $"'{bindings[i].Trim()}' there rather than {wanted}.");
        }
    }

    /// <summary>
    /// The collector name is a CONSUMER API — the issue's acceptance is an exact-match filter on it — so the
    /// literal is pinned, and the read surface's own description is asserted to name the same constant rather
    /// than a second spelling of it.
    /// </summary>
    [Fact]
    public void TheCollectorNameIsTheOneTheReadFiltersOn_AndTheDescriptionNamesIt()
    {
        Assert.Equal("oversized_plan_sweep", DarlingObservability.OversizedPlanSweepCollectorName);

        var tools = ReadRepoFile(DataToolsSource);
        var description = tools[..tools.IndexOf("public static async Task<string> GetCollectionLog", StringComparison.Ordinal)];

        Assert.Contains(DarlingObservability.OversizedPlanSweepCollectorName, description, StringComparison.Ordinal);
        Assert.Contains(DarlingObservability.FleetServerName, description, StringComparison.Ordinal);

        /* The reading rule, not just the name: a row that is always written makes its own ABSENCE the
           signal, and a caller not told that reads an empty-backlog tick as a dead sweep. */
        Assert.Contains("MISSING row", description, StringComparison.Ordinal);
    }

    /* ---- the read that can name the sentinel -------------------------------------------------------- */

    /// <summary>
    /// The sentinel name is matched EXACTLY — trimmed and case-insensitively, like every other name this
    /// resolver takes, but never partially. The fallback path matches on <c>Contains</c>, so a reserved name
    /// that answered to a substring of itself would be reachable from a typo; and a name that merely
    /// contains the sentinel's is a different name.
    /// </summary>
    [Theory]
    [InlineData("(fleet)", true)]
    [InlineData("(FLEET)", true)]
    [InlineData("  (fleet)  ", true)]
    [InlineData("fleet", false)]
    [InlineData("(fleet)-west", false)]
    [InlineData("", false)]
    [InlineData(null, false)]
    public void TheSentinelNameIsMatchedExactly(string? name, bool isSentinel)
    {
        Assert.Equal(isSentinel, DarlingServerResolver.IsFleetSentinelName(name));
    }

    /// <summary>
    /// The sentinel answers even when the registry read cannot: asserted by passing a null data source,
    /// which is the shape a store whose registry is unreadable presents to this resolver. The maintenance
    /// run-records are exactly what somebody reaches for when the store is misbehaving, so making them
    /// conditional on a successful registry read would withdraw them at the worst moment.
    /// </summary>
    [Fact]
    public async Task TheFleetSentinelAnswersWithoutAReachableRegistry()
    {
        var (resolved, error) = await DarlingServerResolver.ResolveOrErrorWithFleetSentinelAsync(null!, "(fleet)");

        Assert.Null(error);
        Assert.Equal(DarlingObservability.FleetServerId, resolved.ServerId);
        Assert.Equal(DarlingObservability.FleetServerName, resolved.ServerName);
    }

    /// <summary>
    /// The sentinel arm runs BEFORE the registry fallback. Structure, because both orderings answer the
    /// sentinel correctly on today's registry and the difference only shows on a registry that holds a name
    /// containing the sentinel's — the fallback matches with <c>Contains</c>, so on the other ordering that
    /// row would win and the shadowing would be invisible, a real server being an ordinary answer.
    ///
    /// <para>Pinned after the obvious behavioural falsifier failed to discriminate: moving the registry read
    /// first leaves the null-data-source test above passing, because the registry read is failure-isolated
    /// and returns an error string rather than throwing.</para>
    /// </summary>
    [Fact]
    public void TheSentinelArmRunsBeforeTheRegistryFallback()
    {
        var code = CSharpSourceWalker.StripCommentsAndStrings(ReadRepoFile(ResolverSource));

        var at = code.IndexOf("ResolveOrErrorWithFleetSentinelAsync(", StringComparison.Ordinal);
        Assert.True(at > 0, "The sentinel-aware resolve is no longer declared here.");

        var parametersClose = code.IndexOf(')', at);
        var body = CSharpSourceWalker.BraceBalanced(code, code.IndexOf('{', parametersClose));

        Assert.Contains("IsFleetSentinelName", body, StringComparison.Ordinal);

        var sentinel = body.IndexOf("IsFleetSentinelName(serverName)", StringComparison.Ordinal);
        var registry = body.IndexOf("ResolveOrErrorAsync(postgres, serverName, cancellationToken)", StringComparison.Ordinal);

        Assert.True(sentinel > 0 && registry > 0, "The sentinel-aware resolve no longer has both arms.");
        Assert.True(sentinel < registry, "The registry fallback can shadow the sentinel with a partial match.");
    }

    /// <summary>
    /// The SHARED resolve still misses the sentinel, which is what keeps every other read scoped to a real
    /// monitored server. A partial match must not reach it either: <c>fleet</c> is a substring of the
    /// reserved name and this resolver falls back to <c>Contains</c>, so an arm written on the wrong
    /// comparison would hand the sentinel to any caller who typed part of it.
    /// </summary>
    [Theory]
    [InlineData("(fleet)")]
    [InlineData("fleet")]
    public void TheRegistryResolveStillMissesTheSentinel(string name)
    {
        var registry = new[]
        {
            new DarlingServerResolver.RegisteredServer(12345, "a-monitored-host", null),
        };

        var (_, error) = DarlingServerResolver.ResolveOrError(registry, name, DarlingPeerDirectory.Snapshot.Empty);

        Assert.NotNull(error);
    }

    /// <summary>
    /// A miss through the sentinel-aware resolve NAMES the reserved name. A name documented only in a tool
    /// description is a name nobody finds at the moment they need it, which is the moment they got the
    /// server wrong.
    /// </summary>
    [Fact]
    public async Task AMissThroughTheSentinelAwareResolveNamesTheReservedName()
    {
        var (_, error) = await DarlingServerResolver.ResolveOrErrorWithFleetSentinelAsync(null!, "not-a-server");

        Assert.NotNull(error);
        Assert.Contains(DarlingServerResolver.FleetSentinelDisclosure, McpHelpers.ErrorMessageOf(error!), StringComparison.Ordinal);
    }

    /// <summary>
    /// Exactly ONE read takes the sentinel-aware resolve, and it is the collection log's. The sentinel is a
    /// row in one table; a second tool adopting this overload would accept a server_name it can return
    /// nothing for, which is a worse answer than the miss it replaced.
    /// </summary>
    [Fact]
    public void OnlyTheCollectionLogReadTakesTheSentinelAwareResolve()
    {
        var callers = new List<string>();

        foreach (var file in Directory.EnumerateFiles(
            PathTo("Darling", "PerformanceMonitor.Darling.Service"),
            "*.cs",
            SearchOption.AllDirectories))
        {
            if (HasBuildOutputSegment(file))
            {
                continue;
            }

            var code = CSharpSourceWalker.StripCommentsAndStrings(File.ReadAllText(file));
            var map = CSharpMemberMap.Of(code);

            foreach (var at in AllIndexesOf(code, "DarlingServerResolver.ResolveOrErrorWithFleetSentinelAsync("))
            {
                callers.Add(CSharpMemberMap.EnclosingMember(map, at));
            }
        }

        Assert.Equal(new[] { "GetCollectionLog" }, callers);
    }

    /* ---- what an EMPTY read of the sentinel says ---------------------------------------------------- */

    /// <summary>
    /// An empty read of the sentinel never borrows the monitored-server reassurance. "This window is
    /// genuinely quiet rather than broken" is true of a server that collected nothing for an hour and FALSE
    /// of a maintenance pass, which writes a row on every tick — so on the sentinel that sentence answers
    /// the question this population was added to answer with the opposite of the truth, in the direction a
    /// wrong answer costs something.
    ///
    /// <para>Asserted over all three branches, because the defect is a sentence LEAKING rather than a
    /// specific branch being wrong, and the branch that would leak it is not the one a reader traces first.</para>
    /// </summary>
    [Theory]
    [InlineData(false, null, null)]
    [InlineData(true, null, null)]
    [InlineData(true, "oversized_plan_sweep", null)]
    [InlineData(true, null, 500d)]
    public void AnEmptySentinelReadNeverBorrowsTheMonitoredServerReassurance(
        bool everRecorded, string? collectorName, double? minDurationMs)
    {
        var (state, message) = DarlingMcpDataTools.FleetMaintenanceLogMiss(
            everRecorded, collectorName, minDurationMs, 24);

        Assert.False(string.IsNullOrWhiteSpace(state));
        Assert.DoesNotContain("genuinely quiet rather than broken", message, StringComparison.Ordinal);
        Assert.DoesNotContain("This server", message, StringComparison.Ordinal);
        Assert.DoesNotContain("widen hours_back", message, StringComparison.Ordinal);
    }

    /// <summary>
    /// The three branches say the three different things they have to. A never-recorded sentinel is a
    /// service that has not completed a maintenance pass — <c>unavailable</c>, not an empty window, because
    /// both passes run at startup. A window with rows elsewhere but none here carries the cadence rule,
    /// which is what turns an absence into an answer. And a filtered miss names its filter back without
    /// sending the caller to <c>get_collection_health</c>, which is scoped to monitored servers and lists
    /// neither maintenance name.
    /// </summary>
    [Fact]
    public void TheThreeSentinelBranchesCarryTheCadenceRule_AndNameTheirFilterBack()
    {
        var never = DarlingMcpDataTools.FleetMaintenanceLogMiss(false, null, null, 24);
        Assert.Equal("unavailable", never.State);
        Assert.Contains("has not completed a maintenance pass", never.Message, StringComparison.Ordinal);

        var quiet = DarlingMcpDataTools.FleetMaintenanceLogMiss(true, null, null, 6);
        Assert.Equal("empty", quiet.State);
        Assert.Contains("last 6 hour(s)", quiet.Message, StringComparison.Ordinal);
        Assert.Contains("means the pass did not RUN", quiet.Message, StringComparison.Ordinal);
        Assert.Contains("Do NOT check get_collection_health", quiet.Message, StringComparison.Ordinal);

        var filtered = DarlingMcpDataTools.FleetMaintenanceLogMiss(
            true, DarlingObservability.OversizedPlanSweepCollectorName, null, 24);
        Assert.Equal("empty", filtered.State);
        Assert.Contains(
            $"collector_name '{DarlingObservability.OversizedPlanSweepCollectorName}'",
            filtered.Message,
            StringComparison.Ordinal);
        Assert.Contains("means the pass did not RUN", filtered.Message, StringComparison.Ordinal);
    }

    /// <summary>
    /// The sentinel branch is reached BEFORE the three server-shaped ones. Structure, because the server
    /// branches return early: placed after them, this arm would be dead code on every path that can reach it
    /// — and a dead arm is exactly as invisible as a missing one.
    /// </summary>
    [Fact]
    public void TheSentinelBranchPreemptsTheServerShapedOnes()
    {
        var code = CSharpSourceWalker.StripCommentsAndStrings(ReadRepoFile(DataToolsSource));

        /* Anchored on the GATE, not on the helper's name: the helper is declared earlier in the file, so a
           name search would find the declaration and compare the wrong two offsets. */
        var sentinel = code.IndexOf("resolved.ServerId == DarlingObservability.FleetServerId", StringComparison.Ordinal);
        var neverCollected = code.IndexOf("if (!everCollected)", StringComparison.Ordinal);

        Assert.True(sentinel > 0, "The sentinel arm is no longer in the empty-read block.");
        Assert.True(neverCollected > 0, "The never-collected arm is no longer there.");
        Assert.True(sentinel < neverCollected, "The server-shaped arms pre-empt the sentinel arm.");
    }

    /* ---- the sentinel row's blast radius ------------------------------------------------------------ */

    /// <summary>
    /// Every cross-server aggregate over <c>v_collection_log</c> in the fleet reader excludes the sentinel.
    /// The sweep's row joins a population the daily purge contributes a single row a day to, and it lands
    /// four times an hour, so a <c>GROUP BY server_id</c> with no guard gains a phantom "server 0" group in
    /// every window a quarter-hour or wider rather than in the rare window that straddles the purge.
    ///
    /// <para>Scoped to this one file, which is where the two unguarded aggregates were, and the literal
    /// count is asserted so the loop cannot pass by matching nothing — a renamed view would otherwise turn
    /// this pin into an empty <c>foreach</c>.</para>
    /// </summary>
    [Fact]
    public void TheFleetAggregatesOverTheCollectionLogExcludeTheSentinel()
    {
        var reads = CSharpSourceWalker
            .StringLiteralBodies(ReadRepoFile(FleetReaderSource))
            .Select(l => l.Text)
            .Where(t => t.Contains("v_collection_log", StringComparison.Ordinal))
            .ToList();

        Assert.Equal(2, reads.Count);

        foreach (var sql in reads)
        {
            Assert.Contains(
                $"server_id <> {DarlingObservability.FleetServerId}",
                sql,
                StringComparison.Ordinal);
        }
    }

    /// <summary>True if any whole path segment is a build-output directory (obj/bin). Segment-based, not a
    /// substring test — a plain Contains("obj") would false-positive on a source file whose name embeds it.</summary>
    private static bool HasBuildOutputSegment(string path) =>
        path.Split(Path.DirectorySeparatorChar, Path.AltDirectorySeparatorChar)
            .Any(seg => seg.Equals("obj", StringComparison.OrdinalIgnoreCase) ||
                        seg.Equals("bin", StringComparison.OrdinalIgnoreCase));

    private static List<int> AllIndexesOf(string text, string needle)
    {
        var found = new List<int>();

        for (var at = text.IndexOf(needle, StringComparison.Ordinal);
             at >= 0;
             at = text.IndexOf(needle, at + 1, StringComparison.Ordinal))
        {
            found.Add(at);
        }

        return found;
    }
}
