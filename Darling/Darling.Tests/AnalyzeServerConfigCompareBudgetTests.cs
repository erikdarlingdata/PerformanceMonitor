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
using System.Linq;
using System.Text.Json;
using System.Text.Json.Nodes;
using System.Threading;
using System.Threading.Tasks;
using Npgsql;
using PerformanceMonitor.Analysis;
using PerformanceMonitor.Collectors;
using PerformanceMonitor.Darling.Analysis;
using PerformanceMonitor.Darling.Service.Mcp;
using PerformanceMonitor.Darling.Storage;
using Xunit;

namespace Darling.Tests;

/// <summary>
/// #9879: what a reply says when the pass left a configuration change's before and after compare out for time. No store
/// is read here: the record is built by hand and given to the reply helper.
/// </summary>
public sealed class AnalyzeServerConfigCompareBudgetTests
{
    [Fact]
    public void TheReplyHelper_NamesTheCompareLeftOut_BetweenTheDetectorsAndTheDrillDowns()
    {
        var options = new JsonSerializerOptions();
        var payload = new { status = "ok" };
        var cuts = new StageTimeCuts(["io"], TimeSpan.FromSeconds(3), ["BAD_ACTOR"], TimeSpan.FromSeconds(4.5))
        {
            Compares = [ConfigChangeAttribution.FactKey],
            CompareBudget = StageTimeBudget.OnDemandConfigCompare
        };

        var reply = Assert.IsType<JsonObject>(StageTimeBudget.Attach(payload, cuts, options));
        var entry = Assert.Single(reply["compares_left_out"]!.AsArray())!.AsObject();
        Assert.Equal(ConfigChangeAttribution.FactKey, entry["finding"]!.GetValue<string>());
        Assert.Equal(StageTimeBudget.Reason, entry["reason"]!.GetValue<string>());
        Assert.Equal(StageTimeBudget.OnDemandConfigCompare.TotalSeconds, entry["budget_seconds"]!.GetValue<double>());
        Assert.Equal(StageTimeBudget.CompareReadItWith, entry["read_it_with"]!.GetValue<string>());
        Assert.Contains("compare_analysis", StageTimeBudget.CompareReadItWith, StringComparison.Ordinal);
        Assert.Equal(
            new[] { "status", "detectors_left_out", "compares_left_out", "drill_downs_left_out" },
            reply.Select(field => field.Key));

        var sentence = StageTimeBudget.Describe(cuts)!;
        Assert.Contains("1 finding has no before and after compare (CONFIG_CHANGED)", sentence, StringComparison.Ordinal);
        Assert.Contains("states the change itself", sentence, StringComparison.Ordinal);
        Assert.Contains(StageTimeBudget.CompareReadItWith, sentence, StringComparison.Ordinal);

        /* A compare left out is the only cut: its field is the only one added, and its sentence the only one said. */
        var compareOnly = cuts with { Detectors = [], DrillDowns = [] };
        var only = Assert.IsType<JsonObject>(StageTimeBudget.Attach(payload, compareOnly, options));
        Assert.Equal(new[] { "status", "compares_left_out" }, only.Select(field => field.Key));
        Assert.StartsWith("1 finding has no before and after compare", StageTimeBudget.Describe(compareOnly)!, StringComparison.Ordinal);

        /* A pass that finished its compare keeps its payload and says nothing. */
        Assert.Same(payload, StageTimeBudget.Attach(payload, StageTimeCuts.None, options));
        Assert.Null(StageTimeBudget.Describe(StageTimeCuts.None));
    }

    [Fact]
    public void TheCompareInstant_IsAfterTheDetectors_AndAheadOfTheDrillDowns()
    {
        Assert.True(StageTimeBudget.OnDemandConfigCompare > StageTimeBudget.OnDemandDetectors);
        Assert.True(StageTimeBudget.OnDemandConfigCompare < StageTimeBudget.OnDemandDrillDowns);
    }
}

/// <summary>
/// #9879: the compare's budget against a real store, on a SQL Server target with a server setting that changed inside
/// the pass window. The compare's two fact collections are held by a table lock another session takes as they start, so
/// they never finish by themselves. The test fires the deadline, releases the lock or cancels the pass by hand once the
/// store shows a read waiting for the lock, so nothing here waits on a clock.
/// </summary>
[Collection("live-postgres")]
public sealed class AnalyzeServerConfigCompareBudgetLiveTests
{
    private const int ServerId = -987966;
    private const string ServerName = "config-compare-budget-9879";
    private const string Maxdop = "max degree of parallelism";
    private const string HeldTable = "collect.cpu_utilization_stats";
    private const string LeftOutForTimeText = "left out of this answer for time";
    private static readonly TimeSpan Limit = TimeSpan.FromSeconds(60);

    private static DateTime Naive(DateTime value) => DateTime.SpecifyKind(value, DateTimeKind.Unspecified);

    /// <summary>How many sessions of this database wait for a lock.</summary>
    private static async Task<long> ReadsWaitingForALockAsync(NpgsqlConnection watcher, CancellationToken ct)
    {
        using var waiting = new NpgsqlCommand(
            "SELECT COUNT(*) FROM pg_locks WHERE NOT granted AND database = (SELECT oid FROM pg_database WHERE datname = current_database())", watcher);
        return (long)(await waiting.ExecuteScalarAsync(ct))!;
    }

    /// <summary>Returns once the number of sessions waiting for a lock is, or is not, zero.</summary>
    private static async Task WaitUntilReadsWaitAsync(NpgsqlConnection watcher, bool waiting, CancellationToken ct)
    {
        using var limit = CancellationTokenSource.CreateLinkedTokenSource(ct);
        limit.CancelAfter(Limit);
        while (await ReadsWaitingForALockAsync(watcher, limit.Token) > 0 != waiting)
            await Task.Delay(TimeSpan.FromMilliseconds(20), limit.Token);
    }

    /// <summary>
    /// Stands in front of the real fact collector. The pass's own collection goes straight through. With a pass to hold,
    /// the collections of any other window, which are the compare's two, first take a lock on a table every collection
    /// reads, so both wait in the store until <see cref="ReleaseAsync"/>.
    /// </summary>
    private sealed class CompareReadsHeld(IFactCollector inner, string connectionString) : IFactCollector
    {
        private readonly object _gate = new();
        private Task? _locked;
        private NpgsqlConnection? _locker;
        private NpgsqlTransaction? _held;

        /// <summary>The pass whose compare is held, or null to hold nothing.</summary>
        public AnalysisContext? PassToHold { get; set; }

        private int _compareCollections;

        /// <summary>How many collections the held compare has started.</summary>
        public int CompareCollections => Volatile.Read(ref _compareCollections);

        public async Task<List<Fact>> CollectFactsAsync(AnalysisContext context)
        {
            if (PassToHold is { } pass && !ReferenceEquals(context, pass))
            {
                Interlocked.Increment(ref _compareCollections);
                Task locked;
                lock (_gate)
                    locked = _locked ??= LockAsync();
                await locked;
            }

            return await inner.CollectFactsAsync(context);
        }

        private async Task LockAsync()
        {
            _locker = new NpgsqlConnection(connectionString);
            await _locker.OpenAsync();
            _held = await _locker.BeginTransactionAsync();
            using var lockTable = new NpgsqlCommand($"LOCK TABLE {HeldTable} IN ACCESS EXCLUSIVE MODE", _locker, _held);
            await lockTable.ExecuteNonQueryAsync();
        }

        /// <summary>Lets the held reads go and holds nothing more.</summary>
        public async Task ReleaseAsync()
        {
            PassToHold = null;
            Task? locked;
            lock (_gate)
            {
                locked = _locked;
                _locked = null;
            }

            if (locked is null)
                return;
            await locked;
            await _held!.RollbackAsync();
            await _held.DisposeAsync();
            await _locker!.DisposeAsync();
            Volatile.Write(ref _compareCollections, 0);
        }
    }

    private static async Task PlantWaitAsync(NpgsqlConnection connection, DateTime at, long waitMs, CancellationToken ct)
    {
        using var command = new NpgsqlCommand(@"
INSERT INTO wait_stats
    (collection_id, collection_time, server_id, server_name, wait_type, delta_waiting_tasks, delta_wait_time_ms)
VALUES ($1, $2, $3, $4, 'FB9879D_TEST_WAIT', 50, $5)", connection);
        command.Parameters.AddWithValue(CollectionIdGenerator.Next());
        command.Parameters.AddWithValue(at);
        command.Parameters.AddWithValue(ServerId);
        command.Parameters.AddWithValue(ServerName);
        command.Parameters.AddWithValue(waitMs);
        await command.ExecuteNonQueryAsync(ct);
    }

    private static async Task PlantServerConfigAsync(NpgsqlConnection connection, DateTime captureTime, long value, CancellationToken ct)
    {
        using var command = new NpgsqlCommand(@"
INSERT INTO server_config
    (config_id, capture_time, server_id, server_name, configuration_name,
     value_configured, value_in_use, is_dynamic, is_advanced)
VALUES ($1, $2, $3, $4, $5, $6, $6, TRUE, TRUE)", connection);
        command.Parameters.AddWithValue(CollectionIdGenerator.Next());
        command.Parameters.AddWithValue(captureTime);
        command.Parameters.AddWithValue(ServerId);
        command.Parameters.AddWithValue(ServerName);
        command.Parameters.AddWithValue(Maxdop);
        command.Parameters.AddWithValue(value);
        await command.ExecuteNonQueryAsync(ct);
    }

    /// <summary>
    /// A registered SQL Server target with more than a day of wait history in one hour-of-day and day-of-week bucket, three
    /// heavy collections a week later at the same hour (one ANOMALY_WAIT_PROFILE finding), and max degree of parallelism
    /// captured as 0 in the history and as 8 twelve minutes into the incident's hour. Returns the start of that hour.
    /// </summary>
    private static async Task<DateTime> PlantAsync(NpgsqlConnection connection, CancellationToken ct)
    {
        using (var register = new NpgsqlCommand(@"
INSERT INTO servers (server_id, server_name, display_name, is_enabled, sql_major_version, created_date, modified_date)
VALUES ($1, $2, $2, TRUE, 15, $3, $3)", connection))
        {
            register.Parameters.AddWithValue(ServerId);
            register.Parameters.AddWithValue(ServerName);
            register.Parameters.AddWithValue(Naive(DateTime.UtcNow));
            await register.ExecuteNonQueryAsync(ct);
        }

        var day = DateTime.UtcNow.Date.AddDays(-8);
        while (day.DayOfWeek != DayOfWeek.Monday) day = day.AddDays(-1);
        var historyStart = Naive(day.AddHours(10));
        for (var i = 0; i < 12; i++)
            await PlantWaitAsync(connection, historyStart.AddMinutes(5 * i), (i == 5 || i == 6) ? 0L : 60_000L, ct);
        var incidentStart = historyStart.AddDays(7);
        for (var minutes = 5; minutes <= 15; minutes += 5)
            await PlantWaitAsync(connection, incidentStart.AddMinutes(minutes), 200_000L, ct);

        await PlantServerConfigAsync(connection, historyStart, 0, ct);
        await PlantServerConfigAsync(connection, incidentStart.AddMinutes(12), 8, ct);

        await TimescaleSupport.EnsureBaselineFallbackViewsAsync(connection, null, ct);
        return incidentStart;
    }

    /// <summary>Every stored finding row of the server: its id and its batch time, in id order.</summary>
    private static async Task<List<string>> StoredFindingRowsAsync(NpgsqlConnection connection, CancellationToken ct)
    {
        using var command = new NpgsqlCommand(
            "SELECT finding_id::text || ' at ' || analysis_time::text FROM analysis_findings WHERE server_id = $1 ORDER BY finding_id", connection);
        command.Parameters.AddWithValue(ServerId);
        var rows = new List<string>();
        using var reader = await command.ExecuteReaderAsync(ct);
        while (await reader.ReadAsync(ct))
            rows.Add(reader.GetString(0));
        return rows;
    }

    /// <summary>The findings that are not the configuration change's, one line each: what a compare left out must not touch.</summary>
    private static List<string> OtherFindings(List<AnalysisFinding> findings) =>
        findings.Where(f => f.RootFactKey != ConfigChangeAttribution.FactKey)
            .Select(f => $"{f.RootFactKey} path={f.StoryPath} severity={f.Severity:0.###} drill_down={f.DrillDown is not null} remediation={f.Remediation is not null}")
            .ToList();

    private static AnalysisFinding ConfigFinding(List<AnalysisFinding> findings) =>
        Assert.Single(findings, f => f.RootFactKey == ConfigChangeAttribution.FactKey);

    [Fact]
    public async Task ACompareHeldOnItsStoreRead_IsLeftOutAtTheDeadline_TheScheduledPassWaitsForIt_AndACancelledPassStopsIt()
    {
        var baseConnectionString = Environment.GetEnvironmentVariable("DARLING_TEST_PG");
        Assert.SkipWhen(string.IsNullOrEmpty(baseConnectionString),
            "Set DARLING_TEST_PG to a Postgres connection string to run the live compare budget test (it mints its own scratch database).");

        var ct = TestContext.Current.CancellationToken;
        await using var scratch = await ScratchPostgres.CreateAsync(baseConnectionString!, ct);
        using var connection = new NpgsqlConnection(scratch.ConnectionString);
        await connection.OpenAsync(ct);
        await PgMigrations.MigrateAsync(connection, ct);
        var incidentStart = await PlantAsync(connection, ct);
        await using var postgres = NpgsqlDataSource.Create(scratch.ConnectionString);

        /* A current pass (no as_of) over the incident's hour. */
        AnalysisContext Pass(CancellationToken passToken) => new()
        {
            ServerId = ServerId,
            ServerName = ServerName,
            TimeRangeStart = incidentStart,
            TimeRangeEnd = incidentStart.AddHours(1),
            ServerUtcOffset = TimeSpan.Zero,
            CancellationToken = passToken
        };

        CompareReadsHeld? collector = null;
        var logger = new CapturingTestLogger();
        var service = new DarlingAnalysisService(
            postgres, logger: logger, sqlServerCollectorWrapper: inner => collector = new CompareReadsHeld(inner, scratch.ConnectionString));

        /* Nothing held and no deadline: the compare runs, the card carries its verdict, and the findings are stored. */
        var whole = Pass(ct);
        var wholeFindings = await service.AnalyzeAsync(whole).WaitAsync(Limit, ct);
        var wholeCard = ConfigFinding(wholeFindings);
        Assert.Equal(0, wholeCard.RootFactMetadata![ConfigChangeAttribution.MetaCompareUnavailable]);
        Assert.False(wholeCard.RootFactMetadata.ContainsKey(ConfigChangeAttribution.MetaCompareLeftOutForTime));
        Assert.DoesNotContain(LeftOutForTimeText, wholeCard.StoryText, StringComparison.Ordinal);
        Assert.Empty(whole.ComparesLeftOutForTime);
        Assert.False(whole.LeftOutForTime);
        Assert.Empty(service.LastStageCuts.Compares);
        var wholeOthers = OtherFindings(wholeFindings);
        Assert.NotEmpty(wholeOthers);
        var storedByTheWholePass = await StoredFindingRowsAsync(connection, ct);
        Assert.NotEmpty(storedByTheWholePass);

        /* The on-demand shape: the compare's reads wait on the lock, and the deadline fires while they wait. */
        using var deadline = new CancellationTokenSource();
        var cut = Pass(ct);
        cut.ConfigCompareDeadline = deadline.Token;
        cut.ConfigCompareBudget = StageTimeBudget.OnDemandConfigCompare;
        collector!.PassToHold = cut;
        var cutRun = service.AnalyzeAsync(cut);
        await WaitUntilReadsWaitAsync(connection, waiting: true, ct);
        Assert.False(cutRun.IsCompleted);
        Assert.Equal(2, collector.CompareCollections);
        deadline.Cancel();
        /* The lock is still held: it is the deadline that ends the wait of the compare's reads. */
        await WaitUntilReadsWaitAsync(connection, waiting: false, ct);
        await collector.ReleaseAsync();
        var cutFindings = await cutRun.WaitAsync(Limit, ct);

        /* The card is in the answer with the change itself and no verdict, and says why. */
        var cutCard = ConfigFinding(cutFindings);
        var metadata = cutCard.RootFactMetadata!;
        Assert.Equal(1, metadata[ConfigChangeAttribution.MetaCompareLeftOutForTime]);
        Assert.Equal(1, metadata[ConfigChangeAttribution.MetaCompareUnavailable]);
        Assert.Equal(0, metadata[ConfigChangeAttribution.OldInUseKey(Maxdop)]);
        Assert.Equal(8, metadata[ConfigChangeAttribution.NewInUseKey(Maxdop)]);
        Assert.Equal(wholeCard.RootFactMetadata[ConfigChangeAttribution.MetaChangeTimeUnix], metadata[ConfigChangeAttribution.MetaChangeTimeUnix]);
        Assert.Equal(0, metadata.GetValueOrDefault(ConfigChangeAttribution.MetaComparedKeys));
        var advice = FactAdvice.TryReadStoryText(cutCard.StoryText)!;
        Assert.Contains($"`{Maxdop}` 0 → 8", advice.Headline, StringComparison.Ordinal);
        Assert.Contains(LeftOutForTimeText, advice.Investigation, StringComparison.Ordinal);
        Assert.Contains("compare_analysis", advice.Investigation, StringComparison.Ordinal);
        Assert.DoesNotContain("The engine compared", advice.Investigation, StringComparison.Ordinal);
        Assert.DoesNotContain("the store read failed", advice.Investigation, StringComparison.Ordinal);

        /* The pass names the compare as left out, and nothing else. */
        Assert.Equal(new[] { ConfigChangeAttribution.FactKey }, cut.ComparesLeftOutForTime);
        Assert.Equal(new[] { ConfigChangeAttribution.FactKey }, service.LastStageCuts.Compares);
        Assert.Equal(StageTimeBudget.OnDemandConfigCompare, service.LastStageCuts.CompareBudget);
        Assert.Empty(cut.FamiliesLeftOutForTime);
        Assert.Empty(cut.DetectorsLeftOutForTime);
        Assert.Empty(cut.DrillDownsLeftOutForTime);
        Assert.Contains("no before and after compare (CONFIG_CHANGED)", StageTimeBudget.Describe(service.LastStageCuts)!, StringComparison.Ordinal);

        /* A compare the deadline stopped is not a failed compare and not a failed pass. */
        Assert.Null(service.EndedEarlyAs);
        Assert.DoesNotContain("Period comparison failed", logger.Joined, StringComparison.Ordinal);
        Assert.DoesNotContain("attribution failed", logger.Joined, StringComparison.Ordinal);

        /* The other findings are the whole pass's, and the cut pass stored nothing. */
        Assert.Equal(wholeOthers, OtherFindings(cutFindings));
        Assert.True(cut.LeftOutForTime);
        Assert.True(cut.PersistFindings);
        Assert.False(cut.StoresFindings);
        Assert.True(service.LastPassLeftOutForTime);
        Assert.Equal(storedByTheWholePass, await StoredFindingRowsAsync(connection, ct));

        /* The scheduled pass has no deadline: with the same hold it waits, and once the reads are let go it has the verdict. */
        var scheduled = Pass(ct);
        collector.PassToHold = scheduled;
        var scheduledRun = service.AnalyzeAsync(scheduled, persistCollectionCaveats: true);
        await WaitUntilReadsWaitAsync(connection, waiting: true, ct);
        Assert.False(scheduledRun.IsCompleted);
        await collector.ReleaseAsync();
        var scheduledFindings = await scheduledRun.WaitAsync(Limit, ct);
        var scheduledCard = ConfigFinding(scheduledFindings);
        Assert.Equal(0, scheduledCard.RootFactMetadata![ConfigChangeAttribution.MetaCompareUnavailable]);
        Assert.False(scheduledCard.RootFactMetadata.ContainsKey(ConfigChangeAttribution.MetaCompareLeftOutForTime));
        Assert.Equal(
            wholeCard.RootFactMetadata.GetValueOrDefault(ConfigChangeAttribution.MetaComparedKeys),
            scheduledCard.RootFactMetadata.GetValueOrDefault(ConfigChangeAttribution.MetaComparedKeys));
        Assert.Contains("The engine compared", FactAdvice.TryReadStoryText(scheduledCard.StoryText)!.Investigation, StringComparison.Ordinal);
        Assert.Empty(scheduled.ComparesLeftOutForTime);
        Assert.False(service.LastPassLeftOutForTime);
        Assert.True(scheduled.StoresFindings);
        Assert.True((await StoredFindingRowsAsync(connection, ct)).Count > storedByTheWholePass.Count);

        /* A pass whose own token is cancelled while the compare waits stops the compare: the pass ends as abandoned, it
           is not a budget cut, and no read is left waiting behind the lock, which is still held. */
        using var passToken = CancellationTokenSource.CreateLinkedTokenSource(ct);
        using var unfired = new CancellationTokenSource();
        var abandoned = Pass(passToken.Token);
        abandoned.ConfigCompareDeadline = unfired.Token;
        abandoned.ConfigCompareBudget = StageTimeBudget.OnDemandConfigCompare;
        collector.PassToHold = abandoned;
        var abandonedRun = service.AnalyzeAsync(abandoned);
        await WaitUntilReadsWaitAsync(connection, waiting: true, ct);
        Assert.False(abandonedRun.IsCompleted);
        passToken.Cancel();
        Assert.Empty(await abandonedRun.WaitAsync(Limit, ct));
        Assert.Equal(AnalysisAbandonKind.Timeout, service.EndedEarlyAs);
        Assert.Empty(abandoned.ComparesLeftOutForTime);
        await WaitUntilReadsWaitAsync(connection, waiting: false, ct);
        await collector.ReleaseAsync();
    }

    /// <summary>Whether any string anywhere in <paramref name="element"/> contains <paramref name="text"/>.</summary>
    private static bool AnyStringContains(JsonElement element, string text) => element.ValueKind switch
    {
        JsonValueKind.String => element.GetString()!.Contains(text, StringComparison.Ordinal),
        JsonValueKind.Object => element.EnumerateObject().Any(property => AnyStringContains(property.Value, text)),
        JsonValueKind.Array => element.EnumerateArray().Any(item => AnyStringContains(item, text)),
        _ => false
    };

    /// <summary>The reply's entries under <paramref name="field"/>, from the top level or from <c>hints</c>, and none when the reply has no such field.</summary>
    private static List<JsonElement> LeftOut(JsonElement reply, string field)
    {
        if (reply.TryGetProperty(field, out var top))
            return top.EnumerateArray().ToList();
        if (reply.TryGetProperty("hints", out var hints) && hints.ValueKind == JsonValueKind.Object
            && hints.TryGetProperty(field, out var nested))
            return nested.EnumerateArray().ToList();
        return [];
    }

    [Fact]
    public async Task AnEndedCompareBudget_NamesTheCompareInTheReply_KeepsTheCard_AndWithNoBudgetTheReplyHasTheVerdict()
    {
        var baseConnectionString = Environment.GetEnvironmentVariable("DARLING_TEST_PG");
        Assert.SkipWhen(string.IsNullOrEmpty(baseConnectionString),
            "Set DARLING_TEST_PG to a Postgres connection string to run the live compare budget reply test (it mints its own scratch database).");

        var ct = TestContext.Current.CancellationToken;
        await using var scratch = await ScratchPostgres.CreateAsync(baseConnectionString!, ct);
        using var connection = new NpgsqlConnection(scratch.ConnectionString);
        await connection.OpenAsync(ct);
        await PgMigrations.MigrateAsync(connection, ct);
        var incidentStart = await PlantAsync(connection, ct);
        await using var postgres = NpgsqlDataSource.Create(scratch.ConnectionString);
        var asOf = incidentStart.AddHours(1).ToString("yyyy-MM-dd'T'HH:mm:ss'Z'", CultureInfo.InvariantCulture);

        /* The compare's budget has ended before the pass starts, so the compare is not started. The other three stages
           have no budget: they are not what this test cuts. */
        var service = new DarlingAnalysisService(postgres)
        {
            OnDemandFamilyBudget = null,
            OnDemandDetectorBudget = null,
            OnDemandDrillDownBudget = null,
            OnDemandConfigCompareBudget = TimeSpan.Zero
        };

        var cut = await DarlingMcpTools.AnalyzeServer(service, postgres, ServerName, 1, asOf);
        using (var doc = JsonDocument.Parse(cut))
        {
            var entry = Assert.Single(LeftOut(doc.RootElement, "compares_left_out"));
            Assert.Equal(ConfigChangeAttribution.FactKey, entry.GetProperty("finding").GetString());
            Assert.Equal(StageTimeBudget.Reason, entry.GetProperty("reason").GetString());
            Assert.Equal(0.0, entry.GetProperty("budget_seconds").GetDouble());
            Assert.Contains("compare_analysis", entry.GetProperty("read_it_with").GetString()!, StringComparison.Ordinal);
            Assert.Empty(LeftOut(doc.RootElement, "families_left_out"));
            Assert.Empty(LeftOut(doc.RootElement, "detectors_left_out"));
            Assert.Empty(LeftOut(doc.RootElement, "drill_downs_left_out"));

            /* The caveat sentence is in the reply, and so is the card: the change, and why it has no verdict. */
            var sentence = StageTimeBudget.Describe(
                new StageTimeCuts([], null, [], null) { Compares = [ConfigChangeAttribution.FactKey], CompareBudget = TimeSpan.Zero })!;
            Assert.True(AnyStringContains(doc.RootElement, sentence), cut);
            Assert.True(AnyStringContains(doc.RootElement, LeftOutForTimeText), cut);
            Assert.True(AnyStringContains(doc.RootElement, $"`{Maxdop}` 0 → 8"), cut);
            Assert.False(AnyStringContains(doc.RootElement, "The engine compared"), cut);
        }
        Assert.Equal(new[] { ConfigChangeAttribution.FactKey }, service.LastStageCuts.Compares);
        Assert.Equal(TimeSpan.Zero, service.LastStageCuts.CompareBudget);
        Assert.True(service.LastPassLeftOutForTime);

        /* The scheduled pass of the same service has no budget, whatever the on-demand one is set to. */
        var scheduledFindings = await service.AnalyzeAsync(
            ServerId, ServerName, 1, ct, asOfUtc: DateTime.SpecifyKind(incidentStart.AddHours(1), DateTimeKind.Utc), persistCollectionCaveats: true);
        Assert.Equal(0, ConfigFinding(scheduledFindings).RootFactMetadata![ConfigChangeAttribution.MetaCompareUnavailable]);
        Assert.Empty(service.LastStageCuts.Compares);
        Assert.Null(service.LastStageCuts.CompareBudget);
        Assert.False(service.LastPassLeftOutForTime);

        /* An on-demand call with no compare budget runs the compare, and its reply has the verdict and no such field. */
        service.OnDemandConfigCompareBudget = null;
        var whole = await DarlingMcpTools.AnalyzeServer(service, postgres, ServerName, 1, asOf);
        using (var doc = JsonDocument.Parse(whole))
        {
            Assert.Empty(LeftOut(doc.RootElement, "compares_left_out"));
            Assert.False(AnyStringContains(doc.RootElement, LeftOutForTimeText), whole);
            Assert.True(AnyStringContains(doc.RootElement, "The engine compared"), whole);
            Assert.True(AnyStringContains(doc.RootElement, $"`{Maxdop}` 0 → 8"), whole);
        }
        Assert.Empty(service.LastStageCuts.Compares);
    }
}
