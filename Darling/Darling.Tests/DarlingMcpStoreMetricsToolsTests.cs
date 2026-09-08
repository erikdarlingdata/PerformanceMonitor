/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using System.ComponentModel;
using System.Linq;
using System.Reflection;
using System.Threading.Tasks;
using ModelContextProtocol.Server;
using Npgsql;
using PerformanceMonitor.Darling.Service.Mcp;
using PerformanceMonitor.Darling.Storage;
using Xunit;

namespace Darling.Tests;

/// <summary>
/// Pins the store self-metrics MCP slice (#2068). The surface is exactly <c>get_store_metrics</c> (static,
/// [McpServerToolType], Task&lt;string&gt;) with the one windowing knob — no <c>server_name</c>, because
/// the monitoring store itself is the subject. Both reads are Postgres-dialect over
/// <c>collect.store_metrics</c>: the latest snapshot per object and the settled LAST-sample-per-day series.
/// The derivable number the issue called out — the per-server daily ingest rate — is a pure computation,
/// pinned here without a store: deltas between settled days, divided by THAT day's enabled-server count,
/// null (never zero or infinity) when the denominator is missing, and negative deltas preserved because
/// retention drops and compression passes genuinely shrink the store.
/// </summary>
public sealed class DarlingMcpStoreMetricsToolsTests
{
    private static MethodInfo[] ToolMethods() => typeof(DarlingMcpStoreMetricsTools)
        .GetMethods(BindingFlags.Public | BindingFlags.NonPublic | BindingFlags.Static | BindingFlags.Instance)
        .Where(m => m.GetCustomAttribute<McpServerToolAttribute>() is not null)
        .ToArray();

    [Fact]
    public void ToolSurface_ExactlyGetStoreMetrics()
    {
        var toolMethods = ToolMethods();
        var names = toolMethods
            .Select(m => m.GetCustomAttribute<McpServerToolAttribute>()!.Name)
            .ToArray();

        Assert.Equal(new[] { "get_store_metrics" }, names);
        Assert.NotNull(typeof(DarlingMcpStoreMetricsTools).GetCustomAttribute<McpServerToolTypeAttribute>());
        Assert.All(toolMethods, m => Assert.True(m.IsStatic, $"{m.Name} must be static for WithGeminiCompatibleTools"));
        Assert.All(toolMethods, m => Assert.True(m.ReturnType == typeof(Task<string>), $"{m.Name} must return Task<string>"));
    }

    [Fact]
    public void ParamContract_DaysBackOnly_Default30_CeilingIsTheSweepsRetention()
    {
        var method = ToolMethods().Single();
        var mcpParams = method.GetParameters()
            .Where(p => p.GetCustomAttribute<DescriptionAttribute>() is not null)
            .Select(p => (p.Name, p.HasDefaultValue, p.DefaultValue))
            .ToArray();

        /* Deliberately NO server_name: the store is the subject, not a monitored server. */
        Assert.Equal(new[] { "days_back" }, mcpParams.Select(p => p.Name).ToArray());
        Assert.Equal(30, mcpParams.Single().DefaultValue);

        /* The window ceiling is the series' own retention — past it there is nothing to read, and the two
           numbers drifting apart would let a caller ask for days the sweep deliberately deleted. */
        Assert.Equal(StoreSelfMetrics.RetentionDays, DarlingMcpStoreMetricsTools.MaxDaysBack);
    }

    [Fact]
    public void StoreMetricsLatestSql_NewestRowPerObject()
    {
        var sql = DarlingStoreMetricsReader.StoreMetricsLatestSql;

        /* DISTINCT ON with a newest-first tiebreak — one settled row per (kind, name). */
        Assert.Contains("SELECT DISTINCT ON (object_kind, object_name)", sql, StringComparison.Ordinal);
        Assert.Contains("FROM collect.store_metrics", sql, StringComparison.Ordinal);
        Assert.Contains("ORDER BY object_kind, object_name, metric_time DESC", sql, StringComparison.Ordinal);

        /* The forecasting columns ride along. */
        Assert.Contains("compressed_before_bytes", sql, StringComparison.Ordinal);
        Assert.Contains("compressed_after_bytes", sql, StringComparison.Ordinal);
        Assert.Contains("enabled_server_count", sql, StringComparison.Ordinal);
    }

    [Fact]
    public void StoreMetricsDailySql_LastSamplePerObjectPerDay_Windowed()
    {
        var sql = DarlingStoreMetricsReader.StoreMetricsDailySql;

        /* The settled-point pin: the LAST sample of each object per day, not 24 near-duplicates — the
           grain a growth question wants. */
        Assert.Contains("SELECT DISTINCT ON (object_kind, object_name, date_trunc('day', metric_time))", sql, StringComparison.Ordinal);
        Assert.Contains("WHERE metric_time >= $1", sql, StringComparison.Ordinal);
        Assert.Contains(
            "ORDER BY object_kind, object_name, date_trunc('day', metric_time), metric_time DESC",
            sql,
            StringComparison.Ordinal);
    }

    [Theory]
    [InlineData(nameof(DarlingStoreMetricsReader.StoreMetricsLatestSql))]
    [InlineData(nameof(DarlingStoreMetricsReader.StoreMetricsDailySql))]
    public void Reads_ArePostgresDialect_NoTsqlIsms_NoBareNow(string sqlName)
    {
        var sql = (string)typeof(DarlingStoreMetricsReader).GetField(sqlName, BindingFlags.Public | BindingFlags.Static)!.GetValue(null)!;
        Assert.DoesNotContain("@", sql, StringComparison.Ordinal);
        Assert.DoesNotContain("N'", sql, StringComparison.Ordinal);
        Assert.DoesNotContain("getdate", sql.ToLowerInvariant());
        Assert.DoesNotContain("[", sql, StringComparison.Ordinal);
        Assert.DoesNotContain("now()", sql, StringComparison.Ordinal);
    }

    /// <summary>
    /// The daily series is a LAST-SNAPSHOT selection, so the description has to say so (#3119). An
    /// operator asking a maximum question of it — this job's longest run that day, whether it entered its
    /// warning band — gets a confident wrong answer, because the day's peak is dropped rather than
    /// smoothed, and nothing else on the surface says so.
    ///
    /// <para><b>Asserted TOGETHER with the shipped SQL rather than on its own.</b> The caveat is true only
    /// while the read is a <c>DISTINCT ON</c> with a newest-first tiebreak; a read that became
    /// max-preserving would make the sentence wrong in the other direction. Requiring both means either
    /// half moving lands here and the pairing gets re-decided, instead of the sentence outliving the query
    /// it describes.</para>
    ///
    /// <para><b>One ordered match rather than three substrings, and deliberately strict.</b> A description
    /// that said "last snapshot" about some other field and "maximum" about a third would satisfy three
    /// independent <c>Contains</c> calls while telling a reader nothing about the daily series. Strictness
    /// here fails toward re-deciding the sentence: a rewording that still carries the claim goes red and
    /// gets re-approved, where a looser match would let a rewording that DROPPED it pass.</para>
    /// </summary>
    [Fact]
    public void TheDescription_SaysADailyPointIsALastSnapshotAndNotAMaximum()
    {
        var description = ToolMethods().Single().GetCustomAttribute<DescriptionAttribute>()?.Description;
        Assert.NotNull(description);

        /* The selection the caveat is about. Both halves: DISTINCT ON alone would keep an arbitrary row,
           and it is the newest-first tiebreak that makes the kept row the day's LAST. */
        var sql = DarlingStoreMetricsReader.StoreMetricsDailySql;
        Assert.Contains(
            "DISTINCT ON (object_kind, object_name, date_trunc('day', metric_time))",
            sql,
            StringComparison.Ordinal);
        Assert.Contains("metric_time DESC", sql, StringComparison.Ordinal);

        /* The claim, in one match: the daily point, what it IS, and what it is not. */
        Assert.Matches(@"daily point is that day's LAST snapshot[^.]*maximum", description!);

        /* And the route that answers what this series cannot, so the caveat leaves a reader somewhere to
           go rather than only telling them to distrust the number in front of them. */
        Assert.Contains("timescaledb_information.job_history", description!, StringComparison.Ordinal);
    }

    /// <summary>
    /// #3175: the description's redirect to <c>timescaledb_information.job_history</c> must carry the
    /// precondition that makes it a real route, and the response must carry a reading of it.
    ///
    /// <para><b>Asserted TOGETHER with the shipped read, for the reason the sibling test above states.</b>
    /// The sentence is only honest while a caller can actually see the setting's state in the same
    /// response. A description that named the precondition with no read behind it would be advice to go
    /// look somewhere this tool declines to look, and a read with no sentence would sit in the JSON
    /// unexplained. Either half disappearing lands here.</para>
    ///
    /// <para>The existing test one class-member up already required the description to NAME job_history —
    /// and passed the whole time job_history was empty on every store older than the release that turned
    /// logging on. Naming a route is not the same as the route working, which is why this is a second,
    /// stricter claim rather than an edit to that one.</para>
    /// </summary>
    [Fact]
    public void TheDescription_SaysJobHistoryNeedsItsGucOn_AndTheResponseReportsIt()
    {
        var description = ToolMethods().Single().GetCustomAttribute<DescriptionAttribute>()?.Description;
        Assert.NotNull(description);

        /* The precondition, the default, and the consequence — in one ordered match, so a rewording that
           kept the GUC's name but dropped WHY it matters goes red instead of passing on a substring. */
        Assert.Matches(
            @"timescaledb\.enable_job_execution_logging is on[^.]*defaults OFF",
            description!);
        Assert.Contains("zero rows", description!, StringComparison.Ordinal);

        /* And the read that makes the sentence actionable, named in the description and present in the
           shipped SQL. The probe counts pg_settings ROWS rather than calling current_setting, which is
           what lets it separate "off" from "this server has no such setting". */
        Assert.Contains("job_history block in every response", description!, StringComparison.Ordinal);
        Assert.Contains("FROM pg_settings", DarlingStoreMetricsReader.JobExecutionLoggingSql, StringComparison.Ordinal);
        Assert.Contains("WHERE name = $1", DarlingStoreMetricsReader.JobExecutionLoggingSql, StringComparison.Ordinal);

        /* The GUC name is NOT retyped into the SQL: it is bound from the one constant the managed conf
           block also writes, so the name the probe asks for and the name the product sets cannot drift.
           A drifted copy would not error — pg_settings would return no row, which this read reports as
           "the server has no such setting", indistinguishable from a plain-PostgreSQL store. */
        Assert.DoesNotContain(
            StoreSelfMetrics.JobExecutionLoggingSetting,
            DarlingStoreMetricsReader.JobExecutionLoggingSql,
            StringComparison.Ordinal);
    }

    /// <summary>
    /// #3175: the four states of the <c>job_history</c> precondition produce four DIFFERENT notes, and the
    /// two that are both "not recording" say different things about what to do.
    ///
    /// <para><b>Distinctness is the claim, not wording.</b> The defect being reported is one absence being
    /// read as another, so a note that hedged across states — or two states sharing a note — would
    /// reproduce it in prose. Comparing the notes to each other rather than to expected strings also means
    /// the pin survives rewording while still failing if two states collapse.</para>
    /// </summary>
    [Fact]
    public void TheJobHistoryNote_IsDifferentForEveryState_AndSplitsOffOnWhoSetIt()
    {
        var statuses = Enum.GetValues<DarlingStoreMetricsReader.JobExecutionLoggingStatus>();

        /* A positive control on the enumeration: a shrunken enum would make the distinctness check below
           vacuous, and the whole point of four states is that there are four. */
        Assert.Equal(4, statuses.Length);

        var notes = statuses
            .Select(s => DarlingMcpStoreMetricsTools.JobHistoryNote(
                new DarlingStoreMetricsReader.JobExecutionLoggingReading(s, null, null, null)))
            .ToArray();

        Assert.Equal(notes.Length, notes.Distinct(StringComparer.Ordinal).Count());
        Assert.All(notes, n => Assert.False(string.IsNullOrWhiteSpace(n)));

        /* Off splits again on provenance, because the two need different actions: a default-sourced off
           heals itself on the next service-owned start, and an ALTER SYSTEM off cannot be healed by the
           conf append at all (postgresql.auto.conf is read last) and needs the override removed. */
        var offByDefault = new DarlingStoreMetricsReader.JobExecutionLoggingReading(
            DarlingStoreMetricsReader.JobExecutionLoggingStatus.Off, "off", "default", null);
        var offByOverride = new DarlingStoreMetricsReader.JobExecutionLoggingReading(
            DarlingStoreMetricsReader.JobExecutionLoggingStatus.Off, "off", "configuration file", "postgresql.auto.conf");

        Assert.False(offByDefault.OffByExplicitOverride);
        Assert.True(offByOverride.OffByExplicitOverride);
        Assert.NotEqual(
            DarlingMcpStoreMetricsTools.JobHistoryNote(offByDefault),
            DarlingMcpStoreMetricsTools.JobHistoryNote(offByOverride));

        /* Only On is Recording. A precondition check whose "yes" leaked into any other state would put a
           maximum question back on an instrument that is off. */
        foreach (var status in statuses)
        {
            var reading = new DarlingStoreMetricsReader.JobExecutionLoggingReading(status, null, null, null);
            Assert.Equal(status == DarlingStoreMetricsReader.JobExecutionLoggingStatus.On, reading.Recording);
        }

        /* OffByExplicitOverride is scoped to Off: an On with a non-default source is on, not overridden. */
        Assert.False(
            new DarlingStoreMetricsReader.JobExecutionLoggingReading(
                DarlingStoreMetricsReader.JobExecutionLoggingStatus.On, "on", "configuration file", "postgresql.conf")
                .OffByExplicitOverride);
    }

    [Fact]
    public void GetStoreMetrics_IsInTheServerInstructions()
    {
        /* The instructions body is how an agent discovers the tool exists — the get_ag_health precedent. */
        Assert.Contains("get_store_metrics", DarlingMcpInstructions.Text, StringComparison.Ordinal);
    }

    /* ---------------- the pure per-server ingest computation ---------------- */

    private static DarlingStoreMetricsReader.StoreMetricDailyPoint StoreDay(
        int day, long? totalBytes, int? servers) => new(
            "store", "darling", new DateTime(2026, 8, day, 0, 0, 0, DateTimeKind.Unspecified),
            totalBytes, null, null, null, null, servers);

    [Fact]
    public void ComputeDailyGrowth_DeltasBetweenSettledDays_DividedByThatDaysServerCount()
    {
        var growth = DarlingStoreMetricsReader.ComputeDailyGrowth(new[]
        {
            StoreDay(1, 100_000_000_000, 52),
            StoreDay(2, 102_300_000_000, 52),
            StoreDay(3, 104_600_000_000, 50),
        });

        /* Two deltas from three days — the first day has no predecessor and yields no point. */
        Assert.Equal(2, growth.Count);

        Assert.Equal(new DateTime(2026, 8, 2), growth[0].Day);
        Assert.Equal(2_300_000_000, growth[0].DeltaBytes);
        Assert.Equal(2_300_000_000 / 52.0, growth[0].PerServerBytes);

        /* The denominator is the day BEING MEASURED's server count, not the baseline day's. */
        Assert.Equal(2_300_000_000 / 50.0, growth[1].PerServerBytes);
    }

    [Fact]
    public void ComputeDailyGrowth_MissingDenominator_IsNull_NeverZeroOrInfinity()
    {
        var growth = DarlingStoreMetricsReader.ComputeDailyGrowth(new[]
        {
            StoreDay(1, 100, 0),
            StoreDay(2, 150, 0),
            StoreDay(3, 175, null),
        });

        Assert.Equal(2, growth.Count);
        Assert.All(growth, g => Assert.Null(g.PerServerBytes));
        /* The byte delta itself still reports — only the rate needs the denominator. */
        Assert.Equal(50, growth[0].DeltaBytes);
    }

    [Fact]
    public void ComputeDailyGrowth_UnrecordedTotals_AreSkipped_And_NegativeDeltasSurvive()
    {
        var growth = DarlingStoreMetricsReader.ComputeDailyGrowth(new[]
        {
            StoreDay(1, 200, 10),
            StoreDay(2, null, 10),
            StoreDay(3, 180, 10),
        });

        /* Day 2 recorded no total: neither its delta nor day 3's (whose predecessor is the hole) is
           invented. Nothing else survives — and nothing throws. */
        Assert.Empty(growth);

        /* Shrinkage is real (retention drops, compression passes) and must not be hidden: a forecast
           extrapolating only the positive days would overstate growth. */
        var shrinking = DarlingStoreMetricsReader.ComputeDailyGrowth(new[]
        {
            StoreDay(1, 200, 10),
            StoreDay(2, 180, 10),
        });
        Assert.Equal(-20, shrinking.Single().DeltaBytes);
        Assert.Equal(-2.0, shrinking.Single().PerServerBytes);
    }

    [Fact]
    public void ComputeDailyGrowth_EmptyAndSingleDay_YieldNothing()
    {
        Assert.Empty(DarlingStoreMetricsReader.ComputeDailyGrowth(Array.Empty<DarlingStoreMetricsReader.StoreMetricDailyPoint>()));
        Assert.Empty(DarlingStoreMetricsReader.ComputeDailyGrowth(new[] { StoreDay(1, 100, 5) }));
    }
}

/// <summary>
/// The <c>job_history</c> precondition probe against a LIVE server (#3175). Two things no text assertion
/// can reach: that the shipped SQL parses and binds its one positional parameter, and that the GUC name the
/// product writes into postgresql.conf is a name PostgreSQL actually knows.
///
/// <para><b>A PAIRED control, because a zero-row result is the whole subject.</b> The reader maps "no
/// pg_settings row" to <c>NotRegistered</c>, so a probe that only ever saw zero rows — because the name was
/// misspelled, say — would report a clean, plausible "this server has no TimescaleDB" on every store
/// forever. The two halves run the SAME shipped string against the same server: the real name must return a
/// row, and a name nobody registered must return none. Neither alone distinguishes a working probe from a
/// silently broken one.</para>
///
/// <para>Deliberately does NOT pin whether the setting is on or off here: that is a property of whatever
/// cluster <c>DARLING_TEST_PG</c> points at, not of the product, and pinning it would make an environment
/// change look like a defect. What is pinned is that the reading is INTERNALLY CONSISTENT — a registered
/// state, a value PostgreSQL renders for a bool, and <c>Recording</c> true for exactly <c>on</c>.</para>
/// </summary>
/* #1776 own-store: this class reads only pg_settings — a server-scoped catalog view, no store tables, no
   DDL, no rows written — but it takes [Collection("live-postgres")] anyway because it shares the cluster
   whose GUCs it reads with every other class that has it, and a class reading the shared store must either
   carry the attribute or record why not. */
[Collection("live-postgres")]
public sealed class DarlingStoreMetricsJobLoggingLivePostgresTests
{
    private static string? ConnectionString => Environment.GetEnvironmentVariable("DARLING_TEST_PG");

    [Fact]
    public async Task JobExecutionLoggingProbe_FindsTheRealGuc_AndFindsNothingForAnUnregisteredName()
    {
        var cs = ConnectionString;
        Assert.SkipWhen(string.IsNullOrEmpty(cs), "Set DARLING_TEST_PG to a Postgres connection string to run the live job-logging probe test.");

        var ct = TestContext.Current.CancellationToken;
        await using var postgres = NpgsqlDataSource.Create(cs!);

        /* The POSITIVE control: the shipped read, the shipped name. A registered state is the claim — the
           name resolves — not which way the setting happens to be pointing on this cluster. */
        var reading = await DarlingStoreMetricsReader.GetJobExecutionLoggingAsync(postgres, ct);

        Assert.NotEqual(DarlingStoreMetricsReader.JobExecutionLoggingStatus.NotRegistered, reading.Status);
        Assert.NotEqual(DarlingStoreMetricsReader.JobExecutionLoggingStatus.Unreadable, reading.Status);
        Assert.Contains(reading.Setting, new[] { "on", "off" });
        Assert.False(string.IsNullOrWhiteSpace(reading.Source));
        Assert.Equal(reading.Setting == "on", reading.Recording);

        /* The NEGATIVE control, through the same shipped string: a name nobody registered returns no row,
           which is the shape the reader reports as NotRegistered. Run here rather than reasoned about,
           because "the query returned nothing" is the answer this whole issue is about mis-reading. */
        await using var command = postgres.CreateCommand(DarlingStoreMetricsReader.JobExecutionLoggingSql);
        command.Parameters.AddWithValue(StoreSelfMetrics.JobExecutionLoggingSetting + "_not_a_real_guc");
        await using var reader = await command.ExecuteReaderAsync(ct);
        Assert.False(await reader.ReadAsync(ct));
    }
}
