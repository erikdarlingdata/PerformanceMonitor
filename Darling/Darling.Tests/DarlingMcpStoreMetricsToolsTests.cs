/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using System.ComponentModel;
using System.IO;
using System.Linq;
using System.Reflection;
using System.Runtime.CompilerServices;
using System.Threading;
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
    public void ParamContract_DaysBackKindNameLimit_NoServerName_CeilingIsTheSweepsRetention()
    {
        var method = ToolMethods().Single();
        var mcpParams = method.GetParameters()
            .Where(p => p.GetCustomAttribute<DescriptionAttribute>() is not null)
            .Select(p => (p.Name, p.HasDefaultValue, p.DefaultValue))
            .ToArray();

        /* Deliberately NO server_name: the store is the subject, not a monitored server. #3903 added the two
           filters and the list bound; each is optional, so a caller sending only days_back — every caller before
           #3903 — gets the summary, whose lists say what bounded them. */
        Assert.Equal(new[] { "days_back", "object_kind", "object_name", "limit" }, mcpParams.Select(p => p.Name).ToArray());
        Assert.All(mcpParams, p => Assert.True(p.HasDefaultValue, $"{p.Name} must stay optional"));
        Assert.Equal(30, mcpParams[0].DefaultValue);
        Assert.Null(mcpParams[1].DefaultValue);
        Assert.Null(mcpParams[2].DefaultValue);
        Assert.Equal(DarlingMcpStoreMetricsTools.DefaultLimit, mcpParams[3].DefaultValue);

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
    [InlineData(nameof(DarlingStoreMetricsReader.JobHistoryEvidenceSql))]
    [InlineData(nameof(DarlingStoreMetricsReader.ContinuousAggregateStateSql))]
    [InlineData(nameof(DarlingStoreMetricsReader.LargestUnenumeratedSql))]
    [InlineData(nameof(DarlingStoreMetricsReader.LargestUnenumeratedPlainSql))]
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

        /* NotApplicable evidence on purpose: it contributes NO text (pinned by its own test below), so what
           is compared here is the GUC half alone — the #3175 arms, byte-for-byte what they were before the
           evidence half was appended to them. */
        var notes = statuses
            .Select(s => DarlingMcpStoreMetricsTools.JobHistoryNote(
                new DarlingStoreMetricsReader.JobExecutionLoggingReading(s, null, null, null),
                DarlingStoreMetricsReader.JobHistoryEvidence.NotApplicable))
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
            DarlingMcpStoreMetricsTools.JobHistoryNote(offByDefault, DarlingStoreMetricsReader.JobHistoryEvidence.NotApplicable),
            DarlingMcpStoreMetricsTools.JobHistoryNote(offByOverride, DarlingStoreMetricsReader.JobHistoryEvidence.NotApplicable));

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

    /* ---------------- #3574: the evidence behind the flag, and who the rows are visible to ---------------- */

    private static DarlingStoreMetricsReader.JobExecutionLoggingReading On() => new(
        DarlingStoreMetricsReader.JobExecutionLoggingStatus.On, "on", "configuration file", null);

    private static DarlingStoreMetricsReader.JobExecutionLoggingReading OffByDefault() => new(
        DarlingStoreMetricsReader.JobExecutionLoggingStatus.Off, "off", "default", null);

    /// <summary>An <c>Observed</c> reading with every fact stated, so a test flips exactly the one it is
    /// about. The defaults are the OWNER's reading on a live store: database-owner member, 110 jobs, 12
    /// rows in the window, 9 jobs started a run in it.</summary>
    private static DarlingStoreMetricsReader.JobHistoryEvidence Observed(
        string role = "darling",
        bool dbOwnerMember = true,
        long jobs = 110,
        long ownerMemberJobs = 110,
        long rows = 12,
        DateTime? newestRow = null,
        long ran = 9,
        DateTime? newestRun = null) => new(
            DarlingStoreMetricsReader.JobHistoryEvidenceStatus.Observed,
            role, dbOwnerMember, jobs, ownerMemberJobs, rows,
            newestRow ?? (rows > 0 ? new DateTime(2026, 9, 18, 10, 0, 0, DateTimeKind.Utc) : null),
            ran,
            newestRun ?? (ran > 0 ? new DateTime(2026, 9, 18, 10, 30, 0, DateTimeKind.Utc) : null));

    /// <summary>The managed-mode <c>mcp</c> role's reading on the same store: a member of nothing, and the
    /// view shows it nothing — zero rows, while the unfiltered <c>job_stats</c> still counts the runs.</summary>
    private static DarlingStoreMetricsReader.JobHistoryEvidence FilteredReader() =>
        Observed(role: "mcp", dbOwnerMember: false, ownerMemberJobs: 0, rows: 0, newestRow: null);

    /// <summary>
    /// #3574: the evidence read evaluates the view's OWN predicate for the connection doing the reading,
    /// and takes its two counts from the two views that disagree about visibility.
    ///
    /// <para><b>Both <c>pg_has_role</c> tests, in the view's own terms.</b> The membership in the database
    /// owner (resolved through <c>pg_get_userbyid(datdba)</c>, as the view does) and the per-job membership
    /// in <c>j.owner</c>. Pinned because a read that counted rows without asking whether it was allowed to
    /// see any would report zero on every managed store — the MCP host connects as the <c>mcp</c> role, which
    /// the view filters out — and manufacture the contradiction the issue exists to prevent.</para>
    ///
    /// <para><b>The population half comes from the UNFILTERED view.</b> <c>job_stats</c> has no ownership
    /// clause, so "jobs started a run in the window" holds whatever the reader's standing; taken from the
    /// filtered view it would be zero exactly when the count it was meant to qualify is zero, and the pair
    /// would agree for the wrong reason.</para>
    /// </summary>
    [Fact]
    public void TheEvidenceSql_EvaluatesTheViewsOwnPredicate_AndCountsFromBothViews()
    {
        var sql = DarlingStoreMetricsReader.JobHistoryEvidenceSql;

        /* The view's two tests, evaluated for this reader. IS TRUE mirrors the view, whose own second test
           can meet a NULL owner (a history row whose job was deleted) and must read it as "not a member". */
        Assert.Contains("current_user::text", sql, StringComparison.Ordinal);
        Assert.Matches(@"pg_has_role\(\s*current_user,\s*\(SELECT pg_get_userbyid\(datdba\) FROM pg_database WHERE datname = current_database\(\)\),\s*'MEMBER'\) IS TRUE", sql);
        Assert.Matches(@"pg_has_role\(current_user, j\.owner, 'MEMBER'\) IS TRUE", sql);

        /* Three views: the filtered one being counted, and the two unfiltered ones the reader checks first. */
        Assert.Contains("FROM timescaledb_information.job_history", sql, StringComparison.Ordinal);
        Assert.Contains("FROM timescaledb_information.jobs", sql, StringComparison.Ordinal);
        Assert.Contains("FROM timescaledb_information.job_stats", sql, StringComparison.Ordinal);

        /* One window, bound once, applied to both halves — so the count and its population share a
           denominator. */
        Assert.Contains("h.start_time >= $1", sql, StringComparison.Ordinal);
        Assert.Contains("js.last_run_started_at >= $1", sql, StringComparison.Ordinal);
        Assert.DoesNotContain("$2", sql, StringComparison.Ordinal);

        /* The never-ran sentinel is -infinity, not NULL (#1760); the newest start must NULLIF it away or a
           store whose jobs have never run reports a start in 4714 BC. */
        Assert.Contains("NULLIF(js.last_run_started_at, '-infinity'::timestamptz)", sql, StringComparison.Ordinal);

        /* The window is fixed and published beside the count. 24 hours: every job this product schedules
           runs at least daily, so a live store always has starts inside it, and it sits inside the history
           view's own one-month default retention. */
        Assert.Equal(24, DarlingStoreMetricsReader.JobHistoryEvidenceWindowHours);
    }

    /// <summary>
    /// #3574: the bind is <c>timestamptz</c> with <c>Kind = Utc</c>, stated explicitly — the INVERSE of
    /// the naive-UTC discipline every other store read follows, because these are TimescaleDB's own
    /// <c>TIMESTAMPTZ</c> catalog columns and here the naive bind would be the bug. A source pin, since the
    /// only server that would catch the wrong Kind is one running off UTC, which no test store does.
    /// </summary>
    [Fact]
    public void TheEvidenceRead_BindsAnExplicitTimestampTz_BecauseTheColumnsAreTimestampTz()
    {
        var source = File.ReadAllText(ReaderSourcePath());
        var method = source[source.IndexOf("GetJobHistoryEvidenceAsync(", StringComparison.Ordinal)..];
        method = method[..method.IndexOf("/// <summary>One object's newest self-metrics row", StringComparison.Ordinal)];

        Assert.Contains("NpgsqlDbType.TimestampTz", method, StringComparison.Ordinal);
        Assert.Contains("DateTimeKind.Utc", method, StringComparison.Ordinal);
        /* And NOT the naive idiom, which is correct one method up and wrong here. */
        Assert.DoesNotContain("DateTimeKind.Unspecified", method, StringComparison.Ordinal);
    }

    /// <summary>
    /// #3574: <see cref="DarlingStoreMetricsReader.JobHistoryEvidence.Visibility"/> is derived from the
    /// view's two membership facts exactly as the view combines them — database-owner membership sees
    /// everything and short-circuits the per-job test; otherwise the per-job count decides — and is
    /// <c>Unknown</c> wherever a verdict would be vacuous.
    /// </summary>
    [Fact]
    public void TheVisibility_IsDerivedFromTheViewsTwoTests_TheWayTheViewCombinesThem()
    {
        /* The owner: a member of the database owner, so All — regardless of the per-job count, which the
           view never reaches for such a reader. */
        Assert.Equal(DarlingStoreMetricsReader.JobHistoryVisibility.All, Observed().Visibility);
        Assert.Equal(DarlingStoreMetricsReader.JobHistoryVisibility.All, Observed(ownerMemberJobs: 0).Visibility);
        Assert.Equal(110L, Observed(ownerMemberJobs: 0).HistoryVisibleJobCount);

        /* Not the database owner, but a member of every job's owner: still All. */
        Assert.Equal(
            DarlingStoreMetricsReader.JobHistoryVisibility.All,
            Observed(dbOwnerMember: false, ownerMemberJobs: 110).Visibility);

        /* The managed-mode mcp role: a member of neither. None, and the visible count is zero. */
        Assert.Equal(DarlingStoreMetricsReader.JobHistoryVisibility.None, FilteredReader().Visibility);
        Assert.Equal(0L, FilteredReader().HistoryVisibleJobCount);

        /* Some jobs' owner but not all: Partial, with the fraction preserved for the note. */
        var partial = Observed(dbOwnerMember: false, ownerMemberJobs: 3);
        Assert.Equal(DarlingStoreMetricsReader.JobHistoryVisibility.Partial, partial.Visibility);
        Assert.Equal(3L, partial.HistoryVisibleJobCount);

        /* No jobs at all: None and All are both vacuously true, so neither is claimed. */
        Assert.Equal(
            DarlingStoreMetricsReader.JobHistoryVisibility.Unknown,
            Observed(jobs: 0, ownerMemberJobs: 0, ran: 0, rows: 0).Visibility);

        /* And nothing was observed: nothing is derived. Every derived field is null, never a plausible zero. */
        foreach (var blank in new[]
        {
            DarlingStoreMetricsReader.JobHistoryEvidence.Unreadable,
            DarlingStoreMetricsReader.JobHistoryEvidence.NotApplicable,
        })
        {
            Assert.Equal(DarlingStoreMetricsReader.JobHistoryVisibility.Unknown, blank.Visibility);
            Assert.Null(blank.HistoryVisibleJobCount);
            Assert.Null(blank.RowsObserved);
            Assert.Null(blank.NewestRowAt);
            Assert.Null(blank.ReaderRole);
        }
    }

    /// <summary>
    /// #3574: the contradiction is the conjunction of FOUR conditions, and each one alone is a different,
    /// non-finding shape. A zero read through a filtered role is the filter; a zero with no runs in the
    /// window is an absence of information; a zero with the GUC off is #3175's arm; rows seen is recording
    /// proven. Only all four together say the instrument is not writing what the GUC says it is. Each row of
    /// the table flips exactly one condition off the positive control, so the pin cannot pass by a
    /// predicate that is simply always false.
    /// </summary>
    [Fact]
    public void TheContradiction_NeedsAllFourConditions_AndEachAloneIsNotOne()
    {
        var positive = Observed(rows: 0, newestRow: null);
        Assert.True(positive.ContradictsRecording(recording: true));

        /* 1. Not recording: the GUC-off arm, not this one. */
        Assert.False(positive.ContradictsRecording(recording: false));

        /* 2. Reader filtered: the mcp role's zero is the view's doing. Partial is not enough either — the
           jobs that ran may be the ones this reader cannot see. */
        Assert.False(FilteredReader().ContradictsRecording(recording: true));
        Assert.False(Observed(dbOwnerMember: false, ownerMemberJobs: 3, rows: 0, newestRow: null).ContradictsRecording(recording: true));

        /* 3. Rows seen: recording is proven, whatever the run count. */
        Assert.False(Observed(rows: 1).ContradictsRecording(recording: true));

        /* 4. Nothing ran: nothing to record, so nothing is contradicted. */
        Assert.False(Observed(rows: 0, newestRow: null, ran: 0, newestRun: null).ContradictsRecording(recording: true));

        /* And not-observed evidence never contradicts anything: a read that did not complete has no
           standing to declare a finding. */
        Assert.False(DarlingStoreMetricsReader.JobHistoryEvidence.Unreadable.ContradictsRecording(recording: true));
        Assert.False(DarlingStoreMetricsReader.JobHistoryEvidence.NotApplicable.ContradictsRecording(recording: true));
    }

    /// <summary>
    /// #3574: the note names the visibility rule and the reader on every arm where the view exists, and
    /// says a DIFFERENT thing for each thing the evidence established — including the new contradiction
    /// arm, in so many words.
    ///
    /// <para><b>Distinctness across evidence shapes with the GUC held constant</b>, the same claim the
    /// #3175 pin makes across GUC states with the evidence held constant. The defect class is one absence
    /// being read as another; two evidence shapes sharing a sentence would reproduce it.</para>
    /// </summary>
    [Fact]
    public void TheVisibilityNote_NamesTheRuleAndTheReader_AndSaysADifferentThingPerShape()
    {
        var on = On();

        var filtered = DarlingMcpStoreMetricsTools.JobHistoryNote(on, FilteredReader());
        var contradiction = DarlingMcpStoreMetricsTools.JobHistoryNote(on, Observed(rows: 0, newestRow: null));
        var proven = DarlingMcpStoreMetricsTools.JobHistoryNote(on, Observed());
        var nothingRan = DarlingMcpStoreMetricsTools.JobHistoryNote(on, Observed(rows: 0, newestRow: null, ran: 0, newestRun: null));
        var partial = DarlingMcpStoreMetricsTools.JobHistoryNote(on, Observed(dbOwnerMember: false, ownerMemberJobs: 3));
        var noJobs = DarlingMcpStoreMetricsTools.JobHistoryNote(on, Observed(jobs: 0, ownerMemberJobs: 0, rows: 0, ran: 0));
        var unreadable = DarlingMcpStoreMetricsTools.JobHistoryNote(on, DarlingStoreMetricsReader.JobHistoryEvidence.Unreadable);

        var all = new[] { filtered, contradiction, proven, nothingRan, partial, noJobs, unreadable };
        Assert.Equal(all.Length, all.Distinct(StringComparer.Ordinal).Count());

        /* The rule, stated with the view's own predicate, and the trap named, on every one of them. */
        foreach (var note in all)
        {
            Assert.Contains("pg_has_role(current_user, <database owner>, 'MEMBER') OR pg_has_role(current_user, <job owner>, 'MEMBER')", note, StringComparison.Ordinal);
            Assert.Contains("jobs and job_stats views show every role every job", note, StringComparison.Ordinal);
            Assert.Contains("contradicts nothing", note, StringComparison.Ordinal);
            /* And the #3175 half is still in front of it, untouched. */
            Assert.StartsWith(
                DarlingMcpStoreMetricsTools.JobHistoryNote(on, DarlingStoreMetricsReader.JobHistoryEvidence.NotApplicable),
                note,
                StringComparison.Ordinal);
        }

        /* The reader is named wherever there was one. */
        Assert.Contains("read as 'mcp'", filtered, StringComparison.Ordinal);
        Assert.Contains("read as 'darling'", contradiction, StringComparison.Ordinal);

        /* The filtered arm: the zero is the filter, the role that can see is named, and the unfiltered
           population is reported so the store does not read as idle. */
        Assert.Contains("NOTHING by construction", filtered, StringComparison.Ordinal);
        Assert.Contains("the filter, not the table", filtered, StringComparison.Ordinal);
        Assert.Contains("owner role", filtered, StringComparison.Ordinal);
        Assert.Contains("9 job(s) started a run", filtered, StringComparison.Ordinal);
        Assert.DoesNotContain("CONTRADICTION", filtered, StringComparison.Ordinal);

        /* The contradiction arm: named as such, with the benign cause and how to settle it, and never
           'quiet' or 'clean'. */
        Assert.Contains("CONTRADICTION", contradiction, StringComparison.Ordinal);
        Assert.Contains("do not read this zero as quiet", contradiction, StringComparison.Ordinal);
        Assert.Contains("switched on AFTER", contradiction, StringComparison.Ordinal);
        Assert.Contains("re-read after", contradiction, StringComparison.Ordinal);
        Assert.Contains("a finding", contradiction, StringComparison.Ordinal);

        /* Rows seen: the flag is a measurement. */
        Assert.Contains("12 row(s)", proven, StringComparison.Ordinal);
        Assert.Contains("a measurement here, not a GUC echo", proven, StringComparison.Ordinal);
        Assert.DoesNotContain("CONTRADICTION", proven, StringComparison.Ordinal);

        /* Zero rows, zero runs: not clean — an absence of information, said so. */
        Assert.Contains("proves nothing either way", nothingRan, StringComparison.Ordinal);
        Assert.Contains("absence of information", nothingRan, StringComparison.Ordinal);
        Assert.DoesNotContain("CONTRADICTION", nothingRan, StringComparison.Ordinal);

        /* Partial: the fraction, and no verdict. */
        Assert.Contains("3 of 110 jobs", partial, StringComparison.Ordinal);
        Assert.Contains("THOSE jobs only", partial, StringComparison.Ordinal);
        Assert.DoesNotContain("CONTRADICTION", partial, StringComparison.Ordinal);

        /* Unreadable: the flag is a GUC echo and the note says so, instead of a zero. */
        Assert.Contains("UNKNOWN", unreadable, StringComparison.Ordinal);
        Assert.Contains("GUC's word alone", unreadable, StringComparison.Ordinal);
        Assert.DoesNotContain("rows_observed = ", unreadable, StringComparison.Ordinal);

        /* The GUC-off arm with an admitted reader: says what it will see once on, and that anything seen
           now is failures — never that logging is secretly on. */
        var offAdmitted = DarlingMcpStoreMetricsTools.JobHistoryNote(OffByDefault(), Observed(rows: 2));
        Assert.Contains("FAILED runs", offAdmitted, StringComparison.Ordinal);
        Assert.DoesNotContain("CONTRADICTION", offAdmitted, StringComparison.Ordinal);
        Assert.DoesNotContain("not a GUC echo", offAdmitted, StringComparison.Ordinal);

        /* The GUC unreadable but the view readable: the rows are counted and NOT classified — neither
           proof of recording nor a failure census, because that split is the setting's to make. */
        var gucUnknownAdmitted = DarlingMcpStoreMetricsTools.JobHistoryNote(
            new DarlingStoreMetricsReader.JobExecutionLoggingReading(
                DarlingStoreMetricsReader.JobExecutionLoggingStatus.Unreadable, null, null, null),
            Observed(rows: 2));
        Assert.Contains("not classified", gucUnknownAdmitted, StringComparison.Ordinal);
        Assert.Contains("2 row(s)", gucUnknownAdmitted, StringComparison.Ordinal);
        Assert.DoesNotContain("CONTRADICTION", gucUnknownAdmitted, StringComparison.Ordinal);
        Assert.DoesNotContain("not a GUC echo", gucUnknownAdmitted, StringComparison.Ordinal);
        Assert.DoesNotContain("any it sees now are FAILED", gucUnknownAdmitted, StringComparison.Ordinal);
    }

    /// <summary>
    /// #3574: on a plain-PostgreSQL connection there is no view to be filtered, so the evidence half adds
    /// NOTHING and the #3175 NotRegistered note is byte-for-byte what it was. The one arm where a sentence
    /// about who may read the view would be noise.
    /// </summary>
    [Fact]
    public void TheNotApplicableEvidence_AddsNoText_SoTheNotRegisteredNoteIsUnchanged()
    {
        var notRegistered = new DarlingStoreMetricsReader.JobExecutionLoggingReading(
            DarlingStoreMetricsReader.JobExecutionLoggingStatus.NotRegistered, null, null, null);

        Assert.Equal(
            "",
            DarlingMcpStoreMetricsTools.JobHistoryVisibilityNote(notRegistered, DarlingStoreMetricsReader.JobHistoryEvidence.NotApplicable));

        var note = DarlingMcpStoreMetricsTools.JobHistoryNote(notRegistered, DarlingStoreMetricsReader.JobHistoryEvidence.NotApplicable);
        Assert.DoesNotContain("WHO CAN SEE", note, StringComparison.Ordinal);
        Assert.DoesNotContain("pg_has_role", note, StringComparison.Ordinal);
        Assert.Contains("does not exist on this connection", note, StringComparison.Ordinal);
    }

    /// <summary>
    /// #3574: the two reads fail SEPARATELY, and the evidence read is not attempted where there is no view.
    ///
    /// <para>No store is needed. The data source points at a port nothing listens on, so any read that
    /// reaches the network fails at connect — which is exactly the failure the isolation has to absorb. The
    /// NotRegistered half goes further: it hands the read an ALREADY-CANCELLED token, and the method's own
    /// contract is that cancellation is never isolated, so a NotApplicable coming back (rather than an
    /// <see cref="OperationCanceledException"/>) proves the read returned before touching the connection
    /// at all.</para>
    /// </summary>
    [Fact]
    public async Task TheEvidenceRead_FailsSeparatelyFromTheGucRead_AndIsSkippedWhereThereIsNoView()
    {
        await using var nowhere = NpgsqlDataSource.Create(
            "Host=127.0.0.1;Port=1;Username=nobody;Password=nobody;Database=nowhere;Timeout=1;Command Timeout=1");

        /* A registered GUC reading, handed in from outside: the evidence read fails at connect and reports
           Unreadable, and the GUC reading it was given is untouched — the count timing out cannot make the
           GUC unknown. */
        var on = On();
        var evidence = await DarlingStoreMetricsReader.GetJobHistoryEvidenceAsync(nowhere, on, TestContext.Current.CancellationToken);
        Assert.Equal(DarlingStoreMetricsReader.JobHistoryEvidenceStatus.Unreadable, evidence.Status);
        Assert.Null(evidence.RowsObserved);
        Assert.Null(evidence.ReaderRole);
        Assert.True(on.Recording);

        /* And the note on that pair still carries the GUC's answer, qualified as an echo. */
        var note = DarlingMcpStoreMetricsTools.JobHistoryNote(on, evidence);
        Assert.Contains("is ON", note, StringComparison.Ordinal);
        Assert.Contains("GUC's word alone", note, StringComparison.Ordinal);

        /* NotRegistered: not attempted. A fired token would throw out of any read that started. */
        var notRegistered = new DarlingStoreMetricsReader.JobExecutionLoggingReading(
            DarlingStoreMetricsReader.JobExecutionLoggingStatus.NotRegistered, null, null, null);
        var skipped = await DarlingStoreMetricsReader.GetJobHistoryEvidenceAsync(
            nowhere, notRegistered, new CancellationToken(canceled: true));
        Assert.Same(DarlingStoreMetricsReader.JobHistoryEvidence.NotApplicable, skipped);

        /* The GUC read's own isolation, for the pairing: it too becomes Unreadable at connect rather than
           throwing or inventing an Off. */
        var guc = await DarlingStoreMetricsReader.GetJobExecutionLoggingAsync(nowhere, TestContext.Current.CancellationToken);
        Assert.Equal(DarlingStoreMetricsReader.JobExecutionLoggingStatus.Unreadable, guc.Status);
    }

    /// <summary>
    /// #3574: the description names the ownership filter and the fields the block now carries for it,
    /// asserted TOGETHER with the shipped SQL evaluating the predicate — the sibling pins' reason: a
    /// sentence about a read that does not exist is advice to look somewhere this tool declines to look,
    /// and a read with no sentence sits in the JSON unexplained.
    /// </summary>
    [Fact]
    public void TheDescription_NamesTheOwnershipFilter_AndTheEvidenceFieldsBehindIt()
    {
        var description = ToolMethods().Single().GetCustomAttribute<DescriptionAttribute>()?.Description;
        Assert.NotNull(description);

        /* The rule, the trap, and the fact that the tool's own managed-mode reader is on the wrong side of
           it — in one ordered match each, so a rewording that kept the words but dropped the claim goes
           red. */
        Assert.Matches(@"job_history is ownership-filtered[^.]*members of the job's owner role or of the database owner[^.]*jobs and job_stats views show every role every job", description!);
        Assert.Matches(@"managed mode[^.]*mcp role[^.]*filters out", description!);

        /* Every evidence field the block publishes is named where a caller reads about the block. */
        foreach (var field in new[] { "reader_role", "visibility", "rows_observed", "newest_row_at", "jobs_run_in_window", "contradiction" })
        {
            Assert.Contains(field, description!, StringComparison.Ordinal);
        }

        /* And the window the counts are over, so the number never travels without its denominator. */
        Assert.Contains($"{DarlingStoreMetricsReader.JobHistoryEvidenceWindowHours}-hour window", description!, StringComparison.Ordinal);

        /* The read behind the sentence. */
        Assert.Contains("pg_has_role", DarlingStoreMetricsReader.JobHistoryEvidenceSql, StringComparison.Ordinal);
    }

    /* ---------------- #3574, managed-mode self-proof: the owner's persisted reading ---------------- */

    /// <summary>
    /// The evidence SELECT is ONE string with two consumers: the reader's constant IS the sweep's, the
    /// window constants agree, and the sweep's INSERT embeds the reader's text verbatim. Two copies of a
    /// nine-column predicate would drift without erroring; this pins that there is one.
    /// </summary>
    [Fact]
    public void TheEvidenceSql_IsSharedWithTheSweep_NotCopied()
    {
        Assert.Equal(StoreSelfMetrics.JobHistoryEvidenceSql, DarlingStoreMetricsReader.JobHistoryEvidenceSql);
        Assert.Equal(StoreSelfMetrics.JobHistoryEvidenceWindowHours, DarlingStoreMetricsReader.JobHistoryEvidenceWindowHours);
        Assert.Contains(DarlingStoreMetricsReader.JobHistoryEvidenceSql, StoreSelfMetrics.JobHistoryInsertSql, StringComparison.Ordinal);
    }

    private static readonly DateTime SweepAt = new(2026, 9, 18, 15, 0, 0, DateTimeKind.Unspecified);

    /// <summary>A <c>job_history</c> row as the sweep writes it: role in the name, count in row_count (null =
    /// filtered), population in total_runs, window in schedule_interval_ms, newest-row AGE in
    /// last_run_duration_ms.</summary>
    private static DarlingStoreMetricsReader.StoreMetricRow OwnerRow(
        DateTime? at = null, long? rows = 48, long? ran = 110, long? ageMs = 780_000, string role = "darling", long? windowMs = 86_400_000) => new(
            StoreSelfMetrics.JobHistoryObjectKind, role, at ?? SweepAt,
            null, null, null, null, rows, null,
            LastRunDurationMs: ageMs, ScheduleIntervalMs: windowMs, TotalRuns: ran, TotalFailures: null);

    private static DarlingStoreMetricsReader.StoreMetricRow StoreRow(DateTime? at = null, long bytes = 1_000) => new(
        StoreSelfMetrics.StoreObjectKind, "darling", at ?? SweepAt, bytes, null, null, null, null, 52);

    private static DarlingStoreMetricsReader.StoreMetricRow Row(string kind, string name, long? bytes, DateTime? at = null, int? chunks = null) => new(
        kind, name, at ?? SweepAt, bytes, null, null, chunks, null, null);

    /// <summary>
    /// #3574: the decoder turns the sweep's overloaded columns back into what they mean — count, population,
    /// window, and the newest row as an INSTANT (metric_time minus the persisted age) — and judges
    /// freshness against the sweep's stamp, not against the newest row. The four statuses each come from
    /// one fact: no row, a NULL count, an old stamp, or none of those.
    /// </summary>
    [Fact]
    public void OwnerEvidence_DecodesTheColumnMapping_AndJudgesFreshnessFromTheSweepStamp()
    {
        var now = new DateTime(2026, 9, 18, 15, 30, 0, DateTimeKind.Utc);

        var observed = DarlingStoreMetricsReader.OwnerJobHistoryEvidence.FromLatest(new[] { StoreRow(), OwnerRow() }, now);
        Assert.Equal(DarlingStoreMetricsReader.OwnerJobHistoryEvidenceStatus.Observed, observed.Status);
        Assert.Equal("darling", observed.ReaderRole);
        Assert.Equal(DateTime.SpecifyKind(SweepAt, DateTimeKind.Utc), observed.ObservedAt);
        Assert.Equal(DateTimeKind.Utc, observed.ObservedAt!.Value.Kind);
        Assert.Equal(0.5, observed.AgeHours);
        Assert.Equal(24.0, observed.WindowHours);
        Assert.Equal(48L, observed.RowsObserved);
        Assert.Equal(110L, observed.JobsRunInWindow);
        /* The age decodes to an instant 13 minutes before the sweep — never surfaced as a duration. */
        Assert.Equal(new DateTime(2026, 9, 18, 14, 47, 0, DateTimeKind.Utc), observed.NewestRowAt);

        /* No row ever seen: the age is NULL and so is the instant — a count of zero and a missing newest
           row are two facts, and the whole issue is one being read as the other. */
        var noneEver = DarlingStoreMetricsReader.OwnerJobHistoryEvidence.FromLatest(new[] { OwnerRow(rows: 0, ageMs: null) }, now);
        Assert.Equal(0L, noneEver.RowsObserved);
        Assert.Null(noneEver.NewestRowAt);

        /* Filtered: the sweep's role could not see, so it wrote no count — and that is a status, not a zero. */
        var filtered = DarlingStoreMetricsReader.OwnerJobHistoryEvidence.FromLatest(new[] { OwnerRow(rows: null, ageMs: null, role: "svc") }, now);
        Assert.Equal(DarlingStoreMetricsReader.OwnerJobHistoryEvidenceStatus.Filtered, filtered.Status);
        Assert.Equal("svc", filtered.ReaderRole);
        Assert.Null(filtered.RowsObserved);
        Assert.Equal(110L, filtered.JobsRunInWindow);

        /* Stale: judged on the SWEEP's age, past the fresh bar. The count is still carried. */
        var stale = DarlingStoreMetricsReader.OwnerJobHistoryEvidence.FromLatest(
            new[] { OwnerRow(at: SweepAt.AddHours(-(DarlingStoreMetricsReader.OwnerEvidenceFreshHours + 1))) }, now);
        Assert.Equal(DarlingStoreMetricsReader.OwnerJobHistoryEvidenceStatus.Stale, stale.Status);
        Assert.Equal(48L, stale.RowsObserved);
        Assert.True(stale.AgeHours > DarlingStoreMetricsReader.OwnerEvidenceFreshHours);

        /* Exactly at the bar is fresh; one second past it is not. */
        var atBar = DarlingStoreMetricsReader.OwnerJobHistoryEvidence.FromLatest(
            new[] { OwnerRow(at: now.AddHours(-DarlingStoreMetricsReader.OwnerEvidenceFreshHours)) }, now);
        Assert.Equal(DarlingStoreMetricsReader.OwnerJobHistoryEvidenceStatus.Observed, atBar.Status);

        /* Absent: no row of the kind at all — every field null, never a plausible zero. */
        var absent = DarlingStoreMetricsReader.OwnerJobHistoryEvidence.FromLatest(new[] { StoreRow() }, now);
        Assert.Same(DarlingStoreMetricsReader.OwnerJobHistoryEvidence.Absent, absent);
        Assert.Null(absent.RowsObserved);
        Assert.Null(absent.ReaderRole);

        /* Two rows (a renamed owner role): the newest sweep's speaks. */
        var renamed = DarlingStoreMetricsReader.OwnerJobHistoryEvidence.FromLatest(
            new[] { OwnerRow(at: SweepAt.AddHours(-2), role: "old_owner", rows: 5), OwnerRow(role: "new_owner", rows: 7) }, now);
        Assert.Equal("new_owner", renamed.ReaderRole);
        Assert.Equal(7L, renamed.RowsObserved);

        Assert.Equal(3, DarlingStoreMetricsReader.OwnerEvidenceFreshHours);
    }

    /// <summary>
    /// #3574: the owner's contradiction is the same conjunction as the connection's own, judged on a FRESH
    /// reading only. Each flip off the positive control is a different non-finding: not recording, stale,
    /// filtered, rows seen, nothing ran.
    /// </summary>
    [Fact]
    public void OwnerContradiction_NeedsAFreshAdmittedZeroWithRuns_AndEachAloneIsNotOne()
    {
        var now = new DateTime(2026, 9, 18, 15, 30, 0, DateTimeKind.Utc);
        DarlingStoreMetricsReader.OwnerJobHistoryEvidence Decode(params DarlingStoreMetricsReader.StoreMetricRow[] rows)
            => DarlingStoreMetricsReader.OwnerJobHistoryEvidence.FromLatest(rows, now);

        var positive = Decode(OwnerRow(rows: 0, ageMs: null));
        Assert.True(positive.ContradictsRecording(recording: true));

        Assert.False(positive.ContradictsRecording(recording: false));
        Assert.False(Decode(OwnerRow(rows: 0, ageMs: null, at: SweepAt.AddHours(-9))).ContradictsRecording(recording: true));
        Assert.False(Decode(OwnerRow(rows: null, ageMs: null)).ContradictsRecording(recording: true));
        Assert.False(Decode(OwnerRow(rows: 1)).ContradictsRecording(recording: true));
        Assert.False(Decode(OwnerRow(rows: 0, ageMs: null, ran: 0)).ContradictsRecording(recording: true));
        Assert.False(DarlingStoreMetricsReader.OwnerJobHistoryEvidence.Absent.ContradictsRecording(recording: true));
    }

    /// <summary>
    /// #3574: the owner's half of the note is appended exactly where this connection is NOT itself an
    /// admitted reader, names its source (the service's hourly sweep) on every arm, and says a different
    /// thing per owner shape — including the contradiction from the owner's numbers, in so many words. On
    /// the <c>All</c> arm and the <c>NotApplicable</c> arm it adds NOTHING: there the connection's own count
    /// is the census, or there is no view.
    /// </summary>
    [Fact]
    public void TheOwnerNote_IsAppendedOnlyWhereThisConnectionCannotSee_AndSaysADifferentThingPerShape()
    {
        var now = new DateTime(2026, 9, 18, 15, 30, 0, DateTimeKind.Utc);
        DarlingStoreMetricsReader.OwnerJobHistoryEvidence Decode(params DarlingStoreMetricsReader.StoreMetricRow[] rows)
            => DarlingStoreMetricsReader.OwnerJobHistoryEvidence.FromLatest(rows, now);
        var on = On();
        var mcp = FilteredReader();

        var proven = DarlingMcpStoreMetricsTools.JobHistoryNote(on, mcp, Decode(OwnerRow()));
        var contradiction = DarlingMcpStoreMetricsTools.JobHistoryNote(on, mcp, Decode(OwnerRow(rows: 0, ageMs: null)));
        var nothingRan = DarlingMcpStoreMetricsTools.JobHistoryNote(on, mcp, Decode(OwnerRow(rows: 0, ageMs: null, ran: 0)));
        var stale = DarlingMcpStoreMetricsTools.JobHistoryNote(on, mcp, Decode(OwnerRow(rows: 0, ageMs: null, at: SweepAt.AddHours(-9))));
        var filtered = DarlingMcpStoreMetricsTools.JobHistoryNote(on, mcp, Decode(OwnerRow(rows: null, ageMs: null, role: "svc")));
        var absent = DarlingMcpStoreMetricsTools.JobHistoryNote(on, mcp, DarlingStoreMetricsReader.OwnerJobHistoryEvidence.Absent);
        var offFailures = DarlingMcpStoreMetricsTools.JobHistoryNote(OffByDefault(), mcp, Decode(OwnerRow(rows: 2)));

        var all = new[] { proven, contradiction, nothingRan, stale, filtered, absent, offFailures };
        Assert.Equal(all.Length, all.Distinct(StringComparer.Ordinal).Count());

        /* Every arm: the connection's own None verdict first, untouched, then the source of the owner's numbers. */
        foreach (var note in all)
        {
            Assert.Contains("NOTHING by construction", note, StringComparison.Ordinal);
            Assert.Contains("THE OWNER'S OWN COUNT", note, StringComparison.Ordinal);
            Assert.Contains("the service's sweep, not from this connection", note, StringComparison.Ordinal);
        }

        Assert.Contains("Reading as 'darling' at 2026-09-18T15:00:00.0000000Z", proven, StringComparison.Ordinal);
        Assert.Contains("48 row(s)", proven, StringComparison.Ordinal);
        Assert.Contains("newest 2026-09-18T14:47:00.0000000Z", proven, StringComparison.Ordinal);
        Assert.Contains("110 job(s) started a run", proven, StringComparison.Ordinal);
        Assert.Contains("a measurement on this store after all", proven, StringComparison.Ordinal);
        Assert.DoesNotContain("CONTRADICTION", proven, StringComparison.Ordinal);

        Assert.Contains("CONTRADICTION (from the owner's numbers)", contradiction, StringComparison.Ordinal);
        Assert.Contains("do not read this zero as quiet", contradiction, StringComparison.Ordinal);
        Assert.Contains("switched on AFTER", contradiction, StringComparison.Ordinal);
        Assert.Contains("none ever", contradiction, StringComparison.Ordinal);

        Assert.Contains("absence of information", nothingRan, StringComparison.Ordinal);
        Assert.DoesNotContain("CONTRADICTION", nothingRan, StringComparison.Ordinal);

        /* Stale: shown with its age, explicitly not read as current, and NO verdict even though the numbers
           alone would be the contradiction's. */
        Assert.Contains("hours old", stale, StringComparison.Ordinal);
        Assert.Contains("not read as current evidence", stale, StringComparison.Ordinal);
        Assert.DoesNotContain("CONTRADICTION", stale, StringComparison.Ordinal);

        Assert.Contains("'svc' was itself NOT admitted", filtered, StringComparison.Ordinal);
        Assert.Contains("recorded no count", filtered, StringComparison.Ordinal);

        Assert.Contains("holds no such row yet", absent, StringComparison.Ordinal);
        Assert.Contains("nothing in this block is a measurement", absent, StringComparison.Ordinal);

        Assert.Contains("FAILED runs, which TimescaleDB writes regardless", offFailures, StringComparison.Ordinal);
        Assert.DoesNotContain("after all", offFailures, StringComparison.Ordinal);

        /* NOT appended where this connection already sees everything, and where there is no view. */
        var ownReader = DarlingMcpStoreMetricsTools.JobHistoryNote(on, Observed(), Decode(OwnerRow()));
        Assert.DoesNotContain("THE OWNER'S OWN COUNT", ownReader, StringComparison.Ordinal);
        Assert.Equal(DarlingMcpStoreMetricsTools.JobHistoryNote(on, Observed()), ownReader);

        var notRegistered = new DarlingStoreMetricsReader.JobExecutionLoggingReading(
            DarlingStoreMetricsReader.JobExecutionLoggingStatus.NotRegistered, null, null, null);
        Assert.DoesNotContain("THE OWNER'S OWN COUNT",
            DarlingMcpStoreMetricsTools.JobHistoryNote(notRegistered, DarlingStoreMetricsReader.JobHistoryEvidence.NotApplicable, Decode(OwnerRow())),
            StringComparison.Ordinal);

        /* Appended on Partial and on Unreadable too — anywhere this connection's count is not a census. */
        Assert.Contains("THE OWNER'S OWN COUNT",
            DarlingMcpStoreMetricsTools.JobHistoryNote(on, Observed(dbOwnerMember: false, ownerMemberJobs: 3), Decode(OwnerRow())),
            StringComparison.Ordinal);
        Assert.Contains("THE OWNER'S OWN COUNT",
            DarlingMcpStoreMetricsTools.JobHistoryNote(on, DarlingStoreMetricsReader.JobHistoryEvidence.Unreadable, Decode(OwnerRow())),
            StringComparison.Ordinal);

        /* And the two-argument form is the three-argument form with Absent, so every older pin still
           describes a note the tool can produce. */
        Assert.Equal(absent, DarlingMcpStoreMetricsTools.JobHistoryNote(on, mcp));
    }

    /// <summary>
    /// #3175 corrected (#3582 follow-up): an OFF setting does not empty the view. TimescaleDB writes a
    /// FAILED run's row regardless of the GUC, so both Off arms now say the view is a census of failures
    /// and neither claims it "returns zero rows" — which contradicted the visibility arm beside it that
    /// (correctly) classifies rows an admitted reader sees with the GUC off as failures. Pinned with a
    /// positive control on the On arm, which is the one allowed to call the rows a census of runs.
    /// </summary>
    [Fact]
    public void TheGucOffNotes_SayFailuresOnly_AndNoLongerClaimZeroRows()
    {
        var none = DarlingStoreMetricsReader.JobHistoryEvidence.NotApplicable;
        var offDefault = DarlingMcpStoreMetricsTools.JobHistoryNote(OffByDefault(), none);
        var offOverride = DarlingMcpStoreMetricsTools.JobHistoryNote(
            new DarlingStoreMetricsReader.JobExecutionLoggingReading(
                DarlingStoreMetricsReader.JobExecutionLoggingStatus.Off, "off", "configuration file", "postgresql.auto.conf"),
            none);

        foreach (var note in new[] { offDefault, offOverride })
        {
            Assert.Contains("recording FAILED runs only", note, StringComparison.Ordinal);
            Assert.Contains("regardless of this setting", note, StringComparison.Ordinal);
            Assert.Contains("census of failures", note, StringComparison.Ordinal);
            Assert.Contains("secretly on", note, StringComparison.Ordinal);
            Assert.DoesNotContain("returns zero rows", note, StringComparison.Ordinal);
            Assert.DoesNotContain("is NOT recording", note, StringComparison.Ordinal);
        }

        /* The two Off arms still differ on what to DO. */
        Assert.Contains("ALTER SYSTEM RESET", offOverride, StringComparison.Ordinal);
        Assert.DoesNotContain("ALTER SYSTEM RESET", offDefault, StringComparison.Ordinal);

        /* Positive control: only the On arm calls it a census of the runs it covers. */
        var onNote = DarlingMcpStoreMetricsTools.JobHistoryNote(On(), none);
        Assert.Contains("census of the runs it covers", onNote, StringComparison.Ordinal);
        Assert.DoesNotContain("census of failures", onNote, StringComparison.Ordinal);
    }

    /// <summary>
    /// #3574 + #3582: the description names the owner fields and where their numbers come from, asserted
    /// TOGETHER with the sweep arm that writes them (the sibling pins' reason).
    /// </summary>
    [Fact]
    public void TheDescription_NamesTheOwnerEvidence_AndTheSweepThatProducesIt()
    {
        var description = ToolMethods().Single().GetCustomAttribute<DescriptionAttribute>()?.Description;
        Assert.NotNull(description);

        Assert.Matches(@"hourly self-metrics sweep runs as it[^.]*persists the owner's own count[^.]*same 24-hour window", description!);
        foreach (var field in new[] { "owner_evidence", "owner_role", "owner_observed_at", "owner_rows_observed", "owner_newest_row_at", "owner_jobs_run_in_window" })
        {
            Assert.Contains(field, description!, StringComparison.Ordinal);
        }

        Assert.Contains("taken by the service's sweep rather than by this connection", description!, StringComparison.Ordinal);
        Assert.Contains($"'{StoreSelfMetrics.JobHistoryObjectKind}'", StoreSelfMetrics.JobHistoryInsertSql, StringComparison.Ordinal);

        /* And the corrected GUC claim: successes need it on, failures are written regardless. */
        Assert.Matches(@"records a SUCCESSFUL run only while timescaledb\.enable_job_execution_logging is on[^.]*defaults OFF[^.]*FAILED run's row is written regardless", description!);
    }

    /* ---------------- #3582: the inventory reconciled ---------------- */

    /// <summary>
    /// #3582: the reconciliation is computed over ONE sweep — the rows sharing the store row's stamp — and
    /// states the two coverages separately: enumerated (named objects) and attributed (any row). Rows from
    /// an older sweep are counted and excluded, not summed against a newer database figure; a missing
    /// catch-all row makes the verdict unjudgeable rather than false-by-arithmetic; a residual past the bar
    /// is NOT reconciled; the bar is the larger of the percent and the floor.
    /// </summary>
    [Fact]
    public void ComputeInventory_ReconcilesOneSweep_SplitsEnumeratedFromAttributed_AndCountsStaleRows()
    {
        const long gib = 1L << 30;
        var older = SweepAt.AddHours(-1);

        var rows = new[]
        {
            StoreRow(bytes: 415 * gib),
            Row(StoreSelfMetrics.HypertableObjectKind, "wait_stats", 100 * gib),
            Row(StoreSelfMetrics.HypertableObjectKind, "query_stats", 20 * gib),
            Row(StoreSelfMetrics.ContinuousAggregateObjectKind, "query_store_stats_hourly", 235 * gib),
            Row(StoreSelfMetrics.DimensionObjectKind, PayloadDimensions.QueryPlanDimTable, 39 * gib),
            Row(StoreSelfMetrics.TableObjectKind, QueryStoreTextStore.TableName, 15 * gib),
            Row(StoreSelfMetrics.OtherObjectKind, StoreSelfMetrics.OtherObjectName, 3 * gib, chunks: 14),
            Row(StoreSelfMetrics.SystemObjectKind, StoreSelfMetrics.SystemObjectName, 2 * gib, chunks: 161),
            /* Kinds with no bytes contribute nothing and are not stale. */
            new DarlingStoreMetricsReader.StoreMetricRow(StoreSelfMetrics.BackgroundJobObjectKind, "policy_compression x [1]", SweepAt, null, null, null, null, null, null, 100, 3_600_000, 5, 0),
            OwnerRow(),
            /* From the previous sweep: excluded and counted. */
            Row(StoreSelfMetrics.HypertableObjectKind, "dropped_since", 7 * gib, at: older),
        };

        var inventory = DarlingStoreMetricsReader.ComputeInventory(rows);
        Assert.NotNull(inventory);
        Assert.Equal(SweepAt, inventory!.SweepAt);
        Assert.Equal(415 * gib, inventory.DatabaseBytes);
        Assert.Equal((100 + 20 + 235 + 39 + 15) * gib, inventory.EnumeratedBytes);
        Assert.Equal((100 + 20 + 235 + 39 + 15 + 3 + 2) * gib, inventory.AttributedBytes);
        Assert.Equal(gib, inventory.ResidualBytes);
        Assert.Equal(1, inventory.StaleRowCount);
        Assert.Equal(3 * gib, inventory.UnenumeratedBytes);
        Assert.Equal(14, inventory.UnenumeratedRelationCount);
        Assert.Equal(2 * gib, inventory.SystemBytes);
        Assert.Equal(161, inventory.SystemRelationCount);
        Assert.True(inventory.CatchAllPresent);

        Assert.Equal(Math.Round(100.0 * 409 / 415, 2), inventory.EnumeratedPercent);
        Assert.Equal(Math.Round(100.0 * 414 / 415, 2), inventory.AttributedPercent);
        Assert.Equal(120 * gib, inventory.BytesByKind[StoreSelfMetrics.HypertableObjectKind]);
        Assert.Equal(235 * gib, inventory.BytesByKind[StoreSelfMetrics.ContinuousAggregateObjectKind]);
        Assert.False(inventory.BytesByKind.ContainsKey(StoreSelfMetrics.BackgroundJobObjectKind));

        /* The bar: 1% of 415 GiB is 4.15 GiB, well above the 64 MiB floor; a 1 GiB residual reconciles. */
        Assert.Equal((long)Math.Ceiling(415 * gib * 0.01), inventory.ToleranceBytes);
        Assert.True(inventory.Reconciled);

        /* Ten GiB attributed to no row: a finding. */
        var gap = DarlingStoreMetricsReader.ComputeInventory(new[]
        {
            StoreRow(bytes: 415 * gib),
            Row(StoreSelfMetrics.HypertableObjectKind, "a", 400 * gib),
            Row(StoreSelfMetrics.OtherObjectKind, StoreSelfMetrics.OtherObjectName, 3 * gib, chunks: 1),
            Row(StoreSelfMetrics.SystemObjectKind, StoreSelfMetrics.SystemObjectName, 2 * gib, chunks: 1),
        })!;
        Assert.Equal(10 * gib, gap.ResidualBytes);
        Assert.False(gap.Reconciled);

        /* The floor: on a 17 MiB store, 1% is 170 KiB and a 160 KiB residual would sit under it — but the
           floor is what carries a small store, and a residual under 64 MiB reconciles regardless. */
        var small = DarlingStoreMetricsReader.ComputeInventory(new[]
        {
            StoreRow(bytes: 17_192_639),
            Row(StoreSelfMetrics.HypertableObjectKind, "a", 1_720_320),
            Row(StoreSelfMetrics.OtherObjectKind, StoreSelfMetrics.OtherObjectName, 827_392, chunks: 40),
            Row(StoreSelfMetrics.SystemObjectKind, StoreSelfMetrics.SystemObjectName, 13_352_960, chunks: 161),
        })!;
        Assert.Equal(DarlingStoreMetricsReader.ReconciliationToleranceFloorBytes, small.ToleranceBytes);
        Assert.True(small.Reconciled);

        /* No catch-all rows: not judgeable, so not reconciled — however small the arithmetic residual. */
        var noCatchAll = DarlingStoreMetricsReader.ComputeInventory(new[]
        {
            StoreRow(bytes: 1_000),
            Row(StoreSelfMetrics.HypertableObjectKind, "a", 1_000),
        })!;
        Assert.False(noCatchAll.CatchAllPresent);
        Assert.False(noCatchAll.Reconciled);
        Assert.Null(noCatchAll.UnenumeratedBytes);

        /* No store row: nothing to reconcile against, and no percentage of nothing. */
        Assert.Null(DarlingStoreMetricsReader.ComputeInventory(new[] { Row(StoreSelfMetrics.HypertableObjectKind, "a", 1) }));
        Assert.Null(DarlingStoreMetricsReader.ComputeInventory(Array.Empty<DarlingStoreMetricsReader.StoreMetricRow>()));

        Assert.Equal(1.0, DarlingStoreMetricsReader.ReconciliationTolerancePercent);
        Assert.Equal(64L * 1024 * 1024, DarlingStoreMetricsReader.ReconciliationToleranceFloorBytes);
    }

    /// <summary>
    /// #3582: the inventory note states coverage in the issue's words, names the largest un-enumerated
    /// relations, calls a residual past the bar a FINDING, calls missing catch-all rows a sweep failure,
    /// explains a low enumerated share on a store with no TimescaleDB rows, and reports aggregates holding
    /// bytes with compression off. Each shape is distinct from the others.
    /// </summary>
    [Fact]
    public void TheInventoryNote_StatesCoverage_NamesTheLargest_AndCallsAGapAFinding()
    {
        const long gib = 1L << 30;
        var rows = new[]
        {
            StoreRow(bytes: 415 * gib),
            Row(StoreSelfMetrics.HypertableObjectKind, "wait_stats", 120 * gib),
            Row(StoreSelfMetrics.ContinuousAggregateObjectKind, "query_store_stats_hourly", 200 * gib),
            Row(StoreSelfMetrics.ContinuousAggregateObjectKind, "query_store_stats_daily", 35 * gib),
            Row(StoreSelfMetrics.DimensionObjectKind, PayloadDimensions.QueryPlanDimTable, 39 * gib),
            Row(StoreSelfMetrics.TableObjectKind, QueryStoreTextStore.TableName, 15 * gib),
            Row(StoreSelfMetrics.OtherObjectKind, StoreSelfMetrics.OtherObjectName, 3 * gib, chunks: 14),
            Row(StoreSelfMetrics.SystemObjectKind, StoreSelfMetrics.SystemObjectName, 2 * gib, chunks: 161),
        };
        var states = new[]
        {
            new DarlingStoreMetricsReader.ContinuousAggregateState("query_store_stats_hourly", false, true, "query_store_stats", 1001, null, null),
            new DarlingStoreMetricsReader.ContinuousAggregateState("query_store_stats_daily", true, true, "query_store_stats_hourly", 1002, 1003, 1004),
        };
        var largest = new[]
        {
            new DarlingStoreMetricsReader.UnenumeratedRelation("collect.store_log_events", "r", 2 * gib),
            new DarlingStoreMetricsReader.UnenumeratedRelation("collect.store_metrics", "r", gib / 2),
        };

        var inventory = DarlingStoreMetricsReader.ComputeInventory(rows)!;
        var reconciled = DarlingMcpStoreMetricsTools.InventoryNote(inventory, rows, states, largest);

        Assert.Contains("account for 409.0 GiB of the 415.0 GiB database (98.55%)", reconciled, StringComparison.Ordinal);
        Assert.Contains("3.0 GiB sits in 14 un-enumerated user-schema relation(s) (object_kind other)", reconciled, StringComparison.Ordinal);
        Assert.Contains("the largest being collect.store_log_events (2.0 GiB), collect.store_metrics (512.0 MiB)", reconciled, StringComparison.Ordinal);
        Assert.Contains("2.0 GiB is PostgreSQL catalog and TimescaleDB bookkeeping in 161 relation(s)", reconciled, StringComparison.Ordinal);
        Assert.Contains("RECONCILED: every row together accounts for 99.76%", reconciled, StringComparison.Ordinal);
        Assert.Contains("1 of 2 continuous aggregate(s) have compression DISABLED and hold 200.0 GiB", reconciled, StringComparison.Ordinal);
        Assert.DoesNotContain("NOT RECONCILED", reconciled, StringComparison.Ordinal);
        Assert.DoesNotContain("OLDER sweep", reconciled, StringComparison.Ordinal);
        Assert.DoesNotContain("plain-PostgreSQL", reconciled, StringComparison.Ordinal);

        /* The gap: a finding, with the direction stated. */
        var gapRows = new[]
        {
            StoreRow(bytes: 415 * gib),
            Row(StoreSelfMetrics.HypertableObjectKind, "a", 400 * gib),
            Row(StoreSelfMetrics.OtherObjectKind, StoreSelfMetrics.OtherObjectName, 3 * gib, chunks: 1),
            Row(StoreSelfMetrics.SystemObjectKind, StoreSelfMetrics.SystemObjectName, 2 * gib, chunks: 1),
        };
        var gap = DarlingMcpStoreMetricsTools.InventoryNote(DarlingStoreMetricsReader.ComputeInventory(gapRows)!, gapRows, states, largest);
        Assert.Contains("NOT RECONCILED — a finding: 10.0 GiB of pg_database_size is attributed to NO row", gap, StringComparison.Ordinal);
        Assert.DoesNotContain("RECONCILED: every", gap, StringComparison.Ordinal);

        /* Missing catch-all rows: a sweep failure, said so, and no coverage verdict dressed up as arithmetic. */
        var partialRows = new[] { StoreRow(bytes: 415 * gib), Row(StoreSelfMetrics.HypertableObjectKind, "a", 400 * gib) };
        var partial = DarlingMcpStoreMetricsTools.InventoryNote(DarlingStoreMetricsReader.ComputeInventory(partialRows)!, partialRows, null, null);
        Assert.Contains("'other' catch-all row is MISSING", partial, StringComparison.Ordinal);
        Assert.Contains("'system' catch-all row is MISSING", partial, StringComparison.Ordinal);
        Assert.Contains("NOT RECONCILED: without both catch-all rows", partial, StringComparison.Ordinal);
        Assert.Contains("live read of each aggregate's compression and policy state did not complete", partial, StringComparison.Ordinal);

        /* Stale rows and the live census failing are each named. */
        var staleRows = rows.Append(Row(StoreSelfMetrics.HypertableObjectKind, "old", gib, at: SweepAt.AddHours(-1))).ToArray();
        var stale = DarlingMcpStoreMetricsTools.InventoryNote(DarlingStoreMetricsReader.ComputeInventory(staleRows)!, staleRows, states, null);
        Assert.Contains("1 object row(s) in the inventory are from an OLDER sweep", stale, StringComparison.Ordinal);
        Assert.Contains("live census naming them did not complete", stale, StringComparison.Ordinal);

        /* No TimescaleDB rows at all: the low share is explained, not flagged. */
        var plainRows = new[]
        {
            StoreRow(bytes: 10 * gib),
            Row(StoreSelfMetrics.DimensionObjectKind, PayloadDimensions.QueryPlanDimTable, gib),
            Row(StoreSelfMetrics.OtherObjectKind, StoreSelfMetrics.OtherObjectName, 8 * gib, chunks: 70),
            Row(StoreSelfMetrics.SystemObjectKind, StoreSelfMetrics.SystemObjectName, gib, chunks: 60),
        };
        var plain = DarlingMcpStoreMetricsTools.InventoryNote(DarlingStoreMetricsReader.ComputeInventory(plainRows)!, plainRows, Array.Empty<DarlingStoreMetricsReader.ContinuousAggregateState>(), Array.Empty<DarlingStoreMetricsReader.UnenumeratedRelation>());
        Assert.Contains("a plain-PostgreSQL store, or TimescaleDB unavailable to the sweep", plain, StringComparison.Ordinal);
        Assert.Contains("not a fault", plain, StringComparison.Ordinal);
        Assert.DoesNotContain("the largest being", plain, StringComparison.Ordinal);

        Assert.Equal(5, new[] { reconciled, gap, partial, stale, plain }.Distinct(StringComparer.Ordinal).Count());
    }

    /// <summary>The byte formatter picks the unit that gives a whole-number part, so a registry table is
    /// not "0.0 GiB" beside a 235 GiB aggregate family.</summary>
    [Fact]
    public void TheByteFormatter_PicksTheUnitWithAWholeNumberPart()
    {
        Assert.Equal("235.0 GiB", DarlingMcpStoreMetricsTools.Gib(235L << 30));
        Assert.Equal("1.5 GiB", DarlingMcpStoreMetricsTools.Gib(3L << 29));
        Assert.Equal("512.0 MiB", DarlingMcpStoreMetricsTools.Gib(1L << 29));
        Assert.Equal("72.0 KiB", DarlingMcpStoreMetricsTools.Gib(73_728));
        Assert.Equal("161 bytes", DarlingMcpStoreMetricsTools.Gib(161));
        Assert.Equal("0 bytes", DarlingMcpStoreMetricsTools.Gib(0));
    }

    /// <summary>
    /// #3582: the aggregate-state read takes its three policy facts from the <c>jobs</c> view by the
    /// aggregate's VIEW name (the view reports a policy on an aggregate under <c>user_view_name</c>), hedges
    /// the compression proc's 2.18+ rebrand the way the rest of the codebase does, and resolves a
    /// hierarchical aggregate's source back to its parent's view name. The live top-N composes the SAME
    /// census fragments the sweep sums with, so the list and the number cannot disagree.
    /// </summary>
    [Fact]
    public void TheAggregateStateAndTopNReads_UseTheCatalogTheWayTheSweepDoes()
    {
        var state = DarlingStoreMetricsReader.ContinuousAggregateStateSql;
        Assert.Contains("FROM timescaledb_information.continuous_aggregates ca", state, StringComparison.Ordinal);
        Assert.Contains("ca.compression_enabled", state, StringComparison.Ordinal);
        Assert.Contains("j.proc_name = 'policy_refresh_continuous_aggregate'", state, StringComparison.Ordinal);
        Assert.Contains("(j.proc_name LIKE '%compression%' OR j.proc_name LIKE '%columnstore%')", state, StringComparison.Ordinal);
        Assert.Contains("j.proc_name = 'policy_retention'", state, StringComparison.Ordinal);
        Assert.Equal(3, System.Text.RegularExpressions.Regex.Matches(state, @"j\.hypertable_schema = ca\.view_schema AND j\.hypertable_name = ca\.view_name").Count);
        Assert.Contains("coalesce(parent.view_name, ca.hypertable_name) AS source_name", state, StringComparison.Ordinal);
        Assert.Contains("parent.materialization_hypertable_name = ca.hypertable_name", state, StringComparison.Ordinal);

        foreach (var (sql, timescale) in new[] { (DarlingStoreMetricsReader.LargestUnenumeratedSql, true), (DarlingStoreMetricsReader.LargestUnenumeratedPlainSql, false) })
        {
            Assert.Contains(StoreSelfMetrics.CensusRelationPredicateSql, sql, StringComparison.Ordinal);
            Assert.Contains("NOT " + StoreSelfMetrics.SystemSchemaPredicateSql, sql, StringComparison.Ordinal);
            Assert.Contains("NOT " + StoreSelfMetrics.NamedRelationPredicateSql, sql, StringComparison.Ordinal);
            Assert.Contains("n.nspname || '.' || c.relname AS relation", sql, StringComparison.Ordinal);
            Assert.Contains("ORDER BY pg_total_relation_size(c.oid) DESC", sql, StringComparison.Ordinal);
            Assert.Contains("LIMIT $1", sql, StringComparison.Ordinal);
            Assert.Equal(timescale, sql.Contains(StoreSelfMetrics.TimescaleInventoriedPredicateSql, StringComparison.Ordinal));
        }

        Assert.Equal(10, DarlingStoreMetricsReader.LargestUnenumeratedLimit);
    }

    /// <summary>
    /// #3582: the two live reads fail to NULL, not to an empty list — an empty list reads as "no aggregates"
    /// / "nothing un-enumerated" and would drop a section without a word — and the aggregate read is not
    /// attempted where the GUC probe said the TimescaleDB catalogs do not exist. The port-1 data source
    /// and the pre-cancelled token are the sibling test's devices.
    /// </summary>
    [Fact]
    public async Task TheLiveInventoryReads_FailToNull_AndSkipTheCatalogsThatDoNotExist()
    {
        await using var nowhere = NpgsqlDataSource.Create(
            "Host=127.0.0.1;Port=1;Username=nobody;Password=nobody;Database=nowhere;Timeout=1;Command Timeout=1");
        var ct = TestContext.Current.CancellationToken;

        Assert.Null(await DarlingStoreMetricsReader.GetContinuousAggregateStatesAsync(nowhere, On(), ct));
        Assert.Null(await DarlingStoreMetricsReader.GetLargestUnenumeratedAsync(nowhere, On(), ct));

        var notRegistered = new DarlingStoreMetricsReader.JobExecutionLoggingReading(
            DarlingStoreMetricsReader.JobExecutionLoggingStatus.NotRegistered, null, null, null);
        var skipped = await DarlingStoreMetricsReader.GetContinuousAggregateStatesAsync(nowhere, notRegistered, new CancellationToken(canceled: true));
        Assert.NotNull(skipped);
        Assert.Empty(skipped!);

        /* The top-N is attempted on a plain store (its plain variant), so it fails to null there too. */
        Assert.Null(await DarlingStoreMetricsReader.GetLargestUnenumeratedAsync(nowhere, notRegistered, ct));
    }

    /// <summary>
    /// #3582: the description names every new kind, the coverage fields and the reconciliation bar,
    /// asserted TOGETHER with the sweep arms that write the kinds and the constants that set the bar.
    /// </summary>
    [Fact]
    public void TheDescription_StatesTheInventorysCoverage_AndNamesTheNewKinds()
    {
        var description = ToolMethods().Single().GetCustomAttribute<DescriptionAttribute>()?.Description;
        Assert.NotNull(description);

        foreach (var kind in new[] { StoreSelfMetrics.ContinuousAggregateObjectKind, StoreSelfMetrics.TableObjectKind, StoreSelfMetrics.OtherObjectKind, StoreSelfMetrics.SystemObjectKind })
        {
            Assert.Contains($"object_kind {kind}", description!, StringComparison.Ordinal);
        }

        Assert.Matches(@"RECONCILES the newest sweep against its own pg_database_size", description!);
        foreach (var field in new[] { "enumerated_percent", "attributed_percent", "residual_bytes", "reconciled", "bytes_by_kind", "largest_unenumerated", "compression_enabled" })
        {
            Assert.Contains(field, description!, StringComparison.Ordinal);
        }

        /* The bar, in the description's words, agrees with the constants. */
        Assert.Contains("the larger of 1% and 64 MiB", description!, StringComparison.Ordinal);
        Assert.Equal(1.0, DarlingStoreMetricsReader.ReconciliationTolerancePercent);
        Assert.Equal(64L << 20, DarlingStoreMetricsReader.ReconciliationToleranceFloorBytes);

        /* The named tables, by the names the sweep writes. */
        Assert.Contains(QueryStoreTextStore.TableName, description!, StringComparison.Ordinal);
        Assert.Contains(StoreSelfMetrics.AlertLogTable, description!, StringComparison.Ordinal);

        /* And the mechanism claim is the one the rig verified: the hypertables view never lists a
           materialization — not "lists it under an internal name". */
        Assert.Contains("timescaledb_information.hypertables never lists a materialization", description!, StringComparison.Ordinal);
    }

    private static string ReaderSourcePath([CallerFilePath] string thisFile = "")
        => Path.GetFullPath(Path.Combine(
            Path.GetDirectoryName(thisFile)!, "..", "PerformanceMonitor.Darling.Service", "Mcp", "DarlingStoreMetricsReader.cs"));

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
/// The <c>job_history</c> precondition probe against a LIVE server (#3175), and the evidence probe behind it
/// (#3574). Things no text assertion can reach: that the shipped SQL parses and binds its positional
/// parameter, that the GUC name the product writes into postgresql.conf is a name PostgreSQL actually knows,
/// and that the view's ownership predicate evaluates for the connection the way the record derives it.
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
/* #1776 own-store: this class reads only pg_settings and TimescaleDB's own information views (#3574) —
   server- and catalog-scoped, no store tables, no DDL, no rows written — but it takes
   [Collection("live-postgres")] anyway because it shares the cluster whose GUCs and jobs it reads with every
   other class that has it, and a class reading the shared store must either carry the attribute or record
   why not. */
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

    /// <summary>
    /// #3574: the evidence read against a live TimescaleDB — the shipped SQL parses, binds its one
    /// <c>timestamptz</c> parameter, evaluates the view's predicate for the connection, and its fields are
    /// INTERNALLY CONSISTENT with each other and with direct reads of the same views.
    ///
    /// <para><b>Inserts nothing and pins no count.</b> How many jobs exist, whether any ran in the last day
    /// and whether the GUC is on are properties of whatever cluster <c>DARLING_TEST_PG</c> points at. What
    /// is pinned is the null-versus-zero contract: <c>RowsObserved</c> is a COUNT and never null once the
    /// read completes (a zero is a zero), while <c>NewestRowAt</c> is null exactly when the view showed this
    /// connection no row at all — the two are different facts, and the whole issue is one being read as the
    /// other. The all-time total is read DIRECTLY first, then the shipped read; rows are only ever added
    /// between two reads (the history retention job runs monthly), so a direct total above zero must be
    /// matched by a non-null newest row, and a direct total of zero by a null one and a zero count.</para>
    ///
    /// <para><b>The predicate is cross-checked, not trusted.</b> The database-owner test is re-evaluated
    /// directly on the same connection and must agree with the record; when it holds and jobs exist, the
    /// derived <c>Visibility</c> must be <c>All</c> and the visible count the job count — which is the
    /// managed-mode OWNER's reading, and the shape under which a zero would be a real contradiction.</para>
    /// </summary>
    [Fact]
    public async Task JobHistoryEvidenceProbe_EvaluatesThePredicateForThisConnection_AndItsFieldsAgree()
    {
        var cs = ConnectionString;
        Assert.SkipWhen(string.IsNullOrEmpty(cs), "Set DARLING_TEST_PG to a Postgres connection string to run the live job-history evidence probe test.");

        var ct = TestContext.Current.CancellationToken;
        await using var postgres = NpgsqlDataSource.Create(cs!);

        /* Direct facts first, through the same views the shipped read uses. */
        long directTotal;
        bool directDbOwnerMember;
        string directRole;
        await using (var direct = postgres.CreateCommand(
            "SELECT (SELECT count(*) FROM timescaledb_information.job_history), "
            + "pg_has_role(current_user, (SELECT pg_get_userbyid(datdba) FROM pg_database WHERE datname = current_database()), 'MEMBER') IS TRUE, "
            + "current_user::text"))
        await using (var directReader = await direct.ExecuteReaderAsync(ct))
        {
            Assert.True(await directReader.ReadAsync(ct));
            directTotal = directReader.GetInt64(0);
            directDbOwnerMember = directReader.GetBoolean(1);
            directRole = directReader.GetString(2);
        }

        var logging = await DarlingStoreMetricsReader.GetJobExecutionLoggingAsync(postgres, ct);
        Assert.NotEqual(DarlingStoreMetricsReader.JobExecutionLoggingStatus.NotRegistered, logging.Status);

        var evidence = await DarlingStoreMetricsReader.GetJobHistoryEvidenceAsync(postgres, logging, ct);

        Assert.Equal(DarlingStoreMetricsReader.JobHistoryEvidenceStatus.Observed, evidence.Status);
        Assert.Equal(directRole, evidence.ReaderRole);
        Assert.Equal(directDbOwnerMember, evidence.ReaderIsDatabaseOwnerMember);

        /* Counts, never nulls, once observed. */
        Assert.NotNull(evidence.JobCount);
        Assert.NotNull(evidence.OwnerMemberJobCount);
        Assert.NotNull(evidence.RowsObserved);
        Assert.NotNull(evidence.JobsRunInWindow);
        Assert.InRange(evidence.OwnerMemberJobCount!.Value, 0, evidence.JobCount!.Value);
        Assert.InRange(evidence.HistoryVisibleJobCount!.Value, 0, evidence.JobCount.Value);

        /* The null-versus-zero contract. */
        if (directTotal == 0)
        {
            Assert.Null(evidence.NewestRowAt);
            Assert.Equal(0L, evidence.RowsObserved);
        }
        else
        {
            Assert.NotNull(evidence.NewestRowAt);
            Assert.Equal(DateTimeKind.Utc, evidence.NewestRowAt!.Value.Kind);
        }

        /* A row inside the window implies a newest row; a run inside the window implies a newest run. */
        if (evidence.RowsObserved > 0)
        {
            Assert.NotNull(evidence.NewestRowAt);
        }

        if (evidence.JobsRunInWindow > 0)
        {
            Assert.NotNull(evidence.NewestRunStartedAt);
            Assert.Equal(DateTimeKind.Utc, evidence.NewestRunStartedAt!.Value.Kind);
        }

        /* The predicate's verdict, derived as the view combines its two tests. */
        if (evidence.JobCount > 0 && directDbOwnerMember)
        {
            Assert.Equal(DarlingStoreMetricsReader.JobHistoryVisibility.All, evidence.Visibility);
            Assert.Equal(evidence.JobCount, evidence.HistoryVisibleJobCount);
        }

        if (evidence.JobCount == 0)
        {
            Assert.Equal(DarlingStoreMetricsReader.JobHistoryVisibility.Unknown, evidence.Visibility);
        }

        /* And the note built on a live reading names the role that read. */
        Assert.Contains(
            $"read as '{directRole}'",
            DarlingMcpStoreMetricsTools.JobHistoryNote(logging, evidence),
            StringComparison.Ordinal);
    }
}
