/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using System.Collections;
using System.Globalization;
using System.Linq;
using System.Reflection;
using PerformanceMonitor.Collectors;
using PerformanceMonitor.Darling.Service;
using PerformanceMonitor.Darling.Storage;
using Xunit;
using static Darling.Tests.RepoFile;

namespace Darling.Tests;

/// <summary>
/// #3392: the oversized-plan backlog, the V121 rung that carries it, and the one property of the sweep that
/// drains it which is not allowed to erode — that a plan fetch is ONE plan, on its OWN connection.
///
/// <para><b>Why that property needs a pin rather than a comment.</b> Batching several large plan fetches into
/// one result set is the exact mechanism <see cref="QueryPlanXmlCaptureLimits"/> exists to prevent: the cost
/// is the client materializing each plan as one managed string, and the Large-Object-Heap churn that produces
/// stalls unrelated collectors on unrelated connections. So "fetch a few at once, it will be faster" reads
/// like an optimization and IS the regression. A future reader looking only at the sweep sees three
/// single-row fetches an hour and a loop, and the cheapest-looking change available to them is to widen the
/// loop into the query. These pins make that fail in the author's own test run.</para>
///
/// <para>Every fact here was verified red-first by mutating the shipped behaviour it describes.</para>
/// </summary>
public sealed class OversizedPlanBacklogPins
{
    private const string SweepSource = "Darling/PerformanceMonitor.Darling.Service/OversizedPlanBacklogSweep.cs";

    private static PgMigrations.Migration V121 =>
        PgMigrations.Scripts.Single(m => m.Version == 121);

    /* ---- the rung ------------------------------------------------------------------------------------ */

    [Fact]
    public void V121_IsTheTopRung_AndCarriesBothFactColumnsPlusTheBacklogTable()
    {
        Assert.Equal(StorageVersion.SchemaVersion, PgMigrations.Scripts[^1].Version);
        Assert.Equal(121, PgMigrations.Scripts[^1].Version);
        Assert.Equal("oversized-plan-backlog", V121.Name);

        /* BOTH fact tables. Only adding one would leave the other's capped rows permanently undescribable,
           and procedure_stats is the collector most likely to produce them — a module-grain plan aggregates
           a whole object, so one heavy stored proc's plan dwarfs a single statement's. */
        foreach (var table in new[] { "query_stats", "procedure_stats" })
        {
            Assert.Contains(
                $"ALTER TABLE {table}{Environment.NewLine}    ADD COLUMN IF NOT EXISTS query_plan_xml_bytes bigint;",
                V121.Sql.Replace("\r\n", Environment.NewLine, StringComparison.Ordinal),
                StringComparison.Ordinal);
        }

        /* The DDL is the SAME constant the statements address, concatenated into the rung rather than
           transcribed — so this asserts the composition, and drift is not merely unpinned, it is
           unexpressible. */
        Assert.Contains(OversizedPlanBacklog.CreateTableSql, V121.Sql, StringComparison.Ordinal);
    }

    [Fact]
    public void V121_RebuildsTheResolvingView_WithTheNewColumn_NeverAsSelectStar()
    {
        /* v_query_stats is the #1767 payload-RESOLVING view on any V38+ store. Re-expanding it as SELECT *
           would overwrite the COALESCE that resolves the dimension tables, and the damage is invisible:
           every text and plan read would return NULL for rows written since #1767, which looks like a
           collection outage rather than a schema regression. */
        Assert.DoesNotContain("CREATE OR REPLACE VIEW v_query_stats AS SELECT * FROM query_stats;", V121.Sql, StringComparison.Ordinal);

        /* DROP first, because the generator emits payload columns BEFORE the trailing digest columns, so the
           new column lands mid-list — an alteration CREATE OR REPLACE VIEW refuses. Without the DROP the
           whole rung fails on every existing store.

           PRESENCE is asserted before ORDER, and that is not belt-and-braces: IndexOf answers -1 for an
           absent anchor, and -1 is less than every real offset — so an ordering comparison ALONE passes
           loudest in exactly the case that breaks the field upgrade. Caught by mutating the rung. */
        var drop = V121.Sql.IndexOf("DROP VIEW IF EXISTS v_query_stats;", StringComparison.Ordinal);
        var create = V121.Sql.IndexOf("CREATE OR REPLACE VIEW v_query_stats AS", StringComparison.Ordinal);
        Assert.True(drop >= 0,
            "V121 does not drop v_query_stats — CREATE OR REPLACE cannot insert a column mid-list, so the rung would fail on every existing store");
        Assert.True(create > drop, "V121 recreates v_query_stats before dropping it");

        /* And the rebuilt view actually exposes the column, which is the only reason to rebuild it. */
        Assert.Contains("f.query_plan_xml_bytes", V121.Sql, StringComparison.Ordinal);
    }

    [Fact]
    public void TheBacklogTable_IsNotACollectorTable_SoNothingCatalogDrivenReachesIt()
    {
        /* TimescaleSupport's hypertable conversion and DarlingRetention's purge loop both enumerate the
           collector catalog. A hypertable could not carry this table's primary key anyway — a hypertable's
           unique constraint must include the partitioning column, and a handle-and-offsets key cannot — so
           conversion would reject the key that makes the sighting upsert an upsert. The store_metrics
           precedent, asserted rather than assumed.

           Derived from the shipped constant, not a literal of it: a pin that retyped the name would keep
           passing if the table were renamed onto a collector's. */
        var bare = OversizedPlanBacklog.TableName.Split('.')[^1];
        Assert.Equal("oversized_plan_backlog", bare);
        Assert.DoesNotContain(bare, CollectorCatalog.All.Select(c => c.TargetTable));
        Assert.DoesNotContain(bare, TimescaleSupport.HypertableTables.Select(c => c.TargetTable));
    }

    /* ---- the cap boundary ---------------------------------------------------------------------------- */

    [Theory]
    [InlineData(null, false)]
    [InlineData(0L, false)]
    [InlineData(524_287L, false)]
    [InlineData(524_288L, false)]
    [InlineData(524_289L, true)]
    [InlineData(8_686_284L, true)]
    public void ExceedsCaptureCap_IsStrictlyGreater_AndTreatsNoMeasurementAsNotOversized(long? bytes, bool expected)
    {
        /* Strictly greater, because that is what the collectors' SQL CASE does: a plan measuring EXACTLY the
           cap is captured, so it is not a backlog candidate. The literals are the shipped cap and its
           neighbours, checked against the constant so a cap change makes this fail rather than drift. */
        Assert.Equal(512 * 1024, QueryPlanXmlCaptureLimits.MaxCapturedPlanXmlBytes);
        Assert.Equal(expected, QueryPlanXmlCaptureLimits.ExceedsCaptureCap(bytes));
    }

    [Fact]
    public void BothCollectors_DescribeAnObservation_OnlyAboveTheCap_AndOnlyWithBothHandles()
    {
        var cap = QueryPlanXmlCaptureLimits.MaxCapturedPlanXmlBytes;

        /* query_stats: the observation carries the row's OWN statement offsets, because a plan_handle with
           the wrong offsets is a different statement's plan — the cross-contamination this codebase already
           recorded once, in that collector's delta key. */
        var atCap = new QueryStatsCollector.Row
        {
            SqlHandle = "0xSH", PlanHandle = "0xPH", QueryHash = "0xQH", DatabaseName = "DB",
            StatementStartOffset = 66, StatementEndOffset = 512, QueryPlanXmlBytes = cap,
        };
        Assert.Null(QueryStatsCollector.Instance.DescribeOversizedPlan(atCap));

        var overCap = new QueryStatsCollector.Row
        {
            SqlHandle = "0xSH", PlanHandle = "0xPH", QueryHash = "0xQH", DatabaseName = "DB",
            StatementStartOffset = 66, StatementEndOffset = 512, QueryPlanXmlBytes = cap + 1,
        };
        var observation = QueryStatsCollector.Instance.DescribeOversizedPlan(overCap);
        Assert.NotNull(observation);
        Assert.Equal(66, observation!.Value.StatementStartOffset);
        Assert.Equal(512, observation.Value.StatementEndOffset);
        Assert.Equal("0xQH", observation.Value.QueryHash);
        Assert.Equal(cap + 1, observation.Value.ObservedBytes);

        /* Either handle missing = no observation. A row with no plan_handle cannot be re-fetched at all, and
           one with no sql_handle has no complete identity to dedupe repeated sightings on — so it would
           occupy a backlog slot forever without ever resolving. */
        foreach (var partial in new[]
        {
            new QueryStatsCollector.Row { SqlHandle = "0xSH", PlanHandle = null, QueryPlanXmlBytes = cap + 1 },
            new QueryStatsCollector.Row { SqlHandle = null, PlanHandle = "0xPH", QueryPlanXmlBytes = cap + 1 },
        })
        {
            Assert.Null(QueryStatsCollector.Instance.DescribeOversizedPlan(partial));
        }

        /* procedure_stats: the module-grain literals, never per-statement values this DMV family does not
           expose. Taken from the constants the plan apply itself splices, so the deferred fetch is provably
           the same call. */
        var procObservation = ProcedureStatsCollector.Instance.DescribeOversizedPlan(MakeProcRow(cap + 1));
        Assert.NotNull(procObservation);
        Assert.Equal(ProcedureStatsCollector.ModuleStatementStartOffset, procObservation!.Value.StatementStartOffset);
        Assert.Equal(ProcedureStatsCollector.ModuleStatementEndOffset, procObservation.Value.StatementEndOffset);
        Assert.Null(procObservation.Value.QueryHash);
        Assert.Null(ProcedureStatsCollector.Instance.DescribeOversizedPlan(MakeProcRow(cap)));
    }

    [Fact]
    public void EveryOtherCollector_DescribesNoObservation()
    {
        /* The default is null on the base class AND on the interface — the RequiredPgExtensions pair — so a
           collector that captures no plan XML cannot accidentally feed the backlog. Two overriders, named
           from the constants the backlog's own reads filter on rather than as fresh literals. */
        var overriders = CollectorCatalog.All
            .Where(c => c.GetType().GetMethod("DescribeOversizedPlan")?.DeclaringType == c.GetType())
            .Select(c => c.Name)
            .OrderBy(n => n, StringComparer.Ordinal)
            .ToArray();

        Assert.Equal(
            new[] { OversizedPlanBacklog.ProcedureStatsCollectorName, OversizedPlanBacklog.QueryStatsCollectorName },
            overriders);
    }

    [Fact]
    public void BothCollectors_DeclareTheSizeAsTheirLastPayloadColumn()
    {
        /* Appended LAST on both, which is what keeps every earlier ordinal — and therefore every existing
           store column's position, the positional binary COPY and the positional DuckDB appender — stable.
           BigInt because DATALENGTH over an nvarchar(max) expression returns bigint, and a narrower store
           column would silently overflow on the megabyte-scale plans this exists to describe.

           This is the declaration half. The SELECT-ordinal-to-payload-slot agreement is driven through the
           real shredder in Lite.Tests' two collector-definition suites, which own the reader fakes. */
        Assert.Equal(52, QueryStatsCollector.Instance.PayloadColumns.Count);
        Assert.Equal(36, ProcedureStatsCollector.Instance.PayloadColumns.Count);

        foreach (ICollectorSchemaInfo collector in new ICollectorSchemaInfo[]
        {
            QueryStatsCollector.Instance,
            ProcedureStatsCollector.Instance,
        })
        {
            var names = collector.PayloadColumns.Select(c => c.Name).ToArray();

            Assert.Equal("query_plan_xml_bytes", names[^1]);
            Assert.Equal(CollectorColumnType.BigInt, collector.PayloadColumns[^1].Type);

            /* The gated content column is still there and still AHEAD of the size, which is the pair a
               reader tests as "measured, not captured". query_stats keeps other columns between them, so
               this is an ORDERING check and not an adjacency one. */
            Assert.Contains("query_plan_xml", names);
            Assert.True(
                Array.IndexOf(names, "query_plan_xml") < Array.IndexOf(names, "query_plan_xml_bytes"),
                $"{collector.Name} declares the plan size ahead of the plan itself");
        }
    }

    /* ---- the never-batched fetch -------------------------------------------------------------------- */

    [Fact]
    public void TheFetch_IsOneSingleRowTvfCall_WithNoSetValuedInput()
    {
        var sql = OversizedPlanBacklogSweep.FetchSql;

        /* ONE reference to the TVF. Two would be two plans in one result set, which is the shape the capture
           cap exists to prevent. */
        Assert.Equal(1, CountOf(sql, "dm_exec_text_query_plan"));
        Assert.Equal(1, CountOf(sql, "SELECT"));

        /* Three SCALAR parameters and nothing that could carry a set: no IN list, no UNION, no table-valued
           parameter, no APPLY over a list of handles. A batched variant cannot be written against this shape
           without changing it, which is the point. */
        Assert.Equal(1, CountOf(sql, "@plan_handle"));
        Assert.Equal(1, CountOf(sql, "@statement_start_offset"));
        Assert.Equal(1, CountOf(sql, "@statement_end_offset"));
        Assert.DoesNotContain(" IN (", sql, StringComparison.Ordinal);
        Assert.DoesNotContain("UNION", sql, StringComparison.Ordinal);
        Assert.DoesNotContain("APPLY", sql, StringComparison.Ordinal);
        Assert.DoesNotContain("READONLY", sql, StringComparison.Ordinal);

        /* The handle round-trips as varbinary from the varchar(130) hex form the backlog stores, the same
           conversion procedure_stats' own apply makes. */
        Assert.Contains("CONVERT(varbinary(64), @plan_handle, 1)", sql, StringComparison.Ordinal);

        /* Zero rows is the ONLY miss shape the caller has to handle, so the predicate has to collapse a
           NULL plan into it — the DMV can find a handle and still render nothing. */
        Assert.Contains("WHERE tqp.query_plan IS NOT NULL", sql, StringComparison.Ordinal);

        /* The collector self-filter marker, so this query does not get collected as a top query next cycle. */
        Assert.Contains("PerformanceMonitorLite", sql, StringComparison.Ordinal);
    }

    [Fact]
    public void OnlyThePerPlanMethod_ReachesAMonitoredServer_SoNPlansCostNConnections()
    {
        var source = ReadRepoFile(SweepSource);
        var map = CSharpMemberMap.Of(source);

        /* Comments and string literals are masked by the walker, so these are CODE occurrences — a mention
           in a doc comment cannot satisfy or break this. These three shapes are TARGET-specific: the store
           side of this file opens Postgres connections and builds NpgsqlCommands, so a generic name like
           ExecuteReaderAsync would not discriminate and would pass while proving nothing. */
        var reachesTarget = new[] { "TargetProviders.For(", "provider.CreateConnection(", "provider.CreateCommand(" };
        var found = 0;

        foreach (var call in reachesTarget)
        {
            for (var i = map.Code.IndexOf(call, StringComparison.Ordinal); i >= 0;
                 i = map.Code.IndexOf(call, i + 1, StringComparison.Ordinal))
            {
                found++;
                Assert.Equal("FetchOnePlanAsync", CSharpMemberMap.EnclosingMember(map, i));
            }
        }

        /* A scan that found nothing would pass vacuously — three call shapes, once each. */
        Assert.Equal(reachesTarget.Length, found);

        /* The other side of it: the STORE is never opened from inside the per-plan fetch. A method holding
           both would be one whose budget covers a target read and a store write together, and the sweep's
           budget is sized for the target read alone. */
        var storeOpens = 0;
        for (var i = map.Code.IndexOf("postgres.OpenConnectionAsync(", StringComparison.Ordinal); i >= 0;
             i = map.Code.IndexOf("postgres.OpenConnectionAsync(", i + 1, StringComparison.Ordinal))
        {
            storeOpens++;
            Assert.NotEqual("FetchOnePlanAsync", CSharpMemberMap.EnclosingMember(map, i));
        }

        Assert.Equal(2, storeOpens);

        /* And that method takes ONE plan. A parameter that could hold several is how a batched fetch would
           arrive, so the signature is the bound rather than the loop that calls it. */
        var fetch = typeof(OversizedPlanBacklogSweep)
            .GetMethod("FetchOnePlanAsync", BindingFlags.NonPublic | BindingFlags.Static)!;
        Assert.Contains(fetch.GetParameters(), p => p.ParameterType == typeof(OversizedPlanBacklog.PendingPlan));
        Assert.DoesNotContain(
            fetch.GetParameters(),
            p => p.ParameterType != typeof(string) && typeof(IEnumerable).IsAssignableFrom(p.ParameterType));
    }

    [Fact]
    public void NoMethodOnTheSweep_AcceptsASetOfPlans()
    {
        /* The claim RETURNS a set and the per-server pass loops it; nothing HANDS a set to anything. A
           method that took one would be the seam a batched fetch needs. */
        var offenders = typeof(OversizedPlanBacklogSweep)
            .GetMethods(BindingFlags.Public | BindingFlags.NonPublic | BindingFlags.Static | BindingFlags.DeclaredOnly)
            .SelectMany(m => m.GetParameters().Select(p => (Method: m.Name, p.ParameterType)))
            .Where(x => x.ParameterType != typeof(string)
                        && typeof(IEnumerable).IsAssignableFrom(x.ParameterType)
                        && CarriesPendingPlans(x.ParameterType))
            .Select(x => x.Method)
            .ToArray();

        Assert.Empty(offenders);
    }

    [Fact]
    public void TheClaimsLimit_AndTheBudgets_AreDerivedFromTheShippedConstants()
    {
        /* The limit is interpolated from the policy constant rather than written into the SQL, so the
           statement and the policy cannot disagree about how many plans one tick may fetch. */
        Assert.Contains(
            "LIMIT " + OversizedPlanBacklogSweep.MaxPlansPerServerPerTick.ToString(CultureInfo.InvariantCulture),
            OversizedPlanBacklog.ClaimSql(OversizedPlanBacklogSweep.MaxPlansPerServerPerTick),
            StringComparison.Ordinal);
        Assert.InRange(OversizedPlanBacklogSweep.MaxPlansPerServerPerTick, 1, 3);

        /* Hourly or slower: the population's shortest measured compile age is 13.6 hours, so the cadence has
           room by orders of magnitude, and nothing about a faster pass would batch anything. */
        Assert.True(OversizedPlanBacklogSweep.SweepInterval >= TimeSpan.FromHours(1),
            "the backlog sweep's cadence dropped below hourly — it is a background errand, not a collector");

        /* The wall clock is the binding bound; the command timeout can only ever fire at or before it. */
        Assert.InRange(OversizedPlanBacklogSweep.PerPlanBudget, TimeSpan.FromSeconds(10), TimeSpan.FromSeconds(15));
        Assert.True(
            OversizedPlanBacklogSweep.FetchCommandTimeoutSeconds <= OversizedPlanBacklogSweep.PerPlanBudget.TotalSeconds,
            "the fetch's command timeout outgrew the wall-clock budget, so the budget is no longer the bound");
    }

    [Fact]
    public void TheHandleParameter_IsBoundWideEnoughForTheHexForm()
    {
        /* The stored handle is '0x' + 128 hex characters = 130. NVarChar128 would truncate it, the truncated
           value would CONVERT cleanly and then match nothing, and this sweep would read that as an evicted
           plan and stamp an expiry — silently, on every row. The parameter enum's NVarChar260 doc records
           the same trap for sys.traces.path. */
        var source = CSharpMemberMap.Of(ReadRepoFile(SweepSource)).Code;
        Assert.Contains("CollectorParameterType.NVarChar260", source, StringComparison.Ordinal);
        Assert.DoesNotContain("CollectorParameterType.NVarChar128", source, StringComparison.Ordinal);
    }

    /* ---- the cadence site --------------------------------------------------------------------------- */

    [Fact]
    public void TheSweep_RunsOffTheHostLoopsOwnCadence_NeverTheCollectorRotation()
    {
        /* The whole reason the sweep exists in this shape is that it must never compete with a live
           collection cycle for a server's wall-clock budget. The collector runner records SIGHTINGS and
           nothing else; if it ever learned to fetch, the fetch would be inside the budget the cap exists to
           protect. */
        var runner = CSharpMemberMap.Of(ReadRepoFile(
            "Darling/PerformanceMonitor.Darling.Service/DarlingCollectorRunner.cs")).Code;
        Assert.DoesNotContain("OversizedPlanBacklogSweep", runner, StringComparison.Ordinal);

        var worker = CSharpMemberMap.Of(ReadRepoFile(
            "Darling/PerformanceMonitor.Darling.Service/DarlingWorker.cs")).Code;
        Assert.Contains("OversizedPlanBacklogSweep.RunAsync", worker, StringComparison.Ordinal);

        /* Its cadence comes from the sweep's own constant, so the interval and the reasoning that sized it
           live in one place. */
        Assert.Contains("OversizedPlanBacklogSweep.SweepInterval", worker, StringComparison.Ordinal);

        /* And the pass is LAUNCHED, never awaited on the tick. Its worst case is fleet width times the
           per-server plan budget times the per-plan budget — over half an hour on a 42-server fleet whose
           targets are all timing out — and awaited that is half an hour in which the fleet loop launches no
           collection bodies. The purge above it is awaited because it talks only to the store. */
        Assert.DoesNotContain("await OversizedPlanBacklogSweep.RunAsync", worker, StringComparison.Ordinal);
        Assert.Contains("_oversizedPlanSweep = OversizedPlanBacklogSweep.RunAsync", worker, StringComparison.Ordinal);
        Assert.Contains("_oversizedPlanSweep.IsCompleted", worker, StringComparison.Ordinal);
    }

    /* ---- retention ---------------------------------------------------------------------------------- */

    [Fact]
    public void TheBacklog_IsPurgedOnItsLastSighting_AtTheBaseDataHorizon()
    {
        /* last_seen_at, not first_seen_at and not captured_at: a plan that is still being collected must
           never lose its content, and one that has stopped recurring explains nothing once the fact rows
           referencing it are gone.

           Read from the SHIPPED call site. Building the statement here from the column name this pin then
           asserts would be vacuous — it would pass for any column, including the wrong one. */
        var retention = ReadRepoFile("Darling/PerformanceMonitor.Darling.Service/DarlingRetention.cs");
        Assert.Contains(
            "TimeSlicedDeleteSql(OversizedPlanBacklog.TableName, \"last_seen_at\")",
            retention, StringComparison.Ordinal);
        Assert.Contains("utcNow.AddDays(-OversizedPlanBacklogRetentionDays)", retention, StringComparison.Ordinal);
        Assert.DoesNotContain(
            "TimeSlicedDeleteSql(OversizedPlanBacklog.TableName, \"first_seen_at\")",
            retention, StringComparison.Ordinal);

        /* Derived from the shared window rather than a literal of the same value, so the backlog stays worth
           exactly as long as the data it explains. */
        Assert.Equal(DarlingRetention.DataRetentionBaseDays, DarlingRetention.OversizedPlanBacklogRetentionDays);
    }

    /* ---- the store statements ----------------------------------------------------------------------- */

    [Fact]
    public void EveryColumnTheUpsertNames_ExistsInTheShippedDdl()
    {
        /* The DDL and the DML live in one file so they cannot drift — this checks that they in fact agree,
           column by column, rather than resting on their proximity. */
        var insertList = Between(OversizedPlanBacklog.UpsertSightingSql, "AS b\r\n(", ")\r\nVALUES");
        var columns = insertList.Split(',').Select(c => c.Trim()).Where(c => c.Length > 0).ToArray();

        Assert.Equal(11, columns.Length);
        foreach (var column in columns)
        {
            Assert.Contains("    " + column + " ", OversizedPlanBacklog.CreateTableSql, StringComparison.Ordinal);
        }
    }

    [Fact]
    public void ASightingClearsAStandingExpiry_AndNeverTouchesCapturedContent()
    {
        var upsert = OversizedPlanBacklog.UpsertSightingSql;

        /* A collector only sees a row while its plan is in cache, so a fresh sighting is positive evidence
           the handle resolves — which retires whatever made the previous fetch come back empty. Without the
           clear, one transient miss retires the row permanently and recreates the blind spot. */
        Assert.Contains("expired_at = NULL", upsert, StringComparison.Ordinal);

        /* And it must not reopen a row whose content is already held: the key pins the handle AND the
           offsets, so that row describes the same document and there is nothing to go back for. */
        var onConflict = upsert.Split("DO UPDATE SET", StringSplitOptions.None)[1];
        Assert.DoesNotContain("captured_at", onConflict, StringComparison.Ordinal);
        Assert.DoesNotContain("plan_xml", onConflict, StringComparison.Ordinal);
    }

    [Fact]
    public void EveryOutcomeStatement_AddressesTheWholePrimaryKey()
    {
        /* Six key columns, or the UPDATE stamps somebody else's row. The claim hands back all six and the
           statements bind all six; the predicate is written once and shared, and this is what proves the
           three of them actually use it. */
        foreach (var sql in new[]
        {
            OversizedPlanBacklog.RecordCaptureSql,
            OversizedPlanBacklog.RecordExpirySql,
            OversizedPlanBacklog.RecordAttemptSql,
        })
        {
            foreach (var column in new[]
            {
                "server_id = $1", "collector_name = $2", "plan_handle = $3",
                "sql_handle = $4", "statement_start_offset = $5", "statement_end_offset = $6",
            })
            {
                Assert.Contains(column, sql, StringComparison.Ordinal);
            }

            /* Every attempt is counted, whatever it established — a chronically unfetchable row has to be
               legible in the TABLE, not only in a log line nobody greps. */
            Assert.Contains("attempt_count = attempt_count + 1", sql, StringComparison.Ordinal);
            Assert.Contains("last_attempt_at = $7", sql, StringComparison.Ordinal);
        }

        /* Only the capture stores content, and only the expiry stamps an expiry. A failed fetch established
           nothing about the handle, so it must not retire the row. */
        Assert.Contains("plan_xml = $8", OversizedPlanBacklog.RecordCaptureSql, StringComparison.Ordinal);
        Assert.DoesNotContain("plan_xml", OversizedPlanBacklog.RecordExpirySql, StringComparison.Ordinal);
        Assert.DoesNotContain("plan_xml", OversizedPlanBacklog.RecordAttemptSql, StringComparison.Ordinal);
        Assert.DoesNotContain("expired_at", OversizedPlanBacklog.RecordAttemptSql, StringComparison.Ordinal);
    }

    [Fact]
    public void TheClaim_SkipsCapturedAndExpiredRows_AndOrdersSoNothingStarves()
    {
        var claim = OversizedPlanBacklog.ClaimSql(OversizedPlanBacklogSweep.MaxPlansPerServerPerTick);

        Assert.Contains("captured_at IS NULL", claim, StringComparison.Ordinal);
        Assert.Contains("expired_at IS NULL", claim, StringComparison.Ordinal);

        /* Oldest ATTEMPT first is what makes head-of-line starvation impossible: one plan that can never be
           fetched cannot occupy a slot every tick forever. Largest plan first only breaks ties among rows
           never attempted — those are the plans the cap cost the most visibility on. */
        Assert.Contains("last_attempt_at ASC NULLS FIRST", claim, StringComparison.Ordinal);
        Assert.Contains("observed_bytes DESC", claim, StringComparison.Ordinal);
        Assert.True(
            claim.IndexOf("last_attempt_at ASC NULLS FIRST", StringComparison.Ordinal)
                < claim.IndexOf("observed_bytes DESC", StringComparison.Ordinal),
            "size overtook attempt age in the claim order — an unfetchable large plan would then starve the rest");
    }

    /* ---- the read surface --------------------------------------------------------------------------- */

    [Fact]
    public void BothStoredPlanReads_FallBackToTheBacklog()
    {
        /* Without this half nothing a user can see changes: the recording half alone moves the blind spot
           into a table. Both MCP reads and both viewer twins go through the same statements. */
        var mcp = CSharpMemberMap.Of(ReadRepoFile(
            "Darling/PerformanceMonitor.Darling.Service/Mcp/DarlingStoredPlanReader.cs")).Code;
        Assert.Contains("OversizedPlanBacklog.QueryStatsFallbackSql", mcp, StringComparison.Ordinal);
        Assert.Contains("OversizedPlanBacklog.ProcedureStatsFallbackBySqlHandleSql", mcp, StringComparison.Ordinal);

        var viewer = CSharpMemberMap.Of(ReadRepoFile(
            "Darling/PerformanceMonitor.Darling.Viewer/ViewerDataService.Plans.cs")).Code;
        Assert.Contains("OversizedPlanBacklog.QueryStatsFallbackSql", viewer, StringComparison.Ordinal);
        Assert.Contains("OversizedPlanBacklog.ProcedureStatsFallbackByObjectSql", viewer, StringComparison.Ordinal);

        /* The primary read still comes FIRST in both: collected content is the fresher of the two, and a
           read that served only the backlog would answer nothing for every plan under the cap. */
        Assert.Contains("QueryStatsPlanXmlByHashSql", mcp, StringComparison.Ordinal);

        /* PRESENCE before ORDER, for the IndexOf reason: -1 is less than every real offset, so an ordering
           comparison alone passes when the primary arm is gone entirely. */
        var primary = mcp.IndexOf("ReadPlanTextOrGzipAsync(command", StringComparison.Ordinal);
        var fallback = mcp.IndexOf("OversizedPlanBacklog.QueryStatsFallbackSql", StringComparison.Ordinal);
        Assert.True(primary >= 0, "the MCP reads lost the collected-content arm and now serve only the backlog");
        Assert.True(fallback > primary, "the MCP query_stats read reaches the backlog before its own collected content");

        /* And the grids' presence flag admits the capped rows, or the viewer's fallback is unreachable from
           the one surface that gates on it. No cap literal is needed to find them: DATALENGTH of a NULL plan
           is NULL, so a size at all means the server had a plan for the row. */
        foreach (var grid in new[] { "QueryStats", "ProcedureStats" })
        {
            Assert.Contains(
                "OR query_plan_xml_bytes IS NOT NULL) AS has_query_plan",
                ReadRepoFile($"Darling/PerformanceMonitor.Darling.Viewer/ViewerDataService.{grid}.cs"),
                StringComparison.Ordinal);
        }
    }

    [Fact]
    public void TheQueryStatsFallback_KeysOnTheHash_BecauseTheFactRowCarriesNoOffsets()
    {
        /* This is the REASON, pinned. query_stats reads the statement offsets for its delta key and never
           stores them, so a join from a stored fact row could only match plan_handle + sql_handle — which
           for a multi-statement plan is several backlog rows describing DIFFERENT statements' plans. Serving
           one of those as "the plan for this query" is worse than serving nothing. If the offsets ever DO
           become stored columns, this pin fails and the fallback can become an exact join. */
        var stored = QueryStatsCollector.Instance.PayloadColumns.Select(c => c.Name).ToArray();
        Assert.DoesNotContain("statement_start_offset", stored);
        Assert.DoesNotContain("statement_end_offset", stored);

        Assert.Contains("query_hash = $2", OversizedPlanBacklog.QueryStatsFallbackSql, StringComparison.Ordinal);
        Assert.DoesNotContain("plan_handle", OversizedPlanBacklog.QueryStatsFallbackSql, StringComparison.Ordinal);

        /* procedure_stats CAN join exactly, and does: its three DMVs expose no offsets at all, so the plan
           apply passes fixed literals and every backlog row for it carries that same pair. */
        Assert.Contains("b.plan_handle = ps.plan_handle", OversizedPlanBacklog.ProcedureStatsFallbackByObjectSql, StringComparison.Ordinal);

        /* Every fallback is scoped to its own collector and returns only rows that actually hold content. */
        foreach (var sql in new[]
        {
            OversizedPlanBacklog.QueryStatsFallbackSql,
            OversizedPlanBacklog.ProcedureStatsFallbackBySqlHandleSql,
            OversizedPlanBacklog.ProcedureStatsFallbackByObjectSql,
        })
        {
            Assert.Contains("collector_name = '", sql, StringComparison.Ordinal);
            Assert.Contains("plan_xml IS NOT NULL", sql, StringComparison.Ordinal);
        }
    }

    [Fact]
    public void TheDirectSqlContract_IsDocumented()
    {
        /* The store's "Reading the store directly" section exists BECAUSE a silent contract change for
           direct-SQL consumers shipped once already (#2171: query_plan_xml went NULL for every new row and
           the release notes did not say so). This change adds a THIRD state to the same question — a fact row
           with a measured size and no digest, whose content is in a different table entirely — so the section
           that answers "why is this NULL" has to name it or it misleads by omission.

           Derived from the shipped names, not retyped, so a rename fails here instead of leaving the README
           confidently wrong. */
        var readme = ReadRepoFile("Darling", "README.md");

        Assert.Contains(OversizedPlanBacklog.TableName, readme, StringComparison.Ordinal);
        Assert.Contains("query_plan_xml_bytes", readme, StringComparison.Ordinal);

        /* And it says the backlog's content is NOT gzip — the one thing a reader of that section would
           otherwise reasonably assume, since every other plan column there is. */
        Assert.Contains("plain text, never gzip", readme, StringComparison.Ordinal);
    }

    [Fact]
    public void TheViewersStoreProbe_HasAV121Sentinel_AtTheTopOfTheMap()
    {
        /* The viewer refuses a store below RequiredStoreSchemaVersion, which is StorageVersion.SchemaVersion.
           MapProbedSchemaVersion answers newest-arm-first from capability sentinels, so a rung that bumps the
           version WITHOUT adding a sentinel makes a fully-migrated store map one rung low and the viewer show
           an upgrade banner on a store that is current. StoreLogViewerGateTests asserts that invariant through
           reflection and needs the WPF assembly to do it; this reads the three source sites instead, so the
           same property is checkable on a machine that cannot load the viewer.

           The sentinel earns its place beyond that invariant: below V121 the viewer's two stored-plan reads
           have no backlog to fall back to and the grids' presence flags read a column that does not exist. */
        var viewer = ReadRepoFile("Darling/PerformanceMonitor.Darling.Viewer/ViewerDataService.cs");

        /* The probe asks the question, the caller reads the answer, the map has the parameter — three sites,
           and a sentinel present at only some of them shifts every LATER ordinal onto the wrong column. */
        Assert.Contains("table_name = 'oversized_plan_backlog'", viewer, StringComparison.Ordinal);
        Assert.Contains("bool hasOversizedPlanBacklog = false)", viewer, StringComparison.Ordinal);
        Assert.Contains("reader.GetBoolean(96));", viewer, StringComparison.Ordinal);

        /* And it is the TOP arm, above V120's. Newest-first is the whole contract of that method. */
        var v121 = viewer.IndexOf("if (hasOversizedPlanBacklog)", StringComparison.Ordinal);
        var v120 = viewer.IndexOf("if (hasDeadlockRateBandKnobs)", StringComparison.Ordinal);
        Assert.True(v121 >= 0, "the viewer has no V121 sentinel arm — a fully-migrated store would map to 120");
        Assert.True(v120 >= 0, "the V120 arm is gone, so this pin is comparing against nothing");
        Assert.True(v121 < v120, "the V121 arm sits below V120's, so a current store maps one rung low");

        /* The arm returns this build's version rather than a literal that could drift from it. */
        Assert.Contains(
            "return " + StorageVersion.SchemaVersion.ToString(CultureInfo.InvariantCulture) + ";",
            viewer[v121..], StringComparison.Ordinal);
    }

    [Fact]
    public void TheSweepsDocCommentNamesThisClassCorrectly()
    {
        /* The sweep's class doc points a reader here for the properties it claims are structural. It has to
           say the name in <c>, not <see cref>, because a product assembly cannot reference a test one — so
           the compiler cannot catch the pointer going stale, and it already had: the doc said
           "OversizedPlanBacklogSweepPins", which has never existed. A reviewer found that, and a name that
           resolves to nothing is worse here than no pointer at all, because the claim it carries is exactly
           the one a future reader would want to check before widening the fetch.

           Derived from the type rather than retyped, so renaming this class fails here instead of leaving
           the sweep pointing at a ghost. */
        var sweep = ReadRepoFile(SweepSource);

        Assert.Contains("<c>" + nameof(OversizedPlanBacklogPins) + "</c>", sweep, StringComparison.Ordinal);
        Assert.DoesNotContain("OversizedPlanBacklogSweepPins", sweep, StringComparison.Ordinal);
    }

    /* ---- helpers ------------------------------------------------------------------------------------ */

    private static ProcedureStatsCollector.Row MakeProcRow(long bytes) => new(
        "DB", "dbo", "usp_X", "PROCEDURE", null, null,
        1L, 1L, 1L, 1L, 1L, 1L, 1L, 1L, 1L, 1L, 1L, 1L, 1L, 1L, 1L, 1L, 1L, 1L, 1L,
        "0xSH", "0xPH", null, bytes);

    private static bool CarriesPendingPlans(Type type) =>
        type.IsArray
            ? type.GetElementType() == typeof(OversizedPlanBacklog.PendingPlan)
            : type.GetGenericArguments().Contains(typeof(OversizedPlanBacklog.PendingPlan));

    private static int CountOf(string text, string needle)
    {
        var count = 0;
        for (var i = text.IndexOf(needle, StringComparison.Ordinal); i >= 0;
             i = text.IndexOf(needle, i + needle.Length, StringComparison.Ordinal))
        {
            count++;
        }

        return count;
    }

    private static string Between(string text, string start, string end)
    {
        var from = text.IndexOf(start, StringComparison.Ordinal);
        Assert.True(from >= 0, $"anchor '{start}' is gone — this pin would read nothing");
        from += start.Length;
        var to = text.IndexOf(end, from, StringComparison.Ordinal);
        Assert.True(to > from, $"anchor '{end}' is gone — this pin would read nothing");
        return text[from..to];
    }
}
