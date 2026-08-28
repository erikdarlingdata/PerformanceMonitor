/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor Lite.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using Lite.Tests.Helpers;
using PerformanceMonitor.Collectors;
using Xunit;

namespace Lite.Tests;

/// <summary>
/// Pins the parity contract of the extracted query_store definition: the actual_state enumeration
/// (on-prem, AG-aware), the live PRODUCTVERSION probe deciding the 2017+/2022+ column gates (default
/// 13 when the probe fails), the last_execution_time incremental watermark with its 60-minute
/// fallback and 24h catch-up clamp, and the 56-column payload. Second Name≠TargetTable case
/// (query_store → query_store_stats).
///
/// <para>Also pins the TWO-SHAPE contract added in #1836: Azure SQL DB runs per database
/// (RunsPerDatabase → BuildQuery, eligibility gate then payload, no cross-database reference), every
/// other target keeps enumeration → BuildPerItemQuery, and both are built from ONE payload body —
/// the containment test is what makes a hand-edited second copy fail loudly instead of drifting.</para>
/// </summary>
public sealed class QueryStoreCollectorDefinitionTests
{
    private static readonly RecordingCollectorDeltaCalculator s_deltas = new();

    private static CollectorContext MakeContext(
        bool isAzureSqlDb = false,
        object? probeResult = null,
        DateTime? watermark = null,
        DateTime? collectionTime = null,
        bool capturePlanXml = false,
        bool fetchQueryTextSeparately = false)
        => new()
        {
            ServerId = 42,
            ServerName = "test-server",
            CollectionTime = collectionTime ?? new DateTime(2026, 7, 2, 12, 0, 0, DateTimeKind.Utc),
            Deltas = s_deltas,
            Target = new CollectorTargetInfo { IsAzureSqlDb = isAzureSqlDb },
            Watermark = watermark,
            EnumerationProbeResult = probeResult,
            CapturePlanXml = capturePlanXml,
            FetchQueryTextSeparately = fetchQueryTextSeparately,
        };

    /// <summary>
    /// The emitted payload, with line endings normalized to LF. The template is a verbatim string literal,
    /// so it carries this source file's CRLF, while the version-gated fragments spliced into it are ordinary
    /// C# strings carrying LF — a mixture that predates #1907 (the replica join fragment has always been
    /// built this way) and that SQL Server does not care about. Assertions spanning a line break normalize
    /// rather than encode one convention and break on the other.
    /// </summary>
    private static string Lf(string sql) => sql.Replace("\r\n", "\n", StringComparison.Ordinal);

    private static string PayloadSql(CollectorContext context)
        => Lf(QueryStoreCollector.Instance.BuildPerItemQuery("SO", context).Text);

    private static string AzurePayloadSql(CollectorContext context)
        => Lf(QueryStoreCollector.Instance.BuildQuery(context).Text);

    [Fact]
    public void Identity_SecondNameTargetTableSplit_WithWatermark()
    {
        Assert.Equal("query_store", QueryStoreCollector.Instance.Name);
        Assert.Equal("query_store_stats", QueryStoreCollector.Instance.TargetTable);
        Assert.Equal("last_execution_time", QueryStoreCollector.Instance.WatermarkColumn);
    }

    [Fact]
    public void AppliesTo_VersionGate_SkipsPreSql2016OnPrem_ButNotAzureOrUnknown()
    {
        /* Query Store first shipped in SQL 2016 (v13). Gate collapsed from Lite's IsCollectorSupported into
           the shared AppliesTo (so Darling gates too); a pre-2016 box has no Query Store at all. */
        Assert.False(QueryStoreCollector.Instance.AppliesTo(new CollectorTargetInfo { SqlMajorVersion = 12 }));
        Assert.True(QueryStoreCollector.Instance.AppliesTo(new CollectorTargetInfo { SqlMajorVersion = 13 }));
        Assert.True(QueryStoreCollector.Instance.AppliesTo(new CollectorTargetInfo { SqlMajorVersion = 16 }));
        /* Unknown (0) assumes newest; Azure SQL DB / MI report a low ProductMajorVersion but ship Query Store. */
        Assert.True(QueryStoreCollector.Instance.AppliesTo(new CollectorTargetInfo { SqlMajorVersion = 0 }));
        Assert.True(QueryStoreCollector.Instance.AppliesTo(new CollectorTargetInfo { IsAzureSqlDb = true, SqlMajorVersion = 12 }));
        Assert.True(QueryStoreCollector.Instance.AppliesTo(new CollectorTargetInfo { IsAzureManagedInstance = true, SqlMajorVersion = 12 }));
    }

    [Fact]
    public void BuildEnumerationQuery_OnPrem_AgAware_ProbesActualState_WithExclusions()
    {
        var plan = QueryStoreCollector.Instance.BuildEnumerationQuery(new CollectorContext
        {
            ServerId = 42,
            ServerName = "test-server",
            CollectionTime = DateTime.UtcNow,
            Deltas = s_deltas,
            ExcludedDatabases = new[] { "SO" },
        });

        Assert.NotNull(plan);
        Assert.Contains("sys.dm_hadr_database_replica_states", plan!.Text, StringComparison.Ordinal);
        Assert.Contains("drs.is_primary_replica = 1", plan.Text, StringComparison.Ordinal);
        /* #1565: the canonical default screen — vendor management dbs (rdsadmin, gcloud_cloudsqladmin),
           SSRS/DW artifacts, the system four as a name belt, and the operator-chosen DBA-convention
           names. Spot-pin one from each group; the full list lives in the query. */
        Assert.Contains("N'rdsadmin'", plan.Text, StringComparison.Ordinal);
        Assert.Contains("N'gcloud_cloudsqladmin'", plan.Text, StringComparison.Ordinal);
        Assert.Contains("N'ReportServerTempDB'", plan.Text, StringComparison.Ordinal);
        Assert.Contains("N'DBAUtils'", plan.Text, StringComparison.Ordinal);
        /* IN (1, 2, 4) = READ_ONLY/READ_WRITE/READ_CAPTURE_SECONDARY, not "> 0": 3 = ERROR must not
           pass the "is QS usable" gate. */
        Assert.Contains("WHERE actual_state IN (1, 2, 4)", plan.Text, StringComparison.Ordinal);
        /* #1558: readable-secondary replicas (readonly_reason bit 8 — AG secondaries slip the HADR
           join on RDS/geo mechanisms) are excluded: their QS is the primary's replicated content.
           Bitmask form, so a combined reason still excludes. */
        Assert.Contains("AND   readonly_reason & 8 = 0", plan.Text, StringComparison.Ordinal);
        Assert.Contains("AND d.name NOT IN (@excl_db_0)", plan.Text, StringComparison.Ordinal);
        Assert.DoesNotContain("/*EXCLUSION_FILTER*/", plan.Text, StringComparison.Ordinal);
        Assert.Equal("SO", Assert.Single(plan.Parameters).Value);
    }

    [Fact]
    public void RunsPerDatabase_OnAzureOnly()
    {
        /* #1836: Azure SQL DB rejects the cross-database [db].sys.sp_executesql reference the
           enumeration path is built on, for EVERY database — so there the host connects per database
           and drives BuildQuery instead. Managed Instance and RDS honor three-part references and keep
           the enumeration. Same gate as procedure_stats (#1833) and the database-scoped siblings. */
        Assert.True(QueryStoreCollector.Instance.RunsPerDatabase(new CollectorTargetInfo { IsAzureSqlDb = true }));
        Assert.False(QueryStoreCollector.Instance.RunsPerDatabase(new CollectorTargetInfo()));
        Assert.False(QueryStoreCollector.Instance.RunsPerDatabase(new CollectorTargetInfo { IsAzureManagedInstance = true }));
    }

    [Fact]
    public void BuildEnumerationQuery_Azure_IsNull_EnumerationIsNotHowAzureCollects()
    {
        /* #1836: the Azure enumeration cursor is GONE, not merely unused. It probed each candidate
           through QUOTENAME(@db) + N'.sys.sp_executesql' — the cross-database reference Azure SQL DB
           rejects for every database — into an empty CATCH, so it could only ever return an empty
           list, which the host logged as SUCCESS with 0 rows forever. Null here is the same signal
           index_object_stats gives on Azure: this target is collected per database, not by enumeration. */
        Assert.Null(QueryStoreCollector.Instance.BuildEnumerationQuery(MakeContext(isAzureSqlDb: true)));

        /* Managed Instance is NOT Azure SQL DB here: it supports cross-database references, so it keeps
           the on-prem enumeration (HADR join and all). */
        var mi = QueryStoreCollector.Instance.BuildEnumerationQuery(new CollectorContext
        {
            ServerId = 42,
            ServerName = "test-server",
            CollectionTime = DateTime.UtcNow,
            Deltas = s_deltas,
            Target = new CollectorTargetInfo { IsAzureManagedInstance = true },
        });

        Assert.NotNull(mi);
        Assert.Contains("sys.dm_hadr_database_replica_states", mi!.Text, StringComparison.Ordinal);
    }

    [Fact]
    public void BuildQuery_Azure_GatesOnEligibility_ThenRunsPayloadAgainstCurrentDatabase()
    {
        var plan = QueryStoreCollector.Instance.BuildQuery(MakeContext(isAzureSqlDb: true));

        /* The SAME eligibility predicates the on-prem enumeration probes with (#1546 / #1558), only
           evaluated locally against the connected database instead of through a three-part reference. */
        Assert.Contains("WHERE actual_state IN (1, 2, 4)", plan.Text, StringComparison.Ordinal);
        Assert.Contains("AND   readonly_reason & 8 = 0", plan.Text, StringComparison.Ordinal);

        /* Ineligible databases cost one catalog lookup and return NO result set — the guard runs
           BEFORE the payload, so an OFF/ERRORed/secondary Query Store never touches the QS views.
           Verified live: on a database whose sys.database_query_store_options is empty the batch
           returns zero fields and zero rows, no error. */
        var gateEnd = plan.Text.IndexOf("RETURN;", StringComparison.Ordinal);
        var payloadStart = plan.Text.IndexOf("sys.query_store_runtime_stats", StringComparison.Ordinal);
        Assert.True(gateEnd > 0, "the Azure query must carry the short-circuit guard");
        Assert.True(payloadStart > gateEnd, "the eligibility guard must precede the payload");

        /* The whole point of the rework: NO cross-database reference anywhere, and no sp_executesql
           wrapper — this batch runs on a connection already scoped to the database. */
        Assert.DoesNotContain("sp_executesql", plan.Text, StringComparison.Ordinal);
        Assert.DoesNotContain(".sys.query_store", plan.Text, StringComparison.Ordinal);
        Assert.DoesNotContain("QUOTENAME", plan.Text, StringComparison.Ordinal);

        /* Payload columns are present and unchanged in shape — the reader contract is the same 55
           ordinals on both paths. */
        Assert.Contains("SELECT /* PerformanceMonitorLite */ TOP (50000) WITH TIES", plan.Text, StringComparison.Ordinal);
        Assert.Contains("query_id = qsq.query_id,", plan.Text, StringComparison.Ordinal);
        Assert.Contains("query_sql_text = qst.query_sql_text,", plan.Text, StringComparison.Ordinal);
        Assert.Contains("query_plan_hash = CONVERT(varchar(64), qsp.query_plan_hash, 1),", plan.Text, StringComparison.Ordinal);

        /* Interval-grain incremental filter since #1907, on this path too — the Azure body IS the shared
           body, so the WHERE→HAVING move lands here by construction rather than by a second edit. */
        Assert.Contains("HAVING\n    MAX(qsrs.last_execution_time) > @cutoff_time", Lf(plan.Text), StringComparison.Ordinal);
        Assert.DoesNotContain("WHERE qsrs.last_execution_time > @cutoff_time", plan.Text, StringComparison.Ordinal);
        Assert.Contains("ORDER BY qsrs.last_execution_time ASC", plan.Text, StringComparison.Ordinal);
        Assert.Contains("OPTION(RECOMPILE);", plan.Text, StringComparison.Ordinal);
        Assert.DoesNotContain("LOOP JOIN", plan.Text, StringComparison.Ordinal);

        var parameter = Assert.Single(plan.Parameters);
        Assert.Equal("@cutoff_time", parameter.Name);
        Assert.Equal(CollectorParameterType.DateTime2, parameter.Type);
    }

    [Fact]
    public void BuildQuery_Azure_AndPerItemWrapper_ShareTheSinglePayloadBody()
    {
        /* The drift guard this rework is designed around (#1836). Both builders are handed the SAME
           context, so they must produce the SAME payload — the per-item form differing ONLY by the
           quote-doubling that nests it inside [db].sys.sp_executesql. Hand-edit either path's columns
           and this fails, which is the point: a 56-column payload maintained in two copies is how the
           two paths silently stop agreeing about what the reader's ordinals mean. */
        var context = MakeContext(isAzureSqlDb: true, probeResult: 16, capturePlanXml: true);

        var azure = QueryStoreCollector.Instance.BuildQuery(context);
        var perItem = QueryStoreCollector.Instance.BuildPerItemQuery("SO", context);

        /* The Azure text is [eligibility guard] + [body]; the body starts at the isolation-level SET. */
        var bodyStart = azure.Text.IndexOf("SET TRANSACTION ISOLATION LEVEL READ UNCOMMITTED;", StringComparison.Ordinal);
        Assert.True(bodyStart > 0);
        var body = azure.Text[bodyStart..];

        /* Long enough that containment cannot be an accident. */
        Assert.True(body.Length > 2000, $"payload body was only {body.Length} chars — did the extraction break?");
        Assert.Contains(body.Replace("'", "''", StringComparison.Ordinal), perItem.Text, StringComparison.Ordinal);
    }

    [Fact]
    public void BuildQuery_NonAzure_Throws_EnumerationDrivesThoseTargets()
    {
        /* On-prem/RDS/MI must never reach BuildQuery: doing so would collect the connection's own
           catalog — master, when the server entry leaves its Database field blank — as if it were the
           whole instance, which is the exact silent-wrong shape #1833 fixed elsewhere. */
        Assert.Throws<NotSupportedException>(() => QueryStoreCollector.Instance.BuildQuery(MakeContext()));
        Assert.Throws<NotSupportedException>(() => QueryStoreCollector.Instance.BuildQuery(new CollectorContext
        {
            ServerId = 42,
            ServerName = "test-server",
            CollectionTime = DateTime.UtcNow,
            Deltas = s_deltas,
            Target = new CollectorTargetInfo { IsAzureManagedInstance = true },
        }));
    }

    [Fact]
    public void BuildQuery_Azure_AttributesReplicaRole_ProvenLiveOnBothServiceTiers()
    {
        /* This pin was DELIBERATELY INVERTED by #1872 — it previously asserted the attribution absent.
           #1836 gated it off for bind safety, not taste: replica_group_id's doc page names SQL Server
           2022+ and is silent on Azure SQL Database while its sibling columns name Azure explicitly,
           and Query Store for secondary replicas is documented as unavailable on Hyperscale. A column
           that does not bind fails the WHOLE payload for that database, and on Azure that means every
           database — so the gate stayed on until the docs' silence was answered by a live run.

           It was, on both tiers the old gate worried about (2026-07-31 UTC, both Microsoft SQL Azure
           (RTM) 12.0.2000.8, EngineEdition 5): General Purpose GP_S_Gen5_1 (#1848) and Hyperscale
           HS_S_Gen5_2 (#1872) each returned OBJECT_ID('sys.query_store_replicas') = -660 and
           COL_LENGTH('sys.query_store_runtime_stats', 'replica_group_id') = 8, and on Hyperscale the
           full 55-column payload composed with the attribution ON bound 55 of 55 columns and returned
           replica_role = 'Primary'. Do not re-flip this without new live evidence that it stopped
           binding — re-read the three issue threads first.

           What #1887 then settled, recorded here so nobody expects more of the column than it can give:
           on Azure SQL Database this attributes a CONSTANT. A provisioned HS_Gen5_2 WITH a live HA
           replica still held only the four static ROLE rows in sys.query_store_replicas, every
           runtime-stats row still carried replica_group_id = 1, and 30 executions run against the
           readable secondary (ApplicationIntent=ReadOnly) reached neither that replica's Query Store
           nor the primary's — secondary workload never enters a Query Store under read scale-out, so
           replica_role always reads 'Primary' on this target. That is honest rather than useless: it
           replaces a bare NULL that said nothing with the server's own statement that the row is the
           primary's, and there is no blending here for it to disambiguate. It is NOT a reason to
           re-gate Azure off. */
        var plan = QueryStoreCollector.Instance.BuildQuery(MakeContext(isAzureSqlDb: true, probeResult: 16));

        Assert.Contains("replica_role = qsr.replica_name", plan.Text, StringComparison.Ordinal);
        Assert.Contains("LEFT JOIN sys.query_store_replicas AS qsr", plan.Text, StringComparison.Ordinal);
        Assert.Contains("ON qsr.replica_group_id = qsrs.replica_group_id", plan.Text, StringComparison.Ordinal);

        /* Attribute, never filter — the same rule the on-prem 2022+ shape holds to. On Azure the
           single-quoted body is NOT quote-doubled, so the literal would read N'Primary' here. */
        Assert.DoesNotContain("replica_name = N'Primary'", plan.Text, StringComparison.Ordinal);

        /* plan_type_desc rides the same "Azure means newest" rule, and always has: it lives on
           sys.query_store_plan, whose applies-to banner names Azure SQL Database and whose only
           documented exclusion is Synapse (engine edition 6, never IsAzureSqlDb). Azure SQL DB is
           evergreen, so the PRODUCTVERSION probe — which reports major 12 there — must not decide it. */
        Assert.Contains("plan_type = qsp.plan_type_desc,", plan.Text, StringComparison.Ordinal);
    }

    [Fact]
    public void BuildQuery_Azure_AttributesReplicaRole_EvenWhenTheVersionProbeFails()
    {
        /* The Azure arm of the gate is an OR, not an AND: Azure SQL DB reports PRODUCTVERSION major 12
           and the probe can fail outright (defaulting to 13), so if attribution depended on the version
           at all it would never turn on there. Pins that the edition alone decides it (#1872). */
        var plan = QueryStoreCollector.Instance.BuildQuery(MakeContext(isAzureSqlDb: true, probeResult: null));

        Assert.Contains("replica_role = qsr.replica_name", plan.Text, StringComparison.Ordinal);
        Assert.Contains("LEFT JOIN sys.query_store_replicas AS qsr", plan.Text, StringComparison.Ordinal);
    }

    [Fact]
    public void BuildQuery_Azure_ClampsStaleWatermark_AndFallsBack60Minutes()
    {
        /* Query Store is the one unbounded-persisted source (it retains ~30 days), so a service that
           was down for days must not ask for the whole backlog in one cycle — the #1556 field
           incident. The host's per-database Azure branch deliberately does NOT clamp (it also serves
           the XE ring-buffer collectors, where clamping would wrongly truncate legitimate catch-up),
           so the clamp travels with the COLLECTOR: it is applied inside the cutoff computation and
           therefore holds on both paths. */
        var collectionTime = new DateTime(2026, 7, 2, 12, 0, 0, DateTimeKind.Utc);

        var stale = QueryStoreCollector.Instance.BuildQuery(
            MakeContext(isAzureSqlDb: true, watermark: collectionTime.AddDays(-5), collectionTime: collectionTime));
        Assert.Equal(collectionTime - WatermarkPolicy.MaxCatchup, Assert.Single(stale.Parameters).Value);

        /* Inside the horizon: passed through untouched. */
        var fresh = QueryStoreCollector.Instance.BuildQuery(
            MakeContext(isAzureSqlDb: true, watermark: collectionTime.AddMinutes(-30), collectionTime: collectionTime));
        Assert.Equal(collectionTime.AddMinutes(-30), Assert.Single(fresh.Parameters).Value);

        /* Nothing collected yet for this database: the documented 60-minute first-run window. */
        var first = QueryStoreCollector.Instance.BuildQuery(
            MakeContext(isAzureSqlDb: true, collectionTime: collectionTime));
        Assert.Equal(collectionTime.AddMinutes(-60), Assert.Single(first.Parameters).Value);
    }

    [Fact]
    public void BuildQuery_Azure_SignalsWhenTheClampFires_SoTheHoleStaysLogged()
    {
        /* WatermarkPolicy's premise is that the hole it opens is "deliberate, LOGGED, bounded". On the
           enumeration path the host clamps and logs before the definition sees the watermark; on the
           Azure per-database path the definition clamps, so it has to hand the host something to log
           or the one platform this PR fixes would be the one platform where the hole is silent. */
        var collectionTime = new DateTime(2026, 7, 2, 12, 0, 0, DateTimeKind.Utc);

        var stale = MakeContext(isAzureSqlDb: true, watermark: collectionTime.AddDays(-5), collectionTime: collectionTime);
        QueryStoreCollector.Instance.BuildQuery(stale);
        Assert.True(stale.CatchupClampApplied);

        /* Assigned unconditionally, not just set: the host reuses ONE context across every database in
           a cycle, so a stale true from the previous database would misreport this one. */
        stale.Watermark = collectionTime.AddMinutes(-30);
        QueryStoreCollector.Instance.BuildQuery(stale);
        Assert.False(stale.CatchupClampApplied);

        /* A first run for the database clamps nothing — there is no watermark to floor, and the
           60-minute fallback is not a hole. */
        var first = MakeContext(isAzureSqlDb: true, collectionTime: collectionTime);
        QueryStoreCollector.Instance.BuildQuery(first);
        Assert.False(first.CatchupClampApplied);

        /* The enumeration path re-clamps an ALREADY-clamped watermark (the host clamped and logged
           first), so the definition must not raise a second, duplicate warning for it. */
        var alreadyClamped = MakeContext(watermark: collectionTime - WatermarkPolicy.MaxCatchup, collectionTime: collectionTime);
        QueryStoreCollector.Instance.BuildPerItemQuery("SO", alreadyClamped);
        Assert.False(alreadyClamped.CatchupClampApplied);
    }

    [Fact]
    public void BuildEnumerationProbe_PinsLiveProductVersionCheck()
    {
        var probe = QueryStoreCollector.Instance.BuildEnumerationProbe(MakeContext());

        Assert.NotNull(probe);
        Assert.Equal(
            "SELECT CONVERT(integer, PARSENAME(CONVERT(sysname, SERVERPROPERTY('PRODUCTVERSION')), 4))",
            probe!.Text);
        Assert.Empty(probe.Parameters);
    }

    [Fact]
    public void BuildPerItemQuery_ProbeFailed_DefaultsTo2016_NullGatedColumns()
    {
        var plan = QueryStoreCollector.Instance.BuildPerItemQuery("SO", MakeContext(probeResult: null));

        Assert.Contains("avg_num_physical_io_reads = NULL", plan.Text, StringComparison.Ordinal);
        Assert.Contains("avg_log_bytes_used = NULL", plan.Text, StringComparison.Ordinal);
        Assert.Contains("avg_tempdb_space_used = NULL", plan.Text, StringComparison.Ordinal);
        Assert.Contains("plan_forcing_type = NULL,", plan.Text, StringComparison.Ordinal);
        Assert.Contains("plan_type = NULL,", plan.Text, StringComparison.Ordinal);
        Assert.Contains("replica_role = CONVERT(nvarchar(1), NULL)", plan.Text, StringComparison.Ordinal);
        Assert.DoesNotContain("sys.query_store_replicas", plan.Text, StringComparison.Ordinal);
    }

    [Fact]
    public void BuildPerItemQuery_2017Probe_RealColumns_NoPlanType()
    {
        var plan = QueryStoreCollector.Instance.BuildPerItemQuery("SO", MakeContext(probeResult: 14));

        Assert.Contains("qsrs.avg_num_physical_io_reads, qsrs.min_num_physical_io_reads, qsrs.max_num_physical_io_reads,", plan.Text, StringComparison.Ordinal);
        Assert.Contains("avg_log_bytes_used = qsrs.avg_log_bytes_used", plan.Text, StringComparison.Ordinal);
        Assert.Contains("avg_tempdb_space_used = qsrs.avg_tempdb_space_used", plan.Text, StringComparison.Ordinal);
        Assert.Contains("plan_forcing_type = qsp.plan_forcing_type_desc,", plan.Text, StringComparison.Ordinal);
        Assert.Contains("plan_type = NULL,", plan.Text, StringComparison.Ordinal);

        /* Replica attribution is 2022+ only — sys.query_store_replicas does not exist on 2017. */
        Assert.Contains("replica_role = CONVERT(nvarchar(1), NULL)", plan.Text, StringComparison.Ordinal);
        Assert.DoesNotContain("sys.query_store_replicas", plan.Text, StringComparison.Ordinal);
    }

    [Fact]
    public void BuildPerItemQuery_2022Probe_PlanTypeColumn()
    {
        var plan = QueryStoreCollector.Instance.BuildPerItemQuery("SO", MakeContext(probeResult: 16));

        Assert.Contains("plan_type = qsp.plan_type_desc,", plan.Text, StringComparison.Ordinal);
        Assert.Contains("plan_forcing_type = qsp.plan_forcing_type_desc,", plan.Text, StringComparison.Ordinal);
    }

    /// <summary>
    /// Replica attribution turns on at major 16 — sys.query_store_replicas and
    /// sys.query_store_runtime_stats.replica_group_id both exist from SQL 2022, verified live against
    /// 16.0.4255.1 (the docs' "2025+" claim for the view is wrong), so the gate is >= 16, not >= 17.
    /// </summary>
    [Fact]
    public void BuildPerItemQuery_2022Probe_ReplicaRole_LeftJoinsReplicas()
    {
        var plan = QueryStoreCollector.Instance.BuildPerItemQuery("SO", MakeContext(probeResult: 16));

        /* replica_name read directly: contrary to the docs it IS populated on box SQL Server
           ('Primary', 'Secondary', 'Geo Secondary', 'Geo HA Secondary'), so no role_type CASE. */
        Assert.Contains("replica_role = qsr.replica_name", plan.Text, StringComparison.Ordinal);

        /* MUST be a LEFT JOIN. On a 2022 standalone sys.query_store_replicas has ZERO rows while real
           runtime-stats rows still carry replica_group_id = 1 — an INNER JOIN would match nothing and
           silently delete ALL Query Store collection on every 2022 standalone server. */
        Assert.Contains(
            "LEFT JOIN sys.query_store_replicas AS qsr",
            plan.Text,
            StringComparison.Ordinal);
        Assert.Contains("ON qsr.replica_group_id = qsrs.replica_group_id", plan.Text, StringComparison.Ordinal);

        /* No filter to a single role: rows are attributed, never dropped. */
        Assert.DoesNotContain("replica_name = N''Primary''", plan.Text, StringComparison.Ordinal);
    }

    /// <summary>SQL 2025 (major 17) keeps the 2022 attribution — the gate is >=, not ==.</summary>
    [Fact]
    public void BuildPerItemQuery_2025Probe_ReplicaRole_StillAttributed()
    {
        var plan = QueryStoreCollector.Instance.BuildPerItemQuery("SO", MakeContext(probeResult: 17));

        Assert.Contains("replica_role = qsr.replica_name", plan.Text, StringComparison.Ordinal);
        Assert.Contains("LEFT JOIN sys.query_store_replicas AS qsr", plan.Text, StringComparison.Ordinal);
    }

    /// <summary>
    /// #1907: the payload combines the flushed and the still-in-memory slice of one runtime-stats interval
    /// before emitting, grouped on exactly the natural key of <c>sys.query_store_runtime_stats</c>.
    ///
    /// <para>Live on SQL 2022 (16.0.4255.1): 100 executions flushed plus 25 still in memory came back as
    /// two rows of one <c>runtime_stats_interval_id</c>, and <c>sys.dm_exec_procedure_stats</c> — a wholly
    /// separate source read at the same instant — said 125. Both slices used to be stored, where they
    /// shared the entire read-side dedup key AND collection_time, so the survivor was whichever the engine
    /// emitted first.</para>
    /// </summary>
    [Fact]
    public void BuildPerItemQuery_CombinesTheSlicesOfOneInterval_OnTheViewsNaturalKey()
    {
        var text = PayloadSql(MakeContext(probeResult: 16));

        /* The grouping key IS the view's natural key: plan, interval, execution type, replica group.
           Anything coarser would merge work that is genuinely distinct; anything finer would leave the
           slices split, which is the bug. */
        Assert.Contains(
            "GROUP BY\n    qsrs.plan_id,\n    qsrs.runtime_stats_interval_id,\n    qsrs.execution_type_desc,\n    qsrs.replica_group_id",
            text,
            StringComparison.Ordinal);

        /* The additive counter is SUMmed, and the interval's span comes from its slices' extremes. */
        Assert.Contains("count_executions = SUM(qsrs.count_executions)", text, StringComparison.Ordinal);
        Assert.Contains("first_execution_time = MIN(qsrs.first_execution_time)", text, StringComparison.Ordinal);
        Assert.Contains("last_execution_time = MAX(qsrs.last_execution_time)", text, StringComparison.Ordinal);

        /* min_* / max_* keep the extreme, never an average of extremes. min_dop / max_dop are the pair
           with no avg_ sibling, which is exactly the pair a mechanical edit is most likely to mishandle. */
        Assert.Contains("min_duration = MIN(qsrs.min_duration)", text, StringComparison.Ordinal);
        Assert.Contains("max_duration = MAX(qsrs.max_duration)", text, StringComparison.Ordinal);
        Assert.Contains("min_dop = MIN(qsrs.min_dop)", text, StringComparison.Ordinal);
        Assert.Contains("max_dop = MAX(qsrs.max_dop)", text, StringComparison.Ordinal);

        /* The pre-filter is a prune, not a semantic: its interval list is a superset of what the HAVING
           keeps, so it can never subtract a row. #2133: the ids resolve from the INTERVAL CATALOG —
           hundreds of rows — never by scanning runtime_stats itself (measured 20 ms vs 426 ms for the
           identical id set on the field store that wedged). */
        Assert.Contains("WHERE qsrs.runtime_stats_interval_id IN", text, StringComparison.Ordinal);
        Assert.Contains("FROM sys.query_store_runtime_stats_interval AS i", text, StringComparison.Ordinal);
        Assert.Contains("WHERE i.end_time > @cutoff_time", text, StringComparison.Ordinal);
        Assert.DoesNotContain("FROM sys.query_store_runtime_stats AS f", text, StringComparison.Ordinal);

        /* #2133 STAGING: the aggregate lands in a temp table and the plan/query/text joins run FROM it,
           so the optimizer joins with real cardinalities instead of TVF fixed guesses — the monolithic
           join re-materialized a TVF per probe, a fixed ≥30s cost on an 82k-plan catalog that no
           catch-up width could reduce (staged: 524 ms, same store, same window). SELECT INTO emits no
           result set, so the batch still returns exactly one; the leading DROP covers Azure's pooled
           direct connections (on-prem the sp_executesql scope self-cleans). */
        Assert.Contains("DROP TABLE IF EXISTS #pm_qs_slice;", text, StringComparison.Ordinal);
        Assert.Contains("INTO #pm_qs_slice", text, StringComparison.Ordinal);
        Assert.Contains("FROM #pm_qs_slice AS qsrs\nJOIN sys.query_store_plan AS qsp", Lf(text), StringComparison.Ordinal);

        /* The row SHAPE must not move: 55 selected columns, and the TOP/ORDER BY stay on the final
           SELECT so the cap counts intervals and can never truncate one interval's slices into a
           partial sum. WITH TIES + ASC are the #1960 never-a-hole pair: oldest-first shipping keeps
           the derived watermark at the shipped boundary, and WITH TIES stops a bare TOP from splitting
           a group of rows tied at that boundary — the strict `> @cutoff_time` would strand the
           unshipped half forever. The LOOP JOIN hint must never return to this query: looping from the
           temp into the TVFs is the per-probe re-materialization #2133 removed. */
        Assert.Contains($"TOP ({QueryStoreCollector.MaxRowsPerDatabase}) WITH TIES", text, StringComparison.Ordinal);
        Assert.Contains("ORDER BY qsrs.last_execution_time ASC\nOPTION(RECOMPILE);", Lf(text), StringComparison.Ordinal);
        Assert.DoesNotContain("LOOP JOIN", text, StringComparison.Ordinal);
    }

    /// <summary>
    /// #2150: with the flag off — which is Lite, always — the payload is UNCHANGED, text and all.
    ///
    /// <para>This is the pin that makes the feature safe to add at all. Lite stores <c>query_sql_text</c>
    /// inline in DuckDB and its grid reads it from there, so nulling that column unconditionally would
    /// blind Lite. The flag exists for exactly that reason, and "off changes nothing" is the property that
    /// has to be enforced rather than assumed.</para>
    /// </summary>
    [Fact]
    public void WithoutTheFlag_TheTextStaysInline()
    {
        foreach (var azure in new[] { false, true })
        {
            var text = PayloadSql(MakeContext(isAzureSqlDb: azure));

            Assert.Contains("query_sql_text = qst.query_sql_text,", text, StringComparison.Ordinal);
            Assert.DoesNotContain("query_sql_text = CONVERT", text, StringComparison.Ordinal);
        }
    }

    /// <summary>
    /// #2150: with the flag on the text column becomes a placeholder AT THE SAME ORDINAL, and nothing else
    /// about the payload moves.
    ///
    /// <para>The ordinal is the load-bearing part: the readers index this row by number, so a column that
    /// changed position would silently shift every later field onto the wrong value. Asserted by
    /// normalizing the one column out of both forms and requiring the remainder to be identical — which
    /// covers "nothing else moved" for every column at once, rather than for the handful someone thought
    /// to list.</para>
    ///
    /// <para>The <c>query_store_query_text</c> join is DROPPED with the flag on: Darling nulls the text
    /// column and fetches text by-ids, so the join fed nothing. Measured -12% on a 40,388-plan catalog.
    /// Lite (flag off) keeps the join because it reads the text inline. Ordinal-safety of the SELECT list
    /// is still asserted below, with both the text column and the Lite-only join normalized out.</para>
    /// </summary>
    [Fact]
    public void WithTheFlag_TheTextIsNulledAtTheSameOrdinal()
    {
        var inline = PayloadSql(MakeContext());
        var nulled = PayloadSql(MakeContext(fetchQueryTextSeparately: true));

        Assert.DoesNotContain("query_sql_text = qst.query_sql_text", nulled, StringComparison.Ordinal);
        Assert.Contains("query_sql_text = CONVERT(nvarchar(1), NULL),", nulled, StringComparison.Ordinal);
        /* Immediately before query_hash, exactly where the real column sat. */
        Assert.Contains("query_sql_text = CONVERT(nvarchar(1), NULL),\n    query_hash", Lf(nulled), StringComparison.Ordinal);
        /* The qst join is dropped with the flag on (text arrives by-ids); Lite keeps it. */
        Assert.DoesNotContain("JOIN sys.query_store_query_text AS qst", nulled, StringComparison.Ordinal);
        Assert.Contains("JOIN sys.query_store_query_text AS qst", inline, StringComparison.Ordinal);

        /* Ordinal-safety still holds: normalize out BOTH the text column and the Lite-only qst join, and
           the remainder - every other column at its ordinal - must be identical between the two forms. */
        const string qstJoin = "JOIN sys.query_store_query_text AS qst\n  ON qst.query_text_id = qsq.query_text_id\n";
        Assert.Equal(
            Lf(inline).Replace("query_sql_text = qst.query_sql_text,", "@@TEXT@@", StringComparison.Ordinal)
                      .Replace(qstJoin, "", StringComparison.Ordinal),
            Lf(nulled).Replace("query_sql_text = CONVERT(nvarchar(1), NULL),", "@@TEXT@@", StringComparison.Ordinal));
    }

    /// <summary>
    /// #2150's split, driven the #2312 way: the text fetch selects exactly the query_ids the caller names —
    /// the cycle's collected rows whose text the store does not hold — cut by an exact byte budget in
    /// <c>query_id</c> order. There is no watermark to resume from any more; the STORE answers what is
    /// missing, and an empty missing set issues no query at all.
    /// </summary>
    [Fact]
    public void TextFetchByIds_SelectsTheNamedIds_AndIsBudgetCutInQueryIdOrder()
    {
        var sql = QueryStoreCollector.Instance.BuildTextFetchByIdsQuery(
            "SO", MakeContext(fetchQueryTextSeparately: true), new long[] { 4242, 4243 },
            budgetBytes: 12 * 1024 * 1024).Text;

        Assert.Contains("EXECUTE [SO].sys.sp_executesql", sql, StringComparison.Ordinal);
        Assert.Contains("WHERE qsq.query_id IN (4242, 4243)", sql, StringComparison.Ordinal);
        Assert.DoesNotContain("qsq.query_id > ", sql, StringComparison.Ordinal);
        Assert.DoesNotContain("SELECT TOP", sql, StringComparison.Ordinal);
        Assert.Contains("ORDER BY b.query_id", sql, StringComparison.Ordinal);
        Assert.Contains("b.running_bytes - b.text_bytes < 12582912", sql, StringComparison.Ordinal);
        /* query_id is only unique until a Query Store reset renumbers it; the hash is the reset detector. */
        Assert.Contains("query_hash = CONVERT(varchar(64), qsq.query_hash, 1)", sql, StringComparison.Ordinal);
        /* ROWS, not the RANGE default: RANGE tie-groups peers and forces a spool, and the frame has to be
           per-row because the cut falls BETWEEN two statements. */
        Assert.Contains("ROWS UNBOUNDED PRECEDING", sql, StringComparison.Ordinal);
        Assert.Contains("OPTION(RECOMPILE)", sql, StringComparison.Ordinal);
        /* It fetches text and nothing else — plan XML has its own fetch. */
        Assert.DoesNotContain("query_plan", sql, StringComparison.Ordinal);
    }

    /// <summary>
    /// Every input that would make the fetch ship nothing — and leave the ids missing forever — throws
    /// instead. A permanent missing set looks exactly like a quiet database, which is why these are
    /// exceptions rather than no-ops; the plan fetch learned this the hard way from several directions.
    /// </summary>
    [Fact]
    public void TextFetchByIds_RefusesInputsThatWouldStall()
    {
        var enabled = MakeContext(fetchQueryTextSeparately: true);
        var ids = new long[] { 1 };

        /* Issuing it while the host still ships text inline would fetch and store text nobody reads. */
        Assert.Throws<InvalidOperationException>(() =>
            QueryStoreCollector.Instance.BuildTextFetchByIdsQuery("SO", MakeContext(), ids, 1024));

        /* `running_bytes - text_bytes < 0` excludes even the first candidate, so the pass ships nothing. */
        Assert.Throws<ArgumentOutOfRangeException>(() =>
            QueryStoreCollector.Instance.BuildTextFetchByIdsQuery("SO", enabled, ids, 0));

        /* Empty means "nothing missing" — the caller must skip, not build IN (). */
        Assert.Throws<ArgumentException>(() =>
            QueryStoreCollector.Instance.BuildTextFetchByIdsQuery("SO", enabled, Array.Empty<long>(), 1024));

        Assert.Throws<ArgumentNullException>(() =>
            QueryStoreCollector.Instance.BuildTextFetchByIdsQuery("SO", null!, ids, 1024));
    }

    /// <summary>
    /// #1907: EVERY <c>avg_*</c> column the payload emits must be the count-weighted mean of the slices.
    ///
    /// <para>This is the pin that makes the rule enforceable rather than remembered. The wrong forms are
    /// not compile errors and do not look wrong: a bare <c>AVG(qsrs.avg_duration)</c> reads perfectly
    /// naturally and silently weights a 25-execution sliver the same as a 100-execution flush, and
    /// <c>MAX</c> reads naturally too. Query Store exposes an average and a count but never a total, so
    /// <c>avg * count</c> is the only way to recover a slice's total. Verified live on SQL 2022: slices of
    /// (1778.42 over 100) and (2245.60 over 25) combined to 1871.856, which is
    /// (1778.42*100 + 2245.60*25) / 125 and is not the 2012.01 a plain average of the two would give.</para>
    ///
    /// <para>Discovers the columns from the emitted SQL rather than listing them, so a newly added
    /// <c>avg_</c> column is covered the moment it appears instead of when someone remembers to add it
    /// here. Runs against the 2017+ shape, which is the one where all eleven exist.</para>
    /// </summary>
    [Fact]
    public void Payload_EveryAverageColumn_IsTheCountWeightedMean()
    {
        var text = PayloadSql(MakeContext(probeResult: 16));

        /* Only the aggregating STAGING statement (#2133: the aggregate lands in #pm_qs_slice and the
           joins run from it) — the final projection references the same names as plain columns, which
           is correct there and must not be mistaken for an un-weighted aggregate. The staging SELECT
           is the marker's first occurrence; the final SELECT carries TOP on the marker line. */
        var open = text.IndexOf("SELECT /* PerformanceMonitorLite */\n", StringComparison.Ordinal);
        var close = text.IndexOf("INTO #pm_qs_slice", StringComparison.Ordinal);
        Assert.True(open > 0 && close > open, "could not locate the slice-aggregating staging statement");
        var aggregate = text[open..close];

        var averages = System.Text.RegularExpressions.Regex
            .Matches(aggregate, @"^\s*(avg_[a-z0-9_]+) = ", System.Text.RegularExpressions.RegexOptions.Multiline)
            .Select(m => m.Groups[1].Value)
            .Distinct()
            .ToList();

        /* duration, cpu_time, logical_io_reads, logical_io_writes, physical_io_reads, clr_time,
           query_max_used_memory, rowcount, num_physical_io_reads, log_bytes_used, tempdb_space_used. */
        Assert.Equal(11, averages.Count);

        foreach (var column in averages)
        {
            Assert.Contains(
                $"{column} = SUM(qsrs.{column} * qsrs.count_executions) / NULLIF(SUM(qsrs.count_executions), 0)",
                aggregate,
                StringComparison.Ordinal);
        }

        /* The shapes that would pass a reading but corrupt the numbers. */
        Assert.DoesNotContain("AVG(qsrs.avg_", aggregate, StringComparison.Ordinal);
        Assert.DoesNotContain("MAX(qsrs.avg_", aggregate, StringComparison.Ordinal);
        Assert.DoesNotContain("MIN(qsrs.avg_", aggregate, StringComparison.Ordinal);
        Assert.DoesNotContain("SUM(qsrs.avg_duration)", aggregate, StringComparison.Ordinal);
    }

    /// <summary>
    /// #1907 bind safety: <c>replica_group_id</c> joins the GROUP BY on exactly the targets where the
    /// column exists, and nowhere else.
    ///
    /// <para>It carries the same 2022+/Azure gate as the attribution column it feeds, for the same reason:
    /// naming a column that does not exist in a GROUP BY fails the whole SELECT just as naming it in a
    /// select list does, and on the Azure per-database path that would fail in EVERY database. It has to
    /// be in the key where it does exist — two replicas' rows for one interval are DIFFERENT work, and
    /// summing them would blend a secondary's executions into the primary's, which is the exact bug
    /// replica attribution was added to prevent.</para>
    /// </summary>
    [Fact]
    public void BuildPerItemQuery_ReplicaGroupIdEntersTheGroupingKey_OnlyWhereItBinds()
    {
        foreach (var probe in new object[] { 16, 17 })
        {
            var attributed = PayloadSql(MakeContext(probeResult: probe));
            Assert.Contains("qsrs.execution_type_desc,\n    qsrs.replica_group_id", attributed, StringComparison.Ordinal);
        }

        var azure = AzurePayloadSql(MakeContext(isAzureSqlDb: true, probeResult: 12));
        Assert.Contains("qsrs.execution_type_desc,\n    qsrs.replica_group_id", azure, StringComparison.Ordinal);

        /* Pre-2022 box and Managed Instance: the column must not be named anywhere, GROUP BY included. */
        foreach (var probe in new object?[] { 13, 14, 15, null })
        {
            var ungated = PayloadSql(MakeContext(probeResult: probe));
            Assert.DoesNotContain("replica_group_id", ungated, StringComparison.Ordinal);
            Assert.Contains("GROUP BY\n    qsrs.plan_id,\n    qsrs.runtime_stats_interval_id,\n    qsrs.execution_type_desc\n", ungated, StringComparison.Ordinal);
        }
    }

    /// <summary>
    /// #1907: the version-gated metric families must vanish from the AGGREGATE on a target that lacks
    /// them while still emitting their typed NULL placeholder in the OUTER projection — the 55-column
    /// reader contract cannot move, but an unbound column inside an aggregate fails just as hard as one
    /// outside it. The comma handling is the fragile part: those three fragments carry a LEADING comma
    /// and end the inner select list precisely because they can be empty.
    /// </summary>
    [Fact]
    public void BuildPerItemQuery_PreSql2017_GatedFamiliesLeaveTheAggregate_ButKeepTheirOrdinals()
    {
        var old = PayloadSql(MakeContext(probeResult: 13));

        /* Not aggregated, not referenced — the columns do not exist on SQL 2016. */
        Assert.DoesNotContain("MIN(qsrs.min_num_physical_io_reads)", old, StringComparison.Ordinal);
        Assert.DoesNotContain("MIN(qsrs.min_log_bytes_used)", old, StringComparison.Ordinal);
        Assert.DoesNotContain("MIN(qsrs.min_tempdb_space_used)", old, StringComparison.Ordinal);
        Assert.DoesNotContain("qsrs.avg_tempdb_space_used * qsrs.count_executions", old, StringComparison.Ordinal);

        /* But the ordinals stay, as typed NULLs, so the reader still sees 55 columns in one order. */
        Assert.Contains("avg_num_physical_io_reads = NULL, min_num_physical_io_reads = NULL, max_num_physical_io_reads = NULL,", old, StringComparison.Ordinal);
        Assert.Contains("avg_log_bytes_used = NULL, min_log_bytes_used = NULL, max_log_bytes_used = NULL,", old, StringComparison.Ordinal);
        Assert.Contains("avg_tempdb_space_used = NULL, min_tempdb_space_used = NULL, max_tempdb_space_used = NULL,", old, StringComparison.Ordinal);

        /* The staging list must end cleanly on the last ungated column when all three are absent —
           #2133: the aggregate lands in #pm_qs_slice, so INTO sits between the list and FROM. */
        Assert.Contains("max_rowcount = MAX(qsrs.max_rowcount)\nINTO #pm_qs_slice\nFROM sys.query_store_runtime_stats AS qsrs", old, StringComparison.Ordinal);

        /* On 2017+ they are present, aggregated, and the list ends with the last gated family instead. */
        var newer = PayloadSql(MakeContext(probeResult: 14));
        Assert.Contains("max_rowcount = MAX(qsrs.max_rowcount),\n", newer, StringComparison.Ordinal);
        Assert.Contains("max_tempdb_space_used = MAX(qsrs.max_tempdb_space_used)\nINTO #pm_qs_slice\nFROM sys.query_store_runtime_stats AS qsrs", newer, StringComparison.Ordinal);
    }

    [Fact]
    public void BuildPerItemQuery_AzureWithFailedProbe_StillNewColumns()
    {
        /* Azure SQL DB reports low PRODUCTVERSION majors historically — the edition overrides
           the version gate, exactly as the original's isNew computation did. Azure SQL DB no longer
           takes this path in production (#1836 routes it through BuildQuery per database); the gates
           are resolved in the one shared body builder, so both builders answer identically for a
           given target and this still pins that answer. */
        var plan = QueryStoreCollector.Instance.BuildPerItemQuery("SO", MakeContext(isAzureSqlDb: true, probeResult: null));

        Assert.Contains("qsrs.avg_num_physical_io_reads", plan.Text, StringComparison.Ordinal);
        Assert.Contains("plan_forcing_type = qsp.plan_forcing_type_desc,", plan.Text, StringComparison.Ordinal);

        /* plan_type flipped ON for Azure SQL DB with #1836 (was NULL here before): the probe reports
           major 12 on Azure, but the engine is evergreen and sys.query_store_plan's applies-to banner
           names Azure SQL Database — the same "Azure means newest" rule isNew has always used.
           replica_role joined it with #1872, once the catalog was proven to bind live on both the
           General Purpose and Hyperscale service tiers. */
        Assert.Contains("plan_type = qsp.plan_type_desc,", plan.Text, StringComparison.Ordinal);
        Assert.Contains("replica_role = qsr.replica_name", plan.Text, StringComparison.Ordinal);

        /* Managed Instance joined both gates with #1886, on its OWN live evidence rather than by
           pattern-matching this Azure SQL DB change — see BuildPerItemQuery_ManagedInstance_CarriesBothVersionGatedColumns below for
           what was measured and why MI needed a stricter bar than Azure did. */
        var managedInstance = QueryStoreCollector.Instance.BuildPerItemQuery("SO", new CollectorContext
        {
            ServerId = 42,
            ServerName = "test-server",
            CollectionTime = new DateTime(2026, 7, 2, 12, 0, 0, DateTimeKind.Utc),
            Deltas = s_deltas,
            Target = new CollectorTargetInfo { IsAzureManagedInstance = true },
        }).Text;

        Assert.Contains("plan_type = qsp.plan_type_desc,", managedInstance, StringComparison.Ordinal);
        Assert.Contains("replica_role = qsr.replica_name", managedInstance, StringComparison.Ordinal);
    }

    [Fact]
    public void BuildPerItemQuery_ManagedInstance_CarriesBothVersionGatedColumns()
    {
        /* #1886. MI reports PRODUCTVERSION major 12 and EngineEdition 8, so the pure version gate can
           NEVER fire on it — before this, every MI row stored the nvarchar(1) NULL placeholder for
           replica_role and NULL for plan_type on every instance, regardless of what its engine
           supported.

           MI was deliberately held back through #1844 and #1872 while Azure SQL DB was flipped, and the
           reason does not transfer: MI is not evergreen. Its feature set follows a per-instance UPDATE
           POLICY, so an instance on an older policy genuinely might not have the catalog, and the bar
           #1886 set for this simple edition gate was correspondingly stricter than Azure's — the catalog
           had to be present on the OLDEST update policy still in support.

           Measured 2026-07-31 on a GPv2 Gen5 4-vCore MI (westus3, torn down after) reporting
           ProductUpdateType = 'CU', i.e. the conservative SQL Server 2022 policy rather than
           Always-up-to-date, which is exactly that oldest-supported case:

             OBJECT_ID('sys.query_store_replicas')                                 = -660
             COL_LENGTH('sys.query_store_runtime_stats', 'replica_group_id')       = 8
             COL_LENGTH('sys.query_store_plan', 'plan_type_desc')                  = 120

           and the same two replica values re-answered from a user-database context through
           [db].sys.sp_executesql — the collector's actual MI mechanism, which is the path the issue
           noted MI has instead of Azure SQL DB's per-database connection.

           This pins the SOURCE the gate emits, not live MI state: the instance is gone, and the
           evidence lives on #1886. */
        var mi = QueryStoreCollector.Instance.BuildPerItemQuery("SO", new CollectorContext
        {
            ServerId = 42,
            ServerName = "test-server",
            CollectionTime = new DateTime(2026, 7, 2, 12, 0, 0, DateTimeKind.Utc),
            Deltas = s_deltas,
            /* No probe result, so productVersion falls to the default — the gate must ride the EDITION,
               which is the whole point on a platform whose version probe under-reports. */
            Target = new CollectorTargetInfo { IsAzureManagedInstance = true },
        }).Text;

        Assert.Contains("plan_type = qsp.plan_type_desc,", mi, StringComparison.Ordinal);
        Assert.Contains("replica_role = qsr.replica_name", mi, StringComparison.Ordinal);

        /* MUST be a LEFT JOIN, and for MI the reason is sharper than it is anywhere else: a standalone
           MI's sys.query_store_replicas is an EMPTY enumeration (zero rows), where Azure SQL DB's is a
           static 4-row roles table even with no replicas. An INNER JOIN would match nothing and silently
           delete ALL Query Store collection on every standalone MI. */
        Assert.Contains("LEFT JOIN sys.query_store_replicas AS qsr", mi, StringComparison.Ordinal);
        Assert.Contains("ON qsr.replica_group_id = qsrs.replica_group_id", mi, StringComparison.Ordinal);

        /* Neither placeholder survives. */
        Assert.DoesNotContain("plan_type = NULL,", mi, StringComparison.Ordinal);
        Assert.DoesNotContain("replica_role = CONVERT(nvarchar(1), NULL)", mi, StringComparison.Ordinal);
    }

    [Fact]
    public void BuildPerItemQuery_ManagedInstance_GatesRideTheEditionNotTheProbe()
    {
        /* The discriminating pin (#1886). MI's version probe reports major 12, so if either gate were
           still reading the probe rather than the edition, a LOW probe value would put the NULL
           placeholders back. Passing the real under-reported major explicitly proves the edition term is
           what carries both columns — which is exactly the condition that made MI never attribute. */
        var mi = QueryStoreCollector.Instance.BuildPerItemQuery("SO", new CollectorContext
        {
            ServerId = 42,
            ServerName = "test-server",
            CollectionTime = new DateTime(2026, 7, 2, 12, 0, 0, DateTimeKind.Utc),
            Deltas = s_deltas,
            EnumerationProbeResult = 12,
            Target = new CollectorTargetInfo { IsAzureManagedInstance = true },
        }).Text;

        Assert.Contains("plan_type = qsp.plan_type_desc,", mi, StringComparison.Ordinal);
        Assert.Contains("replica_role = qsr.replica_name", mi, StringComparison.Ordinal);
    }

    [Fact]
    public void BuildPerItemQuery_OnPremBelow2022_StillGetsNeitherColumn()
    {
        /* The counterfactual that keeps #1886 honest: widening the two gates must not have widened them
           for box SQL Server. A 2019 target still emits both NULL placeholders and never references the
           replicas catalog, which does not exist there. */
        var onPrem = QueryStoreCollector.Instance.BuildPerItemQuery("SO", new CollectorContext
        {
            ServerId = 42,
            ServerName = "test-server",
            CollectionTime = new DateTime(2026, 7, 2, 12, 0, 0, DateTimeKind.Utc),
            Deltas = s_deltas,
            EnumerationProbeResult = 15,
            Target = new CollectorTargetInfo(),
        }).Text;

        Assert.Contains("plan_type = NULL,", onPrem, StringComparison.Ordinal);
        Assert.Contains("replica_role = CONVERT(nvarchar(1), NULL)", onPrem, StringComparison.Ordinal);
        Assert.DoesNotContain("sys.query_store_replicas", onPrem, StringComparison.Ordinal);
    }

    [Fact]
    public void BuildPerItemQuery_PinsWatermarkCutoff_AndSpExecutesqlShape()
    {
        var watermark = new DateTime(2026, 7, 2, 11, 30, 0, DateTimeKind.Utc);
        var plan = QueryStoreCollector.Instance.BuildPerItemQuery("we]ird", MakeContext(watermark: watermark));

        /* Line-ending agnostic: pin the bracket escaping + the sp_executesql shape. */
        Assert.StartsWith("EXECUTE [we]]ird].sys.sp_executesql", plan.Text.TrimStart('\r', '\n'), StringComparison.Ordinal);

        /* The incremental cutoff is asked at INTERVAL grain since #1907, not per slice. Re-pinned
           deliberately: the old form asserted "WHERE qsrs.last_execution_time > @cutoff_time", and keeping
           that would have pinned the bug. A per-slice WHERE cannot survive slice aggregation — the flushed
           slice is STATIC, so as soon as the growing in-memory slice pushes the watermark past it the
           flushed slice stops qualifying and the SUM silently degrades to the sliver alone, which is the
           original defect with an aggregate bolted on. HAVING MAX(...) asks whether the INTERVAL saw new
           activity and then takes all of it. */
        var normalized = plan.Text.Replace("\r\n", "\n", StringComparison.Ordinal);
        Assert.Contains("HAVING\n    MAX(qsrs.last_execution_time) > @cutoff_time", normalized, StringComparison.Ordinal);
        Assert.DoesNotContain("WHERE qsrs.last_execution_time > @cutoff_time", normalized, StringComparison.Ordinal);
        /* #1565: NO SQL-side self-exclusion — the old NOT LIKE was 75% of the read's elapsed time (full
           nvarchar(max) scan per row on a column no index can serve; field A/B: 4.3x without it), and no
           predicate shape fixes a residual text scan. The exclusion is client-side in ReadItemAsync,
           where the text is already materialized (pinned below). The query still CONTAINS the marker —
           in its own leading comment. */
        Assert.DoesNotContain("NOT LIKE", plan.Text, StringComparison.Ordinal);
        Assert.Contains("OPTION(RECOMPILE);", plan.Text, StringComparison.Ordinal);
        Assert.DoesNotContain("LOOP JOIN", plan.Text, StringComparison.Ordinal);
        Assert.Contains("N'@cutoff_time datetime2(7)',", plan.Text, StringComparison.Ordinal);

        var parameter = Assert.Single(plan.Parameters);
        Assert.Equal("@cutoff_time", parameter.Name);
        Assert.Equal(watermark, parameter.Value);
        Assert.Equal(CollectorParameterType.DateTime2, parameter.Type);
    }

    [Fact]
    public void BuildPerItemQuery_PlanCapture_AlwaysEmitsNullPlaceholder_RegardlessOfCapturePlanXml()
    {
        /* #2210: the runtime-stats query no longer carries plan XML at all, in EITHER capture mode — the
           ROW_NUMBER-gated CASE and its watermark predicate are DELETED, not reworked.
           BuildPlanFetchQuery is the only thing that reads plan XML now (it fetches plans in plan_id
           order under a byte budget and is Darling-only), so CapturePlanXml gates that separate fetch
           rather than this query. Lite's off path and Darling's on path are therefore byte-identical
           here — there is no longer a Darling-only branch of this query to pin. */
        var off = QueryStoreCollector.Instance.BuildPerItemQuery("SO", MakeContext());
        var on = QueryStoreCollector.Instance.BuildPerItemQuery("SO", MakeContext(capturePlanXml: true));

        Assert.Contains("query_plan_text = CONVERT(nvarchar(1), NULL),", off.Text, StringComparison.Ordinal);
        Assert.Contains("query_plan_text = CONVERT(nvarchar(1), NULL),", on.Text, StringComparison.Ordinal);
        Assert.DoesNotContain("qsp.query_plan,", off.Text, StringComparison.Ordinal);
        Assert.DoesNotContain("qsp.query_plan,", on.Text, StringComparison.Ordinal);
        Assert.DoesNotContain("ROW_NUMBER()", on.Text, StringComparison.Ordinal);
        Assert.True(string.Equals(off.Text, on.Text, StringComparison.Ordinal), "CapturePlanXml must no longer change this query's text");
    }

    [Fact]
    public void BuildPerItemQuery_RowCapWithTiesAndOrderByAsc_BoundBothCaptureModes()
    {
        /* #1556 gave the read a per-database server-side backstop; #1960 made it hole-free: rows ship
           OLDEST-first (ORDER BY last_execution_time ASC), so the derived watermark — MAX over stored
           rows — sits exactly at the shipped boundary when a cycle is bounded, and WITH TIES stops the
           TOP from splitting a group of rows tied at that boundary (the strict `> @cutoff_time` would
           strand the unshipped half forever). Present in BOTH capture modes: the ORDER BY is
           load-bearing for the client byte budget's early-stop too (its boundary-group completion
           relies on tied rows being adjacent). */
        foreach (var plan in new[]
        {
            QueryStoreCollector.Instance.BuildPerItemQuery("SO", MakeContext()),
            QueryStoreCollector.Instance.BuildPerItemQuery("SO", MakeContext(capturePlanXml: true)),
        })
        {
            Assert.Contains($"TOP ({QueryStoreCollector.MaxRowsPerDatabase}) WITH TIES", plan.Text, StringComparison.Ordinal);
            Assert.Contains("ORDER BY qsrs.last_execution_time ASC", plan.Text, StringComparison.Ordinal);
            /* The row-bounding ORDER BY sits before the existing query hint, which the OPTION pin still
               checks. RECOMPILE only — the old LOOP JOIN hint is the #2133 pathology (per-probe TVF
               re-materialization) and must never return. */
            Assert.Contains("OPTION(RECOMPILE);", plan.Text, StringComparison.Ordinal);
            Assert.DoesNotContain("LOOP JOIN", plan.Text, StringComparison.Ordinal);
        }

        Assert.Equal(50_000, QueryStoreCollector.MaxRowsPerDatabase);
    }

    [Fact]
    public void PerDatabaseBounds_ExposeWatermarkColumnAndBudgets()
    {
        /* #1556: query_store now flushes per database, so its watermark is per-database (keyed on
           database_name — the deadlocks/BPR precedent), and it advertises both the row-cap warn threshold
           and the client byte budget so the host can surface the WARNING and the definition can
           enforce the early-stop. 64 MB since #1960: a bounded cycle resumes from the shipped boundary
           instead of dropping the remainder, so the smaller budget costs catch-up latency, never data. */
        Assert.Equal("database_name", QueryStoreCollector.Instance.PerDatabaseWatermarkColumn);
        Assert.Equal(QueryStoreCollector.MaxRowsPerDatabase, QueryStoreCollector.Instance.PerItemRowCountWarnThreshold);
        Assert.Equal(QueryStoreCollector.MaxTextBytesPerDatabase, QueryStoreCollector.Instance.PerItemTextByteBudget);
        Assert.Equal(64 * 1024 * 1024, QueryStoreCollector.MaxTextBytesPerDatabase);
    }

    [Fact]
    public async Task ReadItemAsync_ResetsPerItemSignals_AndNormalRowsDoNotTripTheBudget()
    {
        /* #1556/#1960: ReadItemAsync resets ALL the per-item signals at entry (pre-set here to prove
           it), and a normal, small row never trips the 64MB budget — the truncation signal stays false,
           so the host emits no spurious WARNING and every row is read. The shipped-boundary and
           bytes-shipped signals are written on EVERY read (they only get logged on a bounded cycle),
           so even this un-bounded read reports what it shipped. */
        var context = MakeContext();
        context.PerItemTextBudgetExceeded = true;
        context.PerItemTextBytesShipped = long.MaxValue;
        context.PerItemShippedBoundary = new DateTime(1999, 1, 1, 0, 0, 0, DateTimeKind.Utc);

        var row = new object[55];
        row[0] = 101L;
        row[1] = 202L;
        row[2] = "Regular";
        row[3] = new DateTimeOffset(2026, 7, 2, 10, 0, 0, TimeSpan.Zero);
        row[4] = new DateTimeOffset(2026, 7, 2, 11, 0, 0, TimeSpan.Zero);
        row[5] = "dbo.Proc";
        row[6] = "SELECT 1";
        row[7] = "0xQH";
        row[8] = 33L;
        for (int i = 9; i <= 43; i++) row[i] = (long)i;
        row[44] = DBNull.Value;
        row[45] = "MANUAL";
        row[46] = true;
        row[47] = 5L;
        row[48] = "NONE";
        row[49] = (short)160;
        row[50] = DBNull.Value;
        row[51] = "0xPH";
        row[52] = DBNull.Value;
        row[53] = 9001L;
        row[54] = new DateTime(2026, 7, 2, 10, 0, 0);

        using var reader = new FakeCollectorDataReader(row);
        var rows = new System.Collections.Generic.List<QueryStoreCollector.Row>();
        await QueryStoreCollector.Instance.ReadItemAsync("SO", reader, rows, context, CancellationToken.None);

        Assert.False(context.PerItemTextBudgetExceeded);
        Assert.Single(rows);

        /* The boundary signal is the kept row's last_execution_time (11:00Z, normalized to UTC
           DateTime) and the byte count is the row's text at chars × 2 — "SELECT 1" (8) with no plan. */
        Assert.Equal(new DateTime(2026, 7, 2, 11, 0, 0), context.PerItemShippedBoundary);
        Assert.Equal(16L, context.PerItemTextBytesShipped);
    }

    [Fact]
    public async Task ReadItemAsync_BudgetCut_CompletesTheBoundaryTieGroup_ThenStops()
    {
        /* The #1960 never-a-hole invariant, from the client side. Rows arrive OLDEST-first; the budget
           trips mid-read; the read must still keep every remaining row TIED at the trip row's
           last_execution_time (the derived watermark lands on that value, and next cycle's strict
           `> @cutoff_time` would strand an unshipped tie forever) and stop at the first row past the
           tie. Two ~34MB texts drive the real 64MB threshold — no test seam, the production constant. */
        var bigText = new string('x', 17 * 1024 * 1024);
        var t1 = new DateTimeOffset(2026, 7, 2, 10, 0, 0, TimeSpan.Zero);
        var t2 = new DateTimeOffset(2026, 7, 2, 11, 0, 0, TimeSpan.Zero);
        var t3 = new DateTimeOffset(2026, 7, 2, 12, 0, 0, TimeSpan.Zero);

        var context = MakeContext();
        using var reader = new FakeCollectorDataReader(
            MakeReaderRow(1, bigText, t1),
            MakeReaderRow(2, bigText, t2),   /* cumulative 68MB ≥ 64MB — the budget trips HERE */
            MakeReaderRow(3, "tied at the boundary", t2),
            MakeReaderRow(4, "past the boundary — next cycle's row", t3));
        var rows = new System.Collections.Generic.List<QueryStoreCollector.Row>();
        await QueryStoreCollector.Instance.ReadItemAsync("SO", reader, rows, context, CancellationToken.None);

        Assert.Equal(new[] { 1L, 2L, 3L }, System.Linq.Enumerable.Select(rows, r => r.QueryId));
        Assert.True(context.PerItemTextBudgetExceeded);
        Assert.Equal(t2.UtcDateTime, context.PerItemShippedBoundary);

        /* Bytes account every KEPT row, including the tie shipped past the trip. */
        var expectedBytes = (bigText.Length * 2L * 2) + ("tied at the boundary".Length * 2L);
        Assert.Equal(expectedBytes, context.PerItemTextBytesShipped);

        /* The invariant itself: nothing kept sits past the boundary, so MAX(last_execution_time) over
           what was stored IS the resume point. */
        Assert.All(rows, r => Assert.True(r.LastExecutionTime <= t2.UtcDateTime));
    }

    [Fact]
    public async Task ReadItemAsync_BudgetCutOnNullBoundaryRow_StopsAtTheNextRow()
    {
        /* Defensive: a NULL last_execution_time can never reach the client (the strict cutoff filters
           it server-side), but if one ever tripped the budget there is no tie to complete — the read
           must stop at the very next row rather than treating "null == null" as a tie and running on. */
        var hugeText = new string('x', 34 * 1024 * 1024);   /* 68MB — trips on the FIRST row */
        var context = MakeContext();
        using var reader = new FakeCollectorDataReader(
            MakeReaderRow(1, hugeText, lastExecRaw: DBNull.Value),
            MakeReaderRow(2, "never shipped this cycle"));
        var rows = new System.Collections.Generic.List<QueryStoreCollector.Row>();
        await QueryStoreCollector.Instance.ReadItemAsync("SO", reader, rows, context, CancellationToken.None);

        Assert.Equal(new[] { 1L }, System.Linq.Enumerable.Select(rows, r => r.QueryId));
        Assert.True(context.PerItemTextBudgetExceeded);
        Assert.Null(context.PerItemShippedBoundary);
    }

    [Fact]
    public async Task ReadItemAsync_DropsSelfMarkerRows_ClientSide()
    {
        /* #1565: the self-exclusion moved OUT of the SQL (the NOT LIKE was 75% of the read's elapsed
           time) and into ReadItemAsync, where the text is already materialized. A row whose query text
           carries the marker never enters the batch — not stored, not counted against the byte budget. */
        var context = MakeContext();

        object[] MakeRow(long queryId, string sqlText)
        {
            var row = new object[55];
            row[0] = queryId;
            row[1] = 202L;
            row[2] = "Regular";
            row[3] = new DateTimeOffset(2026, 7, 2, 10, 0, 0, TimeSpan.Zero);
            row[4] = new DateTimeOffset(2026, 7, 2, 11, 0, 0, TimeSpan.Zero);
            row[5] = "dbo.Proc";
            row[6] = sqlText;
            row[7] = "0xQH";
            row[8] = 33L;
            for (int i = 9; i <= 43; i++) row[i] = (long)i;
            row[44] = DBNull.Value;
            row[45] = "MANUAL";
            row[46] = true;
            row[47] = 5L;
            row[48] = "NONE";
            row[49] = (short)160;
            row[50] = DBNull.Value;
            row[51] = "0xPH";
            row[52] = DBNull.Value;
            row[53] = 9001L;
            row[54] = new DateTime(2026, 7, 2, 10, 0, 0);
            return row;
        }

        using var reader = new FakeCollectorDataReader(
            MakeRow(1, "SELECT 1 FROM UserTable"),
            MakeRow(2, "SELECT /* " + QueryStoreCollector.SelfQueryMarker + " */ TOP (50000) query_id = qsq.query_id"),
            MakeRow(3, "UPDATE Another SET x = 1"));
        var rows = new System.Collections.Generic.List<QueryStoreCollector.Row>();
        await QueryStoreCollector.Instance.ReadItemAsync("SO", reader, rows, context, CancellationToken.None);

        Assert.Equal(2, rows.Count);
        Assert.DoesNotContain(rows, r => r.QueryText!.Contains(QueryStoreCollector.SelfQueryMarker, StringComparison.Ordinal));
        Assert.Equal(new[] { 1L, 3L }, System.Linq.Enumerable.Select(rows, r => r.QueryId));
    }

    [Fact]
    public async Task ReadAsync_Azure_TakesDatabaseNameFromCurrentDatabaseName_AndDropsSelfRows()
    {
        /* #1836: on the per-database path the payload carries no database_name column (the on-prem
           path takes it from the enumerated item), so it comes from the database the host connected
           to — CollectorContext.CurrentDatabaseName, the same authoritative source the XE collectors
           use on this path. One read loop serves both shapes, so the client-side self-exclusion
           applies here too: on Azure our own payload runs INSIDE the database whose Query Store it is
           reading, so without it the collector would collect itself. */
        var context = MakeContext(isAzureSqlDb: true);
        context.CurrentDatabaseName = "ProdDb";

        using var reader = new FakeCollectorDataReader(
            MakeReaderRow(1, "SELECT 1 FROM UserTable"),
            MakeReaderRow(2, "SELECT /* " + QueryStoreCollector.SelfQueryMarker + " */ TOP (50000) query_id = qsq.query_id"),
            MakeReaderRow(3, "UPDATE Another SET x = 1"));

        var rows = await QueryStoreCollector.Instance.ReadAsync(reader, context, CancellationToken.None);

        Assert.Equal(new[] { 1L, 3L }, rows.Select(r => r.QueryId));
        Assert.All(rows, r => Assert.Equal("ProdDb", r.DatabaseName));
        Assert.False(context.PerItemTextBudgetExceeded);
    }

    [Fact]
    public async Task ReadAsync_WithoutCurrentDatabaseName_Throws_RatherThanWritingBlankDatabaseRows()
    {
        /* An empty database_name is not a survivable fallback for this collector: it is the
           per-database watermark key, so those rows could never advance a watermark and would
           re-collect every cycle, under a blank database in every grid. A wiring mistake becomes one
           loud, classified failure instead — the host's per-database catch surfaces it. */
        var context = MakeContext(isAzureSqlDb: true);

        using var reader = new FakeCollectorDataReader(MakeReaderRow(1, "SELECT 1"));

        await Assert.ThrowsAsync<InvalidOperationException>(
            async () => await QueryStoreCollector.Instance.ReadAsync(reader, context, CancellationToken.None));
    }

    /// <summary>
    /// One reader row shaped to the 55-ordinal payload contract both paths select.
    /// <paramref name="lastExecRaw"/> overrides last_execution_time (ordinal 4) for the #1960
    /// boundary tests — pass a <see cref="DateTimeOffset"/> or <see cref="DBNull.Value"/>;
    /// null keeps the standard 11:00Z.
    /// </summary>
    private static object[] MakeReaderRow(long queryId, string sqlText, object? lastExecRaw = null)
    {
        var row = new object[55];
        row[0] = queryId;
        row[1] = 202L;
        row[2] = "Regular";
        row[3] = new DateTimeOffset(2026, 7, 2, 10, 0, 0, TimeSpan.Zero);
        row[4] = lastExecRaw ?? new DateTimeOffset(2026, 7, 2, 11, 0, 0, TimeSpan.Zero);
        row[5] = "dbo.Proc";
        row[6] = sqlText;
        row[7] = "0xQH";
        row[8] = 33L;
        for (int i = 9; i <= 43; i++) row[i] = (long)i;
        row[44] = DBNull.Value;
        row[45] = "MANUAL";
        row[46] = true;
        row[47] = 5L;
        row[48] = "NONE";
        row[49] = (short)160;
        row[50] = DBNull.Value;
        row[51] = "0xPH";
        row[52] = DBNull.Value;
        row[53] = 9001L;                    /* runtime_stats_interval_id */
        row[54] = new DateTime(2026, 7, 2, 10, 0, 0);  /* interval_start_time_utc (already datetime2) */
        return row;
    }

    [Fact]
    public void BuildBackfillPerItemQuery_WindowedDescWithTies_MirrorsTheLiveInvariant()
    {
        /* #2022 phase 2: the SAME payload body, window and direction flipped. The two-sided strict
           window ((floor, ceiling) exclusive) appears in BOTH the interval pre-filter and the
           HAVING; the ship order is DESC (newest history first); TOP ... WITH TIES and the byte
           budget still complete boundary tie groups, which is what makes the strict `<` ceiling
           resumable — the #1960 invariant, mirror-imaged. The live @cutoff_time must NOT appear:
           a backfill slice that accidentally kept the live cutoff would silently re-collect the
           live window instead of the backlog. */
        var floor = new DateTime(2026, 7, 1, 0, 0, 0, DateTimeKind.Utc);
        var ceiling = new DateTime(2026, 7, 3, 0, 0, 0, DateTimeKind.Utc);
        var plan = QueryStoreCollector.Instance.BuildBackfillPerItemQuery("StackOverflow", MakeContext(), floor, ceiling);

        Assert.Contains("EXECUTE [StackOverflow].sys.sp_executesql", plan.Text, StringComparison.Ordinal);
        Assert.Contains("N'@floor_time datetime2(7), @ceiling_time datetime2(7)'", plan.Text, StringComparison.Ordinal);
        /* #2133: the pre-filter's two-sided window asks the INTERVAL CATALOG which intervals OVERLAP
           (floor, ceiling) — end after the floor AND start before the ceiling — a superset the exact
           HAVING below then narrows, exactly like the live path's one-sided form. */
        Assert.Contains("i.end_time > @floor_time", plan.Text, StringComparison.Ordinal);
        Assert.Contains("i.start_time < @ceiling_time", plan.Text, StringComparison.Ordinal);
        Assert.Contains("MAX(qsrs.last_execution_time) > @floor_time", plan.Text, StringComparison.Ordinal);
        Assert.Contains("MAX(qsrs.last_execution_time) < @ceiling_time", plan.Text, StringComparison.Ordinal);
        Assert.Contains("ORDER BY qsrs.last_execution_time DESC", plan.Text, StringComparison.Ordinal);
        Assert.Contains($"TOP ({QueryStoreCollector.MaxRowsPerDatabase}) WITH TIES", plan.Text, StringComparison.Ordinal);
        Assert.DoesNotContain("@cutoff_time", plan.Text, StringComparison.Ordinal);

        Assert.Collection(
            plan.Parameters,
            p => { Assert.Equal("@floor_time", p.Name); Assert.Equal(floor, p.Value); Assert.Equal(CollectorParameterType.DateTime2, p.Type); },
            p => { Assert.Equal("@ceiling_time", p.Name); Assert.Equal(ceiling, p.Value); Assert.Equal(CollectorParameterType.DateTime2, p.Type); });
    }

    [Fact]
    public void BuildBackfillQuery_Azure_SameWindowShape_NoSpExecutesql_ThrowsOffAzure()
    {
        /* #2058, the Azure arm: the same backfill body runs VERBATIM on the per-database connection
           (Azure rejects [db].sys.sp_executesql nesting, #1836), behind the same eligibility gate as
           the live Azure query, with the window as command parameters. Off-Azure it throws — the
           enumerated per-item path drives the slice there, and silently collecting the connection's
           own catalog would be worse than a loud wrong-path error. */
        var floor = new DateTime(2026, 7, 1, 0, 0, 0, DateTimeKind.Utc);
        var ceiling = new DateTime(2026, 7, 3, 0, 0, 0, DateTimeKind.Utc);
        var plan = QueryStoreCollector.Instance.BuildBackfillQuery(MakeContext(isAzureSqlDb: true), floor, ceiling);

        Assert.DoesNotContain("sp_executesql", plan.Text, StringComparison.Ordinal);
        Assert.Contains("i.end_time > @floor_time", plan.Text, StringComparison.Ordinal);
        Assert.Contains("i.start_time < @ceiling_time", plan.Text, StringComparison.Ordinal);
        Assert.Contains("ORDER BY qsrs.last_execution_time DESC", plan.Text, StringComparison.Ordinal);
        Assert.DoesNotContain("@cutoff_time", plan.Text, StringComparison.Ordinal);
        /* The same eligibility gate the live Azure query leads with. */
        Assert.StartsWith(QueryStoreCollector.Instance.BuildQuery(MakeContext(isAzureSqlDb: true)).Text.Split('\n')[0],
            plan.Text, StringComparison.Ordinal);
        Assert.Collection(
            plan.Parameters,
            p => { Assert.Equal("@floor_time", p.Name); Assert.Equal(floor, p.Value); },
            p => { Assert.Equal("@ceiling_time", p.Name); Assert.Equal(ceiling, p.Value); });

        Assert.Throws<NotSupportedException>(
            () => QueryStoreCollector.Instance.BuildBackfillQuery(MakeContext(), floor, ceiling));
    }

    [Fact]
    public void BuildBackfillPerItemQuery_LiveBodyStaysUntouched()
    {
        /* The backfill flag must be a pure additive variant: the LIVE per-item query keeps its
           one-sided cutoff and ASC order byte-for-byte, or phase 1's watermark-exact resume breaks
           in the same PR that builds on it. */
        var live = QueryStoreCollector.Instance.BuildPerItemQuery("StackOverflow", MakeContext());
        Assert.Contains("i.end_time > @cutoff_time", live.Text, StringComparison.Ordinal);
        Assert.Contains("MAX(qsrs.last_execution_time) > @cutoff_time", live.Text, StringComparison.Ordinal);
        Assert.Contains("ORDER BY qsrs.last_execution_time ASC", live.Text, StringComparison.Ordinal);
        Assert.DoesNotContain("@floor_time", live.Text, StringComparison.Ordinal);
        Assert.DoesNotContain("@ceiling_time", live.Text, StringComparison.Ordinal);
    }

    [Fact]
    public void BuildPerItemQuery_NoWatermark_FallsBack60Minutes()
    {
        var collectionTime = new DateTime(2026, 7, 2, 12, 0, 0, DateTimeKind.Utc);
        var plan = QueryStoreCollector.Instance.BuildPerItemQuery("SO", MakeContext(collectionTime: collectionTime));

        Assert.Equal(collectionTime.AddMinutes(-60), Assert.Single(plan.Parameters).Value);
    }

    [Theory]
    [InlineData(13, false)]   /* SQL Server 2016 — the AppliesTo floor */
    [InlineData(16, false)]   /* SQL Server 2022 */
    [InlineData(12, true)]    /* Azure SQL DB, which under-reports PRODUCTVERSION */
    public void Payload_SelectsIntervalIdentity_OnEveryVersionAndTarget(int productVersion, bool isAzureSqlDb)
    {
        /* #1841 tier 2. Query Store rows are cumulative per-interval snapshots, so the interval is the
           unit every aggregate has to collapse to — and until this shipped the schema carried no interval
           key at all, only the first_execution_time proxy, and no interval CLOCK, which is why the slicer
           bucketed an interval into the hour it was last COLLECTED.

           Deliberately NOT version-gated, unlike plan_type_desc / the log-bytes family / replica_role
           beside it: sys.query_store_runtime_stats.runtime_stats_interval_id and the
           sys.query_store_runtime_stats_interval catalog view are original Query Store surface, verified
           present on SQL Server 2016 SP3 (13.0.6300.2) — the same floor AppliesTo enforces. A gate here
           would be dead weight AND would silently deny the identity to the oldest supported servers. */
        var context = MakeContext(isAzureSqlDb: isAzureSqlDb, probeResult: productVersion);
        var body = isAzureSqlDb
            ? QueryStoreCollector.Instance.BuildQuery(context).Text
            : QueryStoreCollector.Instance.BuildEnumerationQuery(context) is not null
                ? QueryStoreCollector.Instance.BuildPerItemQuery("SO", context).Text
                : throw new InvalidOperationException("on-prem must enumerate");

        /* The on-prem form is quote-DOUBLED for [db].sys.sp_executesql nesting, so the timezone literal
           reads ''UTC'' there and 'UTC' on Azure — the one place this pair could have broken the escaping. */
        var quote = isAzureSqlDb ? "'" : "''";
        Assert.Contains("runtime_stats_interval_id = qsrs.runtime_stats_interval_id,", body, StringComparison.Ordinal);
        Assert.Contains($"interval_start_time_utc = CONVERT(datetime2, qsrsi.start_time AT TIME ZONE {quote}UTC{quote})", body, StringComparison.Ordinal);

        /* LEFT JOIN, never INNER: an interval row that failed to resolve must cost this collector ONE
           column, not every runtime-stats row for that database. Same lesson as the replica join. */
        Assert.Contains("LEFT JOIN sys.query_store_runtime_stats_interval AS qsrsi", body, StringComparison.Ordinal);
        Assert.DoesNotContain("JOIN sys.query_store_runtime_stats_interval AS qsrsi\n  ON qsrsi", body.Replace("LEFT JOIN sys.query_store_runtime_stats_interval", "X", StringComparison.Ordinal), StringComparison.Ordinal);
    }

    [Fact]
    public void PayloadColumns_MatchSchemaOrder_56Columns()
    {
        var names = QueryStoreCollector.Instance.PayloadColumns.Select(c => c.Name).ToArray();

        Assert.Equal(56, names.Length);
        Assert.Equal("database_name", names[0]);
        Assert.Equal("query_id", names[1]);
        Assert.Equal("execution_count", names[9]);
        Assert.Equal("avg_num_physical_io_reads", names[36]);
        Assert.Equal("plan_type", names[45]);
        Assert.Equal("is_forced_plan", names[47]);
        Assert.Equal("compatibility_level", names[50]);
        Assert.Equal("query_plan_hash", names[52]);

        /* The appended tail is pinned in ORDER deliberately: both hosts' bulk writers are positional, and
           an upgraded store receives these columns from an ALTER TABLE ADD COLUMN, which can only append.
           Moving any of them earlier would desync a fresh store (DDL generated from this list) from an
           upgraded one. replica_role landed first (#1546), then #1841 tier 2's interval-identity pair;
           anything added later must go AFTER these. See the CollectorColumn comment in QueryStoreCollector. */
        Assert.Equal("replica_role", names[53]);
        Assert.Equal("runtime_stats_interval_id", names[54]);
        Assert.Equal("interval_start_time_utc", names[55]);
    }

    [Fact]
    public async Task ReadItemAsync_WritePayload_Pins56ColumnOrder_AndTypeCoercions()
    {
        var context = MakeContext();
        var firstExec = new DateTimeOffset(2026, 7, 2, 10, 0, 0, TimeSpan.FromHours(-4));
        var lastExec = new DateTimeOffset(2026, 7, 2, 11, 0, 0, TimeSpan.FromHours(-4));

        var row = new object[55];
        row[0] = 101L;                      /* query_id */
        row[1] = 202L;                      /* plan_id */
        row[2] = "Regular";                 /* execution_type_desc */
        row[3] = firstExec;                 /* first_execution_time (datetimeoffset) */
        row[4] = lastExec;                  /* last_execution_time (datetimeoffset) */
        row[5] = "dbo.Proc";                /* module_name */
        row[6] = "SELECT 1";                /* query_sql_text */
        row[7] = "0xQH";                    /* query_hash */
        row[8] = 33L;                       /* count_executions */
        row[9] = 123.7d;                    /* avg_duration: float catalog value -> (long) */
        row[10] = 456.9f;                   /* min_duration: single -> (long) */
        row[11] = 789m;                     /* max_duration: decimal -> (long) */
        row[12] = 42;                       /* avg_cpu_time: int passthrough */
        row[13] = (short)7;                 /* min_cpu_time: short passthrough */
        row[14] = DBNull.Value;             /* max_cpu_time: NULL -> 0 */
        for (int i = 15; i <= 43; i++) row[i] = (long)i;
        row[44] = DBNull.Value;             /* plan_type (pre-2022) */
        row[45] = "MANUAL";                 /* plan_forcing_type */
        row[46] = true;                     /* is_forced_plan */
        row[47] = 5L;                       /* force_failure_count */
        row[48] = "NONE";                   /* last_force_failure_reason */
        row[49] = (short)160;               /* compatibility_level: smallint -> int */
        row[50] = DBNull.Value;             /* query_plan_text (always NULL literal) */
        row[51] = "0xPH";                   /* query_plan_hash */
        row[52] = "Secondary";              /* replica_role (2022+ attributed the row to a secondary) */
        row[53] = 9001L;                    /* runtime_stats_interval_id (#1841 tier 2) */
        row[54] = new DateTime(2026, 7, 2, 10, 0, 0);  /* interval_start_time_utc: datetime2, NOT datetimeoffset —
                                                          the SELECT does the AT TIME ZONE conversion, so unlike
                                                          first/last_execution_time this needs no client shift */

        using var reader = new FakeCollectorDataReader(row);
        var rows = new System.Collections.Generic.List<QueryStoreCollector.Row>();
        await QueryStoreCollector.Instance.ReadItemAsync("SO", reader, rows, context, CancellationToken.None);

        var writer = new RecordingCollectorRowWriter();
        QueryStoreCollector.Instance.WritePayload(Assert.Single(rows), writer, context);

        Assert.Equal(56, writer.Values.Count);
        Assert.Equal("SO", writer.Values[0]);                       /* enumerated item leads the payload */
        Assert.Equal(101L, writer.Values[1]);
        Assert.Equal(firstExec.UtcDateTime, writer.Values[4]);      /* datetimeoffset -> UTC DateTime */
        Assert.Equal(123L, writer.Values[10]);                      /* double 123.7 truncated */
        Assert.Equal(456L, writer.Values[11]);                      /* float 456.9 truncated */
        Assert.Equal(789L, writer.Values[12]);
        Assert.Equal(42L, writer.Values[13]);
        Assert.Equal(7L, writer.Values[14]);
        Assert.Equal(0L, writer.Values[15]);                        /* NULL stat -> 0 */
        Assert.Null(writer.Values[45]);                             /* plan_type NULL */
        Assert.Equal(true, writer.Values[47]);
        Assert.Equal(160, writer.Values[50]);                       /* smallint compat -> int */
        Assert.Null(writer.Values[51]);
        Assert.Equal("0xPH", writer.Values[52]);
        Assert.Equal("Secondary", writer.Values[53]);               /* replica_role, after query_plan_hash */
        Assert.Equal(9001L, writer.Values[54]);                     /* runtime_stats_interval_id (#1841 tier 2) */
        Assert.Equal(new DateTime(2026, 7, 2, 10, 0, 0), writer.Values[55]); /* interval_start_time_utc, no shift applied */
        Assert.Empty(s_deltas.Calls);                               /* incremental snapshot — no deltas */
    }

    /* ---------------- #2312: the open-interval skip cycles ---------------- */

    /// <summary>
    /// The closed-only form (#2312): most cycles exclude the OPEN interval — its cumulative snapshot is
    /// the whole re-read bill on a big primary (40–110 s per run measured) and every snapshot but the
    /// latest is discarded by the read side's <c>rn = 1</c>. Closed intervals are immutable, so shipping
    /// only them is final on first collection; the standing HAVING readmits a newly closed interval
    /// whose content moved past our last open-snapshot, because counters only move with executions.
    /// </summary>
    [Fact]
    public void BuildPerItemQuery_ClosedIntervalsOnly_ExcludesTheOpenInterval()
    {
        var context = MakeContext(probeResult: 16);
        context.IncludeOpenInterval = false;
        var text = PayloadSql(context);

        Assert.Contains(
            "WHERE i.end_time > @cutoff_time\n    AND   i.end_time <= SYSUTCDATETIME()",
            text, StringComparison.Ordinal);

        /* Server-evaluated exclusion — the single-parameter sp_executesql contract is untouched. */
        Assert.Single(QueryStoreCollector.Instance.BuildPerItemQuery("SO", context).Parameters);

        /* The row-level filter is byte-identical: the skip narrows the interval-id PRUNE only, never
           the shipped semantics of the rows that do qualify. */
        Assert.Contains("HAVING\n    MAX(qsrs.last_execution_time) > @cutoff_time", text, StringComparison.Ordinal);
    }

    /// <summary>Default = today's exact form: no exclusion anywhere, so every untouched caller is byte-identical.</summary>
    [Fact]
    public void BuildPerItemQuery_DefaultIncludesTheOpenInterval_TodaysExactForm()
    {
        var text = PayloadSql(MakeContext(probeResult: 16));

        Assert.Contains("WHERE i.end_time > @cutoff_time\n", text, StringComparison.Ordinal);
        Assert.DoesNotContain("SYSUTCDATETIME", text, StringComparison.Ordinal);
    }

    /// <summary>
    /// The remainder pin, same discipline as the text-flag one: normalize the ONE legal difference out
    /// of the closed-only form and everything else must be byte-identical to the open form — the pin
    /// that catches a future edit landing in one arm only.
    /// </summary>
    [Fact]
    public void BuildPerItemQuery_ClosedOnly_ChangesNothingButTheIntervalPrune()
    {
        var closedContext = MakeContext(probeResult: 16);
        closedContext.IncludeOpenInterval = false;

        var open = PayloadSql(MakeContext(probeResult: 16));
        var closed = PayloadSql(closedContext);

        var normalized = closed.Replace(
            "\n    AND   i.end_time <= SYSUTCDATETIME()", "", StringComparison.Ordinal);
        Assert.NotEqual(open, closed);
        Assert.Equal(open, normalized);
    }

    /// <summary>The backfill window pre-dates the open interval by construction; the flag must not touch it.</summary>
    [Fact]
    public void BuildBackfillPerItemQuery_IgnoresTheOpenIntervalFlag()
    {
        var floor = new DateTime(2026, 6, 1, 0, 0, 0, DateTimeKind.Utc);
        var ceiling = new DateTime(2026, 6, 2, 0, 0, 0, DateTimeKind.Utc);

        var flagged = MakeContext(probeResult: 16);
        flagged.IncludeOpenInterval = false;

        Assert.Equal(
            Lf(QueryStoreCollector.Instance.BuildBackfillPerItemQuery("SO", MakeContext(probeResult: 16), floor, ceiling).Text),
            Lf(QueryStoreCollector.Instance.BuildBackfillPerItemQuery("SO", flagged, floor, ceiling).Text));
    }
}
