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
/// Pins the per-table bloat collector (#2542): that the headline number is stored as an ESTIMATE and never
/// as a measurement, that the trust signals which decide whether it may be published all travel with it, and
/// that the never-analyzed sentinel survives the reader instead of being floored into a claim.
/// </summary>
public class PgTableBloatStatsCollectorDefinitionTests
{
    private static readonly RecordingCollectorDeltaCalculator s_deltas = new();

    private static CollectorContext MakeContext(
        int major = 17, ICollectorDeltaCalculator? deltas = null, string? currentDatabase = "appdb")
        => new()
        {
            ServerId = 42,
            ServerName = "pg-target",
            CollectionTime = new DateTime(2026, 8, 20, 12, 0, 0, DateTimeKind.Utc),
            Deltas = deltas ?? s_deltas,
            Target = new CollectorTargetInfo
            {
                Engine = CollectorTargetEngine.PostgreSql,
                PostgresMajorVersion = major,
            },
            ExcludedDatabases = Array.Empty<string>(),
            CurrentDatabaseName = currentDatabase,
        };

    /// <summary>
    /// The table name cannot be changed later without a migration, and pg_catalog is searched before
    /// search_path — so a store table named after a catalog object breaks CREATE INDEX with 42809 and makes
    /// unqualified reads resolve to the MONITORING store's own copy.
    /// </summary>
    [Fact]
    public void Identity_Pinned_AndTheTableDoesNotShadowACatalogObject()
    {
        Assert.Equal("pg_table_bloat_stats", PgTableBloatStatsCollector.Instance.Name);
        Assert.Equal("pg_table_bloat_stats", PgTableBloatStatsCollector.Instance.TargetTable);
        Assert.Equal(CollectorTargetEngine.PostgreSql, PgTableBloatStatsCollector.Instance.TargetEngine);

        Assert.NotEqual("pg_stats", PgTableBloatStatsCollector.Instance.TargetTable);
        Assert.NotEqual("pg_stat_user_tables", PgTableBloatStatsCollector.Instance.TargetTable);
        Assert.NotEqual("pg_class", PgTableBloatStatsCollector.Instance.TargetTable);
    }

    /// <summary>
    /// WRITERS ONLY, on every major and both Aurora and stock — the gate reads <c>IsInRecovery</c> and
    /// nothing else.
    ///
    /// <para>The bloat ARITHMETIC would still be right on a replica, because it reads replicated catalog
    /// rows. What would be wrong is <c>mods_since_analyze</c> and <c>last_analyzed</c>, which read as
    /// "statistics are perfectly fresh" on a replica that has never analyzed anything — silently zeroing the
    /// one signal that detects the estimator's 81-percentage-point failure mode.</para>
    /// </summary>
    [Theory]
    [InlineData(13, false, false, true)]
    [InlineData(15, false, false, true)]
    [InlineData(16, true, false, true)]
    [InlineData(17, true, false, true)]
    [InlineData(18, false, false, true)]
    [InlineData(13, false, true, false)]
    [InlineData(16, true, true, false)]
    [InlineData(17, true, true, false)]
    [InlineData(18, false, true, false)]
    public void AppliesToWritersOnly_OnEveryMajorAndBothAuroraAndStock(
        int major, bool isAurora, bool inRecovery, bool expected)
    {
        Assert.Equal(expected, PgTableBloatStatsCollector.Instance.AppliesTo(new CollectorTargetInfo
        {
            Engine = CollectorTargetEngine.PostgreSql,
            PostgresMajorVersion = major,
            IsAurora = isAurora,
            IsInRecovery = inRecovery,
        }));
    }

    /// <summary>
    /// The composed gate still refuses a SQL Server target. The collector's own AppliesTo only asks about
    /// recovery, so the ENGINE half is the only thing keeping this PostgreSQL query text off a SQL Server
    /// connection.
    /// </summary>
    [Fact]
    public void TheEngineHalfOfTheDispatchGateStillRefusesASqlServerTarget()
    {
        Assert.False(CollectorCatalog.AppliesTo(PgTableBloatStatsCollector.Instance, new CollectorTargetInfo()));
        Assert.True(CollectorCatalog.AppliesTo(
            PgTableBloatStatsCollector.Instance,
            new CollectorTargetInfo { Engine = CollectorTargetEngine.PostgreSql }));
    }

    /// <summary>
    /// <c>pg_stats</c> and <c>pg_stat_user_tables</c> are scoped to the connected database, so this is
    /// necessarily a fan-out — sharing pg_autovacuum_stats's hourly cadence so the CAUSE and the DAMAGE line
    /// up sample for sample.
    /// </summary>
    [Fact]
    public void RunsPerDatabase()
    {
        Assert.True(PgTableBloatStatsCollector.Instance.RunsPerDatabase(MakeContext().Target));
    }

    /// <summary>
    /// NO version branch at all, and that is a MEASURED result rather than an omission: the whole query was
    /// executed against live PostgreSQL 13, 14, 15, 16, 17 and 18 and returned the same shape and the same
    /// numbers for the same fixture. Every catalog column it reads was confirmed present on all six by
    /// listing the live catalogs, so there is nothing to gate on and a gate would be a source of drift.
    /// </summary>
    [Fact]
    public void TheQueryIsOneConstantWithNoVersionBranch()
    {
        var majors = new[] { 13, 14, 15, 16, 17, 18 }
            .Select(m => PgTableBloatStatsCollector.Instance.BuildQuery(MakeContext(m)).Text)
            .Distinct(StringComparer.Ordinal)
            .ToArray();

        Assert.Single(majors);

        /* Same REFERENCE across two majors, not merely equal text: that is what proves the query is one
           stored constant rather than a string rebuilt per call, so there is no interpolation site where a
           version branch could later be introduced unnoticed. */
        Assert.Same(
            PgTableBloatStatsCollector.Instance.BuildQuery(MakeContext(13)).Text,
            PgTableBloatStatsCollector.Instance.BuildQuery(MakeContext(18)).Text);
    }

    /// <summary>
    /// <c>estimate_unavailable</c> is LOAD-BEARING, not advisory: TRUE means the estimate has no basis, and
    /// the read suppresses the number rather than captioning it.
    ///
    /// <para>All three conditions that set it must be present. The middle one is the important one in
    /// production: <c>pg_stats</c> is filtered by <c>has_column_privilege(...'select')</c> and
    /// <c>pg_monitor</c> does NOT confer SELECT on user tables, so a correctly-provisioned monitoring login
    /// sees ZERO pg_stats rows — and MEASURED against exactly such a role on a live target, the estimator
    /// did not fail: it returned a confident 88.59% for a table whose true bloat is 0.50%.</para>
    /// </summary>
    [Theory]
    [InlineData("bool_or(att.atttypid = 'pg_catalog.name'::regtype)")]
    [InlineData("count(sts.attname) <> count(att.attname)")]
    [InlineData("MAX(tbl.reltuples) < 0")]
    public void CarriesEveryConditionThatSetsEstimateUnavailable(string condition)
    {
        Assert.Contains(condition, PgTableBloatStatsCollector.Instance.BuildQuery(MakeContext()).Text, StringComparison.Ordinal);
    }

    /// <summary>
    /// The trust signals travel with the estimate. <c>mods_since_analyze</c> is the one that catches the
    /// estimator's worst failure: MEASURED on two byte-identical 8,998-page tables with an identical true
    /// bloat of 10.93%, the one whose column-width statistics predated a widening UPDATE estimated 92.64%
    /// and the freshly-analyzed one estimated 11.01% — 81 percentage points apart, with nothing in the
    /// arithmetic to show it. Width statistics can only go stale THROUGH modifications, which is what makes
    /// this a sound proxy rather than a coincidence.
    /// </summary>
    [Theory]
    [InlineData("mods_since_analyze")]
    [InlineData("last_analyzed")]
    [InlineData("estimate_unavailable")]
    [InlineData("alignment_bytes")]
    [InlineData("pgstattuple_available")]
    [InlineData("estimated_tuple_bytes")]
    [InlineData("estimated_heap_pages")]
    [InlineData("fillfactor")]
    public void CarriesEveryTrustSignalTheEstimateNeeds(string column)
    {
        Assert.Contains(column, PgTableBloatStatsCollector.Instance.BuildQuery(MakeContext()).Text, StringComparison.Ordinal);
    }

    /// <summary>
    /// MAXALIGN cannot be read from a GUC and is detected from <c>version()</c>. The alternation carries
    /// <c>aarch64</c> and <c>arm64</c> explicitly because Graviton RDS and Aurora instances are aarch64 —
    /// their version() string does also contain "64-bit", but relying on that one token to hold across every
    /// vendor's build is a bet with no upside. The value actually used is STORED, so a platform where the
    /// detection is wrong shows up in the data rather than skewing every estimate on it silently.
    /// </summary>
    [Fact]
    public void DetectsMaxAlignIncludingOnArm()
    {
        var sql = PgTableBloatStatsCollector.Instance.BuildQuery(MakeContext()).Text;

        Assert.Contains("version() ~ '64-bit|x86_64|ppc64|ia64|amd64|aarch64|arm64'", sql, StringComparison.Ordinal);
        Assert.Contains("AS alignment_bytes", sql, StringComparison.Ordinal);
    }

    /// <summary>
    /// The MEASURED columns, which are true whatever the statistics or the grants look like — and the reason
    /// a suppressed estimate degrades the answer rather than removing it.
    /// </summary>
    [Theory]
    [InlineData("pg_relation_size(tbl.oid)")]
    [InlineData("pg_relation_size(tbl.reltoastrelid)")]
    [InlineData("pg_indexes_size(tbl.oid)")]
    [InlineData("MAX(st.n_dead_tup)")]
    public void SelectsTheMeasuredColumnsThatSurviveAPermissionsGap(string fragment)
    {
        Assert.Contains(fragment, PgTableBloatStatsCollector.Instance.BuildQuery(MakeContext()).Text, StringComparison.Ordinal);
    }

    /// <summary>
    /// The size floor is the fan-out cost control. It is stated in BYTES from <c>pg_relation_size</c> rather
    /// than in <c>relpages</c>, because relpages is only refreshed by VACUUM or ANALYZE and would let a table
    /// that has grown since its last maintenance fall through the filter it most needs to pass.
    /// </summary>
    [Fact]
    public void FiltersOnTheHeapSizeFloor_MeasuredNotFromTheCatalogPageCount()
    {
        Assert.Contains(
            "WHERE s.heap_bytes >= 1048576",
            PgTableBloatStatsCollector.Instance.BuildQuery(MakeContext()).Text,
            StringComparison.Ordinal);
    }

    /// <summary>
    /// <c>timestamptz::text</c> renders in the SESSION's TimeZone and the store contract is naive UTC, so the
    /// conversion is explicit. GREATEST ignoring NULLs is wanted here rather than a hazard: whichever of the
    /// manual and automatic analyze actually happened is the later non-NULL one, and a table that has only
    /// ever been autoanalyzed must not report "never analyzed".
    /// </summary>
    [Fact]
    public void ConvertsTheAnalyzeTimestampToUtcExplicitly()
    {
        var sql = PgTableBloatStatsCollector.Instance.BuildQuery(MakeContext()).Text;

        Assert.Contains(
            "GREATEST(MAX(st.last_analyze), MAX(st.last_autoanalyze)) AT TIME ZONE 'UTC'",
            sql,
            StringComparison.Ordinal);

        Assert.DoesNotContain("last_analyze::timestamp", sql, StringComparison.Ordinal);
        Assert.DoesNotContain("last_autoanalyze::timestamp", sql, StringComparison.Ordinal);
    }

    /// <summary>
    /// Every catalog reference is schema-qualified. pg_catalog is searched implicitly and FIRST, so an
    /// unqualified reference is a hostage to whatever a future search_path or a user object named after a
    /// catalog view does to it.
    /// </summary>
    [Theory]
    [InlineData("pg_catalog.pg_attribute")]
    [InlineData("pg_catalog.pg_class")]
    [InlineData("pg_catalog.pg_namespace")]
    [InlineData("pg_catalog.pg_stat_user_tables")]
    [InlineData("pg_catalog.pg_stats")]
    [InlineData("pg_catalog.pg_extension")]
    public void SchemaQualifiesEveryCatalogReference(string reference)
    {
        Assert.Contains(reference, PgTableBloatStatsCollector.Instance.BuildQuery(MakeContext()).Text, StringComparison.Ordinal);
    }

    /// <summary>
    /// The estimate is named <c>_estimate</c> in the STORE, not only in the read's prose, so the qualifier
    /// cannot be lost between the store and a screen. A bloat number that is quietly wrong will be used to
    /// justify a VACUUM FULL on a production table.
    /// </summary>
    [Fact]
    public void TheEstimateIsNamedAsAnEstimateInThePayload()
    {
        var names = PgTableBloatStatsCollector.Instance.PayloadColumns.Select(c => c.Name).ToArray();

        Assert.Contains("bloat_bytes_estimate", names);
        Assert.Contains("bloat_pct_estimate", names);

        /* And no unqualified name that would read as a measurement. */
        Assert.DoesNotContain("bloat_bytes", names);
        Assert.DoesNotContain("bloat_pct", names);
    }

    [Fact]
    public void PayloadColumns_OrderAndKeyTypes_Pinned()
    {
        var columns = PgTableBloatStatsCollector.Instance.PayloadColumns;

        Assert.Equal(19, columns.Count);
        Assert.Equal(
            new[]
            {
                "database_name", "schema_name", "table_name",
                "heap_bytes", "heap_pages", "toast_bytes", "index_bytes",
                "live_tuples", "dead_tuples", "mods_since_analyze", "last_analyzed",
                "estimated_tuple_bytes", "estimated_heap_pages", "fillfactor",
                "bloat_bytes_estimate", "bloat_pct_estimate",
                "estimate_unavailable", "alignment_bytes", "pgstattuple_available",
            },
            columns.Select(c => c.Name).ToArray());

        Assert.Equal(CollectorColumnType.Varchar, columns[0].Type);      // database_name
        Assert.Equal(CollectorColumnType.BigInt, columns[3].Type);       // heap_bytes
        Assert.Equal(CollectorColumnType.Timestamp, columns[10].Type);   // last_analyzed
        Assert.Equal(CollectorColumnType.Double, columns[11].Type);      // estimated_tuple_bytes
        Assert.Equal(CollectorColumnType.Integer, columns[13].Type);     // fillfactor
        Assert.Equal(CollectorColumnType.BigInt, columns[14].Type);      // bloat_bytes_estimate
        Assert.Equal(CollectorColumnType.Boolean, columns[16].Type);     // estimate_unavailable
        Assert.Equal(CollectorColumnType.Integer, columns[17].Type);     // alignment_bytes
        Assert.Equal(CollectorColumnType.Boolean, columns[18].Type);     // pgstattuple_available

        /* The percentage is numeric(5,2), which is what makes "94.28" storable and "94.283" not. */
        var pct = columns[15];
        Assert.Equal(CollectorColumnType.Decimal, pct.Type);
        Assert.Equal(5, pct.Precision);
        Assert.Equal(2, pct.Scale);
    }

    /// <summary>
    /// Every field mapped to its own ordinal, with deliberately distinct values so a transposed pair fails
    /// rather than passing on two equal numbers.
    /// </summary>
    [Fact]
    public async Task ReadsAFullyPopulatedRow_WithEveryFieldOnItsOwnOrdinal()
    {
        var reader = new FakeCollectorDataReader(
            new object[]
            {
                "public", "churny",
                56_516_608L, 6_899L,                        // heap_bytes, heap_pages
                24_000_000L, 6_758_400L,                    // toast_bytes, index_bytes
                100_000L, 500L, 1_200L,                     // live_tuples, dead_tuples, mods_since_analyze
                new DateTime(2026, 8, 19, 3, 15, 0, DateTimeKind.Unspecified),
                436.0d, 1_715L, 70,                         // estimated_tuple_bytes, estimated_heap_pages, fillfactor
                42_467_328L, 75.14m,                        // bloat_bytes_estimate, bloat_pct_estimate
                false, 8, true,                             // estimate_unavailable, alignment_bytes, pgstattuple_available
            });

        var rows = await PgTableBloatStatsCollector.Instance.ReadAsync(reader, MakeContext(), CancellationToken.None);

        var row = Assert.Single(rows);
        Assert.Equal("public", row.SchemaName);
        Assert.Equal("churny", row.TableName);
        Assert.Equal(56_516_608L, row.HeapBytes);
        Assert.Equal(6_899L, row.HeapPages);
        Assert.Equal(24_000_000L, row.ToastBytes);
        Assert.Equal(6_758_400L, row.IndexBytes);
        Assert.Equal(100_000L, row.LiveTuples);
        Assert.Equal(500L, row.DeadTuples);
        Assert.Equal(1_200L, row.ModsSinceAnalyze);
        Assert.Equal(new DateTime(2026, 8, 19, 3, 15, 0), row.LastAnalyzed);
        Assert.Equal(436.0d, row.EstimatedTupleBytes);
        Assert.Equal(1_715L, row.EstimatedHeapPages);
        Assert.Equal(70, row.FillFactor);
        Assert.Equal(42_467_328L, row.BloatBytesEstimate);
        Assert.Equal(75.14m, row.BloatPctEstimate);
        Assert.False(row.EstimateUnavailable);
        Assert.Equal(8, row.AlignmentBytes);
        Assert.True(row.PgstattupleAvailable);
    }

    /// <summary>
    /// The never-analyzed sentinel. PostgreSQL 14 and above report <c>reltuples = -1</c> for a table that has
    /// never been analyzed — a deliberate "unknown" distinct from "empty" — and the reader must preserve that
    /// rather than folding it to 0, which would CLAIM the table holds no rows. It is exactly what
    /// <c>estimate_unavailable</c> keys on, and the state in which the estimator reported 92.68% against a
    /// true 48.86%.
    /// </summary>
    [Fact]
    public async Task PreservesTheNeverAnalyzedSentinelRatherThanClaimingTheTableIsEmpty()
    {
        var reader = new FakeCollectorDataReader(
            new object[]
            {
                "public", "unanalyzed",
                22_863_872L, 2_791L,
                0L, 0L,
                DBNull.Value,                               // live_tuples -> -1, NOT 0
                0L, 0L,
                DBNull.Value,                               // last_analyzed -> null: never analyzed at all
                28.0d,
                DBNull.Value,                               // estimated_heap_pages -> -1: could not be computed
                100,
                21_176_320L, 92.62m,
                true, 8, false,
            });

        var rows = await PgTableBloatStatsCollector.Instance.ReadAsync(reader, MakeContext(), CancellationToken.None);

        var row = Assert.Single(rows);

        /* -1, not 0. 0 would be the claim "this table is empty", which is a different and false statement. */
        Assert.Equal(-1L, row.LiveTuples);

        /* -1 again, and for the same reason: 0 would claim the table should occupy no pages. */
        Assert.Equal(-1L, row.EstimatedHeapPages);

        /* NULL means never analyzed by either route, which is a STRONGER statement than a stale timestamp
           and must not be rendered as "unknown". */
        Assert.Null(row.LastAnalyzed);

        Assert.True(row.EstimateUnavailable);
        Assert.False(row.PgstattupleAvailable);
    }

    /// <summary>
    /// A row from the pg_monitor-only permissions state: the estimate is a large confident number and the
    /// flag beside it is the only thing that says it has no basis. The reader must carry the flag through
    /// untouched — a defaulted-false flag here would publish the number.
    /// </summary>
    [Fact]
    public async Task CarriesTheUnavailableFlagThroughUnchanged()
    {
        var reader = new FakeCollectorDataReader(
            new object[]
            {
                "public", "gadget",
                49_233_920L, 6_010L,
                0L, 26_853_376L,
                200_000L, 0L, 0L,
                new DateTime(2026, 8, 19, 3, 15, 0, DateTimeKind.Unspecified),
                28.0d, 686L, 100,
                43_614_208L, 88.59m,
                true, 8, true,
            });

        var rows = await PgTableBloatStatsCollector.Instance.ReadAsync(reader, MakeContext(), CancellationToken.None);

        var row = Assert.Single(rows);
        Assert.True(row.EstimateUnavailable);

        /* The number is still STORED - suppression is the read's job, not the collector's. Storing it keeps
           the row diagnosable: a reader can see what the arithmetic produced and why it was rejected. */
        Assert.Equal(88.59m, row.BloatPctEstimate);
        Assert.Equal(43_614_208L, row.BloatBytesEstimate);
    }

    /// <summary>
    /// Absent counters fall back to 0 and absent booleans to false, but <c>fillfactor</c> falls back to 100
    /// — the PostgreSQL default. A 0 there would make the estimate divide by nothing.
    /// </summary>
    [Fact]
    public async Task FillFactorFallsBackToThePostgresDefault()
    {
        var reader = new FakeCollectorDataReader(
            new object[]
            {
                "public", "narrow",
                3_629_056L, 443L,
                0L, 2_260_992L,
                100_000L, 0L, 0L,
                DBNull.Value,
                36.0d, 441L,
                DBNull.Value,                               // fillfactor -> 100
                16_384L, 0.45m,
                DBNull.Value, DBNull.Value, DBNull.Value,
            });

        var rows = await PgTableBloatStatsCollector.Instance.ReadAsync(reader, MakeContext(), CancellationToken.None);

        var row = Assert.Single(rows);
        Assert.Equal(100, row.FillFactor);
        Assert.False(row.EstimateUnavailable);
        Assert.Equal(0, row.AlignmentBytes);
        Assert.False(row.PgstattupleAvailable);
    }

    [Fact]
    public async Task ReturnsNoRowsWhenNoTableClearsTheFloor()
    {
        var rows = await PgTableBloatStatsCollector.Instance.ReadAsync(
            new FakeCollectorDataReader(), MakeContext(), CancellationToken.None);

        Assert.Empty(rows);
    }

    /// <summary>
    /// No stored deltas. Every column here is a LEVEL — how big the table is now, how much of it the
    /// estimate thinks is waste now. The interesting reading over time is the trend across stored samples,
    /// which the read computes from the raw levels; differencing at collection time would throw away the
    /// absolute number, which is the one somebody acts on.
    /// </summary>
    [Fact]
    public void TakesNoDeltas()
    {
        var deltas = new RecordingCollectorDeltaCalculator();

        PgTableBloatStatsCollector.Instance.WritePayload(
            SampleRow(),
            new RecordingCollectorRowWriter(),
            MakeContext(deltas: deltas));

        Assert.Empty(deltas.Calls);
    }

    /// <summary>
    /// Every payload column is written, in order. WritePayload is positional, so a column added without a
    /// matching Value() shifts everything after it and stores data that is silently wrong — which on this
    /// collector would mean an estimate landing in the column a reader trusts unconditionally.
    /// </summary>
    [Fact]
    public void WritesEveryPayloadColumnInOrder()
    {
        var writer = new RecordingCollectorRowWriter();

        PgTableBloatStatsCollector.Instance.WritePayload(SampleRow(), writer, MakeContext());

        Assert.Equal(PgTableBloatStatsCollector.Instance.PayloadColumns.Count, writer.Values.Count);

        /* The connection's database, NOT a value parsed from the result set. */
        Assert.Equal("appdb", writer.Values[0]);

        Assert.Equal("public", writer.Values[1]);
        Assert.Equal("churny", writer.Values[2]);
        Assert.Equal(56_516_608L, writer.Values[3]);    // heap_bytes
        Assert.Equal(6_899L, writer.Values[4]);         // heap_pages
        Assert.Equal(24_000_000L, writer.Values[5]);    // toast_bytes
        Assert.Equal(100_000L, writer.Values[7]);       // live_tuples
        Assert.Equal(1_200L, writer.Values[9]);         // mods_since_analyze
        Assert.Equal(new DateTime(2026, 8, 19, 3, 15, 0), writer.Values[10]);
        Assert.Equal(436.0d, writer.Values[11]);        // estimated_tuple_bytes
        Assert.Equal(70, writer.Values[13]);            // fillfactor
        Assert.Equal(42_467_328L, writer.Values[14]);   // bloat_bytes_estimate
        Assert.Equal(75.14m, writer.Values[15]);        // bloat_pct_estimate
        Assert.Equal(false, writer.Values[16]);         // estimate_unavailable
        Assert.Equal(8, writer.Values[17]);             // alignment_bytes
        Assert.Equal(true, writer.Values[18]);          // pgstattuple_available
    }

    private static PgTableBloatStatsCollector.Row SampleRow() => new(
        SchemaName: "public",
        TableName: "churny",
        HeapBytes: 56_516_608,
        HeapPages: 6_899,
        ToastBytes: 24_000_000,
        IndexBytes: 6_758_400,
        LiveTuples: 100_000,
        DeadTuples: 500,
        ModsSinceAnalyze: 1_200,
        LastAnalyzed: new DateTime(2026, 8, 19, 3, 15, 0),
        EstimatedTupleBytes: 436.0,
        EstimatedHeapPages: 1_715,
        FillFactor: 70,
        BloatBytesEstimate: 42_467_328,
        BloatPctEstimate: 75.14m,
        EstimateUnavailable: false,
        AlignmentBytes: 8,
        PgstattupleAvailable: true);

    /// <summary>
    /// HOURLY, matching pg_autovacuum_stats deliberately rather than by copying: this collector measures the
    /// DAMAGE whose CAUSE that one measures, and correlating the two requires a common grain. It is also the
    /// second per-database fan-out, so sharing the cadence means one connection-budget decision instead of
    /// two.
    ///
    /// <para>90 days because the useful reading of bloat is a TREND — is this table's waste growing, holding,
    /// or being reclaimed — and a spot percentage on its own is what gets someone to run VACUUM FULL on a
    /// Tuesday.</para>
    /// </summary>
    [Fact]
    public void RegisteredInBothTheCatalogAndTheSchedule()
    {
        Assert.Contains(CollectorCatalog.All, d => d.Name == "pg_table_bloat_stats");

        var schedule = CollectorScheduleDefaults.All["pg_table_bloat_stats"];

        Assert.Equal(60, schedule.FrequencyMinutes);
        Assert.Equal(90, schedule.RetentionDays);
        Assert.True(schedule.DefaultEnabled);

        /* The cause and the damage share a grain, which is the whole argument for the cadence. Asserted
           against the sibling rather than as a second literal, so the two cannot drift apart silently. */
        Assert.Equal(
            CollectorScheduleDefaults.All["pg_autovacuum_stats"].FrequencyMinutes,
            schedule.FrequencyMinutes);
    }

    /// <summary>
    /// The capability vocabulary has a noun phrase for this collector, so a SQL Server target asked
    /// get_pg_table_bloat is told what it does not collect rather than getting the generic fallback.
    /// </summary>
    [Fact]
    public void TheCapabilityMessageNamesWhatIsNotCollected()
    {
        var message = CollectorEngineCapability.NotCollectedMessage(
            "sql-01", CollectorEngineCapability.UnknownEngineEdition, MonitoredEngineKind.SqlServer, "pg_table_bloat_stats");

        Assert.NotNull(message);
        Assert.Contains("bloat estimate", message, StringComparison.Ordinal);
        Assert.Contains("dead-tuple", message, StringComparison.Ordinal);
        Assert.DoesNotContain("the data this read is served from", message, StringComparison.Ordinal);
    }
}
