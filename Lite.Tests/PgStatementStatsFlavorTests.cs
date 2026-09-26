/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using System.Linq;
using System.Text.RegularExpressions;
using Lite.Tests.Helpers;
using PerformanceMonitor.Collectors;
using Xunit;

namespace Lite.Tests;

/// <summary>
/// #2625: <c>pg_statement_stats</c> reads TWO sources — Aurora's extended function, or the vanilla
/// <c>pg_stat_statements</c> view on any other PostgreSQL — and which one is a <c>BuildQuery</c> decision,
/// never an applicability one.
///
/// <para>
/// The gate it replaced cost more than a few columns. <c>AppliesTo</c> means permanent incapability, and the
/// capability machinery composes a deliberately final sentence from it: "does not collect per-query-shape
/// execution statistics, and never will". Every operator of a non-Aurora PostgreSQL target got that sentence
/// for the single question a database monitor exists to answer — while <c>pg_kernel_stats</c> collected OS
/// CPU and <c>pg_predicate_stats</c> collected selectivity, both keyed by the very queryids nothing was
/// identifying. It survived because no self-hosted PostgreSQL target existed to notice.
/// </para>
///
/// <para>
/// The ordinals are the load-bearing detail. Both queries select the same 30 columns in the same order (the
/// 28th, appended by #3653 A5, is the statements epoch <c>stats_reset</c>, read and not stored; the 29th and
/// 30th, appended by #4428, are <c>stats_since</c> and <c>target_now</c>, also read and not stored) — the
/// vanilla one fills Aurora's six with typed NULL literals — so <c>ReadAsync</c>, <c>PayloadColumns</c> and
/// <c>WritePayload</c> stay single implementations. A shorter vanilla SELECT would have meant a second reader
/// whose ordinals could drift from this one, which is exactly the failure the per-major column naming in this
/// collector already exists to prevent.
/// </para>
/// </summary>
public class PgStatementStatsFlavorTests
{
    private static readonly RecordingCollectorDeltaCalculator s_deltas = new();

    private static CollectorContext MakeContext(bool isAurora, int major = 17)
        => new()
        {
            ServerId = 42,
            ServerName = "test-server",
            CollectionTime = new DateTime(2026, 8, 26, 12, 0, 0, DateTimeKind.Utc),
            Deltas = s_deltas,
            Target = new CollectorTargetInfo
            {
                Engine = CollectorTargetEngine.PostgreSql,
                IsAurora = isAurora,
                PostgresMajorVersion = major,
                PostgresVersionNum = major * 10000,
            },
        };

    private static string Sql(bool isAurora, int major = 17)
        => PgStatementStatsCollector.Instance.BuildQuery(MakeContext(isAurora, major)).Text;

    /// <summary>
    /// The whole point. This assertion is the one that would have caught the gap, and it could not have been
    /// written before a non-Aurora target existed to ask the question of.
    /// </summary>
    [Theory]
    [InlineData(true)]
    [InlineData(false)]
    public void ItAppliesToEveryPostgresTarget_AuroraOrNot(bool isAurora)
        => Assert.True(PgStatementStatsCollector.Instance.AppliesTo(
            new CollectorTargetInfo { Engine = CollectorTargetEngine.PostgreSql, IsAurora = isAurora }));

    [Fact]
    public void AuroraReadsTheExtendedFunction()
    {
        var sql = Sql(isAurora: true);

        Assert.Contains("FROM aurora_stat_statements(false)", sql, StringComparison.Ordinal);
        /* The VIEW is what Aurora must not read - its rows come from the extended function. The extension's
           one-row pg_stat_statements_info (#3653 A5, the statements epoch) sits beside the view on Aurora
           too and is read on both flavors; the word boundary is what tells the two apart, since `_` is a
           word character and the info view's name continues past it. Since #3818 the epoch read also names
           the extension itself - as a pg_extension.extname LITERAL, to find the schema it was created in -
           so the pin is on a FROM of the view, which is the read Aurora must not make. */
        Assert.DoesNotMatch(new Regex(@"FROM\s+(?:public\.)?pg_stat_statements\b"), sql);
        Assert.Contains("pg_stat_statements_info", sql, StringComparison.Ordinal);
        Assert.Contains("WHERE e.extname = 'pg_stat_statements'", sql, StringComparison.Ordinal);
    }

    [Fact]
    public void EveryOtherPostgresReadsTheVanillaView()
    {
        var sql = Sql(isAurora: false);

        Assert.Contains("FROM public.pg_stat_statements", sql, StringComparison.Ordinal);
        Assert.DoesNotContain("aurora_stat_statements", sql, StringComparison.Ordinal);
    }

    /// <summary>
    /// One reader, one column list, one write order — so the two queries must agree on shape, not just on
    /// source. Counted at the top level so a comma inside a cast or a comment cannot inflate it.
    /// </summary>
    [Fact]
    public void BothFlavorsSelectTheSameColumnsInTheSameOrder()
    {
        var aurora = SelectAliases(Sql(isAurora: true));
        var vanilla = SelectAliases(Sql(isAurora: false));

        Assert.Equal(aurora, vanilla);
        /* The SELECT list is the payload minus the four columns computed on the client (the three deltas
           and, since V128 (#3540), the interval they accrued over) PLUS the three columns read and not
           stored: the statements epoch stats_reset (#3653 A5), and — since #4428 — stats_since and
           target_now, last, so every stored ordinal is where it was. */
        Assert.Equal(PgStatementStatsCollector.Instance.PayloadColumns.Count - 4 + 3, aurora.Count);
        Assert.Equal("statements_stats_reset", aurora[^3]);
        Assert.Equal("stats_since", aurora[^2]);
        Assert.Equal("target_now", aurora[^1]);
    }

    /// <summary>
    /// #3818. The statements epoch (#3653 A5) is gated on the RELATION'S EXISTENCE at query time, on every
    /// major and both flavors - never on <c>postgresMajorVersion</c>, which was the first cut's guard and the
    /// defect: <c>pg_stat_statements_info</c> is created by the EXTENSION'S 1.9 update script, and a 14+
    /// engine upgraded in place keeps the extension at 1.8 (RDS and Aurora do not run <c>ALTER EXTENSION
    /// ... UPDATE</c> for you), so the version guard let the whole collector fail 42P01 on this one column
    /// on 23 of 50 clusters in one fleet while the base view was readable the entire time.
    ///
    /// <para>The shape is the measured one (the collector's comment records the measurement): a static
    /// <c>FROM pg_stat_statements_info</c> raises at parse analysis whatever WHERE or CASE surrounds it, so
    /// the relation is named only inside <c>query_to_xml</c>'s SQL text - resolved at EXECUTION - behind a
    /// <c>to_regclass</c> test on the schema-qualified name, with the schema read from <c>pg_extension</c>
    /// rather than assumed <c>public.</c> (the second failure mode) or left to the search_path (which read
    /// NULL on the rig with the extension in a schema off the path).</para>
    /// </summary>
    [Theory]
    [InlineData(true, 13)]
    [InlineData(false, 13)]
    [InlineData(true, 14)]
    [InlineData(false, 14)]
    [InlineData(true, 17)]
    [InlineData(false, 17)]
    public void TheStatementsEpochColumnIsGatedOnTheRelationsExistence_OnEveryMajor(bool isAurora, int major)
    {
        var sql = Sql(isAurora, major);

        /* The one shape, on every major: the class-level constant, verbatim, ending the select list. */
        Assert.Matches(
            new Regex($@"{Regex.Escape(PgStatementStatsCollector.StatementsEpochSql)}\s+AS statements_stats_reset\b"),
            sql);

        /* No version guard left on this column: the typed-NULL arm the first cut emitted below 14 is gone,
           because the server major says nothing about the extension's catalog version. */
        Assert.DoesNotMatch(new Regex(@"NULL::timestamp with time zone\s+AS statements_stats_reset\b"), sql);

        /* Never a static FROM of the info view - that is the read that raised 42P01 at parse analysis. */
        Assert.DoesNotMatch(new Regex(@"FROM\s+(?:public\.)?pg_stat_statements_info\b"), sql);
        Assert.DoesNotContain("public.pg_stat_statements_info", sql, StringComparison.Ordinal);
    }

    /// <summary>
    /// The pieces of the existence gate, each of which the rig measurement depends on: the existence test
    /// is <c>to_regclass</c> on the name qualified with the extension's OWN schema from <c>pg_extension</c>;
    /// the read is <c>query_to_xml</c> of SQL text (resolved at execution, so it cannot raise at parse
    /// analysis) and only in the ELSE arm; both CASE arms are typed <c>timestamp with time zone</c> so the
    /// column's type is the same on every path and <c>ReadAsync</c>'s <c>GetDateTime(27)</c> is unchanged.
    /// </summary>
    [Fact]
    public void TheExistenceGate_ReadsTheExtensionsOwnSchema_AndResolvesTheRelationAtExecution()
    {
        var epoch = PgStatementStatsCollector.StatementsEpochSql;

        Assert.Contains("to_regclass(format('%I.pg_stat_statements_info', n.nspname)) IS NULL THEN NULL::timestamp with time zone", epoch, StringComparison.Ordinal);
        Assert.Contains("query_to_xml(format('SELECT stats_reset FROM %I.pg_stat_statements_info', n.nspname), true, false, '')", epoch, StringComparison.Ordinal);
        Assert.Contains("FROM '<stats_reset>([^<]+)</stats_reset>')::timestamp with time zone", epoch, StringComparison.Ordinal);
        Assert.Contains("FROM pg_catalog.pg_extension AS e", epoch, StringComparison.Ordinal);
        Assert.Contains("ON n.oid = e.extnamespace", epoch, StringComparison.Ordinal);
        Assert.Contains("WHERE e.extname = 'pg_stat_statements'", epoch, StringComparison.Ordinal);

        /* The ELSE arm is the only place the relation is read, and it is inside a string literal. */
        var caseStart = epoch.IndexOf("ELSE", StringComparison.Ordinal);
        Assert.True(caseStart > 0);
        Assert.Contains("query_to_xml", epoch[caseStart..], StringComparison.Ordinal);
        Assert.DoesNotContain("query_to_xml", epoch[..caseStart], StringComparison.Ordinal);

        /* Uncorrelated - a scalar subquery with its own FROM and nothing from the outer row - so the planner
           makes it an InitPlan evaluated once per statement (measured: `InitPlan 1 (returns $0)`). It opens
           and closes as one parenthesised SELECT. */
        Assert.StartsWith("(SELECT CASE", epoch, StringComparison.Ordinal);
        Assert.EndsWith("WHERE e.extname = 'pg_stat_statements')", epoch, StringComparison.Ordinal);
    }

    /// <summary>
    /// The companion declaration the fault mapping reads (#3818): <c>pg_stat_statements_info</c> is an object
    /// the extension gains at 1.9, declared beside the base dependency so a 42P01 naming it is recorded as
    /// "extension present below 1.9" rather than "extension not installed". Lowercase and bare, as the
    /// mapping compares it. The other declaring collectors have no companions - a guard that a declaration
    /// does not appear where nothing reads a versioned object.
    /// </summary>
    [Fact]
    public void TheInfoViewIsDeclaredAsACompanionTheExtensionGainsAt19()
    {
        var dependency = Assert.Single(PgStatementStatsCollector.Instance.RequiredPgExtensions);

        Assert.Equal("pg_stat_statements", dependency.ExtensionName);
        var companion = Assert.Single(dependency.Companions);
        Assert.Equal("pg_stat_statements_info", companion.ObjectName);
        Assert.Equal("1.9", companion.SinceExtensionVersion);
        Assert.Equal(companion.ObjectName, companion.ObjectName.ToLowerInvariant(), StringComparer.Ordinal);
        Assert.DoesNotContain('.', companion.ObjectName);

        /* And the query text really reads it - a declared companion nothing reads would make the mapping
           describe a fault the collector cannot produce. */
        Assert.Contains(companion.ObjectName, Sql(isAurora: false), StringComparison.Ordinal);
        Assert.Contains(companion.ObjectName, Sql(isAurora: true), StringComparison.Ordinal);

        foreach (var other in CollectorCatalog.All.Where(c => c.Name != "pg_statement_stats"))
        {
            foreach (var declared in other.RequiredPgExtensions)
            {
                Assert.Empty(declared.Companions);
            }
        }
    }

    /// <summary>
    /// #3830: the collector also declares what it reads on Aurora INSTEAD of the extension's base object,
    /// and that declaration is the whole difference between two remedies for an absent extension. Where
    /// <c>pg_extension</c> has no row, a vanilla target's collector is dark until someone runs
    /// <c>CREATE EXTENSION</c>; this one is not, because on Aurora it reads a function that needs no
    /// extension - so the create is optional there and buys the companion alone.
    ///
    /// <para>The declared name has to be a surface the Aurora query really reads, or the sentence sends an
    /// operator to a function the collector never calls. Asserted against the generated Aurora SQL, and
    /// against the vanilla SQL NOT containing it, which is the property that makes it an alternative rather
    /// than a second source.</para>
    /// </summary>
    [Fact]
    public void TheAuroraNativeAlternativeIsDeclared_AndIsWhatTheAuroraQueryActuallyReads()
    {
        var dependency = Assert.Single(PgStatementStatsCollector.Instance.RequiredPgExtensions);

        var alternative = dependency.AuroraNativeAlternative;
        Assert.False(string.IsNullOrWhiteSpace(alternative));

        /* Spelled as it would be CALLED, and the Aurora query calls it. The trailing "()" is how the
           sentence reads it back to an operator, so the comparison is on the name it wraps. */
        Assert.EndsWith("()", alternative, StringComparison.Ordinal);
        var called = alternative![..^2];

        Assert.Contains(called, Sql(isAurora: true), StringComparison.Ordinal);
        Assert.DoesNotContain(called, Sql(isAurora: false), StringComparison.Ordinal);

        /* Nothing else declares one: a collector that reads its extension on BOTH flavors and declared this
           anyway would have a required install described as optional. */
        foreach (var other in CollectorCatalog.All.Where(c => c.Name != "pg_statement_stats"))
        {
            foreach (var declared in other.RequiredPgExtensions)
            {
                Assert.Null(declared.AuroraNativeAlternative);
            }
        }
    }

    /// <summary>
    /// #3830: the verdict the remedy branches on, over a fake row set - no row, and the three catalog
    /// versions the extension actually shipped around the companion's arrival. The boundary comes off the
    /// DECLARATION rather than a literal here, so moving the declaration moves these answers with it.
    ///
    /// <para><b>1.10 is the case that decides whether the comparison is a comparison.</b>
    /// <c>pg_stat_statements</c> shipped 1.9 and then 1.10, and an ordinal string compare puts 1.10 BELOW
    /// 1.9 - so the fleet two releases ahead would be told to run an update it has already run twice, which
    /// is #3830's defect one version further on.</para>
    ///
    /// <para>A version nothing can rank answers <c>Undetermined</c> rather than guessing a side.
    /// <c>extversion</c> is free text the extension's author chooses, and a wrong guess here is a confident
    /// wrong remedy - exactly what the verdict exists to stop.</para>
    /// </summary>
    [Theory]
    [InlineData(null, PgExtensionCompanionVerdict.NoRow)]
    [InlineData("1.8", PgExtensionCompanionVerdict.BelowCompanionVersion)]
    [InlineData("1.9", PgExtensionCompanionVerdict.AtOrAboveCompanionVersion)]
    [InlineData("1.10", PgExtensionCompanionVerdict.AtOrAboveCompanionVersion)]
    [InlineData("", PgExtensionCompanionVerdict.NoRow)]
    [InlineData("1.9-rc1", PgExtensionCompanionVerdict.Undetermined)]
    public void TheCompanionVerdictReadsTheRow(string? extversion, PgExtensionCompanionVerdict expected)
    {
        var companion = Assert.Single(
            Assert.Single(PgStatementStatsCollector.Instance.RequiredPgExtensions).Companions);

        Assert.Equal(expected, companion.VerdictFrom(PgExtensionRowObservation.From(extversion, "appdb")));
    }

    /// <summary>
    /// The default is NO ANSWER, not "no row", and the two are opposite remedies: nothing read means the
    /// sentence must claim nothing about the catalog, while a read that found no row means the extension was
    /// never created. A <c>default</c> struct is what a hand-built runtime and a SQL Server target both
    /// produce, so this is the value the whole design has to fail safe on.
    ///
    /// <para>And the row is only an answer for the database it was read in, because <c>pg_extension</c> is
    /// per database. <c>InDatabase</c> is the guard stated as a method rather than left to each caller.</para>
    /// </summary>
    [Fact]
    public void AnUnobservedRowIsNotAnAbsentRow_AndARowOnlyAnswersForItsOwnDatabase()
    {
        var companion = Assert.Single(
            Assert.Single(PgStatementStatsCollector.Instance.RequiredPgExtensions).Companions);

        Assert.False(PgExtensionRowObservation.NotObserved.Observed);
        Assert.Equal(
            PgExtensionCompanionVerdict.Undetermined,
            companion.VerdictFrom(PgExtensionRowObservation.NotObserved));

        var read = PgExtensionRowObservation.From("1.8", "appdb");
        Assert.Equal(read, read.InDatabase("appdb"));
        Assert.Equal(PgExtensionRowObservation.NotObserved, read.InDatabase("otherdb"));
        Assert.Equal(PgExtensionRowObservation.NotObserved, read.InDatabase(null));
        Assert.Equal(PgExtensionRowObservation.NotObserved, PgExtensionRowObservation.NotObserved.InDatabase("appdb"));
    }

    /// <summary>
    /// The version comparison itself, on the pairs a dotted-decimal string compare gets wrong and on the
    /// shapes it cannot rank at all. Segment-wise and numeric, with a missing trailing segment reading as
    /// zero, so 1.9 and 1.9.0 are one version rather than two.
    /// </summary>
    [Theory]
    [InlineData("1.9", "1.10", -1)]
    [InlineData("1.10", "1.9", 1)]
    [InlineData("1.9", "1.9.0", 0)]
    [InlineData("1.9.0", "1.9", 0)]
    [InlineData("2.0", "1.10", 1)]
    public void TheVersionComparisonIsNumericPerSegment(string left, string right, int expected)
    {
        Assert.True(PgExtensionCompanionObject.TryCompareVersions(left, right, out var comparison));
        Assert.Equal(expected, comparison);
    }

    [Theory]
    [InlineData("1.9", null)]
    [InlineData("1.9", "")]
    [InlineData("1.9", "1.9-rc1")]
    [InlineData("v1.9", "1.9")]
    public void AVersionItCannotRankIsDeclinedRatherThanGuessed(string? left, string? right)
    {
        Assert.False(PgExtensionCompanionObject.TryCompareVersions(left, right, out var comparison));
        Assert.Equal(0, comparison);
    }

    /// <summary>
    /// NULL, not 0, and typed — an untyped NULL literal would arrive as text and Npgsql's strict type
    /// checking would throw on <c>GetInt64</c>, which is the same class of defect as the ordinal drift above.
    /// </summary>
    [Theory]
    [InlineData("storage_blks_read", "bigint")]
    [InlineData("orcache_blks_hit", "bigint")]
    [InlineData("storage_blk_read_time", "double precision")]
    [InlineData("orcache_blk_read_time", "double precision")]
    [InlineData("total_exec_peakmem", "bigint")]
    [InlineData("max_exec_peakmem", "bigint")]
    public void TheAuroraOnlyColumnsAreTypedNullsOnTheVanillaPath(string alias, string type)
    {
        Assert.Matches(new Regex($@"NULL::{Regex.Escape(type)}\s+AS {Regex.Escape(alias)}\b"), Sql(isAurora: false));
    }

    /// <summary>
    /// <c>toplevel</c> arrived in <c>pg_stat_statements</c> 1.9 (PostgreSQL 14). Before that, nested tracking
    /// did not exist, so every row IS a top-level statement — <c>true</c> is the CORRECT value on an older
    /// server, not a fallback, and the delta key stays four-part on every version.
    /// </summary>
    [Theory]
    [InlineData(13, "true")]
    [InlineData(14, "toplevel")]
    [InlineData(17, "toplevel")]
    public void ToplevelIsGuardedForPostgresBefore14(int major, string expected)
    {
        Assert.Matches(new Regex($@"{Regex.Escape(expected)}\s+AS toplevel\b"), Sql(isAurora: false, major));
    }

    /// <summary>
    /// The per-major block-time naming applies to the vanilla view too — it is a <c>pg_stat_statements</c>
    /// rename, not an Aurora one, and getting it wrong would silently shift every ordinal after it.
    /// </summary>
    [Theory]
    [InlineData(16, "blk_read_time", "blk_write_time")]
    [InlineData(17, "shared_blk_read_time", "shared_blk_write_time")]
    public void TheBlockTimeColumnsFollowTheMajorVersionOnBothFlavors(int major, string read, string write)
    {
        foreach (var isAurora in new[] { true, false })
        {
            var sql = Sql(isAurora, major);

            Assert.Matches(new Regex($@"(?<![a-z_]){Regex.Escape(read)}\s+AS blk_read_time\b"), sql);
            Assert.Matches(new Regex($@"(?<![a-z_]){Regex.Escape(write)}\s+AS blk_write_time\b"), sql);
        }
    }

    /// <summary>
    /// The vanilla query must not reach for anything Aurora-only by accident — a stray reference would fail
    /// at parse time on every non-Aurora target, which is a total outage of the read rather than a missing
    /// column.
    /// </summary>
    [Fact]
    public void TheVanillaQueryTouchesNoAuroraSurface()
        => Assert.DoesNotContain("aurora_", Sql(isAurora: false), StringComparison.OrdinalIgnoreCase);

    /// <summary>
    /// Column aliases of the outermost SELECT, in order. Comments are stripped first so a comma inside one
    /// cannot be counted — the same correction the probe-arity guard needed.
    /// </summary>
    private static System.Collections.Generic.List<string> SelectAliases(string sql)
    {
        var body = Regex.Replace(sql, @"/\*.*?\*/", " ", RegexOptions.Singleline);
        var start = body.IndexOf("SELECT", StringComparison.Ordinal) + "SELECT".Length;
        /* The OUTER FROM starts a line; the statements-epoch scalar subquery (#3653 A5) carries an inline
           `FROM public.pg_stat_statements_info` inside the select list, and a bare "FROM " search stopped
           there and counted the list short. */
        var end = body.IndexOf("\nFROM ", start, StringComparison.Ordinal);
        Assert.True(end > start, "the outer FROM must start its own line");

        return Regex.Matches(body[start..end], @"AS\s+([a-z_]+)\s*(?:,|$)", RegexOptions.IgnoreCase)
            .Select(m => m.Groups[1].Value)
            .ToList();
    }
}
