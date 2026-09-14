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
using System.Runtime.CompilerServices;
using System.Text.RegularExpressions;
using PerformanceMonitor.Darling.Service.Mcp;
using Xunit;

namespace Darling.Tests;

/// <summary>
/// #3206: an MCP payload may not carry a server-LOCAL timestamp beside the naive-UTC fields every other
/// timestamp on that payload uses. This is the fourth instance of the class — #2991/#2992 was
/// <c>query_stats.creation_time</c> in analysis SQL, #3198 was <c>default_trace_events.event_time</c> in one
/// MCP tool, and #3206's sweep found twelve more fields across four more tools — so what is pinned here is
/// the PAYLOAD BOUNDARY across every affected read at once, in both SKUs.
///
/// <para><b>Shipping a DMV verbatim is not evidence of its frame, and the census below may not rest on
/// it.</b> #3419: <c>PlanCorrectionsSql</c>' four lifecycle times were declared server-local on exactly
/// that reasoning and are UTC, so de-skewing them put every actively-refreshing recommendation four hours
/// after the read that observed it — the same defect with the sign flipped. Every (read, column) pair in
/// <see cref="ServerLocalPayloadColumns"/> therefore carries provenance the DMV's OWN frame decides, and
/// <see cref="AlreadyUtcReads"/> names the measurement that keeps a pair out. Both halves are live-verified
/// against the collector-written UTC <c>collection_time</c> by
/// <c>PlanCorrectionFrameLiveTests</c>.</para>
///
/// <para><b>The frame cannot be judged by column name, and this file does not try.</b>
/// <c>StoreSqlClockDisciplineTests</c> maintains <c>AmbiguousFrameColumns</c> because <c>sample_time</c>,
/// <c>event_time</c> and <c>last_execution_time</c> are each two frames across different tables;
/// <c>CollectorTimestampFrameTests</c> records that its own first cut was a store-wide "all naive timestamps
/// are UTC" rule that would have forbidden the CPU collector's intentional local clock. So every fact below
/// is scoped to a (READ, COLUMN) pair, with the collector evidence for that pair named in
/// <see cref="ServerLocalPayloadColumns"/>.</para>
///
/// <para><b>What the three existing guards could not see.</b>
/// <c>CreationTimeClockFrameDisciplineTests</c> has the right discriminator shape but its roots are only
/// <c>Darling/PerformanceMonitor.Darling.Analysis</c> and <c>Lite/Analysis</c>.
/// <c>StoreSqlClockDisciplineTests</c> has the right roots — it includes <c>Service/Mcp/</c> — but hunts bare
/// clock FUNCTIONS and explicitly disclaims projections: "a bare clock in a SET, a VALUES row or a
/// projection is a different defect shape ... deliberately out of scope". <c>CollectorTimestampFrameTests</c>
/// has the right per-column reasoning but reads collector source only, so it says what a frame IS and never
/// who consumes it. The gap is exactly the diagonal, and it is what this file closes.</para>
///
/// <para><b>Both directions, and a positive control on each side.</b>
/// <see cref="TheDiscriminators_FlagABareProjection_AndPassADeSkewedOne"/> exercises the regexes against
/// literals written for the purpose, because a scan whose matcher has quietly stopped matching reports a
/// clean bill of health — worse than no scan. And the census is a floor AND a ceiling per read: a bare total
/// would let a Darling site vanish and a Lite one appear and still add up, which is the one-sided-port
/// regression #2992 found nothing guarding against.</para>
/// </summary>
public sealed class McpPayloadClockFrameDisciplineTests
{
    /// <summary>The Postgres de-skew, spelled exactly as every Darling read carries it. Anchored on the
    /// <c>svr.offset_minutes</c> CTE alias rather than on any column, so it cannot be satisfied by a rename.</summary>
    private static readonly Regex PgDeSkew =
        new(@"make_interval\(mins => svr\.offset_minutes\)");

    /// <summary>The single-row COALESCE offset CTE. Pinned on the ORDER BY + LIMIT because those are what make
    /// it the NEWEST collected offset and exactly one row — a cross join to a multi-row CTE would silently
    /// multiply every result row, and a cross join to an empty one would drop them all.</summary>
    private static readonly Regex OffsetCte = new(
        @"SELECT COALESCE\(\(\s*SELECT sp\.utc_offset_minutes\s*FROM server_properties AS sp\s*"
        + @"WHERE sp\.server_id = \$1\s*AND\s+sp\.utc_offset_minutes IS NOT NULL\s*"
        + @"ORDER BY sp\.collection_time DESC\s*LIMIT 1\), 0\) AS offset_minutes",
        RegexOptions.Singleline);

    /// <summary>
    /// Lite's de-skew: the offset is subtracted from the row in C#, because DuckDB has no
    /// <c>make_interval</c> and the affected reads are shared with the WPF grids.
    ///
    /// <para>The <c>?.</c> is OPTIONAL because nullability is a property of the row type rather than of this
    /// convention — <c>RunningJobRow.StartTime</c> is a non-nullable <c>DateTime</c> and ships without it,
    /// the other eleven are <c>DateTime?</c> and ship with it. Requiring the null-conditional made this
    /// assertion vacuous for the one field that cannot carry it, which is what CI caught.</para>
    /// </summary>
    private static Regex LiteDeSkew(string property) =>
        new(Regex.Escape(property) + @"\??\.AddMinutes\(-utcOffsetMinutes\)");

    /// <summary>
    /// One payload column: its output alias, the SQL expression whose value must be de-skewed, and the bare
    /// select-list form that must NOT survive. <paramref name="Source"/> defaults to the alias because
    /// eleven of the twelve are stored columns projected straight through; only <c>last_user_access</c>
    /// differs, and defaulting rather than requiring it keeps that one visible as the exception it is.
    /// </summary>
    public readonly record struct PayloadColumn(string Alias, string? Source = null, string? BareForm = null)
    {
        public string Expression => Source ?? Alias;

        /// <summary>
        /// The de-skewed projection this column must carry, spelled as the read ships it.
        ///
        /// <para><b>The output alias is the column's own name, with no <c>_utc</c> suffix.</b> Not cosmetic:
        /// <c>ConsumedTimestampFrameDisciplineTests.RenamedServerLocalProjections</c> reads a projection
        /// alias's SOURCE columns and cannot see the conversion, so a suffixed alias over a server-local
        /// column is reported as inheriting a server-local frame — wrong, and it also makes the payload field
        /// diverge from the column name, which drops the site out of that census. Keeping the alias equal to
        /// the column name routes every site through the ordinary column path, where the frame check applies
        /// and the register answers correctly. The conversion is pinned by
        /// <c>EveryDeSkewedAtReadSite_CarriesItsConversionInTheReaderItDependsOn</c> — a stronger claim than
        /// a suffix nothing checks. #3202's <c>event_time_utc</c> keeps its suffix and is unaffected:
        /// <c>event_time</c> spans five tables and two frames, so that helper declines it.</para>
        /// </summary>
        public string DeSkewed =>
            $"{Expression} - make_interval(mins => svr.offset_minutes) AS {Alias}";
    }

    /// <summary>
    /// Every (Darling read, column) pair whose STORED value is the monitored server's local wall clock and
    /// which reaches an MCP caller, with the evidence for each. Twelve columns across five reads — four
    /// tools, because <c>get_blocking</c> is served by two. <c>default_trace_events.event_time</c> is the
    /// thirteenth and belongs to #3198/#3202.
    ///
    /// <para><b>Ten of the twelve are measured, and the two that are not say so.</b> The frame of each was
    /// read off the live stores against the collector-written UTC <c>collection_time</c> on the same row:
    /// the newest value of a column in the server's local clock cannot come nearer than the offset behind
    /// <c>collection_time</c>, and on a fleet at -240 every one of the ten lands between -238.9 and -242.1
    /// minutes on two independent stores, while <c>blocked_process_reports.event_time</c> and
    /// <c>deadlocks.deadlock_time</c> — the two UTC controls in the same run — land at 0.0. The instrument
    /// therefore discriminates rather than answering the same way to everything, which is the property a
    /// census resting on "the collector ships the DMV verbatim" never had.</para>
    /// </summary>
    public static readonly (string Read, PayloadColumn[] Columns, string Evidence)[] ServerLocalPayloadColumns =
    [
        /* The one column in the set whose frame is NOT live-measured, and it is the one whose T-SQL says it
           outright: running_jobs holds no rows on either store — the collector has logged SUCCESS with
           rows_collected 0 on every one of 41,278 and 105,134 runs, because no Agent job has been mid-flight
           at a collection instant — so there is nothing to compare against collection_time. What decides it
           is the collector's own arithmetic rather than the fact that it projects the column: the next line
           DATEDIFFs the same value against GETDATE(), which is local-vs-local and only correct if the stored
           value is local. */
        ("RunningJobsSql", [new("start_time")],
            "RunningJobsCollector ships start_time = ja.start_execution_date, the msdb Agent local clock, and "
            + "computes current_duration_seconds against GETDATE() on the next line"),
        /* blocking_last_tran_started is the other unmeasured one, and only on THIS read: the column is NULL
           in all 4,066 blocked_process_reports rows across both stores, because a blocker holding no open
           transaction has no lasttranstarted to render. Its frame is inherited from the sibling parsed out
           of the same XML document by the same collector line — blocked_last_tran_started, measured at
           -240.2 — and it is measured directly on DmvBlockingSnapshotsSql below. */
        ("BlockedProcessReportsSql",
            [new("blocked_last_tran_started"), new("blocking_last_tran_started"),
             new("blocked_last_batch_started"), new("blocking_last_batch_started"),
             new("blocked_last_batch_completed"), new("blocking_last_batch_completed")],
            "BlockedProcessReportCollector parses lasttranstarted / lastbatchstarted / lastbatchcompleted out "
            + "of the blocked-process-report XML, which SQL Server renders in the server's local clock; the "
            + "five populated columns measure -240.2 to -242.1 against the UTC event_time on the same row, "
            + "and de-skewing them takes (event_time - blocked_last_batch_started) - wait_time_ms from an "
            + "impossible +240.0 minutes to 0.0"),
        ("DmvBlockingSnapshotsSql", [new("blocked_last_tran_started"), new("blocking_last_tran_started")],
            "DmvBlockingSnapshotCollector ships sys.dm_tran_active_transactions.transaction_begin_time "
            + "verbatim, and both columns measure -239.9 to -240.1 against collection_time on two stores"),
        /* The one DERIVED column in the set: the alias is last_user_access, but the expression de-skewed is
           the GREATEST of the four stored DMV columns, so its source has to be named explicitly. All four
           share one offset, which is what makes subtracting once, after the GREATEST, equivalent to
           subtracting four times before it. */
        ("IndexUsageSql",
            [new("last_user_access",
                 "GREATEST(last_user_seek, last_user_scan, last_user_lookup, last_user_update)",
                 BareForm: "GREATEST(last_user_seek, last_user_scan, last_user_lookup, last_user_update) AS last_user_access,")],
            "IndexObjectStatsCollector ships us.last_user_seek/scan/lookup/update from "
            + "sys.dm_db_index_usage_stats verbatim; the read GREATESTs the four, and the GREATEST measures "
            + "-238.9 against collection_time over 1,989,067 rows and -239.4 over 266,073 on the other store"),
        ("PvsStatsLatestSql",
            [new("aborted_version_cleaner_start_time"), new("aborted_version_cleaner_end_time"),
             new("offrow_version_cleaner_start_time"), new("offrow_version_cleaner_end_time")],
            "PvsStatsCollector ships sys.dm_tran_persistent_version_store_stats verbatim, and all four "
            + "measure -240.0 against collection_time on two stores — the offrow pair with every one of "
            + "5,754 and 1,008 rows inside a 10-minute band around -240, the cleaner running continuously"),
    ];

    /// <summary>
    /// Darling reads that must NOT carry the offset CTE, each with why. This half is what stops the guard
    /// being satisfied by sprinkling the de-skew everywhere: applying it to a column that is already UTC is
    /// the same defect with the sign flipped, and #3207 is the open issue for exactly that mistake on the
    /// desktop surface.
    /// </summary>
    public static readonly (string Read, string Why)[] AlreadyUtcReads =
    [
        ("RecentDeadlocksSql", "deadlock_time is the XE @timestamp — DeadlocksCollector reads (@timestamp)[1]"),
        ("IndexUsageMatchCountSql", "returns a count, no timestamp at all"),
        ("PvsTrendSql", "returns only collection_time, which the collector stamps in naive UTC"),
        ("AutomaticTuningSql", "returns only collection_time"),
        /* #3419, and the one entry here that had to be MEASURED to earn its place rather than reasoned
           into it. PlanCorrectionCollector ships sys.dm_db_tuning_recommendations verbatim exactly as
           PvsStatsCollector ships its DMV verbatim, so the provenance sentence is word-for-word the one
           that put four columns in ServerLocalPayloadColumns above — and the DMV's answer is the opposite.
           What separates them is the measurement, not the sentence. */
        ("PlanCorrectionsSql",
            "sys.dm_db_tuning_recommendations reports valid_since, last_refresh and the two action-initiated "
            + "times in UTC: against the collector-written UTC collection_time on the same row, the newest "
            + "value of each lands within +0.2 minutes of it over 27,719,040 rows on one store and +0.7 over "
            + "637,558 on the other, where a value in a -240 server's local clock could not come nearer than "
            + "240 minutes behind. The two stored-but-unread siblings, execute_action_start_time and "
            + "revert_action_start_time, measure the same. De-skewing these puts every actively-refreshing "
            + "recommendation AFTER the read that observed it — 189,701 rows of one 24-hour window did "
            + "exactly that"),
    ];

    private static string DarlingSql(string name) => name switch
    {
        "RunningJobsSql" => DarlingJobReader.RunningJobsSql,
        "BlockedProcessReportsSql" => DarlingBlockingReader.BlockedProcessReportsSql,
        "DmvBlockingSnapshotsSql" => DarlingBlockingReader.DmvBlockingSnapshotsSql,
        "RecentDeadlocksSql" => DarlingBlockingReader.RecentDeadlocksSql,
        "IndexUsageSql" => DarlingObjectStatsReader.IndexUsageSql,
        "IndexUsageMatchCountSql" => DarlingObjectStatsReader.IndexUsageMatchCountSql,
        "PvsStatsLatestSql" => DarlingPvsReader.PvsStatsLatestSql,
        "PvsTrendSql" => DarlingPvsReader.PvsTrendSql,
        "PlanCorrectionsSql" => DarlingPlanCorrectionReader.PlanCorrectionsSql,
        "AutomaticTuningSql" => DarlingPlanCorrectionReader.AutomaticTuningSql,
        _ => throw new ArgumentOutOfRangeException(nameof(name), name, "unknown read"),
    };

    /* ───────────────────────── the Darling reads ───────────────────────── */

    [Fact]
    public void EveryServerLocalColumn_IsProjectedDeSkewed_UnderAUtcSuffixedAlias()
    {
        foreach (var (read, columns, evidence) in ServerLocalPayloadColumns)
        {
            var sql = DarlingSql(read);

            Assert.True(
                OffsetCte.IsMatch(sql),
                $"{read} projects a server-local column but carries no offset CTE. {evidence}. Add the "
                + "single-row COALESCE CTE and subtract it: "
                + "column - make_interval(mins => svr.offset_minutes) AS column_utc.");

            foreach (var column in columns)
            {
                /* The de-skew must be spelled on the expression the read actually projects — which for
                   last_user_access is a GREATEST over four columns, not a column of that name. Deriving the
                   assertion from the alias alone passed the other four reads and quietly asserted nothing
                   here, which is how this guard first shipped and what its own red run caught. */
                Assert.Contains(column.DeSkewed, sql, StringComparison.Ordinal);

                Assert.False(
                    BareProjection(column).IsMatch(sql),
                    $"{read} still projects a bare {column.Alias}. {evidence}, so an un-de-skewed value is "
                    + "wrong by the server's whole offset — 4 hours on the production fleet, measured at 42 "
                    + "of 42 servers in #2932 — beside a collection_time / as_of on the SAME payload that is "
                    + "naive UTC. The direction inverts causality: a transaction that began during a block "
                    + "reads as having begun hours before it.");
            }
        }
    }

    /// <summary>
    /// The census, as a floor and a ceiling: exactly these Darling reads carry the de-skew. A read that grows
    /// a server-local projection has to be added here deliberately, and one that loses its de-skew fails even
    /// if another gains one.
    /// </summary>
    [Fact]
    public void ExactlyTheDeclaredReads_CarryTheDeSkew()
    {
        var expected = ServerLocalPayloadColumns.Select(x => x.Read).OrderBy(x => x, StringComparer.Ordinal);
        var all = ServerLocalPayloadColumns.Select(x => x.Read).Concat(AlreadyUtcReads.Select(x => x.Read));

        var actual = all
            .Where(r => PgDeSkew.IsMatch(DarlingSql(r)))
            .OrderBy(x => x, StringComparer.Ordinal);

        Assert.Equal(expected.ToArray(), actual.ToArray());

        foreach (var (read, why) in AlreadyUtcReads)
        {
            var sql = DarlingSql(read);

            Assert.False(
                sql.Contains("utc_offset_minutes", StringComparison.Ordinal),
                $"{read} acquired an offset de-skew, but {why}. De-skewing a value that is already UTC is the "
                + "same defect with the sign flipped, and it is silent in the same way.");
        }
    }

    /// <summary>
    /// The de-skew must not change which ROWS come back. Every affected read still windows, snapshots and
    /// orders on a naive-UTC column, so the offset can only move values. This is what makes the change safe
    /// to ship without a data migration, and it is the half #3198 got right while getting the projection
    /// wrong — so it is worth its own assertion rather than an assumption.
    /// </summary>
    [Fact]
    public void TheDeSkew_TouchesNoWindowBound_NoSnapshotSubquery_AndNoOrdering()
    {
        foreach (var (read, columns, _) in ServerLocalPayloadColumns)
        {
            var sql = DarlingSql(read);

            foreach (var line in sql.Split('\n').Select(l => l.Trim()))
            {
                var isPredicateOrOrdering =
                    line.StartsWith("WHERE ", StringComparison.Ordinal)
                    || line.StartsWith("AND ", StringComparison.Ordinal)
                    || line.StartsWith("ORDER BY ", StringComparison.Ordinal);

                /* The CTE's own WHERE/ORDER BY name sp.* and are how the offset is fetched, not a use of it. */
                if (!isPredicateOrOrdering || line.Contains("sp.", StringComparison.Ordinal))
                {
                    continue;
                }

                foreach (var column in columns)
                {
                    Assert.DoesNotContain(column.Alias, line, StringComparison.Ordinal);
                }
            }
        }
    }

    /* ───────────────────────── Lite parity ───────────────────────── */

    /// <summary>
    /// The same twelve fields in Lite, which serves them from DuckDB. Lite de-skews in C# at the MCP
    /// projection rather than in SQL, for two reasons worth stating: DuckDB has no <c>make_interval</c>, and
    /// the underlying <c>LocalDataService</c> reads are shared with the WPF grids, which render through
    /// <c>ServerTimeHelper</c> and pick their renderer per column.
    ///
    /// <para><c>Lite/Mcp/McpPlanCorrectionTools.cs</c> is deliberately absent: its four fields are UTC in
    /// the store (see <see cref="AlreadyUtcReads"/>) and it emits them unconverted, which is why
    /// <see cref="EveryLiteToolSite_ResolvesThisServersOffset_AndDeSkewsEveryAffectedField"/> must not
    /// reach it — that test would otherwise demand the offset resolution the tool has no use for.</para>
    /// </summary>
    public static readonly (string RelativePath, string[] Properties)[] LiteToolSites =
    [
        ("Lite/Mcp/McpJobTools.cs", ["StartTime"]),
        ("Lite/Mcp/McpBlockingTools.cs",
            ["BlockedLastTranStarted", "BlockingLastTranStarted", "BlockedLastBatchStarted",
             "BlockingLastBatchStarted", "BlockedLastBatchCompleted", "BlockingLastBatchCompleted"]),
        ("Lite/Mcp/McpObjectStatsTools.cs", ["LastUserAccess"]),
        ("Lite/Mcp/McpPvsTools.cs",
            ["AbortedCleanerStartTime", "AbortedCleanerEndTime", "OffrowCleanerStartTime", "OffrowCleanerEndTime"]),
    ];

    /// <summary>
    /// Lite MCP tools that emit a <c>plan_correction</c> lifecycle time and must NOT apply an offset to it,
    /// with the property whose bare emission is the correct form. The mirror of
    /// <see cref="AlreadyUtcReads"/> on the Lite side: without it, removing the tool from
    /// <see cref="LiteToolSites"/> would drop the file out of every assertion in this file, and a
    /// re-introduced <c>AddMinutes(-utcOffsetMinutes)</c> would be invisible here.
    /// </summary>
    public static readonly (string RelativePath, string[] Properties)[] LiteAlreadyUtcToolSites =
    [
        ("Lite/Mcp/McpPlanCorrectionTools.cs",
            ["ValidSince", "LastRefresh", "ExecuteActionInitiatedTime", "RevertActionInitiatedTime"]),
    ];

    [Fact]
    public void EveryLiteToolSite_ResolvesThisServersOffset_AndDeSkewsEveryAffectedField()
    {
        foreach (var (relativePath, properties) in LiteToolSites)
        {
            var path = RepoPath(relativePath);
            Assert.True(File.Exists(path), $"{relativePath} is gone — update this guard deliberately");
            var text = File.ReadAllText(path);

            /* The offset must come from the RESOLVED server, never from ServerTimeHelper's process-wide
               static: #2967 found that static holds whichever server the desktop last selected, or the Lite
               HOST's own offset when no tab has ever been opened, so an MCP call would de-skew one server's
               rows by another server's offset. McpServerLocalWindow exists to make that unavailable. */
            Assert.Contains(
                "McpServerLocalWindow.OffsetForAsync(dataService, resolved.ServerId)",
                text,
                StringComparison.Ordinal);

            Assert.DoesNotContain("ServerTimeHelper.UtcOffsetMinutes", text, StringComparison.Ordinal);

            foreach (var property in properties)
            {
                Assert.True(
                    LiteDeSkew(property).IsMatch(text),
                    $"{relativePath} emits {property} without subtracting this server's offset. The stored "
                    + "value is the monitored server's local wall clock and every other timestamp on the "
                    + "payload is naive UTC, so leaving it raw is wrong by the whole offset — 4 hours on the "
                    + "production fleet, measured at 42 of 42 servers in #2932.");

                /* And no bare emission of the same property survives. */
                Assert.DoesNotContain($"= r.{property}?.ToString(\"o\")", text, StringComparison.Ordinal);
                Assert.DoesNotContain($"= r.{property}.ToString(\"o\")", text, StringComparison.Ordinal);
            }
        }
    }

    /// <summary>
    /// The Lite half of <see cref="AlreadyUtcReads"/>, asserted in BOTH directions on the same matcher the
    /// test above uses: the field is emitted bare, and no <c>AddMinutes(-utcOffsetMinutes)</c> reaches it.
    /// The offset resolution must be gone from the file too — left behind it is a call whose result nothing
    /// consumes, which is the state a partly-reverted fix sits in.
    /// </summary>
    [Fact]
    public void EveryLiteAlreadyUtcToolSite_EmitsTheFieldUnconverted_AndResolvesNoOffset()
    {
        foreach (var (relativePath, properties) in LiteAlreadyUtcToolSites)
        {
            var path = RepoPath(relativePath);
            Assert.True(File.Exists(path), $"{relativePath} is gone — update this guard deliberately");
            var text = File.ReadAllText(path);

            Assert.DoesNotContain("McpServerLocalWindow", text, StringComparison.Ordinal);
            Assert.DoesNotContain("utcOffsetMinutes", text, StringComparison.Ordinal);

            foreach (var property in properties)
            {
                Assert.False(
                    LiteDeSkew(property).IsMatch(text),
                    $"{relativePath} subtracts an offset from {property}, but the stored value is already "
                    + "naive UTC — measured within +0.7 minutes of the collector-written collection_time on "
                    + "the same row, on two stores. Subtracting a -240 offset ADDS four hours, which places "
                    + "an actively-refreshing recommendation after the read that observed it.");

                Assert.Contains(
                    $"= r.{property}?.ToString(\"o\")",
                    text,
                    StringComparison.Ordinal);
            }
        }
    }

    /// <summary>
    /// No Lite tool file appears in both lists. Without this the two censuses could disagree about one file
    /// and each pass on its own half, which is how a de-skew gets asserted and denied at once.
    /// </summary>
    [Fact]
    public void TheLiteToolSiteLists_AreDisjoint()
    {
        Assert.Empty(LiteToolSites
            .Select(s => s.RelativePath)
            .Intersect(LiteAlreadyUtcToolSites.Select(s => s.RelativePath), StringComparer.Ordinal));
    }

    /// <summary>
    /// Darling and Lite must cover the SAME twelve payload FIELDS. Pinned as a count rather than a name
    /// mapping, because the two SKUs legitimately spell the same column differently (snake_case SQL alias vs
    /// PascalCase row property) and a name map would be either a second source of truth or a tautology. What
    /// must not drift is the arity: a field de-skewed on one SKU and not the other is the one-sided port that
    /// #2992 found nothing guarding against.
    ///
    /// <para><b>Distinct fields, not per-read declarations.</b> Darling declares FOURTEEN (read, column)
    /// pairs for twelve fields, because <c>get_blocking</c> is served by two reads and the always-on DMV
    /// fallback arm re-serves <c>blocked_last_tran_started</c> and <c>blocking_last_tran_started</c> — the
    /// two columns a DMV snapshot has. Lite reaches the same six through one <c>LocalDataService</c> call, so
    /// it declares them once. Comparing the raw sums asserted 18 == 16 and failed for a reason that had
    /// nothing to do with parity, which is what CI caught.</para>
    ///
    /// <para><b>The already-UTC side is pinned with the same arity discipline</b>, so removing a field from
    /// one census and forgetting the other fails here rather than shrinking both lists to nothing. #3419
    /// moved four fields across; the two totals must still add to the sixteen a payload-boundary sweep
    /// reaches in each SKU.</para>
    /// </summary>
    [Fact]
    public void TheTwoSkus_DeSkewTheSameNumberOfFields()
    {
        var darlingDeclarations = ServerLocalPayloadColumns.Sum(x => x.Columns.Length);
        var darlingFields = ServerLocalPayloadColumns
            .SelectMany(x => x.Columns.Select(c => c.Alias))
            .Distinct(StringComparer.Ordinal)
            .Count();
        var liteFields = LiteToolSites.Sum(x => x.Properties.Length);

        Assert.Equal(12, darlingFields);
        Assert.Equal(darlingFields, liteFields);

        /* And the overlap is exactly the DMV fallback's two columns — named as a number so that a THIRD read
           quietly re-serving a field has to be a deliberate edit here rather than absorbed silently. */
        Assert.Equal(2, darlingDeclarations - darlingFields);

        /* Both censuses together, per SKU, so a field cannot leave one list without joining the other. */
        var liteAlreadyUtcFields = LiteAlreadyUtcToolSites.Sum(x => x.Properties.Length);

        Assert.Equal(4, liteAlreadyUtcFields);
        Assert.Equal(16, darlingFields + liteAlreadyUtcFields);
        Assert.Equal(16, liteFields + liteAlreadyUtcFields);
    }

    /* ───────────────────────── the discriminators, both directions ───────────────────────── */

    [Fact]
    public void TheDiscriminators_FlagABareProjection_AndPassADeSkewedOne()
    {
        /* PgDeSkew recognises the shipped fix, and nothing weaker. */
        Assert.Matches(PgDeSkew, "    start_time - make_interval(mins => svr.offset_minutes) AS start_time_utc,");
        Assert.DoesNotMatch(PgDeSkew, "    start_time,");
        /* A different CTE alias is not this convention, and must not pass as it. */
        Assert.DoesNotMatch(PgDeSkew, "    start_time - make_interval(mins => o.utc_offset_minutes) AS start_time_utc,");

        /* OffsetCte requires the newest single row; the shapes that would silently multiply or drop rows fail. */
        var good = "        WITH svr AS (\n            SELECT COALESCE((\n                SELECT sp.utc_offset_minutes\n"
            + "                FROM server_properties AS sp\n                WHERE sp.server_id = $1\n"
            + "                AND   sp.utc_offset_minutes IS NOT NULL\n                ORDER BY sp.collection_time DESC\n"
            + "                LIMIT 1), 0) AS offset_minutes\n        )";
        Assert.Matches(OffsetCte, good);
        Assert.DoesNotMatch(OffsetCte, good.Replace("LIMIT 1)", "LIMIT 2)", StringComparison.Ordinal));
        Assert.DoesNotMatch(OffsetCte, good.Replace(", 0) AS offset_minutes", ") AS offset_minutes", StringComparison.Ordinal));
        Assert.DoesNotMatch(OffsetCte, good.Replace("ORDER BY sp.collection_time DESC", "", StringComparison.Ordinal));

        /* The bare-projection matcher: the select-list forms that shipped in the defect, and the forms it
           must not drag in — the de-skewed projection, a WHERE, and an ORDER BY. */
        var bare = BareProjection(new PayloadColumn("start_time"));
        Assert.Matches(bare, "            start_time,\n");
        Assert.Matches(bare, "            rj.start_time,\n");
        Assert.DoesNotMatch(bare, "            rj.start_time - make_interval(mins => svr.offset_minutes) AS start_time_utc,\n");
        Assert.DoesNotMatch(bare, "        AND   start_time >= $2\n");
        Assert.DoesNotMatch(bare, "        ORDER BY start_time DESC\n");

        /* And the derived column, whose bare form is a whole expression. Both directions, because this is
           the case a name-derived matcher silently passed. */
        var greatest = new PayloadColumn(
            "last_user_access",
            "GREATEST(last_user_seek, last_user_scan, last_user_lookup, last_user_update)",
            BareForm: "GREATEST(last_user_seek, last_user_scan, last_user_lookup, last_user_update) AS last_user_access,");
        Assert.Equal(
            "GREATEST(last_user_seek, last_user_scan, last_user_lookup, last_user_update)"
            + " - make_interval(mins => svr.offset_minutes) AS last_user_access",
            greatest.DeSkewed);
        Assert.Matches(
            BareProjection(greatest),
            "            GREATEST(last_user_seek, last_user_scan, last_user_lookup, last_user_update) AS last_user_access,\n");
        Assert.DoesNotMatch(
            BareProjection(greatest),
            "            GREATEST(last_user_seek, last_user_scan, last_user_lookup, last_user_update)"
            + " - make_interval(mins => svr.offset_minutes) AS last_user_access,\n");

        /* LiteDeSkew recognises BOTH shipped Lite forms — the nullable one and the non-nullable one — and
           neither bare emission. The non-nullable case is real: RunningJobRow.StartTime is a DateTime. */
        Assert.Matches(LiteDeSkew("StartTime"), "start_time = r.StartTime.AddMinutes(-utcOffsetMinutes).ToString(\"o\"),");
        Assert.Matches(LiteDeSkew("ValidSince"), "valid_since = r.ValidSince?.AddMinutes(-utcOffsetMinutes).ToString(\"o\"),");
        Assert.DoesNotMatch(LiteDeSkew("StartTime"), "start_time = r.StartTime.ToString(\"o\"),");
        Assert.DoesNotMatch(LiteDeSkew("ValidSince"), "valid_since = r.ValidSince?.ToString(\"o\"),");
        /* And it must not match a DIFFERENT property that merely shares a prefix. */
        Assert.DoesNotMatch(LiteDeSkew("LastRefreshed"), "last_refresh = r.LastRefresh?.AddMinutes(-utcOffsetMinutes).ToString(\"o\"),");
    }

    /* ───────────────────────── plumbing ───────────────────────── */

    /// <summary>
    /// The bare select-list form of a column — what the defect looked like. Scoped to a line that both names
    /// the expression and closes the select-list item, so a WHERE, an ORDER BY and the de-skewed projection
    /// itself cannot satisfy it. A column with an explicit <c>BareForm</c> matches that literal instead,
    /// because a multi-column expression has no single-identifier form to key on.
    /// </summary>
    private static Regex BareProjection(PayloadColumn column) =>
        column.BareForm is { } literal
            ? new Regex(@"^\s*" + Regex.Escape(literal) + @"\s*$", RegexOptions.Multiline)
            : new Regex(@"^\s*(?:\w+\.)?" + Regex.Escape(column.Alias) + @",\s*$", RegexOptions.Multiline);

    private static string RepoPath(string relative, [CallerFilePath] string thisFile = "")
    {
        /* This file lives at <repo>/Darling/Darling.Tests/, so the repo root is two levels up. */
        var repoRoot = Path.GetFullPath(Path.Combine(Path.GetDirectoryName(thisFile)!, "..", ".."));
        return Path.GetFullPath(Path.Combine(repoRoot, relative.Replace('/', Path.DirectorySeparatorChar)));
    }
}
