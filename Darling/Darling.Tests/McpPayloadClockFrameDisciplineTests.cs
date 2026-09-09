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
/// MCP tool, and #3206's sweep found sixteen more fields across five more tools — so what is pinned here is
/// the PAYLOAD BOUNDARY across every affected read at once, in both SKUs.
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
    /// the other fifteen are <c>DateTime?</c> and ship with it. Requiring the null-conditional made this
    /// assertion vacuous for the one field that cannot carry it, which is what CI caught.</para>
    /// </summary>
    private static Regex LiteDeSkew(string property) =>
        new(Regex.Escape(property) + @"\??\.AddMinutes\(-utcOffsetMinutes\)");

    /// <summary>
    /// One payload column: its output alias, the SQL expression whose value must be de-skewed, and the bare
    /// select-list form that must NOT survive. <paramref name="Source"/> defaults to the alias because
    /// fifteen of the sixteen are stored columns projected straight through; only <c>last_user_access</c>
    /// differs, and defaulting rather than requiring it keeps that one visible as the exception it is.
    /// </summary>
    public readonly record struct PayloadColumn(string Alias, string? Source = null, string? BareForm = null)
    {
        public string Expression => Source ?? Alias;

        /// <summary>The de-skewed projection this column must carry, spelled as the read ships it.</summary>
        public string DeSkewed => $"{Expression} - make_interval(mins => svr.offset_minutes) AS {Alias}_utc";
    }

    /// <summary>
    /// Every (Darling read, column) pair whose STORED value is the monitored server's local wall clock and
    /// which reaches an MCP caller, with the collector evidence for each. Sixteen columns across five reads;
    /// <c>default_trace_events.event_time</c> is the seventeenth and belongs to #3198/#3202.
    /// </summary>
    public static readonly (string Read, PayloadColumn[] Columns, string Evidence)[] ServerLocalPayloadColumns =
    [
        ("RunningJobsSql", [new("start_time")],
            "RunningJobsCollector ships start_time = ja.start_execution_date, the msdb Agent local clock, and "
            + "computes current_duration_seconds against GETDATE() on the next line"),
        ("BlockedProcessReportsSql",
            [new("blocked_last_tran_started"), new("blocking_last_tran_started"),
             new("blocked_last_batch_started"), new("blocking_last_batch_started"),
             new("blocked_last_batch_completed"), new("blocking_last_batch_completed")],
            "BlockedProcessReportCollector parses lasttranstarted / lastbatchstarted / lastbatchcompleted out "
            + "of the blocked-process-report XML, which SQL Server renders in the server's local clock"),
        ("DmvBlockingSnapshotsSql", [new("blocked_last_tran_started"), new("blocking_last_tran_started")],
            "DmvBlockingSnapshotCollector ships sys.dm_tran_active_transactions.transaction_begin_time verbatim"),
        /* The one DERIVED column in the set: the alias is last_user_access, but the expression de-skewed is
           the GREATEST of the four stored DMV columns, so its source has to be named explicitly. All four
           share one offset, which is what makes subtracting once, after the GREATEST, equivalent to
           subtracting four times before it. */
        ("IndexUsageSql",
            [new("last_user_access",
                 "GREATEST(ios.last_user_seek, ios.last_user_scan, ios.last_user_lookup, ios.last_user_update)",
                 BareForm: "GREATEST(ios.last_user_seek, ios.last_user_scan, ios.last_user_lookup, ios.last_user_update) AS last_user_access,")],
            "IndexObjectStatsCollector ships us.last_user_seek/scan/lookup/update from "
            + "sys.dm_db_index_usage_stats verbatim; the read GREATESTs the four"),
        ("PvsStatsLatestSql",
            [new("aborted_version_cleaner_start_time"), new("aborted_version_cleaner_end_time"),
             new("offrow_version_cleaner_start_time"), new("offrow_version_cleaner_end_time")],
            "PvsStatsCollector ships sys.dm_tran_persistent_version_store_stats verbatim"),
        ("PlanCorrectionsSql",
            [new("valid_since"), new("last_refresh"), new("execute_action_initiated_time"),
             new("revert_action_initiated_time")],
            "PlanCorrectionCollector ships sys.dm_db_tuning_recommendations verbatim"),
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
                   assertion from the alias alone passed the other five reads and quietly asserted nothing
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
    /// The same sixteen fields in Lite, which serves them from DuckDB. Lite de-skews in C# at the MCP
    /// projection rather than in SQL, for two reasons worth stating: DuckDB has no <c>make_interval</c>, and
    /// the underlying <c>LocalDataService</c> reads are shared with the WPF grids, which render through
    /// <c>ServerTimeHelper</c> and have their own frame defect under #3207 — de-skewing in the data service
    /// would fix the MCP surface by breaking the desktop one.
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
    /// Darling and Lite must cover the SAME sixteen payload FIELDS. Pinned as a count rather than a name
    /// mapping, because the two SKUs legitimately spell the same column differently (snake_case SQL alias vs
    /// PascalCase row property) and a name map would be either a second source of truth or a tautology. What
    /// must not drift is the arity: a field de-skewed on one SKU and not the other is the one-sided port that
    /// #2992 found nothing guarding against.
    ///
    /// <para><b>Distinct fields, not per-read declarations.</b> Darling declares EIGHTEEN (read, column)
    /// pairs for sixteen fields, because <c>get_blocking</c> is served by two reads and the always-on DMV
    /// fallback arm re-serves <c>blocked_last_tran_started</c> and <c>blocking_last_tran_started</c> — the
    /// two columns a DMV snapshot has. Lite reaches the same six through one <c>LocalDataService</c> call, so
    /// it declares them once. Comparing the raw sums asserted 18 == 16 and failed for a reason that had
    /// nothing to do with parity, which is what CI caught.</para>
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

        Assert.Equal(16, darlingFields);
        Assert.Equal(darlingFields, liteFields);

        /* And the overlap is exactly the DMV fallback's two columns — named as a number so that a THIRD read
           quietly re-serving a field has to be a deliberate edit here rather than absorbed silently. */
        Assert.Equal(2, darlingDeclarations - darlingFields);
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
            "GREATEST(ios.last_user_seek, ios.last_user_scan, ios.last_user_lookup, ios.last_user_update)",
            BareForm: "GREATEST(ios.last_user_seek, ios.last_user_scan, ios.last_user_lookup, ios.last_user_update) AS last_user_access,");
        Assert.Equal(
            "GREATEST(ios.last_user_seek, ios.last_user_scan, ios.last_user_lookup, ios.last_user_update)"
            + " - make_interval(mins => svr.offset_minutes) AS last_user_access_utc",
            greatest.DeSkewed);
        Assert.Matches(
            BareProjection(greatest),
            "            GREATEST(ios.last_user_seek, ios.last_user_scan, ios.last_user_lookup, ios.last_user_update) AS last_user_access,\n");
        Assert.DoesNotMatch(
            BareProjection(greatest),
            "            GREATEST(ios.last_user_seek, ios.last_user_scan, ios.last_user_lookup, ios.last_user_update) - make_interval(mins => svr.offset_minutes) AS last_user_access_utc,\n");

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
