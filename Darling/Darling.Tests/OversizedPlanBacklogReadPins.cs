/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using System.Collections.Generic;
using System.ComponentModel;
using System.Globalization;
using System.Linq;
using System.Reflection;
using System.Text.Json;
using System.Text.RegularExpressions;
using System.Threading;
using System.Threading.Tasks;
using ModelContextProtocol.Server;
using Npgsql;
using PerformanceMonitor.Collectors;
using PerformanceMonitor.Darling.Service;
using PerformanceMonitor.Darling.Service.Mcp;
using PerformanceMonitor.Darling.Storage;
using Xunit;
using static Darling.Tests.RepoFile;

namespace Darling.Tests;

/// <summary>
/// #3398: the read surface over <c>collect.oversized_plan_backlog</c>, and the four properties of it that
/// are not allowed to erode.
///
/// <para><b>Why these four and not a shape assertion.</b> The table was already in the store and already
/// had two silent fallback readers; what it did not have was any way to see the sweep's verdicts, so the
/// failure this read exists to prevent is a plausible-looking answer rather than a missing one. Each pin
/// below covers one way this read could return numbers that look right and are not: counting "pending"
/// differently from the sweep's own claim, letting the three verdict buckets overlap or leave a gap,
/// reporting a CHARACTER count under a byte-shaped name, and losing rows whose server has left the
/// registry. Every one of those reads as a smaller or larger backlog than exists, with nothing to say
/// so.</para>
///
/// <para>The SQL is asserted from the shipped constants rather than from the file text, and then EXECUTED
/// against a live store in <see cref="OversizedPlanBacklogReadLivePostgresTests"/> — the two halves a text
/// pin alone cannot cover are whether the statements parse at all and whether the buckets really partition,
/// and the fixture there carries the <c>expired_at</c> and both-stamps states that no production store has
/// produced yet.</para>
/// </summary>
public sealed class OversizedPlanBacklogReadPins
{
    private const string ToolName = "get_oversized_plan_backlog";

    private const string HostSource =
        "Darling/PerformanceMonitor.Darling.Service/Mcp/DarlingMcpHostService.cs";

    private const string ToolSource =
        "Darling/PerformanceMonitor.Darling.Service/Mcp/DarlingMcpOversizedPlanBacklogTools.cs";

    /// <summary>The three statements this read is built from, as one sequence for the pins that hold a
    /// property across all of them.</summary>
    private static string[] AllReadSql =>
    [
        DarlingOversizedPlanBacklogReader.PerServerRollupSql,
        DarlingOversizedPlanBacklogReader.CollectorCensusSql,
        DarlingOversizedPlanBacklogReader.RowsSql,
    ];

    private static MethodInfo ToolMethod =>
        typeof(DarlingMcpOversizedPlanBacklogTools).GetMethod(
            nameof(DarlingMcpOversizedPlanBacklogTools.GetOversizedPlanBacklog),
            BindingFlags.Public | BindingFlags.Static)
        ?? throw new InvalidOperationException("the tool method is gone, so every pin here would pass vacuously");

    /* ---- registration ------------------------------------------------------------------------------- */

    [Fact]
    public void TheToolIsRegisteredOnTheMcpHost_AndOnTheWebReadSurface()
    {
        /* Three separate registrations, and the tool is invisible on one surface per missing one. The MCP
           host is where an agent finds it; the /api/read catalog is what the viewer's own HTTP surface and
           the compose catalog enumerate. DarlingWebEndpointsTests holds the general rule that every
           read-only tool has an endpoint — this names THIS tool, so a failure says which one. */
        Assert.Equal(ToolName, ToolMethod.GetCustomAttribute<McpServerToolAttribute>()?.Name);

        var host = CSharpMemberMap.Of(ReadRepoFile(HostSource)).Code;
        Assert.Contains(
            ".WithGeminiCompatibleTools<" + nameof(DarlingMcpOversizedPlanBacklogTools) + ">()",
            host,
            StringComparison.Ordinal);

        Assert.Contains(ToolName, DarlingWebEndpoints.BuildReadDispatch().Keys);
        Assert.Contains(ToolName, DarlingWebEndpoints.CatalogDescriptors.Keys);
    }

    [Fact]
    public void TheReadTakesNoTimeWindow_AndExactlyTheThreeDeclaredKnobs()
    {
        /* A worklist whose rows are UPDATED in place has no "last 24 hours of it", so an hours_back or an
           as_of would have to invent a time column to apply itself to — and the obvious candidate,
           last_seen_at, is the SIGHTING clock rather than the verdict clock, so a windowed read would hide
           exactly the long-resident plans this table exists to hold. The verdict timestamps carry the time
           axis instead. Asserted over the parameter list rather than the SQL text because the defect
           arrives as a parameter someone adds for symmetry with the neighbouring self-monitoring reads. */
        Assert.Equal(
            new[] { "postgres", "server_name", "include_rows", "limit" },
            ToolMethod.GetParameters()
                .Where(p => p.ParameterType != typeof(CancellationToken))
                .Select(p => p.Name)
                .ToArray());

        foreach (var sql in AllReadSql)
        {
            Assert.DoesNotContain("now()", sql, StringComparison.Ordinal);
            Assert.DoesNotContain("date_trunc", sql, StringComparison.Ordinal);
            Assert.DoesNotContain("CURRENT_TIMESTAMP", sql, StringComparison.OrdinalIgnoreCase);
        }
    }

    /* ---- the four properties ------------------------------------------------------------------------ */

    [Fact]
    public void PendingCountsExactlyWhatTheSweepsClaimWouldTake()
    {
        /* The read's whole purpose is to say whether the sweep still has work, so a read that defines
           "still to do" independently of ClaimSql can report an empty backlog while the sweep has three
           plans queued, or the reverse. Compared as normalised text here and then executed side by side in
           the live test below, which is the half that would catch a predicate that is spelled the same and
           means something else because of an alias. */
        var claim = Normalize(OversizedPlanBacklog.ClaimSql(1));
        Assert.Contains("captured_at is null", claim, StringComparison.Ordinal);
        Assert.Contains("expired_at is null", claim, StringComparison.Ordinal);

        foreach (var sql in new[]
        {
            DarlingOversizedPlanBacklogReader.PerServerRollupSql,
            DarlingOversizedPlanBacklogReader.CollectorCensusSql,
        })
        {
            Assert.Contains(
                "count(*) filter (where captured_at is null and expired_at is null)",
                Normalize(sql),
                StringComparison.Ordinal);
        }
    }

    [Theory]
    [InlineData(false, false, "pending")]
    [InlineData(true, false, "captured")]
    [InlineData(false, true, "expired")]
    [InlineData(true, true, "captured")]
    public void TheRowVerdict_MatchesTheBucketTheRollupWouldCountItIn(bool captured, bool expired, string expected)
    {
        /* The fourth row is the one that matters. A capture clears any standing expiry, so a row carrying
           both stamps is a row that was retired and then fetched anyway — and the two halves of this read
           have to agree about it or a listed row contradicts the count it contributed to. Content wins in
           VerdictOf, and the rollup's expired bucket excludes a row with content for the same reason, which
           is what makes the three buckets a partition rather than three overlapping filters. */
        var at = new DateTime(2026, 9, 13, 14, 50, 0, DateTimeKind.Unspecified);

        Assert.Equal(
            expected,
            DarlingOversizedPlanBacklogReader.VerdictOf(
                captured ? at : null,
                expired ? at : null));
    }

    [Fact]
    public void TheExpiredBucket_ExcludesARowThatAlsoCarriesContent()
    {
        /* The SQL half of the partition, and the arm that is easiest to "simplify" into
           expired_at IS NOT NULL on its own — at which point a both-stamps row is counted twice and
           pending + captured + expired exceeds total_rows, which reads as a bigger backlog than exists. */
        foreach (var sql in new[]
        {
            DarlingOversizedPlanBacklogReader.PerServerRollupSql,
            DarlingOversizedPlanBacklogReader.CollectorCensusSql,
        })
        {
            Assert.Contains(
                "count(*) filter (where captured_at is null and expired_at is not null) as expired_rows",
                Normalize(sql),
                StringComparison.Ordinal);
        }
    }

    [Fact]
    public void NoReadDerivesASizeFromTheStoredContent()
    {
        /* observed_bytes is the monitored server's DATALENGTH over nvarchar(max) — UTF-16 BYTES, the unit
           MaxCapturedPlanXmlBytes is in. PostgreSQL's length() over the same content is CHARACTERS and
           measured at exactly half of it on production data, so a size taken from the stored column and
           reported under a byte-shaped name puts every reader a factor of two below the cap they are
           checking against. It is also the expensive read on this table: the captured column is TOASTed,
           measured at 525 MB logical against 32 MB of pg_total_relation_size on one production store, so an
           aggregate over its length detoasts all of it to produce a number nobody can use. Presence is read
           from the varlena header and detoasts nothing, which is why has_plan_xml exists and a size does
           not. */
        foreach (var sql in AllReadSql)
        {
            /* The instrument has to be looking at something, or the absence below is vacuous. */
            Assert.Contains("observed_bytes", sql, StringComparison.Ordinal);

            foreach (var forbidden in new[] { "length(", "pg_column_size(", "substring(", "left(" })
            {
                Assert.DoesNotContain(forbidden, sql, StringComparison.OrdinalIgnoreCase);
            }
        }

        /* And the one thing the reads DO take from the content column is a null test. */
        Assert.Contains("b.plan_xml IS NOT NULL", DarlingOversizedPlanBacklogReader.RowsSql, StringComparison.Ordinal);
        Assert.Contains(
            "count(*) FILTER (WHERE b.plan_xml IS NOT NULL)",
            DarlingOversizedPlanBacklogReader.PerServerRollupSql,
            StringComparison.Ordinal);
    }

    [Fact]
    public void TheMedianIsADiscretePercentile_AndNothingRoundsADoublePrecisionValue()
    {
        /* percentile_cont interpolates and returns double precision, so it reports a byte count no plan
           ever had and then needs a cast to survive a two-argument round — PostgreSQL has no
           round(double precision, integer), and an arm of a statement that errors takes the whole statement
           with it. percentile_disc returns a value that is IN the column and keeps its bigint type, so
           every byte figure this read prints is a size something really measured and is comparable to the
           cap without arithmetic. OversizedPlanBacklogReadLivePostgresTests proves the failure mode is
           real rather than remembered. */
        var roundWithPrecision = new Regex(@"round\s*\([^()]*(?:\([^()]*\)[^()]*)*,", RegexOptions.IgnoreCase);

        foreach (var sql in new[]
        {
            DarlingOversizedPlanBacklogReader.PerServerRollupSql,
            DarlingOversizedPlanBacklogReader.CollectorCensusSql,
        })
        {
            Assert.Contains("percentile_disc(0.5) WITHIN GROUP (ORDER BY b.observed_bytes)", sql, StringComparison.Ordinal);
            Assert.DoesNotContain("percentile_cont", sql, StringComparison.OrdinalIgnoreCase);
            Assert.DoesNotContain("double precision", sql, StringComparison.OrdinalIgnoreCase);
            Assert.False(roundWithPrecision.IsMatch(sql),
                "a two-argument round() appeared in a statement whose only float-producing candidate was "
                + "removed on purpose; PostgreSQL has no round(double precision, integer) and the whole "
                + "statement fails, not just that column");
        }
    }

    [Fact]
    public void TheRollup_KeepsRowsWhoseServerHasLeftTheRegistry()
    {
        /* Retention prunes this table on last_seen_at (DarlingRetention) and nothing prunes it when a
           server is removed or disabled, so an inner join or an is_enabled predicate would silently drop
           rows that are really in the table and hand back a total that is short — the same class of
           quiet undercount the read exists to remove. A row whose server_id no longer resolves reports a
           null server_name and still counts. */
        var rollup = DarlingOversizedPlanBacklogReader.PerServerRollupSql;

        Assert.Contains("LEFT JOIN servers AS s", rollup, StringComparison.Ordinal);
        Assert.DoesNotContain("is_enabled", rollup, StringComparison.Ordinal);

        /* And the census carries no registry join at all, so it cannot lose a row that way either. */
        Assert.DoesNotContain("JOIN servers", DarlingOversizedPlanBacklogReader.CollectorCensusSql, StringComparison.Ordinal);
    }

    /* ---- the statements address the shipped table ---------------------------------------------------- */

    [Fact]
    public void EveryColumnTheReadsName_ExistsInTheShippedDdl()
    {
        /* The same check the sighting upsert already carries, applied to the read half: the DDL and these
           statements live in different projects, so proximity proves nothing and a renamed column is a
           runtime 42703 on a surface nobody exercises until they need it. Derived from the SQL rather than
           listed, so a column added to a read is covered without editing this. */
        var columns = AllReadSql
            .SelectMany(sql => Regex.Matches(sql, @"\bb\.([a-z_]+)\b").Select(m => m.Groups[1].Value))
            .Distinct(StringComparer.Ordinal)
            .OrderBy(c => c, StringComparer.Ordinal)
            .ToArray();

        /* Sixteen columns in the table; the reads name every one of them except the content itself, which
           they only null-test. A floor rather than an equality so adding a column to the table does not
           red this, and high enough that a regex that stopped matching cannot pass. */
        Assert.True(columns.Length >= 15,
            "the alias scan found only " + columns.Length + " columns, so this pin is not reading the "
            + "statements any more: [" + string.Join(", ", columns) + "]");

        foreach (var column in columns)
        {
            Assert.Contains("    " + column + " ", OversizedPlanBacklog.CreateTableSql, StringComparison.Ordinal);
        }
    }

    [Fact]
    public void TheRowListing_CarriesTheClaimKeyAndBothFallbackKeys()
    {
        /* The listing's job is to make the fallback reachable: a query_hash for the query_stats fallback,
           a sql_handle for the procedure_stats one, and the whole six-column claim key so a row here can be
           matched to what the sweep reports about it. Miss one and the listing is a table of sizes. */
        var rows = DarlingOversizedPlanBacklogReader.RowsSql;

        foreach (var column in new[]
        {
            "b.collector_name", "b.plan_handle", "b.sql_handle",
            "b.statement_start_offset", "b.statement_end_offset",
            "b.database_name", "b.query_hash", "b.observed_bytes",
            "b.first_seen_at", "b.last_seen_at", "b.captured_at", "b.expired_at",
            "b.last_attempt_at", "b.attempt_count",
        })
        {
            Assert.Contains(column, rows, StringComparison.Ordinal);
        }

        /* Server-scoped, and bound rather than interpolated. */
        Assert.Contains("WHERE b.server_id = $1", rows, StringComparison.Ordinal);
        Assert.Contains("LIMIT $2", rows, StringComparison.Ordinal);
    }

    [Fact]
    public void TheListing_IsNotOrderedLikeTheClaim()
    {
        /* The claim takes oldest ATTEMPT first so nothing starves, and only breaks ties by size. Ordering
           the listing the same way would read as a prediction of the sweep's next picks while silently
           omitting the rows it will skip — a listing that answers a question it was not asked. Largest
           first answers the one a reader has, and the key columns in the tiebreak keep the page stable
           across calls. */
        var rows = DarlingOversizedPlanBacklogReader.RowsSql;
        var order = rows[(rows.IndexOf("ORDER BY", StringComparison.Ordinal) + 8)..];

        Assert.DoesNotContain("last_attempt_at", order, StringComparison.Ordinal);
        Assert.True(
            order.IndexOf("b.observed_bytes DESC", StringComparison.Ordinal) >= 0
            && order.IndexOf("b.observed_bytes DESC", StringComparison.Ordinal)
                < order.IndexOf("b.first_seen_at", StringComparison.Ordinal),
            "size no longer leads the listing's order, so it is neither the claim's order nor size-first: "
            + order);
    }

    /* ---- the cap the whole table is about ----------------------------------------------------------- */

    [Fact]
    public void TheCapIsReportedFromTheSharedConstant_AndTheProseAgreesWithIt()
    {
        /* The number a caller compares observed_bytes to has one declaration, in the collectors project
           beside the decision that produces these rows. The payload takes it from there. The tool
           DESCRIPTION cannot — an attribute argument must be a compile-time constant string and a const int
           does not concatenate into one — so the figure is written out there and held against the constant
           here instead, which is the only thing that stops a cap change leaving the prose an agent plans
           against confidently wrong. */
        var code = CSharpMemberMap.Of(ReadRepoFile(ToolSource)).Code;
        Assert.Contains(
            nameof(QueryPlanXmlCaptureLimits) + "." + nameof(QueryPlanXmlCaptureLimits.MaxCapturedPlanXmlBytes),
            code,
            StringComparison.Ordinal);

        var description = ToolMethod.GetCustomAttribute<DescriptionAttribute>()?.Description ?? "";
        Assert.Contains(
            QueryPlanXmlCaptureLimits.MaxCapturedPlanXmlBytes.ToString(CultureInfo.InvariantCulture),
            description,
            StringComparison.Ordinal);

        /* And it routes the caller onward: a listed query_hash is only useful because get_plan_xml resolves
           it through the over-cap fallback, and an agent that is not told so has a table of handles. */
        Assert.Contains("get_plan_xml", description, StringComparison.Ordinal);
    }

    /* ---- the refusals, exercised rather than read ---------------------------------------------------- */

    [Fact]
    public async Task IncludeRowsWithoutAServerName_IsRefusedRatherThanWidenedToTheFleet()
    {
        /* Both refusals run before the store is touched, so this exercises the real method against a data
           source that would fail to connect — which is also the proof that they run FIRST. A silently
           dropped include_rows would come back as a rollup with no listing and nothing to say why. */
        await using var unusable = UnusableDataSource();

        var refusal = await DarlingMcpOversizedPlanBacklogTools.GetOversizedPlanBacklog(
            unusable, server_name: null, include_rows: true);

        Assert.Equal("precondition", StatusOf(refusal));
        Assert.Contains("server_name", refusal, StringComparison.Ordinal);
    }

    [Theory]
    [InlineData(0)]
    [InlineData(-1)]
    [InlineData(1001)]
    public async Task AnUnusableLimit_IsRefused(int limit)
    {
        await using var unusable = UnusableDataSource();

        var refusal = await DarlingMcpOversizedPlanBacklogTools.GetOversizedPlanBacklog(
            unusable, server_name: null, include_rows: false, limit: limit);

        Assert.Contains("limit", refusal, StringComparison.Ordinal);
        Assert.DoesNotContain("by_collector", refusal, StringComparison.Ordinal);
    }

    /* ---- helpers ----------------------------------------------------------------------------------- */

    /// <summary>
    /// A data source pointed at nothing reachable. Constructing one opens no connection, so a refusal that
    /// runs BEFORE the store is touched returns normally and one that does not fails — which is the
    /// ordering being asserted, not merely the message.
    /// </summary>
    private static NpgsqlDataSource UnusableDataSource() =>
        NpgsqlDataSource.Create("Host=127.0.0.1;Port=1;Username=nobody;Database=nothing;Timeout=1;Pooling=false");

    private static string? StatusOf(string json) =>
        JsonDocument.Parse(json).RootElement.TryGetProperty("status", out var status) ? status.GetString() : null;

    /// <summary>
    /// Lowercased, alias-stripped, whitespace-collapsed — so a predicate written across two lines under one
    /// alias compares equal to the same predicate written on one line under another. The comparison is
    /// about which rows are selected, and neither of those changes that.
    /// </summary>
    private static string Normalize(string sql) =>
        Regex.Replace(sql.Replace("b.", "", StringComparison.Ordinal), @"\s+", " ").ToLowerInvariant();
}
