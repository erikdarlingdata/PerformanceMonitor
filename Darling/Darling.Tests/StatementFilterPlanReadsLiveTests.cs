/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using System.Diagnostics;
using System.Globalization;
using System.Text;
using System.Text.Json;
using System.Threading;
using System.Threading.Tasks;
using Npgsql;
using NpgsqlTypes;
using PerformanceMonitor.Collectors;
using PerformanceMonitor.Common;
using PerformanceMonitor.Darling.Service;
using PerformanceMonitor.Darling.Service.Mcp;
using PerformanceMonitor.Darling.Storage;
using Xunit;

namespace Darling.Tests;

/// <summary>
/// #4348, Layer 1: every stored-plan read in <c>DarlingStoredPlanReader</c> answers with the plan the statement filter judged, so
/// a tool that reads a stored plan cannot hand the canary statement, or the values its plan carries, to a caller. Each case is
/// planted with <see cref="StatementScrubCanary.CanaryPlan"/> in the form the store keeps it, then read through the public
/// method: the stored query_stats and procedure_stats plan inline and in the gzip <c>query_plan_dim</c> row, a Query Store plan
/// through <c>query_store_plan_map</c> (text and gzip) and through the inline fallback, the snapshot's <c>query_plan</c> and
/// <c>live_query_plan</c>, and the blocked, blocking and deadlock-victim plans. The cut cases read a plan larger than the
/// tools' 500 KB transport limit and check the cut is made on the filtered plan.
/// </summary>
[Collection("live-postgres")]
public sealed class StatementFilterPlanReadsLiveTests
{
    private const string ServerName = "darling-ssf-plan-reads";
    private static readonly int ServerId = ServerIdHelper.GetDeterministicHashCode(ServerName);
    private const string Db = "SsfPlanDb";
    private static readonly DateTime Anchor = new DateTime(2026, 3, 4, 5, 6, 7, DateTimeKind.Unspecified).AddTicks(1_234_560);

    private static string? ConnectionString => Environment.GetEnvironmentVariable("DARLING_TEST_PG");

    /// <summary>The canary plan, made distinct per case (two cases that stored the same plan would share one dimension row,
    /// and a text row and a gzip row cannot both be the one row).</summary>
    private static string Variant(int n) => StatementScrubCanary.CanaryPlan().Replace("16.0.4000.1\"", "16.0.4000." + n + "\"", StringComparison.Ordinal);

    private static void AssertFiltered(string label, string raw, string? filtered)
    {
        Assert.True(filtered is not null, label + ": the read found no plan (seeding or key is wrong)");
        StatementFilterCensus.AssertRawHoldsTheCanary(raw);
        try
        {
            StatementFilterCensus.AssertPlanFilteredKeepsTheRest(raw, filtered!);
        }
        catch (Exception ex)
        {
            throw new Xunit.Sdk.XunitException(label + ": " + ex.Message);
        }
    }

    [Fact]
    public async Task EveryStoredPlanRead_ReturnsThePlanWithTheCanaryStatementWithheld()
    {
        var cs = ConnectionString;
        Assert.SkipWhen(string.IsNullOrEmpty(cs), "Set DARLING_TEST_PG to a Postgres connection string to run the live plan-read filter test.");
        var ct = TestContext.Current.CancellationToken;

        await using var connection = new NpgsqlConnection(cs);
        await connection.OpenAsync(ct);
        await PgMigrations.MigrateAsync(connection, ct);
        await DeleteRowsAsync(connection, ct);
        await using var postgres = NpgsqlDataSource.Create(cs!);

        var bodySucceeded = false;
        try
        {
            await RegisterServerAsync(connection, ct);

            string statsInline = Variant(1), statsGz = Variant(2), procInline = Variant(3), procGz = Variant(4);
            string qsMapText = Variant(5), qsMapGz = Variant(6), qsInline = Variant(7);
            string snapshot = Variant(8), snapshotLive = Variant(9);
            string blocked = Variant(10), blocking = Variant(11), victim = Variant(12);

            /* query_stats and procedure_stats: the plan inline on the row, and in the gzip query_plan_dim row the row's digest names. */
            await InsertStatsAsync(connection, "query_stats", Anchor, "0xQSINLINE", null, statsInline, null, ct);
            await InsertDimAsync(connection, statsGz, gz: true, ct);
            await InsertStatsAsync(connection, "query_stats", Anchor, "0xQSGZ", null, null, statsGz, ct);
            await InsertStatsAsync(connection, "procedure_stats", Anchor, null, "0xA0A0A001", procInline, null, ct);
            await InsertDimAsync(connection, procGz, gz: true, ct);
            await InsertStatsAsync(connection, "procedure_stats", Anchor, null, "0xA0A0A002", null, procGz, ct);

            /* Query Store: through the map to a text and a gzip dimension row, and the inline column with no map row. */
            await InsertQueryStoreAsync(connection, Anchor, queryId: 1, planId: 11, inline: null, ct);
            await InsertDimAsync(connection, qsMapText, gz: false, ct);
            await InsertMapAsync(connection, 11, qsMapText, ct);
            await InsertQueryStoreAsync(connection, Anchor, queryId: 2, planId: 21, inline: null, ct);
            await InsertDimAsync(connection, qsMapGz, gz: true, ct);
            await InsertMapAsync(connection, 21, qsMapGz, ct);
            await InsertQueryStoreAsync(connection, Anchor, queryId: 3, planId: 31, inline: qsInline, ct);

            await InsertSnapshotAsync(connection, Anchor, 51, 3, snapshot, snapshotLive, ct);
            await InsertReportAsync(connection, Anchor, 61, 62, blocked, blocking, ct);
            var deadlockCollected = Anchor.AddMinutes(10);
            var deadlockStamp = deadlockCollected.AddSeconds(-7);
            await InsertDeadlockAsync(connection, deadlockCollected, deadlockStamp, "process1a2b", victim, ct);

            AssertFiltered("query_stats inline", statsInline,
                await DarlingStoredPlanReader.GetQueryStatsPlanXmlByHashAsync(postgres, ServerId, "0xQSINLINE", Db, cancellationToken: ct));
            AssertFiltered("query_stats gz dim", statsGz,
                await DarlingStoredPlanReader.GetQueryStatsPlanXmlByHashAsync(postgres, ServerId, "0xQSGZ", null, cancellationToken: ct));
            AssertFiltered("procedure inline", procInline,
                await DarlingStoredPlanReader.GetProcedurePlanXmlBySqlHandleAsync(postgres, ServerId, "0xA0A0A001", cancellationToken: ct));
            AssertFiltered("procedure gz dim", procGz,
                await DarlingStoredPlanReader.GetProcedurePlanXmlBySqlHandleAsync(postgres, ServerId, "0xA0A0A002", cancellationToken: ct));

            AssertFiltered("query store map text (text read)", qsMapText,
                await DarlingStoredPlanReader.GetQueryStorePlanTextAsync(postgres, ServerId, Db, 1, null, cancellationToken: ct));
            AssertFiltered("query store map gz (text read, pinned)", qsMapGz,
                await DarlingStoredPlanReader.GetQueryStorePlanTextAsync(postgres, ServerId, Db, 2, 21, cancellationToken: ct));
            AssertFiltered("query store inline fallback (text read)", qsInline,
                await DarlingStoredPlanReader.GetQueryStorePlanTextAsync(postgres, ServerId, Db, 3, null, cancellationToken: ct));
            var resolved = await DarlingStoredPlanReader.ResolveQueryStorePlanAsync(postgres, ServerId, Db, 1, null, cancellationToken: ct);
            AssertFiltered("query store map text (resolve)", qsMapText, resolved?.PlanXml);
            Assert.Equal(11, resolved!.PlanId);
            AssertFiltered("query store inline fallback (resolve)", qsInline,
                (await DarlingStoredPlanReader.ResolveQueryStorePlanAsync(postgres, ServerId, Db, 3, 31, cancellationToken: ct))?.PlanXml);
            AssertFiltered("query store map gz (by id)", qsMapGz,
                await DarlingStoredPlanReader.ReadQueryStorePlanByIdAsync(postgres, ServerId, Db, 2, 21, cancellationToken: ct));
            AssertFiltered("query store inline fallback (by id)", qsInline,
                await DarlingStoredPlanReader.ReadQueryStorePlanByIdAsync(postgres, ServerId, Db, 3, 31, cancellationToken: ct));

            AssertFiltered("snapshot query_plan", snapshot,
                await DarlingStoredPlanReader.GetQuerySnapshotPlanXmlAsync(postgres, ServerId, Anchor, 51, 3, live: false, cancellationToken: ct));
            AssertFiltered("snapshot live_query_plan", snapshotLive,
                await DarlingStoredPlanReader.GetQuerySnapshotPlanXmlAsync(postgres, ServerId, Anchor, 51, 3, live: true, cancellationToken: ct));

            AssertFiltered("blocked plan", blocked,
                await DarlingStoredPlanReader.GetBlockingPlanXmlAsync(postgres, ServerId, Anchor, 61, 0, 62, 0, blockingSide: false, cancellationToken: ct));
            AssertFiltered("blocking plan", blocking,
                await DarlingStoredPlanReader.GetBlockingPlanXmlAsync(postgres, ServerId, Anchor, 61, 0, 62, 0, blockingSide: true, cancellationToken: ct));
            var deadlock = await DarlingStoredPlanReader.GetDeadlockVictimPlanXmlAsync(postgres, ServerId, deadlockCollected, deadlockStamp, "process1a2b", cancellationToken: ct);
            Assert.False(deadlock.Ambiguous);
            AssertFiltered("deadlock victim plan", victim, deadlock.PlanXml);

            /* The tools that read through the seam: the same plan, filtered, and the analysis of it holds no secret. */
            AssertFiltered("get_plan_xml", statsInline, await DarlingMcpPlanTools.GetPlanXml(postgres, "0xQSINLINE", ServerName, cancellationToken: ct));
            string analysis = await DarlingMcpPlanTools.AnalyzeQueryPlan(postgres, "0xQSINLINE", ServerName, cancellationToken: ct);
            foreach (string needle in StatementScrubCanary.SecretNeedles)
                Assert.DoesNotContain(needle, analysis, StringComparison.Ordinal);
            using (var doc = JsonDocument.Parse(analysis))
                Assert.Contains(StatementFilterCensus.Marker, doc.RootElement.ToString(), StringComparison.Ordinal);

            bodySucceeded = true;
        }
        finally
        {
            await LiveStoreCleanup.RunAsync(cs!, bodySucceeded, async (cleanup, cleanupCt) =>
                await DeleteRowsAsync(cleanup, cleanupCt));
        }
    }

    /// <summary>
    /// A plan over the tools' 500 KB limit whose FIRST statement is the canary. The filter runs on the plan and the cut is made on
    /// what it returns, so the answer is a recognizable cut plan, ending in the truncation marker, with that statement withheld
    /// by its own placeholder. It is not the whole-output placeholder a sweep of the cut text would have to fall back to. The
    /// same plan read with the reader's default (no cut) comes back whole, with the same statement withheld.
    /// </summary>
    [Fact]
    public async Task APlanOverTheTransportLimit_WithTheCanaryFirst_IsCutOnTheFilteredPlan_NotWithheldWhole()
    {
        var cs = ConnectionString;
        Assert.SkipWhen(string.IsNullOrEmpty(cs), "Set DARLING_TEST_PG to a Postgres connection string to run the live plan-read filter test.");
        var ct = TestContext.Current.CancellationToken;

        await using var connection = new NpgsqlConnection(cs);
        await connection.OpenAsync(ct);
        await PgMigrations.MigrateAsync(connection, ct);
        await DeleteRowsAsync(connection, ct);
        await using var postgres = NpgsqlDataSource.Create(cs!);

        var bodySucceeded = false;
        try
        {
            await RegisterServerAsync(connection, ct);

            string big = BigPlan(600_000, canaryFirst: true);
            Assert.True(big.Length > 600_000);
            await InsertStatsAsync(connection, "query_stats", Anchor, "0xBIGHIT", null, big, null, ct);

            string cut = await DarlingMcpPlanTools.GetPlanXml(postgres, "0xBIGHIT", ServerName, cancellationToken: ct);
            Assert.NotEqual(SensitiveStatements.PlaceholderText, cut);
            Assert.StartsWith("<ShowPlanXML", cut, StringComparison.Ordinal);
            Assert.EndsWith("... (truncated)", cut, StringComparison.Ordinal);
            Assert.True(cut.Length <= 512_000 + "... (truncated)".Length + 8, "the cut plan is " + cut.Length + " characters");
            foreach (string needle in StatementScrubCanary.SecretNeedles)
                Assert.DoesNotContain(needle, cut, StringComparison.Ordinal);
            Assert.Contains(StatementFilterCensus.Marker, cut, StringComparison.Ordinal);
            Assert.Contains("canary_plain_ssf", cut, StringComparison.Ordinal);

            string whole = (await DarlingStoredPlanReader.GetQueryStatsPlanXmlByHashAsync(postgres, ServerId, "0xBIGHIT", null, cancellationToken: ct))!;
            Assert.True(whole.Length > 512_000);
            foreach (string needle in StatementScrubCanary.SecretNeedles)
                Assert.DoesNotContain(needle, whole, StringComparison.Ordinal);
            Assert.Contains(StatementFilterCensus.Marker, whole, StringComparison.Ordinal);
            Assert.EndsWith("</ShowPlanXML>", whole, StringComparison.Ordinal);

            bodySucceeded = true;
        }
        finally
        {
            await LiveStoreCleanup.RunAsync(cs!, bodySucceeded, async (cleanup, cleanupCt) =>
                await DeleteRowsAsync(cleanup, cleanupCt));
        }
    }

    /// <summary>
    /// The answer a client receives for a stored plan over the tools' 500 KB limit, at the two wire seams: the host's registered
    /// filters (MCP) and the web read's <c>ToHttpResult</c>. A plan with no auto-parameter token comes back as a cut, filtered plan
    /// ending in the truncation marker, with the named statement's placeholder inside. The same plan padded with auto-parameter
    /// tokens (<c>@1</c>) comes back as the whole-output placeholder: the cut plan does not parse, so the second check cannot
    /// probe it and withholds it whole (fails closed, accepted behaviour).
    /// </summary>
    [Fact]
    public async Task APlanOverTheTransportLimit_OnTheWire_IsACutFilteredPlanWithoutAutoParameterTokens_AndThePlaceholderWithThem()
    {
        var cs = ConnectionString;
        Assert.SkipWhen(string.IsNullOrEmpty(cs), "Set DARLING_TEST_PG to a Postgres connection string to run the live plan-read filter test.");
        var ct = TestContext.Current.CancellationToken;

        await using var connection = new NpgsqlConnection(cs);
        await connection.OpenAsync(ct);
        await PgMigrations.MigrateAsync(connection, ct);
        await DeleteRowsAsync(connection, ct);
        await using var postgres = NpgsqlDataSource.Create(cs!);
        using var host = await StatementFilterCensus.BuildHostAsync();

        var bodySucceeded = false;
        try
        {
            await RegisterServerAsync(connection, ct);
            await InsertStatsAsync(connection, "query_stats", Anchor, "0xWIRENOTOKEN", null, BigPlan(600_000, canaryFirst: true), null, ct);
            await InsertStatsAsync(connection, "query_stats", Anchor, "0xWIRETOKENS", null, BigPlan(600_000, canaryFirst: true, autoParameterTokens: true), null, ct);

            /* No tokens: the cut, filtered plan, on both wires. */
            string noTokens = await DarlingMcpPlanTools.GetPlanXml(postgres, "0xWIRENOTOKEN", ServerName, cancellationToken: ct);
            string mcpCut = (await StatementFilterCensus.FilterThroughHostAsync(host, noTokens)).Text;
            AssertCutFilteredPlan("host filter, no tokens", mcpCut, markerSuffix: true);
            /* The web wrap moves the "... (truncated)" suffix into a truncated flag, so the plan_xml text is the cut plan without it. */
            using var webCutDoc = JsonDocument.Parse(await WebAnswerAsync(noTokens, "0xWIRENOTOKEN"));
            Assert.True(webCutDoc.RootElement.GetProperty("truncated").GetBoolean());
            AssertCutFilteredPlan("web read, no tokens", webCutDoc.RootElement.GetProperty("plan_xml").GetString()!, markerSuffix: false);

            /* Tokens: the tool's own answer is the cut plan; the second check withholds it whole on both wires. */
            string withTokens = await DarlingMcpPlanTools.GetPlanXml(postgres, "0xWIRETOKENS", ServerName, cancellationToken: ct);
            Assert.EndsWith("... (truncated)", withTokens, StringComparison.Ordinal);
            string mcpHeld = (await StatementFilterCensus.FilterThroughHostAsync(host, withTokens)).Text;
            Assert.Equal(SensitiveStatements.PlaceholderText, mcpHeld);
            string webHeld = JsonDocument.Parse(await WebAnswerAsync(withTokens, "0xWIRETOKENS")).RootElement.GetProperty("plan_xml").GetString()!;
            Assert.Equal(SensitiveStatements.PlaceholderText, webHeld);

            bodySucceeded = true;
        }
        finally
        {
            await LiveStoreCleanup.RunAsync(cs!, bodySucceeded, async (cleanup, cleanupCt) =>
                await DeleteRowsAsync(cleanup, cleanupCt));
        }
    }

    private static void AssertCutFilteredPlan(string label, string answer, bool markerSuffix)
    {
        Assert.True(answer.StartsWith("<ShowPlanXML", StringComparison.Ordinal), label + ": not a plan: " + answer[..Math.Min(80, answer.Length)]);
        Assert.Equal(markerSuffix, answer.EndsWith("... (truncated)", StringComparison.Ordinal));
        Assert.True(answer.Length >= 500_000, label + ": not a cut plan (" + answer.Length + " chars)");
        Assert.False(answer.EndsWith("</ShowPlanXML>", StringComparison.Ordinal), label + ": the whole plan came back");
        Assert.Contains(StatementFilterCensus.Marker, answer, StringComparison.Ordinal);
        foreach (string needle in StatementScrubCanary.SecretNeedles)
            Assert.DoesNotContain(needle, answer, StringComparison.Ordinal);
    }

    /// <summary>What the web plan read sends for a tool answer: the wrapped answer through <c>ToHttpResult</c>.</summary>
    private static async Task<string> WebAnswerAsync(string toolAnswer, string queryHash)
    {
        var result = DarlingWebEndpoints.ToHttpResult(
            DarlingWebEndpoints.WrapPlanXml(toolAnswer, queryHash, null), "/api/read/get_plan_xml",
            Microsoft.Extensions.Logging.Abstractions.NullLogger.Instance, 1);
        var services = new Microsoft.Extensions.DependencyInjection.ServiceCollection();
        Microsoft.Extensions.DependencyInjection.LoggingServiceCollectionExtensions.AddLogging(services);
        var context = new Microsoft.AspNetCore.Http.DefaultHttpContext
        {
            RequestServices = Microsoft.Extensions.DependencyInjection.ServiceCollectionContainerBuilderExtensions.BuildServiceProvider(services),
        };
        context.Response.Body = new System.IO.MemoryStream();
        await result.ExecuteAsync(context);
        return Encoding.UTF8.GetString(((System.IO.MemoryStream)context.Response.Body).ToArray());
    }

    /// <summary>
    /// A 20 MB stored plan read through <c>get_plan_xml</c>, with the sensitive statement at the start and with none: the filter
    /// is told where the tool will cut, so neither shape walks the whole document. The ceiling is loose on purpose (a slow
    /// runner is not a failure); it catches a read that goes back to filtering all 20 MB.
    /// </summary>
    [Fact]
    public async Task ATwentyMegabytePlan_ReadThroughGetPlanXml_StaysInsideTheCeiling_WithAndWithoutAHit()
    {
        var cs = ConnectionString;
        Assert.SkipWhen(string.IsNullOrEmpty(cs), "Set DARLING_TEST_PG to a Postgres connection string to run the live plan-read filter test.");
        var ct = TestContext.Current.CancellationToken;

        await using var connection = new NpgsqlConnection(cs);
        await connection.OpenAsync(ct);
        await PgMigrations.MigrateAsync(connection, ct);
        await DeleteRowsAsync(connection, ct);
        await using var postgres = NpgsqlDataSource.Create(cs!);

        var bodySucceeded = false;
        try
        {
            await RegisterServerAsync(connection, ct);
            await InsertStatsAsync(connection, "query_stats", Anchor, "0xTWENTYHIT", null, BigPlan(20_000_000, canaryFirst: true), null, ct);
            await InsertStatsAsync(connection, "query_stats", Anchor, "0xTWENTYMISS", null, BigPlan(20_000_000, canaryFirst: false), null, ct);

            var timings = new StringBuilder();
            foreach (string hash in new[] { "0xTWENTYHIT", "0xTWENTYMISS" })
            {
                /* The same read before this change: the stored text and the tool's own cut, with nothing between. */
                var before = Stopwatch.StartNew();
                string raw;
                await using (var command = new NpgsqlCommand("SELECT query_plan_xml FROM query_stats WHERE server_id = $1 AND query_hash = $2", connection))
                {
                    command.Parameters.AddWithValue(ServerId);
                    command.Parameters.AddWithValue(hash);
                    raw = (string)(await command.ExecuteScalarAsync(ct))!;
                }

                string unfiltered = McpHelpers.Truncate(raw, 512_000)!;
                before.Stop();

                var after = Stopwatch.StartNew();
                string answer = await DarlingMcpPlanTools.GetPlanXml(postgres, hash, ServerName, cancellationToken: ct);
                after.Stop();

                timings.Append(CultureInfo.InvariantCulture, $"{hash}: unfiltered {before.ElapsedMilliseconds} ms, filtered {after.ElapsedMilliseconds} ms; ");
                Assert.EndsWith("... (truncated)", answer, StringComparison.Ordinal);
                Assert.NotEqual(SensitiveStatements.PlaceholderText, answer);
                Assert.True(after.Elapsed < TimeSpan.FromSeconds(15), timings.ToString());
                Assert.Equal(hash == "0xTWENTYHIT", answer.Contains(StatementFilterCensus.Marker, StringComparison.Ordinal));
                Assert.Equal(hash == "0xTWENTYMISS", answer == unfiltered);
            }

            if (Environment.GetEnvironmentVariable("SSF_L5A_TIMINGS") == "1")
                Assert.Fail(timings.ToString());

            bodySucceeded = true;
        }
        finally
        {
            await LiveStoreCleanup.RunAsync(cs!, bodySucceeded, async (cleanup, cleanupCt) =>
                await DeleteRowsAsync(cleanup, cleanupCt));
        }
    }

    /// <summary>A plan of at least <paramref name="minLength"/> characters: the canary plan's statements first (when
    /// <paramref name="canaryFirst"/>; the plain statement otherwise), then plain padding statements.</summary>
    private static string BigPlan(int minLength, bool canaryFirst, bool autoParameterTokens = false)
    {
        string canary = StatementScrubCanary.CanaryPlan();
        int open = canary.IndexOf("<StmtSimple", StringComparison.Ordinal);
        int second = canary.IndexOf("<StmtSimple", open + 1, StringComparison.Ordinal);
        int third = canary.IndexOf("<StmtSimple", second + 1, StringComparison.Ordinal);
        string head = canary.Substring(0, open);
        string first = canaryFirst ? canary.Substring(open, second - open) : string.Empty;
        string plain = canary.Substring(second, third - second);
        const string Tail = "</Statements></Batch></BatchSequence></ShowPlanXML>";

        var sb = new StringBuilder(minLength + 1024);
        sb.Append(head).Append(first).Append(plain);
        /* The pad's parameter is the auto-parameter shape (@1) when asked, so the cut text holds a token as well as the
           plain statement's ParameterCompiledValue. */
        string pad = autoParameterTokens ? "@1" : "@c";
        for (int i = 10; sb.Length < minLength; i++)
            sb.Append("<StmtSimple StatementText=\"SELECT canary_pad_ssf FROM dbo.pad WHERE id = ").Append(pad).Append("\" StatementId=\"").Append(i).Append("\" StatementType=\"SELECT\" />");
        return sb.Append(Tail).ToString();
    }

    // ── seeding ──

    private static async Task RegisterServerAsync(NpgsqlConnection connection, CancellationToken ct)
    {
        await using var command = new NpgsqlCommand(@"
INSERT INTO servers (server_id, server_name, display_name, is_enabled, created_date, modified_date)
VALUES ($1, $2, $2, TRUE, $3, $3)
ON CONFLICT (server_id) DO UPDATE SET server_name = EXCLUDED.server_name, display_name = EXCLUDED.display_name,
    is_enabled = TRUE, modified_date = EXCLUDED.modified_date;", connection);
        command.Parameters.AddWithValue(ServerId);
        command.Parameters.AddWithValue(ServerName);
        command.Parameters.AddWithValue(DateTime.SpecifyKind(Anchor, DateTimeKind.Unspecified));
        await command.ExecuteNonQueryAsync(ct);
    }

    /// <summary>One query_stats or procedure_stats row carrying the plan inline (<paramref name="inline"/>) or by the digest of a
    /// dimension row (<paramref name="dimPlan"/>, which <see cref="InsertDimAsync"/> stored).</summary>
    private static async Task InsertStatsAsync(
        NpgsqlConnection connection, string table, DateTime collectionTime, string? queryHash, string? sqlHandle,
        string? inline, string? dimPlan, CancellationToken ct)
    {
        string columns = table == "query_stats"
            ? "query_hash, sql_handle, plan_handle"
            : "schema_name, object_name, sql_handle";
        string values = table == "query_stats" ? "$5, $5, $5" : "'dbo', 'usp_ssf', $5";
        await using var command = new NpgsqlCommand($@"
INSERT INTO {table}
    (collection_id, collection_time, server_id, server_name, database_name, {columns}, query_plan_xml, query_plan_digest)
VALUES ($1, $2, $3, $4, '{Db}', {values}, $6, $7)", connection);
        command.Parameters.AddWithValue(CollectionIdGenerator.Next());
        command.Parameters.AddWithValue(DateTime.SpecifyKind(collectionTime, DateTimeKind.Unspecified));
        command.Parameters.AddWithValue(ServerId);
        command.Parameters.AddWithValue(ServerName);
        command.Parameters.AddWithValue((object?)queryHash ?? (object?)sqlHandle ?? DBNull.Value);
        command.Parameters.Add(new NpgsqlParameter { NpgsqlDbType = NpgsqlDbType.Text, Value = (object?)inline ?? DBNull.Value });
        command.Parameters.Add(new NpgsqlParameter
        {
            NpgsqlDbType = NpgsqlDbType.Bytea,
            Value = dimPlan is null ? DBNull.Value : PayloadDimensions.Digest(dimPlan),
        });
        await command.ExecuteNonQueryAsync(ct);
    }

    private static async Task InsertDimAsync(NpgsqlConnection connection, string plan, bool gz, CancellationToken ct)
    {
        await using var command = new NpgsqlCommand(
            gz
                ? "INSERT INTO query_plan_dim (digest, query_plan_gz, last_seen) VALUES ($1, $2, $3) ON CONFLICT DO NOTHING"
                : "INSERT INTO query_plan_dim (digest, query_plan_xml, last_seen) VALUES ($1, $2, $3) ON CONFLICT DO NOTHING",
            connection);
        command.Parameters.AddWithValue(PayloadDimensions.Digest(plan));
        if (gz)
            command.Parameters.AddWithValue(PayloadDimensions.CompressContent(plan));
        else
            command.Parameters.AddWithValue(plan);
        command.Parameters.AddWithValue(DateTime.SpecifyKind(Anchor, DateTimeKind.Unspecified));
        await command.ExecuteNonQueryAsync(ct);
    }

    private static async Task InsertQueryStoreAsync(
        NpgsqlConnection connection, DateTime collectionTime, long queryId, long planId, string? inline, CancellationToken ct)
    {
        await using var command = new NpgsqlCommand(@"
INSERT INTO query_store_stats
    (collection_id, collection_time, server_id, server_name, database_name, query_id, plan_id, query_plan_text)
VALUES ($1, $2, $3, $4, $5, $6, $7, $8)", connection);
        command.Parameters.AddWithValue(CollectionIdGenerator.Next());
        command.Parameters.AddWithValue(DateTime.SpecifyKind(collectionTime, DateTimeKind.Unspecified));
        command.Parameters.AddWithValue(ServerId);
        command.Parameters.AddWithValue(ServerName);
        command.Parameters.AddWithValue(Db);
        command.Parameters.AddWithValue(queryId);
        command.Parameters.AddWithValue(planId);
        command.Parameters.Add(new NpgsqlParameter { NpgsqlDbType = NpgsqlDbType.Text, Value = (object?)inline ?? DBNull.Value });
        await command.ExecuteNonQueryAsync(ct);
    }

    private static async Task InsertMapAsync(NpgsqlConnection connection, long planId, string plan, CancellationToken ct)
    {
        await using var command = new NpgsqlCommand(
            "INSERT INTO collect.query_store_plan_map (server_id, database_name, plan_id, digest, plan_hash, last_seen) VALUES ($1, $2, $3, $4, $5, $6)",
            connection);
        command.Parameters.AddWithValue(ServerId);
        command.Parameters.AddWithValue(Db);
        command.Parameters.AddWithValue(planId);
        command.Parameters.Add(new NpgsqlParameter { NpgsqlDbType = NpgsqlDbType.Bytea, Value = PayloadDimensions.Digest(plan) });
        command.Parameters.AddWithValue("0x5257");
        command.Parameters.AddWithValue(DateTime.SpecifyKind(Anchor, DateTimeKind.Unspecified));
        await command.ExecuteNonQueryAsync(ct);
    }

    private static async Task InsertSnapshotAsync(
        NpgsqlConnection connection, DateTime collectionTime, int sessionId, int requestId, string plan, string livePlan, CancellationToken ct)
    {
        await using var command = new NpgsqlCommand(@"
INSERT INTO query_snapshots
    (collection_id, collection_time, server_id, server_name, session_id, request_id, query_plan, live_query_plan)
VALUES ($1, $2, $3, $4, $5, $6, $7, $8)", connection);
        command.Parameters.AddWithValue(CollectionIdGenerator.Next());
        command.Parameters.AddWithValue(DateTime.SpecifyKind(collectionTime, DateTimeKind.Unspecified));
        command.Parameters.AddWithValue(ServerId);
        command.Parameters.AddWithValue(ServerName);
        command.Parameters.AddWithValue(sessionId);
        command.Parameters.AddWithValue(requestId);
        command.Parameters.AddWithValue(plan);
        command.Parameters.AddWithValue(livePlan);
        await command.ExecuteNonQueryAsync(ct);
    }

    private static async Task InsertReportAsync(
        NpgsqlConnection connection, DateTime eventTime, int blockedSpid, int blockingSpid, string blockedPlan, string blockingPlan, CancellationToken ct)
    {
        await using var command = new NpgsqlCommand(@"
INSERT INTO blocked_process_reports
    (blocked_report_id, collection_time, server_id, server_name, event_time, wait_time_ms, blocking_spid, blocked_spid,
     blocked_ecid, blocking_ecid, blocking_status, database_name, blocked_query_plan_xml, blocking_query_plan_xml)
VALUES ($1, $2, $3, $4, $5, 12000, $6, $7, 0, 0, 'suspended', $8, $9, $10)", connection);
        command.Parameters.AddWithValue(CollectionIdGenerator.Next());
        command.Parameters.AddWithValue(DateTime.SpecifyKind(eventTime.AddSeconds(5), DateTimeKind.Unspecified));
        command.Parameters.AddWithValue(ServerId);
        command.Parameters.AddWithValue(ServerName);
        command.Parameters.AddWithValue(DateTime.SpecifyKind(eventTime, DateTimeKind.Unspecified));
        command.Parameters.AddWithValue(blockingSpid);
        command.Parameters.AddWithValue(blockedSpid);
        command.Parameters.AddWithValue(Db);
        command.Parameters.AddWithValue(blockedPlan);
        command.Parameters.AddWithValue(blockingPlan);
        await command.ExecuteNonQueryAsync(ct);
    }

    private static async Task InsertDeadlockAsync(
        NpgsqlConnection connection, DateTime collectionTime, DateTime deadlockTime, string victimProcessId, string victimPlan, CancellationToken ct)
    {
        await using var command = new NpgsqlCommand(@"
INSERT INTO deadlocks
    (deadlock_id, collection_time, server_id, server_name, deadlock_time, victim_process_id, victim_query_plan_xml, database_name)
VALUES ($1, $2, $3, $4, $5, $6, $7, $8)", connection);
        command.Parameters.AddWithValue(CollectionIdGenerator.Next());
        command.Parameters.AddWithValue(DateTime.SpecifyKind(collectionTime, DateTimeKind.Unspecified));
        command.Parameters.AddWithValue(ServerId);
        command.Parameters.AddWithValue(ServerName);
        command.Parameters.AddWithValue(DateTime.SpecifyKind(deadlockTime, DateTimeKind.Unspecified));
        command.Parameters.AddWithValue(victimProcessId);
        command.Parameters.AddWithValue(victimPlan);
        command.Parameters.AddWithValue(Db);
        await command.ExecuteNonQueryAsync(ct);
    }

    private static async Task DeleteRowsAsync(NpgsqlConnection connection, CancellationToken ct)
    {
        await using (var cleanup = new NpgsqlCommand(
            $"DELETE FROM blocked_process_reports WHERE server_id = {ServerId}; " +
            $"DELETE FROM deadlocks WHERE server_id = {ServerId}; " +
            $"DELETE FROM query_snapshots WHERE server_id = {ServerId}; " +
            $"DELETE FROM query_store_stats WHERE server_id = {ServerId}; " +
            $"DELETE FROM collect.query_store_plan_map WHERE server_id = {ServerId}; " +
            $"DELETE FROM query_stats WHERE server_id = {ServerId}; " +
            $"DELETE FROM procedure_stats WHERE server_id = {ServerId}; " +
            $"DELETE FROM servers WHERE server_id = {ServerId};", connection))
        {
            await cleanup.ExecuteNonQueryAsync(ct);
        }

        /* The dimension rows are keyed by digest, so they go by the plans this class planted. */
        for (int n = 1; n <= 12; n++)
        {
            await using var dim = new NpgsqlCommand("DELETE FROM query_plan_dim WHERE digest = $1", connection);
            dim.Parameters.AddWithValue(PayloadDimensions.Digest(Variant(n)));
            await dim.ExecuteNonQueryAsync(ct);
        }
    }
}
