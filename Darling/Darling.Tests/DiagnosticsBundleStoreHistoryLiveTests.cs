/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using System.Globalization;
using System.IO;
using System.Linq;
using System.Runtime.CompilerServices;
using System.Text.Json.Nodes;
using System.Threading;
using System.Threading.Tasks;
using Npgsql;
using PerformanceMonitor.Collectors;
using PerformanceMonitor.Darling.Service;
using PerformanceMonitor.Darling.Service.Mcp;
using PerformanceMonitor.Darling.Storage;
using Xunit;

namespace Darling.Tests;

/// <summary>
/// #5097: the diagnostics bundle's statement-history member reads through <c>get_store_query_history</c>, not through the
/// two V163 tables. Against a real server: the member is the tool's ranked answer for the same hours (up to the cut the bundle
/// makes after it has aliased the text), a window the tool cannot serve is clamped and says so, a store before V163 still
/// reads <c>not_present</c>, and an error the tool answers is the member as it is. Each fact mints its own scratch database.
/// </summary>
[Collection("live-postgres")]
public sealed class DiagnosticsBundleStoreHistoryLiveTests
{
    private static string? ConnectionString => Environment.GetEnvironmentVariable("DARLING_TEST_PG");

    private static readonly DateTime Now = DateTime.UtcNow;

    /// <summary>More than 240 characters of text once pg_stat_statements has stored its literals as <c>$n</c>, so the tool's preview cuts it.</summary>
    private static readonly string LongStatement =
        $"SELECT 1 AS {new string('l', 60)}, 2 AS {new string('m', 60)}, 3 AS {new string('n', 60)}, 4 AS {new string('o', 60)}, 5 AS tail_marker";

    private const string None = "false, false, false";

    private static string Ts(double hoursAgo) =>
        "'" + DateTime.SpecifyKind(Now.AddHours(-hoursAgo), DateTimeKind.Unspecified).ToString("yyyy-MM-dd HH:mm:ss.ffffff", CultureInfo.InvariantCulture) + "'";

    private static async Task<(ScratchPostgres Scratch, NpgsqlDataSource Source)> StartAsync(CancellationToken ct, bool migrate = true)
    {
        Assert.SkipWhen(string.IsNullOrEmpty(ConnectionString), "Set DARLING_TEST_PG to run the bundle's statement-history live pins (each mints its own scratch database).");
        var scratch = await ScratchPostgres.CreateAsync(ConnectionString!, ct);
        if (migrate)
        {
            await using var connection = new NpgsqlConnection(scratch.ConnectionString);
            await connection.OpenAsync(ct);
            await PgMigrations.MigrateAsync(connection, ct);
        }

        return (scratch, NpgsqlDataSource.Create(scratch.ConnectionString));
    }

    private static async Task ExecAsync(NpgsqlDataSource source, string sql, CancellationToken ct)
    {
        await using var command = source.CreateCommand(sql);
        await command.ExecuteNonQueryAsync(ct);
    }

    private static Task Capture(NpgsqlDataSource s, double hoursAgo, CancellationToken ct) =>
        ExecAsync(s, $"INSERT INTO collect.store_statement_captures VALUES ({Ts(hoursAgo)}, 3600, NULL, 0, 0, 3, 3, 0, 'ok')", ct);

    private static Task Row(NpgsqlDataSource s, double hoursAgo, long queryId, long calls, double totalMs, CancellationToken ct) =>
        ExecAsync(s, $"INSERT INTO collect.store_statement_history VALUES ({Ts(hoursAgo)}, 3600, 'owner', {queryId}, {calls}, {totalMs.ToString(CultureInfo.InvariantCulture)}, 1, 2, 3, 4, 9.5, {None})", ct);

    private static string Reason(JsonNode? member) => member!["reason"]!.GetValue<string>();

    [Fact]
    public async Task TheHistoryMember_IsTheToolsRankedAnswer_ForTheSameHours_UpToTheCutMadeAfterAliasing()
    {
        var ct = TestContext.Current.CancellationToken;
        var (scratch, source) = await StartAsync(ct);
        await using var _ = scratch;
        await using var __ = source;
        var ok = false;
        try
        {
            /* Real statement text: the extension is installed in the scratch database and a statement longer than the preview runs.
               The run must happen: a missing extension fails here, it does not skip. */
            await using (var connection = new NpgsqlConnection(scratch.ConnectionString))
            {
                await connection.OpenAsync(ct);
                var ensured = await StoreStatementStats.EnsureAsync(connection, "config", Array.Empty<string>(), Microsoft.Extensions.Logging.Abstractions.NullLogger.Instance, ct);
                Assert.Equal(StoreStatementStats.SetupOutcome.Ready, ensured);
                /* Many runs: the extension's table is shared by every database on the server and evicts the least-used entries first. */
                for (var i = 0; i < 25; i++)
                {
                    await using var statement = new NpgsqlCommand(LongStatement, connection);
                    await statement.ExecuteNonQueryAsync(ct);
                }
            }

            long longQueryId;
            await using (var find = source.CreateCommand(
                "SELECT queryid FROM pg_stat_statements WHERE query LIKE '%AS tail_marker' AND dbid = (SELECT oid FROM pg_database WHERE datname = current_database()) LIMIT 1"))
            {
                longQueryId = (long)(await find.ExecuteScalarAsync(ct))!;
            }

            /* The earliest capture is inside the window, so effective_start is that capture and does not move between the two reads. */
            await Capture(source, 3.5, ct);
            await Capture(source, 2.5, ct);
            await Row(source, 3.5, longQueryId, 4, 900, ct);
            await Row(source, 2.5, longQueryId, 6, 100, ct);
            await Row(source, 2.5, 2, 5, 50, ct);
            await Row(source, 3.5, 3, 9, 10, ct);

            var member = await DiagnosticsBundleRunner.StatementHistoryAsync(source, 24, ct);
            var tool = JsonNode.Parse(await DarlingMcpStoreQueryHistoryTools.GetStoreQueryHistory(
                source, query_id: null, role: null, hours_back: 24, top: DiagnosticsBundleRunner.StatementHistoryTop, cancellationToken: ct));

            Assert.Equal("ranked", member!["mode"]!.GetValue<string>());
            Assert.Equal(DiagnosticsBundleRunner.StatementHistoryTop, member["top"]!.GetValue<int>());
            Assert.Equal(3, member["statements"]!.AsArray().Count);
            Assert.Null(member["hours_back_clamped_from"]);

            /* The bundle reads each statement's text whole, so it can alias it before it cuts it; the tool's own answer is already cut. */
            var wholeText = member["statements"]![0]!["query"]!.GetValue<string>();
            var cutText = tool!["statements"]![0]!["query"]!.GetValue<string>();
            Assert.Equal(longQueryId.ToString(CultureInfo.InvariantCulture), member["statements"]![0]!["query_id"]!.GetValue<string>());
            Assert.True(wholeText.Length > 240, $"the bundle's text should be whole, was {wholeText.Length} characters");
            Assert.EndsWith("tail_marker", wholeText, StringComparison.Ordinal);
            Assert.True(cutText.Length <= 243 && cutText.EndsWith("...", StringComparison.Ordinal), "the tool's answer should be cut to the preview");

            /* After the cut the bundle makes once the text is aliased, the member is the tool's answer, key for key. */
            DiagnosticsBundleRunner.CompactStatementTexts(member);
            Assert.Equal(tool.ToJsonString(), member.ToJsonString());
            ok = true;
        }
        finally
        {
            await LiveStoreCleanup.RunAsync(scratch.ConnectionString, ok, static (_, _) => Task.CompletedTask);
        }
    }

    [Fact]
    public async Task AWindowTheToolCannotServe_IsClamped_AndTheMemberSaysWhatWasAsked()
    {
        var ct = TestContext.Current.CancellationToken;
        var (scratch, source) = await StartAsync(ct);
        await using var _ = scratch;
        await using var __ = source;
        var ok = false;
        try
        {
            await Capture(source, 0.5, ct);
            await Row(source, 0.5, 7, 3, 30, ct);

            var tooLong = await DiagnosticsBundleRunner.StatementHistoryAsync(source, 5000, ct);
            Assert.Equal(DarlingMcpStoreQueryHistoryTools.MaxHours, tooLong!["hours_back"]!.GetValue<int>());
            Assert.Equal(5000, tooLong["hours_back_clamped_from"]!.GetValue<int>());

            var tooShort = await DiagnosticsBundleRunner.StatementHistoryAsync(source, 0, ct);
            Assert.Equal(1, tooShort!["hours_back"]!.GetValue<int>());
            Assert.Equal(0, tooShort["hours_back_clamped_from"]!.GetValue<int>());

            /* The longest window the verb accepts is inside the tool's range and is not touched. */
            var asked = await DiagnosticsBundleRunner.StatementHistoryAsync(source, DiagnosticsBundle.MaxHours, ct);
            Assert.Equal(DiagnosticsBundle.MaxHours, asked!["hours_back"]!.GetValue<int>());
            Assert.Null(asked["hours_back_clamped_from"]);
            ok = true;
        }
        finally
        {
            await LiveStoreCleanup.RunAsync(scratch.ConnectionString, ok, static (_, _) => Task.CompletedTask);
        }
    }

    [Fact]
    public async Task AStoreBeforeV163_ReadsNotPresent_WithTodaysReason()
    {
        var ct = TestContext.Current.CancellationToken;
        var (scratch, source) = await StartAsync(ct, migrate: false);
        await using var _ = scratch;
        await using var __ = source;
        var ok = false;
        try
        {
            var member = await DiagnosticsBundleRunner.StatementHistoryAsync(source, 24, ct);

            Assert.Equal("not_present", member!["status"]!.GetValue<string>());
            Assert.Equal("This store has no statement-history tables yet.", Reason(member));
            Assert.False(DiagnosticsBundleRunner.MemberFailed(new JsonObject { ["history"] = member }));
            ok = true;
        }
        finally
        {
            await LiveStoreCleanup.RunAsync(scratch.ConnectionString, ok, static (_, _) => Task.CompletedTask);
        }
    }

    [Fact]
    public async Task AnErrorTheToolAnswers_IsTheMemberAsItIs_AndFailsTheSection()
    {
        var ct = TestContext.Current.CancellationToken;
        var (scratch, source) = await StartAsync(ct, migrate: false);
        await using var _ = scratch;
        await using var __ = source;
        var ok = false;
        try
        {
            /* Both tables exist, so the probe lets the read through, but they are not V163's shape, so the tool's reads fail. */
            await ExecAsync(source, "CREATE SCHEMA collect", ct);
            await ExecAsync(source, "CREATE TABLE collect.store_statement_history (queryid bigint)", ct);
            await ExecAsync(source, "CREATE TABLE collect.store_statement_captures (capture_time timestamp)", ct);

            var member = await DiagnosticsBundleRunner.StatementHistoryAsync(source, 24, ct);

            Assert.Equal("error", member!["status"]!.GetValue<string>());
            Assert.True(DiagnosticsBundleRunner.MemberFailed(new JsonObject { ["cumulative"] = new JsonObject(), ["history"] = member }));
            ok = true;
        }
        finally
        {
            await LiveStoreCleanup.RunAsync(scratch.ConnectionString, ok, static (_, _) => Task.CompletedTask);
        }
    }

    /// <summary>The status the manifest lists for a section.</summary>
    private static string ManifestStatus(JsonNode bundle, string section) =>
        bundle["manifest"]!["sections"]!.AsArray().Single(s => s!["name"]!.GetValue<string>() == section)!["status"]!.GetValue<string>();

    [Fact]
    public async Task AHistoryReadThatErrors_ListsTheSectionAsError_InTheManifestAndInTheSection_ExitsPartial_AndKeepsTheOtherMembers()
    {
        var ct = TestContext.Current.CancellationToken;
        var (scratch, source) = await StartAsync(ct);
        await using var _ = scratch;
        await using var __ = source;
        var root = Directory.CreateTempSubdirectory("darling-bundle-historyerror-");
        var ok = false;
        try
        {
            var options = DiagnosticsBundle.ParseArgs(new[] { Path.Combine(root.FullName, "b.json"), "--log-dir", root.FullName }).Options!;
            var config = new DarlingConfig { Servers = { new MonitoredServer { Name = "alpha-sql-01" } } };

            /* The control: the same store with a history that reads. The section is ok in the manifest and in its own body. */
            var control = await DiagnosticsBundleRunner.BuildAsync(options, config, scratch.ConnectionString, source, null, null, ct);
            var controlTree = JsonNode.Parse(control.Text!)!;
            Assert.Equal("ok", ManifestStatus(controlTree, "store_statements"));
            Assert.Null(controlTree["sections"]!["store_statements"]!["status"]);
            Assert.NotEqual("error", controlTree["sections"]!["store_statements"]!["history"]!["status"]?.GetValue<string>());
            Assert.Equal(DarlingCliCommands.DiagnosticsBundleExitCode.Ok, control.ExitCode);

            /* Break only the history read. The existence probe still finds both tables, so the tool is asked, and its read of the
               capture table names a column that is gone, so the tool answers an error. */
            await ExecAsync(source, "ALTER TABLE collect.store_statement_captures RENAME COLUMN outcome TO outcome_renamed", ct);

            var broken = await DiagnosticsBundleRunner.BuildAsync(options, config, scratch.ConnectionString, source, null, null, ct);
            var tree = JsonNode.Parse(broken.Text!)!;

            /* The exit code reports a partial bundle and the manifest agrees: the section reads "error", the status a section that throws gets. */
            Assert.Equal(DarlingCliCommands.DiagnosticsBundleExitCode.PartialBundle, broken.ExitCode);
            Assert.Equal("error", ManifestStatus(tree, "store_statements"));

            /* The section says so itself, and keeps its other member and the history member with the tool's own error. */
            var section = tree["sections"]!["store_statements"]!;
            Assert.Equal("error", section["status"]!.GetValue<string>());
            Assert.NotNull(section["cumulative"]);
            Assert.Equal("error", section["history"]!["status"]!.GetValue<string>());
            Assert.Contains("get_store_query_history", section["history"]!["message"]!.GetValue<string>(), StringComparison.Ordinal);

            /* Nothing else in the bundle was marked failed by it. */
            Assert.Equal(
                new[] { "store_statements" },
                tree["manifest"]!["sections"]!.AsArray().Where(s => s!["status"]!.GetValue<string>() == "error").Select(s => s!["name"]!.GetValue<string>()).ToArray());
            ok = true;
        }
        finally
        {
            await LiveStoreCleanup.RunAsync(scratch.ConnectionString, ok, static (_, _) => Task.CompletedTask);
            root.Delete(recursive: true);
        }
    }

    /// <summary>
    /// A name and a literal that the first two planted statements carry. Neither may reach the tool's answer or any member of the
    /// bundle, whatever the reader function returned.
    /// </summary>
    private const string PlantedName = "planted_zq9";

    private const string PlantedLiteral = "zq9-literal";

    private const long CredentialId = 9001;

    private const long RawLiteralId = 9002;

    private const long OrdinaryId = 9003;

    /// <summary>Role DDL with a password: the statement the shared sensitive-statement filter names, withheld by its name alone.</summary>
    private static readonly string CredentialText = $"ALTER ROLE {PlantedName} PASSWORD '{PlantedLiteral}'";

    /// <summary>
    /// A SELECT with its literals as typed, the text pg_stat_statements keeps when an entry is gone at executor end. The filter does
    /// not name it, so the lexer is what masks it.
    /// </summary>
    private static readonly string RawLiteralText = $"SELECT '{PlantedLiteral}'::text AS raw_col, 4111111111111111";

    private const string MaskedRawLiteralText = "SELECT '?'::text AS raw_col, ?";

    /// <summary>Ordinary normalized text, longer than the tool's 240-character preview.</summary>
    private static readonly string OrdinaryText =
        $"SELECT $1 AS {new string('l', 60)}, $2 AS {new string('m', 60)}, $3 AS {new string('n', 60)}, $4 AS {new string('o', 60)}, $5 AS tail_marker";

    /// <summary>
    /// A store whose reader function is OLDER than the sensitive-statement pattern, the one case the tools' second layer is for. The
    /// function is built as the product builds it and then replaced by one of the same signature that returns the three planted
    /// texts as they are: the current body withholds the first text itself, so nothing else can put it in front of that layer. One
    /// capture and one history row per statement, the credential statement ranking first and the ordinary one last.
    /// </summary>
    private static async Task PlantAsync(string connectionString, NpgsqlDataSource source, CancellationToken ct)
    {
        await using (var connection = new NpgsqlConnection(connectionString))
        {
            await connection.OpenAsync(ct);
            var ensured = await StoreStatementStats.EnsureAsync(connection, "config", Array.Empty<string>(), Microsoft.Extensions.Logging.Abstractions.NullLogger.Instance, ct);
            Assert.Equal(StoreStatementStats.SetupOutcome.Ready, ensured);
        }

        static string Values(long id, double totalMs, string text) =>
            $"('owner'::text, {id}::bigint, 5::bigint, {totalMs.ToString(CultureInfo.InvariantCulture)}::double precision, 10::double precision, 20::double precision, 0::double precision, 1::bigint, 2::bigint, 3::bigint, 4::bigint, {PgSensitiveStatementFilter.SqlLiteral(text)}::text)";

        await ExecAsync(source, $@"
CREATE OR REPLACE FUNCTION config.{StoreStatementStats.FunctionName}()
RETURNS TABLE (role_name text, queryid bigint, calls bigint, total_exec_ms double precision, mean_exec_ms double precision, max_exec_ms double precision, total_plan_ms double precision, rows_returned bigint, shared_blks_hit bigint, shared_blks_read bigint, temp_blks_written bigint, query text)
LANGUAGE sql
SECURITY DEFINER
SET search_path = pg_catalog, pg_temp
SET standard_conforming_strings = on
AS $stub$
    SELECT * FROM (VALUES
        {Values(CredentialId, 900, CredentialText)},
        {Values(RawLiteralId, 500, RawLiteralText)},
        {Values(OrdinaryId, 100, OrdinaryText)}
    ) AS v
$stub$", ct);

        await Capture(source, 3.5, ct);
        await Capture(source, 2.5, ct);
        await Row(source, 2.5, CredentialId, 5, 900, ct);
        await Row(source, 2.5, RawLiteralId, 5, 500, ct);
        await Row(source, 2.5, OrdinaryId, 5, 100, ct);
    }

    /// <summary>The <c>query</c> of the statement with this id, in a tool answer or a bundle member (both list <c>statements</c> by <c>query_id</c>).</summary>
    private static string QueryOf(JsonNode? answer, long queryId) =>
        answer!["statements"]!.AsArray()
            .Single(s => s!["query_id"]!.GetValue<string>() == queryId.ToString(CultureInfo.InvariantCulture))!["query"]!.GetValue<string>();

    [Fact]
    public async Task AStatementTheSharedFilterNames_ReadsAsWithheld_InTheToolAndInTheMember_AndARawLiteralIsMasked()
    {
        var ct = TestContext.Current.CancellationToken;
        var (scratch, source) = await StartAsync(ct);
        await using var _ = scratch;
        await using var __ = source;
        var ok = false;
        try
        {
            await PlantAsync(scratch.ConnectionString, source, ct);

            var member = await DiagnosticsBundleRunner.StatementHistoryAsync(source, 24, ct);
            var tool = JsonNode.Parse(await DarlingMcpStoreQueryHistoryTools.GetStoreQueryHistory(
                source, query_id: null, role: null, hours_back: 24, top: DiagnosticsBundleRunner.StatementHistoryTop, cancellationToken: ct));
            Assert.Equal(3, member!["statements"]!.AsArray().Count);
            Assert.Equal(3, tool!["statements"]!.AsArray().Count);

            foreach (var answer in new[] { tool, member })
            {
                var json = answer.ToJsonString();
                Assert.DoesNotContain(PlantedName, json, StringComparison.Ordinal);
                Assert.DoesNotContain(PlantedLiteral, json, StringComparison.Ordinal);

                /* The statement the filter names reads as withheld: not its text, and not an empty string either. */
                Assert.Equal(StoreStatementStats.WithheldText, QueryOf(answer, CredentialId));

                /* A raw-literal text is masked by the lexer, on the bundle's whole-text branch as on the tool's cut one. */
                Assert.Equal(MaskedRawLiteralText, QueryOf(answer, RawLiteralId));
            }

            /* The ordinary statement survives: whole in the member, which the bundle aliases before it cuts, and cut in the tool's own answer. */
            Assert.Equal(OrdinaryText, QueryOf(member, OrdinaryId));
            var cut = QueryOf(tool, OrdinaryId);
            Assert.True(cut.Length <= 243 && cut.EndsWith("...", StringComparison.Ordinal), "the tool's answer should be cut to the preview");
            ok = true;
        }
        finally
        {
            await LiveStoreCleanup.RunAsync(scratch.ConnectionString, ok, static (_, _) => Task.CompletedTask);
        }
    }

    [Fact]
    public async Task AStatementTheSharedFilterNames_ReadsAsWithheld_InBothMembersOfTheBundlesStoreStatementsSection()
    {
        var ct = TestContext.Current.CancellationToken;
        var (scratch, source) = await StartAsync(ct);
        await using var _ = scratch;
        await using var __ = source;
        var root = Directory.CreateTempSubdirectory("darling-bundle-withheld-");
        var ok = false;
        try
        {
            await PlantAsync(scratch.ConnectionString, source, ct);
            var options = DiagnosticsBundle.ParseArgs(new[] { Path.Combine(root.FullName, "b.json"), "--log-dir", root.FullName }).Options!;
            var config = new DarlingConfig { Servers = { new MonitoredServer { Name = "alpha-sql-01" } } };

            var built = await DiagnosticsBundleRunner.BuildAsync(options, config, scratch.ConnectionString, source, null, null, ct);
            Assert.True(built.Text is not null, built.Message);
            Assert.Equal(DarlingCliCommands.DiagnosticsBundleExitCode.Ok, built.ExitCode);

            /* Nowhere in the file: the whole text and the literal of the statement the filter names, and the literal of the raw one. */
            Assert.DoesNotContain(PlantedName, built.Text, StringComparison.Ordinal);
            Assert.DoesNotContain(PlantedLiteral, built.Text, StringComparison.Ordinal);

            var section = JsonNode.Parse(built.Text!)!["sections"]!["store_statements"]!;
            Assert.Null(section["status"]);
            foreach (var name in new[] { "cumulative", "history" })
            {
                var member = section[name];
                Assert.Equal(3, member!["statements"]!.AsArray().Count);
                Assert.True(StoreStatementStats.WithheldText == QueryOf(member, CredentialId), $"{name}: the statement the filter names should read as withheld");
                Assert.True(MaskedRawLiteralText == QueryOf(member, RawLiteralId), $"{name}: the raw literal should be masked");

                /* Cut after aliasing, as every statement text in the bundle is. */
                var cut = QueryOf(member, OrdinaryId);
                Assert.True(cut.Length <= 243 && cut.EndsWith("...", StringComparison.Ordinal), $"{name}: the bundle's statement text should be cut to the preview");
            }

            ok = true;
        }
        finally
        {
            await LiveStoreCleanup.RunAsync(scratch.ConnectionString, ok, static (_, _) => Task.CompletedTask);
            root.Delete(recursive: true);
        }
    }

    [Fact]
    public void AnErrorInEitherMember_FailsTheSection_AndAPreconditionOrARefusalDoesNot()
    {
        static JsonObject Member(string? status) => status is null ? new JsonObject { ["mode"] = "ranked" } : new JsonObject { ["status"] = status };

        Assert.True(DiagnosticsBundleRunner.MemberFailed(new JsonObject { ["cumulative"] = Member(null), ["history"] = Member("error") }));
        Assert.True(DiagnosticsBundleRunner.MemberFailed(new JsonObject { ["cumulative"] = Member("error"), ["history"] = Member(null) }));
        Assert.True(DiagnosticsBundleRunner.MemberFailed(new JsonObject { ["cumulative"] = Member("error"), ["history"] = Member("error") }));

        Assert.False(DiagnosticsBundleRunner.MemberFailed(new JsonObject { ["cumulative"] = Member(null), ["history"] = Member(null) }));
        foreach (var status in new[] { "precondition", "invalid", "not_present", "empty" })
        {
            Assert.False(DiagnosticsBundleRunner.MemberFailed(new JsonObject { ["cumulative"] = Member(status), ["history"] = Member(status) }));
        }

        Assert.False(DiagnosticsBundleRunner.MemberFailed(null));
    }

    [Fact]
    public async Task ACumulativeReadThatErrors_ListsTheSectionAsError_InTheManifestAndInTheSection_ExitsPartial_AndKeepsTheHistoryMember()
    {
        var ct = TestContext.Current.CancellationToken;
        var (scratch, source) = await StartAsync(ct);
        await using var _ = scratch;
        await using var __ = source;
        var root = Directory.CreateTempSubdirectory("darling-bundle-cumulativeerror-");
        var ok = false;
        try
        {
            /* The reader functions must exist for the stats tool to get past its precondition, so the break below is a read error. */
            await using (var connection = new NpgsqlConnection(scratch.ConnectionString))
            {
                await connection.OpenAsync(ct);
                var ensured = await StoreStatementStats.EnsureAsync(connection, "config", Array.Empty<string>(), Microsoft.Extensions.Logging.Abstractions.NullLogger.Instance, ct);
                Assert.Equal(StoreStatementStats.SetupOutcome.Ready, ensured);
            }

            var options = DiagnosticsBundle.ParseArgs(new[] { Path.Combine(root.FullName, "b.json"), "--log-dir", root.FullName }).Options!;
            var config = new DarlingConfig { Servers = { new MonitoredServer { Name = "alpha-sql-01" } } };

            /* The control: both members read, and the section is ok in the manifest and in its own body. */
            var control = await DiagnosticsBundleRunner.BuildAsync(options, config, scratch.ConnectionString, source, null, null, ct);
            var controlTree = JsonNode.Parse(control.Text!)!;
            Assert.Equal("ok", ManifestStatus(controlTree, "store_statements"));
            Assert.Null(controlTree["sections"]!["store_statements"]!["status"]);
            Assert.NotEqual("error", controlTree["sections"]!["store_statements"]!["cumulative"]!["status"]?.GetValue<string>());
            Assert.Equal(DarlingCliCommands.DiagnosticsBundleExitCode.Ok, control.ExitCode);

            /* Break only the cumulative read. The stats tool's precondition looks at the ranking function alone, so it goes on to read
               the info function, which is gone, and answers an error. The history member does not touch it. */
            await ExecAsync(source, $"DROP FUNCTION config.{StoreStatementStats.InfoFunctionName}()", ct);

            var broken = await DiagnosticsBundleRunner.BuildAsync(options, config, scratch.ConnectionString, source, null, null, ct);
            var tree = JsonNode.Parse(broken.Text!)!;

            /* The exit code reports a partial bundle and the manifest agrees: the section reads "error", the status a section that throws gets. */
            Assert.Equal(DarlingCliCommands.DiagnosticsBundleExitCode.PartialBundle, broken.ExitCode);
            Assert.Equal("error", ManifestStatus(tree, "store_statements"));

            /* The section says so itself, and keeps both members: the cumulative one with the tool's own error, the history one as it read. */
            var section = tree["sections"]!["store_statements"]!;
            Assert.Equal("error", section["status"]!.GetValue<string>());
            Assert.Equal("error", section["cumulative"]!["status"]!.GetValue<string>());
            Assert.Contains("get_store_query_stats", section["cumulative"]!["message"]!.GetValue<string>(), StringComparison.Ordinal);
            Assert.NotNull(section["history"]);
            Assert.NotEqual("error", section["history"]!["status"]?.GetValue<string>());

            /* Nothing else in the bundle was marked failed by it. */
            Assert.Equal(
                new[] { "store_statements" },
                tree["manifest"]!["sections"]!.AsArray().Where(s => s!["status"]!.GetValue<string>() == "error").Select(s => s!["name"]!.GetValue<string>()).ToArray());
            ok = true;
        }
        finally
        {
            await LiveStoreCleanup.RunAsync(scratch.ConnectionString, ok, static (_, _) => Task.CompletedTask);
            root.Delete(recursive: true);
        }
    }

    [Fact]
    public void TheBundle_ReadsNoHistoryTableItself_ExceptTheExistenceProbe()
    {
        /* Before #5097 the runner selected from both V163 tables and the reader function itself. Now the rows and the text come from the tool. */
        var source = File.ReadAllText(Path.Combine(RepoRoot(), "Darling", "PerformanceMonitor.Darling.Service", "DiagnosticsBundleRunner.cs"));

        Assert.DoesNotContain("FROM collect.store_statement", source, StringComparison.Ordinal);
        Assert.DoesNotContain(StoreStatementStats.FunctionName, source, StringComparison.Ordinal);
        Assert.Contains("to_regclass('collect.store_statement_history')", source, StringComparison.Ordinal);
        Assert.Contains("DarlingMcpStoreQueryHistoryTools.GetStoreQueryHistoryUncut(", source, StringComparison.Ordinal);
    }

    private static string RepoRoot([CallerFilePath] string thisFile = "")
    {
        var dir = Path.GetDirectoryName(thisFile);
        while (dir is not null && !File.Exists(Path.Combine(dir, "PerformanceMonitor.sln")))
        {
            dir = Path.GetDirectoryName(dir);
        }

        Assert.True(dir is not null, "the repository root was not found above " + thisFile);
        return dir!;
    }
}
