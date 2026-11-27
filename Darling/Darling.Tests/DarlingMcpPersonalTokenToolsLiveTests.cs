/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using System.Collections.Generic;
using System.Globalization;
using System.Linq;
using System.Text.Json.Nodes;
using System.Threading;
using System.Threading.Tasks;
using Microsoft.Extensions.Logging;
using ModelContextProtocol.Protocol;
using Npgsql;
using PerformanceMonitor.Common;
using PerformanceMonitor.Darling.Service;
using PerformanceMonitor.Darling.Service.Hosting;
using PerformanceMonitor.Darling.Service.Mcp;
using PerformanceMonitor.Darling.Storage;
using Xunit;

namespace Darling.Tests;

/* #1776 own-store: this class runs on the PersonalTokenWebStore fixture's own scratch database, and every fact names its own person, so none reads
   another test's rows or the shared database. */

/// <summary>
/// #9643: the personal MCP token tools through their handlers on a live store. A token is made for a named person through the same core the web page
/// runs, appears once in the answer and in no log line and no journal row, the gate's snapshot holds it before the answer returns, a revoke kills it
/// at once, and the list read never shows a value.
/// </summary>
public sealed class DarlingMcpPersonalTokenToolsLiveTests : IClassFixture<PersonalTokenWebStore>
{
    private const string Admin = "test-admin";
    private const string Caller = "mcp:test caller";

    private readonly PersonalTokenWebStore _store;

    public DarlingMcpPersonalTokenToolsLiveTests(PersonalTokenWebStore store) => _store = store;

    private sealed class LineLog : ILogger
    {
        public List<string> Lines { get; } = new();

        public IDisposable? BeginScope<TState>(TState state) where TState : notnull => null;

        public bool IsEnabled(LogLevel logLevel) => true;

        public void Log<TState>(LogLevel logLevel, EventId eventId, TState state, Exception? exception, Func<TState, Exception?, string> formatter)
        {
            lock (Lines)
            {
                Lines.Add(formatter(state, exception) + (exception is null ? string.Empty : " " + exception));
            }
        }
    }

    private sealed class Rig : IAsyncDisposable
    {
        public Rig(string connectionString, PersonalTokenSettings? settings)
        {
            Source = NpgsqlDataSource.Create(connectionString);
            Owner = new StoreOwnerSource();
            Owner.Publish(Source);
            Snapshot = new DarlingPersonalTokenSnapshot();
            Seat = new DarlingPersonalTokenSeat(settings, settings is null ? null : Snapshot, Owner);
        }

        public NpgsqlDataSource Source { get; }

        public StoreOwnerSource Owner { get; }

        public DarlingPersonalTokenSnapshot Snapshot { get; }

        public DarlingPersonalTokenSeat Seat { get; }

        public LineLog Log { get; } = new();

        public ValueTask DisposeAsync()
        {
            Snapshot.Dispose();
            return Source.DisposeAsync();
        }
    }

    private static PersonalTokenSettings Settings(string adminRole = McpTokenEntry.OperatorRole, int cap = 5, McpDataDetail? detail = null,
        McpDataDetail serviceDetail = McpDataDetail.Full) => new(30, cap, adminRole, detail, serviceDetail);

    private static string KeyFor(string label) => DarlingWebSeat.ComputeOwnerKey("https://idp.example.test", label)!;

    private static async Task<long> AddPersonAsync(NpgsqlDataSource source, string display, string? webKey, CancellationToken ct)
    {
        var added = await PersonDirectory.AddPersonAsync(source, display, Admin, ServerActionSurfaces.Viewer, ct);
        var id = added.PersonId!.Value;
        if (webKey is not null)
        {
            Assert.True((await PersonDirectory.AddIdentityAsync(source, id, PersonIdentityKinds.Web, webKey, Admin, ServerActionSurfaces.Web, ct)).Succeeded);
        }

        return id;
    }

    private static Task<string> MintAsync(Rig rig, string person, string name, int days = 7, string role = McpTokenEntry.ReaderRole,
        McpDataDetail callerDetail = McpDataDetail.Full, CancellationToken ct = default) =>
        DarlingMcpPersonalTokenTools.MintAsync(rig.Source, rig.Seat, rig.Log, person, name, days, role, Caller, TimeProvider.System, callerDetail, ct);

    /// <summary>The code of a refusal (its <c>status</c> is <c>refused</c>), else the status of an answer that is not one.</summary>
    private static string? CodeOrStatus(string answer)
    {
        var body = JsonNode.Parse(answer)!;
        return (string?)body["code"] ?? (string?)body["status"];
    }

    private static string WriteOutcome(string answer) =>
        McpToolLatencyFilter.WriteOutcomeOf(new CallToolResult { Content = [new TextContentBlock { Text = answer }] });

    private static async Task<string> ScalarAsync(NpgsqlDataSource source, string sql, string? parameter, CancellationToken ct)
    {
        await using var command = source.CreateCommand(sql);
        if (parameter is not null)
        {
            command.Parameters.AddWithValue(parameter);
        }

        return Convert.ToString(await command.ExecuteScalarAsync(ct), CultureInfo.InvariantCulture) ?? string.Empty;
    }

    private static int Occurrences(string text, string value)
    {
        var count = 0;
        for (var at = text.IndexOf(value, StringComparison.Ordinal); at >= 0; at = text.IndexOf(value, at + value.Length, StringComparison.Ordinal))
        {
            count++;
        }

        return count;
    }

    [Fact]
    public async Task Mint_ForANamedPerson_TheTokenIsInTheAnswerOnce_AndInNoLogLineOrJournalRow_AndTheGateHoldsIt()
    {
        var connectionString = _store.ConnectionString;
        Assert.SkipWhen(string.IsNullOrEmpty(connectionString), "Set DARLING_TEST_PG to run the personal token tool live tests.");
        var ct = TestContext.Current.CancellationToken;
        await using var rig = new Rig(connectionString!, Settings());
        var key = KeyFor("mint-one");
        await AddPersonAsync(rig.Source, "Mint One", key, ct);

        var answer = await MintAsync(rig, "mint one", "ci-bot", ct: ct);

        var body = JsonNode.Parse(answer)!;
        Assert.Equal("minted", (string?)body["status"]);
        var token = (string)body["token"]!;
        Assert.Equal(1, Occurrences(answer, token));
        Assert.Equal("reader", (string?)body["record"]!["role"]);
        var check = rig.Snapshot.Check(token, DateTimeOffset.UtcNow);
        Assert.Equal(PersonalTokenOutcome.Live, check.Outcome);
        Assert.Equal(key, check.Row!.OwnerKey);
        Assert.Equal("Mint One", check.Row.Subject);

        Assert.NotEmpty(rig.Log.Lines);
        Assert.DoesNotContain(rig.Log.Lines, line => line.Contains(token, StringComparison.Ordinal));
        var journalWithToken = await ScalarAsync(rig.Source,
            "SELECT count(*) FROM config.investigation_journal j WHERE row_to_json(j)::text LIKE '%' || $1 || '%'", token, ct);
        Assert.Equal("0", journalWithToken);
        var journalActor = await ScalarAsync(rig.Source,
            "SELECT count(*) FROM config.investigation_journal WHERE actor = '" + Caller + "' AND detail::text LIKE '%ci-bot%'", null, ct);
        Assert.Equal("1", journalActor);
    }

    [Fact]
    public async Task List_ShowsTheTokensWithNoValueAndNoHash()
    {
        var connectionString = _store.ConnectionString;
        Assert.SkipWhen(string.IsNullOrEmpty(connectionString), "Set DARLING_TEST_PG to run the personal token tool live tests.");
        var ct = TestContext.Current.CancellationToken;
        await using var rig = new Rig(connectionString!, Settings());
        await AddPersonAsync(rig.Source, "List One", KeyFor("list-one"), ct);
        var minted = JsonNode.Parse(await MintAsync(rig, "List One", "list-token", ct: ct))!;
        var token = (string)minted["token"]!;
        var id = (string)minted["record"]!["id"]!;

        var answer = await DarlingMcpPersonalTokenTools.ListAsync(rig.Seat, TimeProvider.System, ct);

        Assert.DoesNotContain(token, answer, StringComparison.Ordinal);
        var rows = JsonNode.Parse(answer)!["tokens"]!.AsArray().Where(r => (string?)r!["id"] == id).ToList();
        var row = Assert.Single(rows)!;
        Assert.Equal("live", (string?)row["state"]);
        Assert.Equal("List One", (string?)row["subject"]);
        Assert.Equal("list-token", (string?)row["name"]);
        Assert.Null(row["token"]);
    }

    [Fact]
    public async Task Revoke_KillsTheTokenInTheGateAtOnce_AndASecondRevokeIsNotFound()
    {
        var connectionString = _store.ConnectionString;
        Assert.SkipWhen(string.IsNullOrEmpty(connectionString), "Set DARLING_TEST_PG to run the personal token tool live tests.");
        var ct = TestContext.Current.CancellationToken;
        await using var rig = new Rig(connectionString!, Settings());
        await AddPersonAsync(rig.Source, "Revoke One", KeyFor("revoke-one"), ct);
        var minted = JsonNode.Parse(await MintAsync(rig, "Revoke One", "to-revoke", ct: ct))!;
        var token = (string)minted["token"]!;
        var id = (string)minted["record"]!["id"]!;
        Assert.Equal(PersonalTokenOutcome.Live, rig.Snapshot.Check(token, DateTimeOffset.UtcNow).Outcome);

        var first = await DarlingMcpPersonalTokenTools.RevokeAsync(rig.Seat, rig.Log, id, Caller, TimeProvider.System, ct);
        var second = await DarlingMcpPersonalTokenTools.RevokeAsync(rig.Seat, rig.Log, id, Caller, TimeProvider.System, ct);

        Assert.Equal("revoked", (string?)JsonNode.Parse(first)!["status"]);
        Assert.Equal(PersonalTokenOutcome.Dead, rig.Snapshot.Check(token, DateTimeOffset.UtcNow).Outcome);
        Assert.Equal("not_found", CodeOrStatus(second));
        Assert.DoesNotContain(rig.Log.Lines, line => line.Contains(token, StringComparison.Ordinal));
        var listed = JsonNode.Parse(await DarlingMcpPersonalTokenTools.ListAsync(rig.Seat, TimeProvider.System, ct))!["tokens"]!.AsArray()
            .Single(r => (string?)r!["id"] == id)!;
        Assert.Equal("revoked", (string?)listed["state"]);
        Assert.Equal(Caller, (string?)listed["revoked_by"]);
        Assert.Equal("not_found", CodeOrStatus(
            await DarlingMcpPersonalTokenTools.RevokeAsync(rig.Seat, rig.Log, "not-a-token-id", Caller, TimeProvider.System, ct)));
    }

    [Fact]
    public async Task Mint_BadInput_IsRefusedAndWritesNothing()
    {
        var connectionString = _store.ConnectionString;
        Assert.SkipWhen(string.IsNullOrEmpty(connectionString), "Set DARLING_TEST_PG to run the personal token tool live tests.");
        var ct = TestContext.Current.CancellationToken;
        await using var readerOnly = new Rig(connectionString!, Settings(McpTokenEntry.ReaderRole, cap: 1));
        var key = KeyFor("bad-input");
        await AddPersonAsync(readerOnly.Source, "Bad Input", key, ct);
        await AddPersonAsync(readerOnly.Source, "Keyless Person", null, ct);
        var disabled = await AddPersonAsync(readerOnly.Source, "Gone Person", KeyFor("gone-person"), ct);
        Assert.True((await PersonDirectory.SetPersonEnabledAsync(readerOnly.Source, disabled, false, Admin, ServerActionSurfaces.Viewer, ct)).Succeeded);

        async Task<string?> StatusOf(Task<string> call) => CodeOrStatus(await call);

        Assert.Equal("person_not_found", await StatusOf(MintAsync(readerOnly, "Nobody At All", "t1", ct: ct)));
        Assert.Equal("person_disabled", await StatusOf(MintAsync(readerOnly, "Gone Person", "t1", ct: ct)));
        Assert.Equal("person_identity", await StatusOf(MintAsync(readerOnly, "Keyless Person", "t1", ct: ct)));
        Assert.Equal("bad_name", await StatusOf(MintAsync(readerOnly, "Bad Input", "web", ct: ct)));
        Assert.Equal("bad_name", await StatusOf(MintAsync(readerOnly, "Bad Input", "has space", ct: ct)));
        Assert.Equal("bad_lifetime", await StatusOf(MintAsync(readerOnly, "Bad Input", "t1", days: 31, ct: ct)));
        Assert.Equal("bad_lifetime", await StatusOf(MintAsync(readerOnly, "Bad Input", "t1", days: 0, ct: ct)));
        Assert.Equal("role_not_allowed", await StatusOf(MintAsync(readerOnly, "Bad Input", "t1", role: McpTokenEntry.OperatorRole, ct: ct)));
        Assert.Equal("role_not_allowed", await StatusOf(MintAsync(readerOnly, "Bad Input", "t1", role: "agent", ct: ct)));
        Assert.Equal("0", await ScalarAsync(readerOnly.Source, "SELECT count(*) FROM config.mcp_personal_token WHERE owner_key = $1", key, ct));

        Assert.Equal("minted", await StatusOf(MintAsync(readerOnly, "Bad Input", "first", ct: ct)));
        Assert.Equal("at_cap", await StatusOf(MintAsync(readerOnly, "Bad Input", "second", ct: ct)));
        Assert.Equal("1", await ScalarAsync(readerOnly.Source, "SELECT count(*) FROM config.mcp_personal_token WHERE owner_key = $1", key, ct));
    }

    /* #9643: the service does not store a person's role, so this tool makes readers only, even when the settings let an admin's web page make operators. */
    [Fact]
    public async Task Mint_OperatorRole_IsRefusedEvenWhenTheSettingsAllowOperators_AndAReaderIsMade_AndDuplicateNameIsRefused()
    {
        var connectionString = _store.ConnectionString;
        Assert.SkipWhen(string.IsNullOrEmpty(connectionString), "Set DARLING_TEST_PG to run the personal token tool live tests.");
        var ct = TestContext.Current.CancellationToken;
        await using var rig = new Rig(connectionString!, Settings());
        var key = KeyFor("operator-person");
        await AddPersonAsync(rig.Source, "Operator Person", key, ct);

        var refused = JsonNode.Parse(await MintAsync(rig, "Operator Person", "op", role: McpTokenEntry.OperatorRole, ct: ct))!;
        Assert.Equal("refused", (string?)refused["status"]);
        Assert.Equal("role_not_allowed", (string?)refused["code"]);
        Assert.Null(refused["token"]);
        Assert.Equal("0", await ScalarAsync(rig.Source, "SELECT count(*) FROM config.mcp_personal_token WHERE owner_key = $1", key, ct));

        var made = JsonNode.Parse(await MintAsync(rig, "Operator Person", "op", ct: ct))!;
        var again = JsonNode.Parse(await MintAsync(rig, "Operator Person", "op", ct: ct))!;

        Assert.Equal("reader", (string?)made["record"]!["role"]);
        Assert.Equal("reader", rig.Snapshot.Check((string)made["token"]!, DateTimeOffset.UtcNow).Row!.Role);
        Assert.Equal("name_taken", (string?)again["code"]);
        Assert.Null(again["token"]);
    }

    /* #9643: a caller narrowed to metadataOnly cannot make a token that reads at the service's own level. */
    [Fact]
    public async Task Mint_ByACallerWhoseDataDetailIsStricterThanTheNewTokens_IsRefused_AndASameOrWiderCallerMakesIt()
    {
        var connectionString = _store.ConnectionString;
        Assert.SkipWhen(string.IsNullOrEmpty(connectionString), "Set DARLING_TEST_PG to run the personal token tool live tests.");
        var ct = TestContext.Current.CancellationToken;
        var key = KeyFor("detail-person");
        await using var open = new Rig(connectionString!, Settings());
        await AddPersonAsync(open.Source, "Detail Person", key, ct);

        var refused = await MintAsync(open, "Detail Person", "narrow", callerDetail: McpDataDetail.MetadataOnly, ct: ct);
        Assert.Equal("data_detail_wider", CodeOrStatus(refused));
        Assert.Equal("refused", (string?)JsonNode.Parse(refused)!["status"]);
        Assert.Equal("0", await ScalarAsync(open.Source, "SELECT count(*) FROM config.mcp_personal_token WHERE owner_key = $1", key, ct));

        Assert.Equal("data_detail_wider", CodeOrStatus(await MintAsync(open, "Detail Person", "mask", callerDetail: McpDataDetail.MaskLiterals, ct: ct)));
        Assert.Equal("minted", CodeOrStatus(await MintAsync(open, "Detail Person", "full", callerDetail: McpDataDetail.Full, ct: ct)));

        /* Tokens served at metadataOnly (by the personal setting, or because the whole service is), so the narrowed caller reads no less. */
        await using var narrowTokens = new Rig(connectionString!, Settings(detail: McpDataDetail.MetadataOnly));
        Assert.Equal("minted", CodeOrStatus(await MintAsync(narrowTokens, "Detail Person", "same", callerDetail: McpDataDetail.MetadataOnly, ct: ct)));
        await using var narrowService = new Rig(connectionString!, Settings(serviceDetail: McpDataDetail.MetadataOnly));
        Assert.Equal("minted", CodeOrStatus(await MintAsync(narrowService, "Detail Person", "service", callerDetail: McpDataDetail.MetadataOnly, ct: ct)));
    }

    /* #9643: the tool names a token's subject by the person's display name, the web page by the sign-in principal. A display name that is another
       sign-in's subject is refused, and the person's own earlier tokens do not count as a clash. */
    [Fact]
    public async Task Mint_ForAPersonWhoseDisplayNameIsAnotherSignInsSubject_IsRefused()
    {
        var connectionString = _store.ConnectionString;
        Assert.SkipWhen(string.IsNullOrEmpty(connectionString), "Set DARLING_TEST_PG to run the personal token tool live tests.");
        var ct = TestContext.Current.CancellationToken;
        await using var rig = new Rig(connectionString!, Settings());
        var key = KeyFor("clash-person");
        await AddPersonAsync(rig.Source, "Clash Person", key, ct);
        var other = PersonalTokenFormat.Mint();
        await PersonalTokenStore.InsertAsync(rig.Source, other.TokenId, other.SecretSha256, KeyFor("someone-else"), "Clash Person", "web-made", McpTokenEntry.ReaderRole,
            DateTimeOffset.UtcNow, 7, 5, Caller, ct);

        Assert.Equal("subject_clash", CodeOrStatus(await MintAsync(rig, "Clash Person", "t1", ct: ct)));
        Assert.Equal("0", await ScalarAsync(rig.Source, "SELECT count(*) FROM config.mcp_personal_token WHERE owner_key = $1", key, ct));

        var own = PersonalTokenFormat.Mint();
        await PersonalTokenStore.InsertAsync(rig.Source, own.TokenId, own.SecretSha256, KeyFor("own-person"), "Own Person", "first", McpTokenEntry.ReaderRole,
            DateTimeOffset.UtcNow, 7, 5, Caller, ct);
        await AddPersonAsync(rig.Source, "Own Person", KeyFor("own-person"), ct);
        Assert.Equal("minted", CodeOrStatus(await MintAsync(rig, "Own Person", "second", ct: ct)));
    }

    /* #9643: the clash is decided in the store whichever mint comes first. A tool mint for one person, then a web-side mint (the shared core the page
       uses) for another sign-in whose principal is that display name in a different case, is refused, and a tool mint for a case variant of an
       existing subject is refused too. */
    [Fact]
    public async Task Mint_SubjectClash_IsRefusedInEitherOrder_AndInAnyCase()
    {
        var connectionString = _store.ConnectionString;
        Assert.SkipWhen(string.IsNullOrEmpty(connectionString), "Set DARLING_TEST_PG to run the personal token tool live tests.");
        var ct = TestContext.Current.CancellationToken;
        await using var rig = new Rig(connectionString!, Settings());
        await AddPersonAsync(rig.Source, "Order Person", KeyFor("order-person"), ct);
        Assert.Equal("minted", CodeOrStatus(await MintAsync(rig, "Order Person", "t1", ct: ct)));

        var webSide = await DarlingPersonalTokenAdmin.MintAsync(rig.Source, rig.Seat.Owner, rig.Seat.Snapshot!, rig.Seat.Settings!, KeyFor("web-signin"), "order person", "t1",
            McpTokenEntry.ReaderRole, 7, "person:order person", TimeProvider.System, rig.Log, ct, ct);
        Assert.Equal(PersonalTokenCreateOutcome.SubjectTaken, webSide.Outcome);
        Assert.Equal("0", await ScalarAsync(rig.Source, "SELECT count(*) FROM config.mcp_personal_token WHERE owner_key = $1", KeyFor("web-signin"), ct));

        var web = await DarlingPersonalTokenAdmin.MintAsync(rig.Source, rig.Seat.Owner, rig.Seat.Snapshot!, rig.Seat.Settings!, KeyFor("case-signin"), "casey@corp", "t1",
            McpTokenEntry.ReaderRole, 7, "person:casey@corp", TimeProvider.System, rig.Log, ct, ct);
        Assert.Equal(PersonalTokenCreateOutcome.Created, web.Outcome);
        await AddPersonAsync(rig.Source, "CASEY@corp", KeyFor("other-casey"), ct);
        Assert.Equal("subject_clash", CodeOrStatus(await MintAsync(rig, "CASEY@corp", "t1", ct: ct)));
    }

    /* #9643: the write log reads a refusal as refused, which needs the answer's status to say so. */
    [Fact]
    public async Task EveryRefusal_AnswersStatusRefused_SoTheWriteLogCallsItRefused_AndAMintIsCompleted()
    {
        var connectionString = _store.ConnectionString;
        Assert.SkipWhen(string.IsNullOrEmpty(connectionString), "Set DARLING_TEST_PG to run the personal token tool live tests.");
        var ct = TestContext.Current.CancellationToken;
        await using var rig = new Rig(connectionString!, Settings());
        await AddPersonAsync(rig.Source, "Outcome Person", KeyFor("outcome-person"), ct);

        Assert.Equal("refused", WriteOutcome(await MintAsync(rig, "Outcome Person", "web", ct: ct)));
        Assert.Equal("refused", WriteOutcome(await MintAsync(rig, "Nobody At All", "t1", ct: ct)));
        Assert.Equal("refused", WriteOutcome(await DarlingMcpPersonalTokenTools.RevokeAsync(rig.Seat, rig.Log, "not-a-token-id", Caller, TimeProvider.System, ct)));
        Assert.Equal("completed", WriteOutcome(await MintAsync(rig, "Outcome Person", "ok", ct: ct)));
    }

    [Fact]
    public async Task AllThreeTools_PersonalTokensOff_AnswerNotServed_AndTouchNothing()
    {
        var connectionString = _store.ConnectionString;
        Assert.SkipWhen(string.IsNullOrEmpty(connectionString), "Set DARLING_TEST_PG to run the personal token tool live tests.");
        var ct = TestContext.Current.CancellationToken;
        await using var rig = new Rig(connectionString!, settings: null);
        await AddPersonAsync(rig.Source, "Off Person", KeyFor("off-person"), ct);

        Assert.Equal("not_served", CodeOrStatus(await MintAsync(rig, "Off Person", "t1", ct: ct)));
        Assert.Equal("not_served", CodeOrStatus(
            await DarlingMcpPersonalTokenTools.RevokeAsync(rig.Seat, rig.Log, "pt_" + new string('a', 20), Caller, TimeProvider.System, ct)));
        Assert.Equal("not_served", CodeOrStatus(await DarlingMcpPersonalTokenTools.ListAsync(rig.Seat, TimeProvider.System, ct)));
        Assert.Equal("0", await ScalarAsync(rig.Source, "SELECT count(*) FROM config.mcp_personal_token WHERE owner_key = $1", KeyFor("off-person"), ct));
    }
}
