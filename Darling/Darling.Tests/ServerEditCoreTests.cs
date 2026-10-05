/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using System.Collections.Generic;
using System.Linq;
using System.Text.Json.Nodes;
using System.Threading;
using System.Threading.Tasks;
using Microsoft.Extensions.Logging;
using NpgsqlTypes;
using PerformanceMonitor.Collectors;
using PerformanceMonitor.Darling.Service;
using PerformanceMonitor.Darling.Service.Mcp;
using Xunit;
using Edit = PerformanceMonitor.Darling.Service.Mcp.DarlingMcpServerAdminTools;

namespace Darling.Tests;

/// <summary>
/// The edit core (#5240) over a stand-in store and probe: field validation, the refused keys, the optimistic token,
/// the credential rules, the audit line and the rule that no secret value reaches an answer. The statements the core
/// really runs are proved against a store in <see cref="ServerEditLiveTests"/>.
/// </summary>
[Collection("darling-owned-secrets")]
public sealed class ServerEditCoreTests : IDisposable
{
    private const string Secret = "env:SYNTH_EDIT_SECRET_REF";
    private const string LiteralSecret = "Lit3ral-Sw0rd-Value";

    private readonly DarlingOwnedSet _ownedBefore = DarlingOwnedSecrets.Current;

    public ServerEditCoreTests() => DarlingOwnedSecrets.Set(DarlingOwnedSet.Empty);

    public void Dispose() => DarlingOwnedSecrets.Set(_ownedBefore);

    private static readonly DateTime Stamp = new(2026, 10, 5, 12, 0, 0, 123, DateTimeKind.Unspecified);

    private static Edit.ServerEditRow SqlRow(string host = "alpha-01.example.test", string auth = "sql", string engine = "sqlserver") => new(
        ServerId: 41, Name: "alpha-01", Host: host, Port: 0, Database: null, ReadOnlyIntent: false, Engine: engine,
        Auth: auth, Username: auth == "integrated" ? null : "monitor", EncryptMode: "Mandatory", TrustServerCertificate: false,
        MultiSubnetFailover: false, MonthlyCostUsd: 10m, ModifiedAt: Stamp.AddTicks(7));

    private static readonly Edit.ServerProbe Reachable = (_, _) => Task.FromResult(
        new ConnectionProbeResult(true, 15, 3, "Enterprise", false, false, false, true, null));

    private static Edit.ServerProbe Unreachable(string error) => (_, _) => Task.FromResult(
        new ConnectionProbeResult(false, 0, 0, null, false, false, false, false, error));

    private static readonly Edit.ServerProbe ThrowingProbe =
        (_, _) => throw new InvalidOperationException("the probe must not run for this request");

    private sealed class FakeStore : Edit.IServerEditStore
    {
        public Edit.ServerEditRow? Row { get; set; }
        public List<string> OtherKeys { get; } = [];
        public int Reads { get; private set; }
        public int Writes { get; private set; }
        public List<Edit.EditColumnValue> LastSets { get; private set; } = [];
        public string? LastNewKey { get; private set; }
        public Edit.ServerEditWriteKind WriteResult { get; set; } = Edit.ServerEditWriteKind.Written;

        public Task<Edit.ServerEditRow?> ReadRowAsync(int serverId, CancellationToken cancellationToken)
        {
            Reads++;
            return Task.FromResult(Row);
        }

        public Task<List<string>> LoadOtherStorageKeysAsync(int serverId, CancellationToken cancellationToken) =>
            Task.FromResult(OtherKeys.ToList());

        public Task<Edit.ServerEditWrite> WriteAsync(
            int serverId, DateTime expectedModifiedAt, IReadOnlyList<Edit.EditColumnValue> sets, string? newStorageKey, CancellationToken cancellationToken)
        {
            Writes++;
            LastSets = sets.ToList();
            LastNewKey = newStorageKey;
            return Task.FromResult(new Edit.ServerEditWrite(WriteResult, Stamp.AddSeconds(1)));
        }
    }

    private sealed class CapturingLogger : ILogger
    {
        public List<string> Lines { get; } = [];

        public IDisposable? BeginScope<TState>(TState state) where TState : notnull => null;

        public bool IsEnabled(LogLevel logLevel) => true;

        public void Log<TState>(LogLevel logLevel, EventId eventId, TState state, Exception? exception, Func<TState, Exception?, string> formatter) =>
            Lines.Add(formatter(state, exception));
    }

    private static Task<string> Run(FakeStore store, string changes, Edit.ServerProbe? probe = null, bool isWindows = true, ILogger? logger = null) =>
        Edit.EditServerCoreAsync(store, 41, changes, probe ?? ThrowingProbe, isWindows, logger, CancellationToken.None);

    private static string Status(string answer) => JsonNode.Parse(answer)!["status"]!.GetValue<string>();

    /* ---------------- parse ---------------- */

    [Theory]
    [InlineData("engine")]
    [InlineData("is_enabled")]
    [InlineData("excluded_databases")]
    [InlineData("server_id")]
    [InlineData("capture_plans")]
    [InlineData("alert_delivery_mode_override")]
    [InlineData("plan_force_bot_enabled")]
    [InlineData("remediation_username")]
    [InlineData("remediation_encrypted_password")]
    public async Task ARefusedKey_IsInvalid_AndNothingIsReadOrWritten(string key)
    {
        var store = new FakeStore { Row = SqlRow() };
        var answer = await Run(store, $"{{\"display_name\":\"x\",\"{key}\":1}}");

        Assert.Equal("invalid", Status(answer));
        Assert.Contains(key.StartsWith("remediation_", StringComparison.Ordinal) ? "remediation_*" : key, answer, StringComparison.Ordinal);
        Assert.Contains("cannot be changed here", answer, StringComparison.Ordinal);
        Assert.Equal(0, store.Reads);
        Assert.Equal(0, store.Writes);
    }

    [Fact]
    public void AnUnknownKey_IsRefused_NamingTheEditableKeys_AndNeverEchoingAValue()
    {
        var (changes, error) = Edit.ParseEditChanges("{\"passwrd\":\"" + LiteralSecret + "\"}");

        Assert.Null(changes);
        Assert.Contains("not an editable field", error, StringComparison.Ordinal);
        Assert.Contains("display_name", error, StringComparison.Ordinal);
        Assert.DoesNotContain(LiteralSecret, error, StringComparison.Ordinal);
    }

    [Theory]
    [InlineData("")]
    [InlineData("   ")]
    [InlineData("not json")]
    [InlineData("[]")]
    [InlineData("{}")]
    [InlineData("{\"display_name\":\"a\",\"display_name\":\"b\"}")]
    [InlineData("{\"port\":70000}")]
    [InlineData("{\"port\":\"abc\"}")]
    [InlineData("{\"host\":\"  \"}")]
    [InlineData("{\"host\":5}")]
    [InlineData("{\"read_only_intent\":\"yes\"}")]
    [InlineData("{\"trust_server_certificate\":1}")]
    [InlineData("{\"multi_subnet_failover\":null}")]
    [InlineData("{\"auth\":\"EntraMFA\"}")]
    [InlineData("{\"auth\":\"\"}")]
    [InlineData("{\"encrypt_mode\":\"Sometimes\"}")]
    [InlineData("{\"monthly_cost_usd\":-1}")]
    [InlineData("{\"monthly_cost_usd\":\"lots\"}")]
    [InlineData("{\"password\":\"\"}")]
    [InlineData("{\"expected_modified_at\":\"\"}")]
    public void ABadBody_IsInvalid_BeforeAnyStoreAccess(string json)
    {
        var (changes, error) = Edit.ParseEditChanges(json);

        Assert.Null(changes);
        Assert.False(string.IsNullOrWhiteSpace(error));
    }

    [Fact]
    public void AGoodBody_ParsesEveryField_AndLeavesTheUnnamedOnesUntouched()
    {
        var (c, error) = Edit.ParseEditChanges(
            "{\"display_name\":\" Orders \",\"host\":\"b.example.test\",\"port\":\"5433\",\"database\":null,\"read_only_intent\":true," +
            "\"auth\":\"sql\",\"username\":\"u\",\"password\":\"" + Secret + "\",\"encrypt_mode\":\"strict\"," +
            "\"trust_server_certificate\":true,\"multi_subnet_failover\":true,\"monthly_cost_usd\":12.5,\"expected_modified_at\":\"t\"}");

        Assert.Null(error);
        Assert.Equal("Orders", c!.DisplayName);
        Assert.Equal("b.example.test", c.Host);
        Assert.Equal(5433, c.Port);
        Assert.True(c.HasDatabase);
        Assert.Null(c.Database);
        Assert.Equal("sql", c.StoreAuth);
        Assert.Equal("Strict", c.EncryptMode);
        Assert.Equal(12.5m, c.MonthlyCostUsd);

        var (only, _) = Edit.ParseEditChanges("{\"monthly_cost_usd\":1}");
        Assert.False(only!.HasDatabase);
        Assert.False(only.HasUsername);
        Assert.Null(only.Host);
        Assert.Null(only.Port);
    }

    /* ---------------- the SET list ---------------- */

    [Fact]
    public void TheWritableColumns_NeverIncludeAnIdentityOrAnOwnerOnlyColumn()
    {
        var forbidden = new[]
        {
            "server_id", "engine", "is_enabled", "excluded_databases", "capture_plans", "alert_delivery_mode_override",
            "plan_force_bot_enabled", "remediation_username", "remediation_encrypted_password", "created_at",
        };

        Assert.Empty(Edit.EditColumnOfField.Values.Intersect(forbidden));
        Assert.Equal(12, Edit.EditColumnOfField.Count);
    }

    [Fact]
    public void TheUpdateSql_ListsOnlyTheColumnsGiven_CarriesTheTokenPredicate_AndNeverReadsTheSecretColumn()
    {
        var sql = Edit.BuildEditUpdateSql(
        [
            new Edit.EditColumnValue("display_name", "name", NpgsqlDbType.Text, "x"),
            new Edit.EditColumnValue("password", "encrypted_password", NpgsqlDbType.Text, "blob"),
        ]);

        Assert.Equal(
            "UPDATE config_monitored_servers SET name = $3, encrypted_password = $4, modified_at = (now() AT TIME ZONE 'UTC') " +
            "WHERE server_id = $1 AND modified_at = $2 RETURNING modified_at", sql);
        Assert.DoesNotContain("COALESCE", sql, StringComparison.OrdinalIgnoreCase);
        Assert.DoesNotContain("SET server_id", sql, StringComparison.Ordinal);
        Assert.DoesNotContain("now()", sql.Replace("(now() AT TIME ZONE 'UTC')", ""), StringComparison.Ordinal);
    }

    [Fact]
    public void ASetOnAnUnlistedColumn_ThrowsBeforeAnySql()
    {
        var ex = Assert.Throws<InvalidOperationException>(() =>
            Edit.BuildEditUpdateSql([new Edit.EditColumnValue("x", "is_enabled", NpgsqlDbType.Boolean, false)]));
        Assert.Contains("is_enabled", ex.Message, StringComparison.Ordinal);
    }

    /* ---------------- plan: the credential rules ---------------- */

    private static (Edit.ServerEditPlan? Plan, string? Error) Plan(Edit.ServerEditRow row, string json, bool isWindows = true)
    {
        var (changes, error) = Edit.ParseEditChanges(json);
        Assert.Null(error);
        return Edit.PlanEdit(row, changes!, isWindows);
    }

    [Fact]
    public void ANonConnectionEdit_OnASqlRow_NeedsNoSecret_NoProbe_AndNoReconnectForCost()
    {
        var (cost, error) = Plan(SqlRow(), "{\"monthly_cost_usd\":99}");
        Assert.Null(error);
        Assert.Equal(["monthly_cost_usd"], cost!.ChangedFields);
        Assert.False(cost.NeedsProbe);
        Assert.False(cost.Reconnects);
        Assert.DoesNotContain(cost.Sets, s => s.Column == "encrypted_password");

        var (rename, _) = Plan(SqlRow(), "{\"display_name\":\"renamed\"}");
        Assert.False(rename!.NeedsProbe);
        Assert.True(rename.Reconnects, "the worker compares the display name, so a rename reconnects");
    }

    [Theory]
    [InlineData("{\"host\":\"beta-01.example.test\"}")]
    [InlineData("{\"database\":\"Orders\"}")]
    [InlineData("{\"read_only_intent\":true}")]
    [InlineData("{\"username\":\"someone-else\"}")]
    [InlineData("{\"encrypt_mode\":\"Strict\"}")]
    [InlineData("{\"trust_server_certificate\":true}")]
    [InlineData("{\"multi_subnet_failover\":true}")]
    public void AConnectionEdit_OnASqlRow_WithoutTheSecret_IsRefused_WithTheDocumentedSentence(string json)
    {
        var (plan, error) = Plan(SqlRow(), json);

        Assert.Null(plan);
        Assert.Equal(Edit.EditPasswordNeededText, error);
    }

    [Fact]
    public void AConnectionEdit_WithTheSecret_IsProbed_AndWritesTheSecretColumn()
    {
        var (plan, error) = Plan(SqlRow(), "{\"host\":\"beta-01.example.test\",\"password\":\"" + Secret + "\"}");

        Assert.Null(error);
        Assert.True(plan!.NeedsProbe);
        Assert.True(plan.Reconnects);
        Assert.True(plan.AddressChanged);
        Assert.Equal(["host", "password"], plan.ChangedFields);
        Assert.Equal(Secret, plan.PlaintextSecret);
    }

    [Fact]
    public void APortOnASqlServerRow_IsRefused_ThePortGoesInTheHost()
    {
        var (plan, error) = Plan(SqlRow(), "{\"port\":1444}");

        Assert.Null(plan);
        Assert.Equal("A SQL Server port goes in host, as host,port.", error);
    }

    [Fact]
    public void APortOnAPostgresRow_IsAccepted_AndNeedsTheSecret()
    {
        var (needs, needsError) = Plan(SqlRow(engine: "postgres"), "{\"port\":5433}");
        Assert.Null(needs);
        Assert.Equal(Edit.EditPasswordNeededText, needsError);

        var (plan, error) = Plan(SqlRow(engine: "postgres"), "{\"port\":5433,\"password\":\"" + Secret + "\"}");
        Assert.Null(error);
        Assert.Contains("port", plan!.ChangedFields);
    }

    [Fact]
    public void ACaseOnlyEncryptModeChange_IsUnchanged_NotAWriteOrAReconnect()
    {
        var (plan, error) = Plan(SqlRow(), "{\"encrypt_mode\":\"mandatory\"}");

        Assert.Null(error);
        Assert.Empty(plan!.Sets);
        Assert.False(plan.NeedsProbe);
    }

    [Theory]
    [InlineData("integrated")]
    [InlineData("managedidentity")]
    public void ACostOnlyEdit_OnANonSecretAuthRow_DoesNotReconnect(string auth)
    {
        var (plan, error) = Plan(SqlRow(auth: auth), "{\"monthly_cost_usd\":99}");

        Assert.Null(error);
        Assert.Equal(["monthly_cost_usd"], plan!.ChangedFields);
        Assert.False(plan.Reconnects);
    }

    [Fact]
    public void ASwitchFromASecretModeToANonSecretMode_StillReconnects()
    {
        var (plan, error) = Plan(SqlRow(), "{\"auth\":\"Windows\"}");

        Assert.Null(error);
        Assert.True(plan!.Reconnects);
    }

    [Fact]
    public async Task ARefusedKey_IsTruncated_AndARemediationKeyNamesOnlyThePrefix()
    {
        var longKey = new string('k', 60);
        var answer = await Run(new FakeStore { Row = SqlRow() }, "{\"remediation_password_hint_" + longKey + "\":1}");
        Assert.Equal("invalid", Status(answer));
        Assert.Contains("remediation_*", answer, StringComparison.Ordinal);
        Assert.DoesNotContain(longKey, answer, StringComparison.Ordinal);
    }

    [Fact]
    public async Task AConflictInsideTheWrite_CarriesTheCurrentValues_LikeThePreProbeConflict()
    {
        var store = new FakeStore { Row = SqlRow(), WriteResult = Edit.ServerEditWriteKind.Conflict };

        var answer = await Run(store, "{\"monthly_cost_usd\":5}");

        Assert.Equal("conflict", Status(answer));
        var current = JsonNode.Parse(answer)!["current"]!;
        Assert.Equal("alpha-01", current["display_name"]!.GetValue<string>());
        Assert.Equal(Edit.ModifiedAtToken(store.Row!.ModifiedAt), current["modified_at"]!.GetValue<string>());
    }

    [Fact]
    public void AWindowsRow_NeedsNoSecret_ToChangeItsHost_ButIsProbed()
    {
        var (plan, error) = Plan(SqlRow(auth: "integrated"), "{\"host\":\"beta-01.example.test\"}");

        Assert.Null(error);
        Assert.True(plan!.NeedsProbe);
        Assert.DoesNotContain(plan.Sets, s => s.Column == "encrypted_password");
    }

    [Fact]
    public void ASwitchIntoSql_RequiresThePassword_AndASwitchOutClearsTheStoredSecret()
    {
        var (refused, error) = Plan(SqlRow(auth: "integrated"), "{\"auth\":\"SQL\",\"username\":\"u\"}");
        Assert.Null(refused);
        Assert.Contains("needs the password", error, StringComparison.Ordinal);

        var (into, _) = Plan(SqlRow(auth: "integrated"), "{\"auth\":\"SQL\",\"username\":\"u\",\"password\":\"" + Secret + "\"}");
        Assert.Equal(["auth", "username", "password"], into!.ChangedFields);

        var (spRefused, spError) = Plan(SqlRow(), "{\"auth\":\"ServicePrincipal\",\"username\":\"app-id\"}");
        Assert.Null(spRefused);
        Assert.Contains("client secret", spError, StringComparison.Ordinal);

        var (outOf, outError) = Plan(SqlRow(), "{\"auth\":\"Windows\"}");
        Assert.Null(outError);
        var cleared = Assert.Single(outOf!.Sets, s => s.Column == "encrypted_password");
        Assert.Null(cleared.Value);
        Assert.Contains(outOf.Sets, s => s.Column == "username" && s.Value is null);
        Assert.Null(outOf.PlaintextSecret);
    }

    [Fact]
    public void APasswordSentToAWindowsRow_IsIgnored_AsAddIgnoresIt()
    {
        var (plan, error) = Plan(SqlRow(auth: "integrated"), "{\"monthly_cost_usd\":1,\"password\":\"" + Secret + "\"}");

        Assert.Null(error);
        Assert.Null(plan!.PlaintextSecret);
        Assert.DoesNotContain(plan.Sets, s => s.Column == "encrypted_password");
    }

    [Fact]
    public void ALiteralSecret_OffWindows_IsRefused_AsAddRefusesIt_AndTheTextNeverHoldsIt()
    {
        var (plan, error) = Plan(SqlRow(), "{\"host\":\"b.example.test\",\"password\":\"" + LiteralSecret + "\"}", isWindows: false);

        Assert.Null(plan);
        Assert.Equal(Edit.LiteralSecretRefusal(LiteralSecret, isWindows: false, isServicePrincipal: false), error);
        Assert.DoesNotContain(LiteralSecret, error, StringComparison.Ordinal);
    }

    [Fact]
    public void AReferenceToDarlingsOwnSecrets_IsRefused_WithTheOwnedSecretSentence()
    {
        DarlingOwnedSecrets.Set(new DarlingOwnedSet(Array.Empty<string>(), new[] { "DARLING_CONFIG" }));

        var (plan, error) = Plan(SqlRow(), "{\"host\":\"b.example.test\",\"password\":\"env:DARLING_CONFIG\"}");

        Assert.Null(plan);
        Assert.Equal(DarlingOwnedSecrets.ReferenceRefusalText, error);
    }

    [Fact]
    public void APostgresRow_CannotLeaveSqlAuth()
    {
        var row = SqlRow() with { Engine = "postgres", Port = 5432 };

        var (plan, error) = Plan(row, "{\"auth\":\"Windows\"}");

        Assert.Null(plan);
        Assert.Contains("PostgreSQL target requires", error, StringComparison.Ordinal);
    }

    [Fact]
    public void ABlankDisplayName_FallsBackToTheHost_AsAddDoes()
    {
        var (plan, _) = Plan(SqlRow(), "{\"display_name\":\"  \"}");

        Assert.Equal("alpha-01.example.test", plan!.NewName);
    }

    /* ---------------- the core ---------------- */

    [Fact]
    public async Task AStaleToken_IsAConflict_WithTheCurrentValues_AndNothingIsWritten()
    {
        var store = new FakeStore { Row = SqlRow() };

        var answer = await Run(store, "{\"monthly_cost_usd\":5,\"expected_modified_at\":\"2026-10-05T11:00:00.0000000\"}");

        Assert.Equal("conflict", Status(answer));
        Assert.Equal("alpha-01", JsonNode.Parse(answer)!["current"]!["display_name"]!.GetValue<string>());
        Assert.Equal(Edit.ModifiedAtToken(store.Row!.ModifiedAt), JsonNode.Parse(answer)!["current"]!["modified_at"]!.GetValue<string>());
        Assert.Equal(0, store.Writes);
    }

    [Fact]
    public async Task TheCurrentToken_KeepsMicroseconds_AndIsAccepted()
    {
        var store = new FakeStore { Row = SqlRow() };
        var token = Edit.ModifiedAtToken(store.Row!.ModifiedAt);
        Assert.EndsWith("1230007", token, StringComparison.Ordinal);

        var answer = await Run(store, $"{{\"monthly_cost_usd\":5,\"expected_modified_at\":\"{token}\"}}");

        Assert.Equal("updated", Status(answer));
        Assert.Equal(1, store.Writes);
    }

    [Fact]
    public async Task ARequestEqualToTheStoredValues_IsUnchanged_WithZeroWrites_AndNoProbe()
    {
        var store = new FakeStore { Row = SqlRow() };

        var answer = await Run(store, "{\"monthly_cost_usd\":10,\"host\":\"alpha-01.example.test\"}");

        Assert.Equal("unchanged", Status(answer));
        Assert.Equal(0, store.Writes);
    }

    [Fact]
    public async Task AMissingRow_IsNotFound()
    {
        var answer = await Run(new FakeStore { Row = null }, "{\"monthly_cost_usd\":5}");

        Assert.Equal("not_found", Status(answer));
    }

    [Fact]
    public async Task AnAddressAnotherServerHolds_Collides_AsOccupied_BeforeTheProbe()
    {
        var store = new FakeStore { Row = SqlRow() };
        store.OtherKeys.Add("BETA-01.example.test");

        var answer = await Run(store, "{\"host\":\"beta-01.example.test\",\"password\":\"" + Secret + "\"}");

        Assert.Equal("collides", Status(answer));
        Assert.Equal("occupied", JsonNode.Parse(answer)!["reason"]!.GetValue<string>());
        Assert.Equal(0, store.Writes);
    }

    [Fact]
    public async Task AProbeThatLandsInADatabaseAnotherServerCovers_Collides_AndNothingIsWritten()
    {
        var store = new FakeStore { Row = SqlRow() };
        store.OtherKeys.Add("alpha-01.example.test:Covered");
        Edit.ServerProbe lands = (_, _) => Task.FromResult(new ConnectionProbeResult(
            true, 15, 3, "Enterprise", false, false, false, true, null, ConnectedDatabase: "Covered"));

        var answer = await Run(store, "{\"read_only_intent\":false,\"database\":\"Wrong\",\"password\":\"" + Secret + "\"}", lands);

        Assert.Equal("collides", Status(answer));
        Assert.Equal(0, store.Writes);
    }

    [Fact]
    public async Task AFailedProbe_IsConnectionFailed_NothingIsWritten_AndTheSecretIsRedactedFromTheDetail()
    {
        var store = new FakeStore { Row = SqlRow() };

        var answer = await Run(store, "{\"host\":\"b.example.test\",\"password\":\"" + Secret + "\"}", Unreachable("login failed for " + Secret));

        Assert.Equal("connection_failed", Status(answer));
        Assert.DoesNotContain(Secret, answer, StringComparison.Ordinal);
        Assert.Equal(0, store.Writes);
    }

    [Theory]
    [InlineData(0, "updated")]
    [InlineData(1, "not_found")]
    [InlineData(2, "conflict")]
    [InlineData(3, "collides")]
    public async Task TheStoresWriteVerdict_IsTheAnswer(int verdict, string expected)
    {
        var store = new FakeStore { Row = SqlRow(), WriteResult = (Edit.ServerEditWriteKind)verdict };

        var answer = await Run(store, "{\"monthly_cost_usd\":5}");

        Assert.Equal(expected, Status(answer));
    }

    [Fact]
    public async Task AnUpdated_NamesWhatChanged_TheToken_AndTheOneTimeAddressSentence()
    {
        var store = new FakeStore { Row = SqlRow() };

        var answer = JsonNode.Parse(await Run(store, "{\"host\":\"b.example.test\",\"display_name\":\"Beta\",\"password\":\"" + Secret + "\"}", Reachable))!;

        Assert.Equal("updated", answer["status"]!.GetValue<string>());
        Assert.Equal(41, answer["server_id"]!.GetValue<int>());
        Assert.Equal(["display_name", "host", "password"], answer["changed"]!.AsArray().Select(n => n!.GetValue<string>()).ToArray());
        Assert.True(answer["reconnects"]!.GetValue<bool>());
        Assert.True(answer["tested"]!.GetValue<bool>());
        Assert.Equal(Edit.ModifiedAtToken(Stamp.AddSeconds(1)), answer["modified_at"]!.GetValue<string>());
        Assert.Contains("keeps its id and history", answer["note"]!.GetValue<string>(), StringComparison.Ordinal);
        Assert.Equal("b.example.test", store.LastNewKey);

        var costOnly = JsonNode.Parse(await Run(new FakeStore { Row = SqlRow() }, "{\"monthly_cost_usd\":5}"))!;
        Assert.DoesNotContain("keeps its id", costOnly["note"]!.GetValue<string>(), StringComparison.Ordinal);
        Assert.False(costOnly["tested"]!.GetValue<bool>());
        Assert.False(costOnly["reconnects"]!.GetValue<bool>());
    }

    [Fact]
    public async Task TheColumnsWritten_AreExactlyTheChangedOnes_NeverAnIdentityOrOwnerColumn()
    {
        var store = new FakeStore { Row = SqlRow(engine: "postgres") };

        await Run(store, "{\"host\":\"b.example.test\",\"port\":1444,\"database\":\"D\",\"read_only_intent\":true,\"monthly_cost_usd\":3," +
                         "\"encrypt_mode\":\"Strict\",\"trust_server_certificate\":true,\"multi_subnet_failover\":true,\"username\":\"u2\"," +
                         "\"display_name\":\"n\",\"password\":\"" + Secret + "\"}", Reachable);

        var columns = store.LastSets.Select(s => s.Column).OrderBy(c => c, StringComparer.Ordinal).ToArray();
        Assert.Equal(
            ["database", "encrypt_mode", "encrypted_password", "host", "monthly_cost_usd", "multi_subnet_failover", "name", "port",
             "read_only_intent", "trust_server_certificate", "username"],
            columns);
        var sql = Edit.BuildEditUpdateSql(store.LastSets);
        foreach (var banned in new[] { "server_id =", "engine", "is_enabled", "excluded_databases", "remediation_", "plan_force_bot_enabled", "capture_plans" })
        {
            Assert.DoesNotContain(banned, sql.Replace("WHERE server_id = $1", ""), StringComparison.Ordinal);
        }
    }

    [Fact]
    public async Task TheStoredSecret_IsTheProtectedReference_NotThePlaintextForm()
    {
        var store = new FakeStore { Row = SqlRow() };

        await Run(store, "{\"host\":\"b.example.test\",\"password\":\"" + Secret + "\"}", Reachable);

        var stored = Assert.Single(store.LastSets, s => s.Column == "encrypted_password");
        Assert.Equal(Secret, stored.Value);
    }

    [Fact]
    public async Task NoAnswerAndNoLogLine_ForAnyStatus_HoldsTheSubmittedSecret()
    {
        var logger = new CapturingLogger();
        var answers = new List<string>();
        var secretBody = "\"password\":\"" + Secret + "\"";

        answers.Add(await Run(new FakeStore { Row = SqlRow() }, "{\"host\":\"b.example.test\"," + secretBody + "}", Reachable, logger: logger));
        answers.Add(await Run(new FakeStore { Row = SqlRow() }, "{\"host\":\"b.example.test\"," + secretBody + ",\"expected_modified_at\":\"stale\"}", Reachable, logger: logger));
        answers.Add(await Run(new FakeStore { Row = SqlRow() }, "{\"host\":\"b.example.test\"," + secretBody + "}", Unreachable("bad " + Secret), logger: logger));
        answers.Add(await Run(new FakeStore { Row = null }, "{\"host\":\"b.example.test\"," + secretBody + "}", Reachable, logger: logger));
        answers.Add(await Run(new FakeStore { Row = SqlRow(), OtherKeys = { "b.example.test" } }, "{\"host\":\"b.example.test\"," + secretBody + "}", Reachable, logger: logger));
        answers.Add(await Run(new FakeStore { Row = SqlRow() }, "{\"engine\":\"postgres\"," + secretBody + "}", Reachable, logger: logger));
        answers.Add(await Run(new FakeStore { Row = SqlRow() }, "{\"passwrd\":\"" + Secret + "\"}", Reachable, logger: logger));
        answers.Add(await Run(new FakeStore { Row = SqlRow(), WriteResult = Edit.ServerEditWriteKind.Conflict }, "{\"host\":\"b.example.test\"," + secretBody + "}", Reachable, logger: logger));
        answers.Add(await Run(new FakeStore { Row = SqlRow() }, "{\"host\":\"b.example.test\",\"password\":\"" + LiteralSecret + "\"}", Reachable, isWindows: false, logger: logger));

        Assert.Equal(9, answers.Count);
        Assert.All(answers, a =>
        {
            Assert.DoesNotContain(Secret, a, StringComparison.Ordinal);
            Assert.DoesNotContain(LiteralSecret, a, StringComparison.Ordinal);
        });
        Assert.All(logger.Lines, l =>
        {
            Assert.DoesNotContain(Secret, l, StringComparison.Ordinal);
            Assert.DoesNotContain("SYNTH_EDIT_SECRET_REF", l, StringComparison.Ordinal);
        });
    }

    [Fact]
    public async Task TheAuditLine_NamesTheIdAndTheFieldNames_NeverAValue()
    {
        var logger = new CapturingLogger();
        var store = new FakeStore { Row = SqlRow() };

        await Run(store, "{\"display_name\":\"Distinct-New-Name\",\"host\":\"distinct-new-host.example.test\",\"password\":\"" + Secret + "\"}", Reachable, logger: logger);

        var line = Assert.Single(logger.Lines);
        Assert.Contains("41", line, StringComparison.Ordinal);
        Assert.Contains("display_name,host,password", line, StringComparison.Ordinal);
        Assert.DoesNotContain("Distinct-New-Name", line, StringComparison.Ordinal);
        Assert.DoesNotContain("distinct-new-host", line, StringComparison.Ordinal);
        Assert.DoesNotContain("SYNTH_EDIT_SECRET_REF", line, StringComparison.Ordinal);

        var quiet = new CapturingLogger();
        await Run(new FakeStore { Row = SqlRow() }, "{\"monthly_cost_usd\":10}", logger: quiet);
        await Run(new FakeStore { Row = SqlRow(), WriteResult = Edit.ServerEditWriteKind.Conflict }, "{\"monthly_cost_usd\":11}", logger: quiet);
        Assert.Empty(quiet.Lines);
    }
}
