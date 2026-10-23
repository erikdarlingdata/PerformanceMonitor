/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor Lite.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Threading.Tasks;
using DuckDB.NET.Data;
using PerformanceMonitor.Common;
using PerformanceMonitor.Notifications;
using PerformanceMonitorLite.Database;
using PerformanceMonitorLite.Helpers;
using PerformanceMonitorLite.Services;
using Xunit;

namespace PerformanceMonitorLite.Tests;

/// <summary>
/// #5750 security review, round 1, for Lite's route secrets. Credential Manager can refuse a value (the wrapper stops at 512
/// characters) and the old store ignored that: <c>UpdateAsync</c> committed the row (new scope, has-value flag set) and then
/// saved secrets through a delegate that swallowed the result, so a route moved to another server with a long URL kept sending
/// to the OLD URL while the dialog reported success. These pin the order (#5775: new keys under a new generation first, then
/// the row, with the new keys removed on any failure), the inline message, the delete and reused-id cases, and the rule that
/// an unreadable saved destination keeps its flag unless the user removes it. The crash and fault cases are in
/// <see cref="NotificationRouteSecretGenerationTests"/>.
/// </summary>
[Collection("app-alert-statics")]
public sealed class NotificationRouteSecretSaveTests : IDisposable
{
    private const string OldTeams = "https://teams.old-secret.test/hook/aaa111";
    private const string OldSlack = "https://hooks.slack.old-secret.test/T0/B0/xyz";
    private const int StorableLength = CredentialStore.MaxSecretLength;

    private readonly string _tempDir;
    private readonly string _dbPath;
    private readonly DuckDbInitializer _initializer;
    private readonly FakeRouteSecrets _secrets = new();
    private readonly DuckDbNotificationRouteStore _store;

    public NotificationRouteSecretSaveTests()
    {
        _tempDir = Path.Combine(Path.GetTempPath(), "NotificationRouteSecretSaveTests_" + Guid.NewGuid().ToString("N")[..8]);
        Directory.CreateDirectory(_tempDir);
        _dbPath = Path.Combine(_tempDir, "lite.duckdb");
        _initializer = new DuckDbInitializer(_dbPath);
        _initializer.InitializeAsync().GetAwaiter().GetResult();
        _store = new DuckDbNotificationRouteStore(
            _initializer,
            () => new[] { 7101, 7102 },
            _secrets.Access);
    }

    public void Dispose()
    {
        NotificationRouteCache.Configure(null);
        _initializer.Dispose();
        try { if (Directory.Exists(_tempDir)) Directory.Delete(_tempDir, recursive: true); }
        catch (IOException) { /* best-effort cleanup */ }
        catch (UnauthorizedAccessException) { /* best-effort cleanup */ }
    }

    private static NotificationRoute Route(string match, int? server = null, string teams = "", string slack = "") =>
        new(0, match, teams, slack, "", "", "", true) { ScopeServerId = server };

    /// <summary>A refusal like the wrapper's: nothing over the limit is stored.</summary>
    private void RefuseOverLimit() => _secrets.WriteGate = (_, value) => value.Length <= StorableLength;

    private async Task<bool> FlagAsync(int id, string column)
    {
        using var conn = new DuckDBConnection($"Data Source={_dbPath}");
        await conn.OpenAsync();
        using var cmd = conn.CreateCommand();
        cmd.CommandText = $"SELECT {column} FROM notification_routes WHERE route_id = {id}";
        return Convert.ToBoolean(await cmd.ExecuteScalarAsync());
    }

    [Fact]
    public async Task MovingARouteWithAnOverLimitUrl_IsRefused_AndKeepsTheOldRowAndTheOldDestination()
    {
        var id = await _store.InsertAsync(Route("High CPU", server: 7101, teams: OldTeams));
        RefuseOverLimit();
        var longUrl = "https://teams.new-secret.test/" + new string('x', StorableLength + 20);
        var saved = (await _store.GetAsync(id))!;

        var ex = await Assert.ThrowsAsync<RouteSecretException>(() =>
            _store.UpdateAsync(saved with { ScopeServerId = 7102, TeamsUrl = longUrl }));

        Assert.Contains("Teams", ex.Message);
        Assert.Contains("at most 1,280", ex.Message);
        Assert.DoesNotContain("new-secret", ex.Message);
        Assert.DoesNotContain(longUrl, ex.Message);
        var after = (await _store.GetAsync(id))!;
        Assert.Equal(7101, after.ScopeServerId);
        Assert.Equal(OldTeams, after.TeamsUrl);
        Assert.Equal(OldTeams, _secrets.ValueOf(id, "Teams"));
        Assert.Single(_secrets.Keys);
    }

    [Fact]
    public async Task ARefusedValueUnderTheLimit_GetsNoLengthHint()
    {
        var id = await _store.InsertAsync(Route("High CPU", server: 7101, teams: OldTeams));
        _secrets.WriteGate = (_, value) => value == OldTeams;
        var saved = (await _store.GetAsync(id))!;

        var ex = await Assert.ThrowsAsync<RouteSecretException>(() =>
            _store.UpdateAsync(saved with { TeamsUrl = "https://teams.short-secret.test/h" }));

        Assert.DoesNotContain("characters", ex.Message);
        Assert.DoesNotContain("short-secret", ex.Message);
    }

    [Fact]
    public async Task ASecondChannelThatFails_RemovesTheFirstChannelsNewKey_AndKeepsBothOldOnes()
    {
        var id = await _store.InsertAsync(Route("High CPU", server: 7101, teams: OldTeams, slack: OldSlack));
        _secrets.WriteGate = (key, value) => !key.Contains("_Slack_", StringComparison.Ordinal) || value == OldSlack;
        var saved = (await _store.GetAsync(id))!;

        await Assert.ThrowsAsync<RouteSecretException>(() => _store.UpdateAsync(saved with
        {
            ScopeServerId = 7102,
            TeamsUrl = "https://teams.second-secret.test/new",
            SlackUrl = "https://hooks.slack.second-secret.test/new"
        }));

        _secrets.WriteGate = (_, _) => true;
        var after = (await _store.GetAsync(id))!;
        Assert.Equal(7101, after.ScopeServerId);
        Assert.Equal(OldTeams, after.TeamsUrl);
        Assert.Equal(OldSlack, after.SlackUrl);
        Assert.Equal(2, _secrets.Count);
    }

    [Fact]
    public async Task AValueThatReadsBackDifferently_IsRefused()
    {
        _secrets.Keeps = v => v.Length > 20 ? v[..20] : v;

        await Assert.ThrowsAsync<RouteSecretException>(() => _store.InsertAsync(Route("High CPU", server: 7101, teams: OldTeams)));

        Assert.Empty(await _store.ListAsync());
        Assert.Equal(0, _secrets.Count);
    }

    [Fact]
    public async Task AnInsertWhoseSecretIsRefused_WritesNoRowAndLeavesNoCredential()
    {
        RefuseOverLimit();
        var longUrl = "https://teams.new-secret.test/" + new string('x', StorableLength);

        var ex = await Assert.ThrowsAsync<RouteSecretException>(() =>
            _store.InsertAsync(Route("High CPU", server: 7101, slack: OldSlack, teams: longUrl)));

        Assert.Contains("at most 1,280", ex.Message);
        Assert.Empty(await _store.ListAsync());
        Assert.Equal(0, _secrets.Count);
    }

    /* ADeleteWhoseCredentialWillNotGo_StillDeletesTheRow and AReusedRouteId_DoesNotInheritACredentialLeftByAFailedDelete live in
       NotificationRouteSecretGenerationTests: a delete that fails logs the "could not be removed" warning into the shared buffer,
       and that class (app-logger-statics) counts those warnings while this one (app-alert-statics) runs beside it. */

    [Fact]
    public async Task ANamedOffChannelWithNoSavedFlag_IsNotInvented()
    {
        var id = await _store.InsertAsync(Route("High CPU", server: 7101, teams: OldTeams));
        var loaded = (await _store.GetAsync(id))!;

        Assert.True(await _store.UpdateAsync(loaded with { OffChannels = new[] { NotificationRouter.SlackChannel } }));

        Assert.False(await FlagAsync(id, "slack_set"));
    }

    [Fact]
    public void TheEditDialogHelpers_KeepAnUnreadableChannel_OnlyWhileItsMaskIsUntouched()
    {
        Assert.Equal(NotificationRouteUi.KeepMask, NotificationRouteUi.SecretBoxInitialText("", unreadable: true));
        Assert.Equal("", NotificationRouteUi.SecretBoxInitialText("", unreadable: false));

        Assert.True(NotificationRouteUi.KeepsUnreadable(NotificationRouteUi.KeepMask, "", unreadable: true));
        Assert.False(NotificationRouteUi.KeepsUnreadable("", "", unreadable: true));
        Assert.False(NotificationRouteUi.KeepsUnreadable("https://x.test/new", "", unreadable: true));
        Assert.False(NotificationRouteUi.KeepsUnreadable(NotificationRouteUi.KeepMask, "", unreadable: false));
        Assert.False(NotificationRouteUi.KeepsUnreadable(NotificationRouteUi.KeepMask, OldTeams, unreadable: true));
    }
}
