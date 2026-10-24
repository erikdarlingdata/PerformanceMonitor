/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor Lite.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using System.IO;
using System.Linq;
using System.Threading.Tasks;
using DuckDB.NET.Data;
using PerformanceMonitor.Common;
using PerformanceMonitor.Notifications;
using PerformanceMonitorLite.Database;
using PerformanceMonitorLite.Services;
using Xunit;

namespace PerformanceMonitorLite.Tests;

/// <summary>
/// #5775: a route's row (DuckDB) and its destinations (Credential Manager) agree after a crash at any point of a save. The
/// destinations live under <c>NotificationRoute_&lt;id&gt;_&lt;Channel&gt;_&lt;gen&gt;</c>, a new random generation per write, and
/// the row names the generation it reads. The order is (1) new keys, (2) one row write, (3) delete the old keys. "Crash" here
/// is a hook that throws at that point after switching the fake store off (<see cref="FakeRouteSecrets.Dead"/>), so the
/// store's own cleanup cannot run, as after a real process death; a second store over the same file then stands in for the
/// restart. Credential Manager is an in-memory fake, never the real one.
///
/// <para>Collection <c>app-logger-statics</c>, not <c>app-alert-statics</c>: these tests drain the process-wide log buffer,
/// and a drain from beside that collection took the lines its classes count (#5829: <c>WatermarkReadFailureLogTests</c> saw 2
/// of its 4). Nothing here configures the route cache; the store's writes only invalidate it, which the cache tests already
/// allow from other classes.</para>
/// </summary>
[Collection("app-logger-statics")]
public sealed class NotificationRouteSecretGenerationTests : IDisposable
{
    private const string OldTeams = "https://teams.old-secret.test/hook/aaa111";
    private const string NewTeams = "https://teams.new-secret.test/hook/bbb222";
    private const string OldSlack = "https://hooks.slack.old-secret.test/T0/B0/xyz";
    private const string NewSlack = "https://hooks.slack.new-secret.test/T1/B1/uvw";
    private const int StorableLength = CredentialStore.MaxSecretLength;

    private readonly string _tempDir;
    private readonly string _dbPath;
    private readonly DuckDbInitializer _initializer;
    private readonly FakeRouteSecrets _secrets = new();
    private readonly DuckDbNotificationRouteStore _store;

    public NotificationRouteSecretGenerationTests()
    {
        _tempDir = Path.Combine(Path.GetTempPath(), "NotificationRouteSecretGenerationTests_" + Guid.NewGuid().ToString("N")[..8]);
        Directory.CreateDirectory(_tempDir);
        _dbPath = Path.Combine(_tempDir, "lite.duckdb");
        _initializer = new DuckDbInitializer(_dbPath);
        _initializer.InitializeAsync().GetAwaiter().GetResult();
        _store = NewStore();
    }

    public void Dispose()
    {
        _initializer.Dispose();
        try { if (Directory.Exists(_tempDir)) Directory.Delete(_tempDir, recursive: true); }
        catch (IOException) { /* best-effort cleanup */ }
        catch (UnauthorizedAccessException) { /* best-effort cleanup */ }
    }

    /// <summary>A store over the same file and the same fake secrets: what a restart builds.</summary>
    private DuckDbNotificationRouteStore NewStore() =>
        new(_initializer, () => new[] { 7101, 7102 }, _secrets.Access);

    private static NotificationRoute Route(string match, int? server = null, string teams = "", string slack = "") =>
        new(0, match, teams, slack, "", "", "", true) { ScopeServerId = server };

    private async Task<string?> GenAsync(int id, string column)
    {
        using var conn = new DuckDBConnection($"Data Source={_dbPath}");
        await conn.OpenAsync();
        using var cmd = conn.CreateCommand();
        cmd.CommandText = $"SELECT {column} FROM notification_routes WHERE route_id = {id}";
        var value = await cmd.ExecuteScalarAsync();
        return value is null or DBNull ? null : Convert.ToString(value);
    }

    /* The log buffer is process-wide and classes in other collections write to it at the same time, so a test reads only the
       lines with its own wording. A drain here can also take lines another class counts, so every class that counts buffered
       lines is in app-logger-statics. The reverse holds too: a class outside it must not log this wording. The failed-delete
       tests from NotificationRouteSecretSaveTests (app-alert-statics) were moved into this class for that reason, because their
       "could not be removed" warning carries only a route id, and ids are not unique across the two classes' stores. */
    private static string[] RouteLines(string wording) =>
        AppLogger.DrainBufferedLines()
            .Where(l => l.Contains("[NotificationRoutes]", StringComparison.Ordinal) && l.Contains(wording, StringComparison.Ordinal))
            .ToArray();

    // ---- the key shape -----------------------------------------------------------------------------

    [Fact]
    public async Task EachWrite_GoesToAFreshRandomGeneration_TheRowNamesIt_AndTheOldKeyIsGone()
    {
        var id = await _store.InsertAsync(Route("High CPU", server: 7101, teams: OldTeams));
        var firstGen = await GenAsync(id, "teams_secret_gen");
        Assert.Matches("^[0-9a-f]{32}$", firstGen!);
        Assert.Equal(new[] { DuckDbNotificationRouteStore.SecretKey(id, "Teams", firstGen!) }, _secrets.Keys.ToArray());
        Assert.Null(await GenAsync(id, "slack_secret_gen"));

        Assert.True(await _store.UpdateAsync((await _store.GetAsync(id))! with { TeamsUrl = NewTeams }));

        var secondGen = await GenAsync(id, "teams_secret_gen");
        Assert.NotEqual(firstGen, secondGen);
        Assert.Equal(new[] { DuckDbNotificationRouteStore.SecretKey(id, "Teams", secondGen!) }, _secrets.Keys.ToArray());
        Assert.Equal(NewTeams, (await _store.GetAsync(id))!.TeamsUrl);
    }

    [Fact]
    public async Task AnEditThatChangesNoDestination_WritesAndDeletesNothingInCredentialManager()
    {
        var id = await _store.InsertAsync(Route("High CPU", server: 7101, teams: OldTeams, slack: OldSlack));
        var gen = await GenAsync(id, "teams_secret_gen");
        var writes = _secrets.WriteCalls;
        var deletes = _secrets.DeleteCalls;

        Assert.True(await _store.UpdateAsync((await _store.GetAsync(id))! with { ScopeServerId = 7102 }));

        Assert.Equal(writes, _secrets.WriteCalls);
        Assert.Equal(deletes, _secrets.DeleteCalls);
        Assert.Equal(gen, await GenAsync(id, "teams_secret_gen"));
        Assert.Equal(7102, (await _store.GetAsync(id))!.ScopeServerId);
    }

    // ---- a crash at each step ----------------------------------------------------------------------

    [Fact]
    public async Task ACrashAfterTheNewKeysAreWritten_LeavesTheOldDestinationsInForce_AndTheSweepRemovesTheNewKeys()
    {
        var id = await _store.InsertAsync(Route("High CPU", server: 7101, teams: OldTeams));
        var saved = (await _store.GetAsync(id))!;
        _store.BeforeRowWriteForTests = () => { _secrets.Dead = true; throw new InvalidOperationException("process died"); };

        await Assert.ThrowsAsync<InvalidOperationException>(() =>
            _store.UpdateAsync(saved with { ScopeServerId = 7102, TeamsUrl = NewTeams }));

        /* The restart: the row is untouched, so the old scope sends to the old URL; the new key is there but nothing reads it. */
        _secrets.Dead = false;
        var restarted = NewStore();
        var after = (await restarted.GetAsync(id))!;
        Assert.Equal(7101, after.ScopeServerId);
        Assert.Equal(OldTeams, after.TeamsUrl);
        Assert.Equal(2, _secrets.Count);

        Assert.Equal(1, await restarted.SweepOrphanedSecretsAsync());

        Assert.Single(_secrets.Keys);
        Assert.Equal(OldTeams, (await restarted.GetAsync(id))!.TeamsUrl);
    }

    [Fact]
    public async Task ACrashAfterTheRowIsWritten_LeavesTheNewDestinationsInForce_AndTheSweepRemovesTheOldKeys()
    {
        var id = await _store.InsertAsync(Route("High CPU", server: 7101, teams: OldTeams));
        var saved = (await _store.GetAsync(id))!;
        _store.AfterRowWriteForTests = () => { _secrets.Dead = true; throw new InvalidOperationException("process died"); };

        /* The save is kept: the row holds the new values, so there is nothing to roll back. */
        Assert.True(await _store.UpdateAsync(saved with { ScopeServerId = 7102, TeamsUrl = NewTeams }));

        _secrets.Dead = false;
        var restarted = NewStore();
        var after = (await restarted.GetAsync(id))!;
        Assert.Equal(7102, after.ScopeServerId);
        Assert.Equal(NewTeams, after.TeamsUrl);
        Assert.Equal(2, _secrets.Count);

        Assert.Equal(1, await restarted.SweepOrphanedSecretsAsync());

        Assert.Single(_secrets.Keys);
        Assert.Equal(NewTeams, (await restarted.GetAsync(id))!.TeamsUrl);
    }

    [Fact]
    public async Task ADeleteWhoseCredentialWillNotGo_StillDeletesTheRow()
    {
        var id = await _store.InsertAsync(Route("High CPU", server: 7101, teams: OldTeams));
        _secrets.DeleteGate = _ => false;

        Assert.True(await _store.DeleteAsync(id));

        Assert.Empty(await _store.ListAsync());
    }

    [Fact]
    public async Task AReusedRouteId_DoesNotInheritACredentialLeftByAFailedDelete()
    {
        var id = await _store.InsertAsync(Route("High CPU", server: 7101, teams: OldTeams, slack: OldSlack));
        _secrets.DeleteGate = _ => false;
        await _store.DeleteAsync(id);
        Assert.True(_secrets.Has(id, "Slack"), "precondition: the failed delete left the credential");
        _secrets.DeleteGate = _ => true;

        var reused = await _store.InsertAsync(Route("Blocking", server: 7102, teams: "https://teams.fresh-secret.test/h"));

        /* The leftover is under another generation than anything the new row points at, so it is never read (#5775). */
        Assert.Equal(id, reused);
        Assert.False(await FlagAsync(reused, "slack_set"));
        Assert.Equal("", (await _store.GetAsync(reused))!.SlackUrl);
        Assert.Equal("https://teams.fresh-secret.test/h", (await _store.GetAsync(reused))!.TeamsUrl);
    }

    [Fact]
    public async Task AFailedDeleteOfTheOldKeys_IsLoggedOnce_TheRouteWorks_AndTheNextEditIsNotBlocked()
    {
        var id = await _store.InsertAsync(Route("High CPU", server: 7101, teams: OldTeams));
        _secrets.DeleteGate = _ => false;
        AppLogger.DrainBufferedLines();

        Assert.True(await _store.UpdateAsync((await _store.GetAsync(id))! with { TeamsUrl = NewTeams }));

        var lines = RouteLines("could not be removed");
        Assert.Single(lines);
        Assert.Contains("[WARN ]", lines[0], StringComparison.Ordinal);
        Assert.Contains("Teams", lines[0], StringComparison.Ordinal);
        Assert.DoesNotContain("secret.test", lines[0], StringComparison.Ordinal);
        Assert.Equal(NewTeams, (await _store.GetAsync(id))!.TeamsUrl);
        Assert.Equal(2, _secrets.Count);

        const string Third = "https://teams.third-secret.test/h";
        Assert.True(await _store.UpdateAsync((await _store.GetAsync(id))! with { TeamsUrl = Third }));

        Assert.Equal(Third, (await _store.GetAsync(id))!.TeamsUrl);
        _secrets.DeleteGate = _ => true;
        Assert.Equal(2, await _store.SweepOrphanedSecretsAsync());
        Assert.Single(_secrets.Keys);
        Assert.Equal(Third, (await _store.GetAsync(id))!.TeamsUrl);
    }

    [Fact]
    public async Task AFailedCleanupAfterAFailedWrite_IsLogged_LeavesTheRouteUnchanged_AndTheSweepFinishesIt()
    {
        var id = await _store.InsertAsync(Route("High CPU", server: 7101, teams: OldTeams, slack: OldSlack));
        var saved = (await _store.GetAsync(id))!;
        /* Slack is refused after Teams' new key was written, and Credential Manager will not take that key back out. */
        _secrets.WriteGate = (key, value) => !key.Contains("_Slack_", StringComparison.Ordinal) || value == OldSlack;
        _secrets.DeleteGate = _ => false;
        AppLogger.DrainBufferedLines();

        await Assert.ThrowsAsync<RouteSecretException>(() =>
            _store.UpdateAsync(saved with { ScopeServerId = 7102, TeamsUrl = NewTeams, SlackUrl = NewSlack }));

        var lines = RouteLines("could not be removed");
        Assert.Single(lines);
        Assert.Contains("Teams", lines[0], StringComparison.Ordinal);
        Assert.All(lines, l => Assert.DoesNotContain("secret.test", l, StringComparison.Ordinal));
        var unchanged = (await _store.GetAsync(id))!;
        Assert.Equal(7101, unchanged.ScopeServerId);
        Assert.Equal(OldTeams, unchanged.TeamsUrl);
        Assert.Equal(OldSlack, unchanged.SlackUrl);
        Assert.Equal(3, _secrets.Count);

        _secrets.DeleteGate = _ => true;
        Assert.Equal(1, await _store.SweepOrphanedSecretsAsync());
        Assert.Equal(2, _secrets.Count);
    }

    [Fact]
    public async Task ARowWriteThatThrowsAfterCommitting_IsNotRolledBack()
    {
        var id = await _store.InsertAsync(Route("High CPU", server: 7101, teams: OldTeams));
        var saved = (await _store.GetAsync(id))!;
        _store.AfterRowWriteForTests = () => throw new InvalidOperationException("the connection dropped after the commit");

        Assert.True(await _store.UpdateAsync(saved with { ScopeServerId = 7102, TeamsUrl = NewTeams }));

        var after = (await _store.GetAsync(id))!;
        Assert.Equal(7102, after.ScopeServerId);
        Assert.Equal(NewTeams, after.TeamsUrl);
        Assert.Single(_secrets.Keys);
    }

    [Fact]
    public async Task ARowWriteThatThrowsWithoutCommitting_RemovesTheNewKeys_AndTheOldOnesApply()
    {
        var id = await _store.InsertAsync(Route("High CPU", server: 7101, teams: OldTeams));
        var saved = (await _store.GetAsync(id))!;
        _store.BeforeRowWriteForTests = () => throw new InvalidOperationException("the write never reached the file");

        await Assert.ThrowsAsync<InvalidOperationException>(() =>
            _store.UpdateAsync(saved with { ScopeServerId = 7102, TeamsUrl = NewTeams }));

        var after = (await _store.GetAsync(id))!;
        Assert.Equal(7101, after.ScopeServerId);
        Assert.Equal(OldTeams, after.TeamsUrl);
        Assert.Single(_secrets.Keys);
    }

    [Fact]
    public async Task AnInsertWhoseRowWriteThrowsWithoutCommitting_RemovesTheNewKeys()
    {
        _store.BeforeRowWriteForTests = () => throw new InvalidOperationException("the write never reached the file");

        await Assert.ThrowsAsync<InvalidOperationException>(() => _store.InsertAsync(Route("High CPU", server: 7101, teams: OldTeams)));

        Assert.Empty(await _store.ListAsync());
        Assert.Equal(0, _secrets.Count);
    }

    [Fact]
    public async Task AnInsertWhoseRowWriteThrowsAfterCommitting_KeepsTheRouteAndItsKeys()
    {
        _store.AfterRowWriteForTests = () => throw new InvalidOperationException("the connection dropped after the commit");

        var id = await _store.InsertAsync(Route("High CPU", server: 7101, teams: OldTeams));

        Assert.Equal(OldTeams, (await _store.GetAsync(id))!.TeamsUrl);
        Assert.Single(_secrets.Keys);
    }

    // ---- reused ids, kept channels, limits ---------------------------------------------------------

    [Fact]
    public async Task AReusedRouteId_WithLeftoverKeysAtOtherGenerations_InheritsNothing_AndTheSweepRemovesThem()
    {
        _secrets.Plant("NotificationRoute_1_Slack_" + new string('a', 32), "https://hooks.slack.leftover-secret.test/h");
        _secrets.Plant("NotificationRoute_1_Teams_" + new string('b', 32), "https://teams.leftover-secret.test/h");
        _secrets.Plant("NotificationRoute_1_Teams", "https://teams.legacy-shape-secret.test/h");

        var id = await _store.InsertAsync(Route("High CPU", server: 7101, teams: NewTeams));

        Assert.Equal(1, id);
        var route = (await _store.GetAsync(id))!;
        Assert.Equal(NewTeams, route.TeamsUrl);
        Assert.Equal("", route.SlackUrl);
        Assert.Null(route.OffChannels);

        Assert.Equal(3, await _store.SweepOrphanedSecretsAsync());
        Assert.Single(_secrets.Keys);
        Assert.Equal(NewTeams, (await _store.GetAsync(id))!.TeamsUrl);
    }

    /* Moved here from NotificationRouteSecretSaveTests: both lose a saved secret and reload the route, which logs the
       "could not be read from Credential Manager" warning into the process-wide buffer. In app-alert-statics that write
       landed inside NotificationRouteTruthfulnessTests' drain window (a third line, route 1, same channel). */
    private async Task<bool> FlagAsync(int id, string column)
    {
        using var conn = new DuckDBConnection($"Data Source={_dbPath}");
        await conn.OpenAsync();
        using var cmd = conn.CreateCommand();
        cmd.CommandText = $"SELECT {column} FROM notification_routes WHERE route_id = {id}";
        return Convert.ToBoolean(await cmd.ExecuteScalarAsync());
    }

    [Fact]
    public async Task AnUnreadableDestinationLeftAlone_KeepsItsFlag_AndTheChannelStaysOff()
    {
        var id = await _store.InsertAsync(Route("High CPU", server: 7101, teams: OldTeams, slack: OldSlack));
        _secrets.Lose(id, "Teams");
        var loaded = (await _store.GetAsync(id))!;
        Assert.True(loaded.IsOffFor(NotificationRouter.TeamsChannel));

        /* What the dialog saves with the unreadable channel's mask untouched: empty value, channel still named. */
        Assert.True(await _store.UpdateAsync(loaded with { MetricMatch = "Blocking" }));

        Assert.True(await FlagAsync(id, "teams_set"));
        var after = (await _store.GetAsync(id))!;
        Assert.True(after.IsOffFor(NotificationRouter.TeamsChannel));
        Assert.Equal("Blocking", after.MetricMatch);
        Assert.Equal(OldSlack, after.SlackUrl);
    }

    [Fact]
    public async Task AnUnreadableDestinationTheUserRemoves_ClearsItsFlag()
    {
        var id = await _store.InsertAsync(Route("High CPU", server: 7101, teams: OldTeams));
        _secrets.Lose(id, "Teams");
        var loaded = (await _store.GetAsync(id))!;

        Assert.True(await _store.UpdateAsync(loaded with { OffChannels = null }));

        Assert.False(await FlagAsync(id, "teams_set"));
        Assert.Null((await _store.GetAsync(id))!.OffChannels);
    }

    [Fact]
    public async Task AKeptUnreadableChannel_KeepsItsGeneration_AndIsNeverCopied()
    {
        var id = await _store.InsertAsync(Route("High CPU", server: 7101, teams: OldTeams, slack: OldSlack));
        var teamsGen = await GenAsync(id, "teams_secret_gen");
        _secrets.Lose(id, "Teams");
        var loaded = (await _store.GetAsync(id))!;
        Assert.True(loaded.IsOffFor(NotificationRouter.TeamsChannel));
        var writes = _secrets.WriteCalls;

        Assert.True(await _store.UpdateAsync(loaded with { ScopeServerId = 7102 }));

        Assert.Equal(teamsGen, await GenAsync(id, "teams_secret_gen"));
        Assert.Equal(writes, _secrets.WriteCalls);
        Assert.False(_secrets.Has(id, "Teams"));
        Assert.True((await _store.GetAsync(id))!.IsOffFor(NotificationRouter.TeamsChannel));
        Assert.Equal(OldSlack, (await _store.GetAsync(id))!.SlackUrl);
    }

    [Fact]
    public async Task AFlagWithNoGeneration_IsUnreadable_AndTheChannelStaysOff()
    {
        var id = await _store.InsertAsync(Route("High CPU", server: 7101, teams: OldTeams));
        using (var conn = new DuckDBConnection($"Data Source={_dbPath}"))
        {
            await conn.OpenAsync();
            using var cmd = conn.CreateCommand();
            cmd.CommandText = $"UPDATE notification_routes SET teams_secret_gen = NULL WHERE route_id = {id}";
            await cmd.ExecuteNonQueryAsync();
        }

        var route = (await _store.GetAsync(id))!;

        Assert.Equal("", route.TeamsUrl);
        Assert.True(route.IsOffFor(NotificationRouter.TeamsChannel));
    }

    [Fact]
    public async Task AValueOverTheLimit_FailsLoudly_WithTheRouteAndItsKeysUnchanged()
    {
        var id = await _store.InsertAsync(Route("High CPU", server: 7101, teams: OldTeams));
        var gen = await GenAsync(id, "teams_secret_gen");
        _secrets.WriteGate = (_, value) => value.Length <= StorableLength;
        var longUrl = "https://teams.long-secret.test/" + new string('x', StorableLength);

        var ex = await Assert.ThrowsAsync<RouteSecretException>(async () =>
            await _store.UpdateAsync((await _store.GetAsync(id))! with { ScopeServerId = 7102, TeamsUrl = longUrl }));

        Assert.Contains("at most 1,280", ex.Message);
        Assert.DoesNotContain("long-secret", ex.Message, StringComparison.Ordinal);
        Assert.Equal(gen, await GenAsync(id, "teams_secret_gen"));
        Assert.Equal(new[] { DuckDbNotificationRouteStore.SecretKey(id, "Teams", gen!) }, _secrets.Keys.ToArray());
        var after = (await _store.GetAsync(id))!;
        Assert.Equal(7101, after.ScopeServerId);
        Assert.Equal(OldTeams, after.TeamsUrl);
    }

    // ---- delete and sweep --------------------------------------------------------------------------

    [Fact]
    public async Task Delete_RemovesEveryGenerationUnderTheRouteId_AndNoOtherRoutesKeys()
    {
        var keep = await _store.InsertAsync(Route("High CPU", server: 7101, teams: OldTeams));
        var doomed = await _store.InsertAsync(Route("High CPU", server: 7102, teams: OldTeams, slack: OldSlack));
        _secrets.Plant($"NotificationRoute_{doomed}_Teams_" + new string('c', 32), "https://teams.stale-secret.test/h");
        _secrets.Plant($"NotificationRoute_{doomed}0_Teams_" + new string('d', 32), "https://teams.other-secret.test/h");

        Assert.True(await _store.DeleteAsync(doomed));

        Assert.False(_secrets.Has(doomed, "Teams"));
        Assert.False(_secrets.Has(doomed, "Slack"));
        Assert.True(_secrets.Has(keep, "Teams"));
        Assert.Equal(2, _secrets.Count);
    }

    [Fact]
    public async Task TheSweep_KeepsEveryKeyARowPointsAt_AndLogsOnlyACount()
    {
        var id = await _store.InsertAsync(Route("High CPU", server: 7101, teams: OldTeams, slack: OldSlack));
        _secrets.Plant("NotificationRoute_99_Generic_" + new string('e', 32), "https://example.leftover-secret.test/h");
        AppLogger.DrainBufferedLines();

        Assert.Equal(1, await _store.SweepOrphanedSecretsAsync());

        var lines = RouteLines("leftover notification route credential");
        Assert.Single(lines);
        Assert.Contains("Removed 1 ", lines[0], StringComparison.Ordinal);
        Assert.DoesNotContain("secret.test", lines[0], StringComparison.Ordinal);
        Assert.DoesNotContain("NotificationRoute_", lines[0], StringComparison.Ordinal);
        Assert.Equal(OldTeams, (await _store.GetAsync(id))!.TeamsUrl);
        Assert.Equal(OldSlack, (await _store.GetAsync(id))!.SlackUrl);
        Assert.Equal(2, _secrets.Count);
    }

    [Fact]
    public async Task TheSweep_WithTheSecretStoreDown_DoesNothingAndDoesNotThrow()
    {
        await _store.InsertAsync(Route("High CPU", server: 7101, teams: OldTeams));
        _secrets.Dead = true;

        Assert.Equal(0, await _store.SweepOrphanedSecretsAsync());

        _secrets.Dead = false;
        Assert.Single(_secrets.Keys);
    }
}
