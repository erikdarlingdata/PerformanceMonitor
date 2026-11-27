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
using System.Threading;
using System.Threading.Tasks;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Logging;
using Npgsql;
using PerformanceMonitor.Darling.Service;
using PerformanceMonitor.Darling.Service.Hosting;
using PerformanceMonitor.Darling.Service.Mcp;
using PerformanceMonitor.Darling.Storage;
using Xunit;

namespace Darling.Tests;

/// <summary>The personal-token snapshot (#8737) with an injected loader: the gate's outcome order, the refusal while unavailable (a
/// failed refresh never keeps the last good copy), and the log and flush rate limits on a fake clock.</summary>
public sealed class DarlingPersonalTokenSnapshotTests
{
    private static readonly DateTimeOffset T0 = new(2026, 11, 16, 12, 0, 0, TimeSpan.Zero);

    private static PersonalTokenSecretRow Row(MintedToken token, DateTimeOffset expiresAt, DateTimeOffset? revokedAt = null, string owner = "owner",
        string role = "viewer") =>
        new(new PersonalTokenRow(token.TokenId, owner, "subject", "name", role, T0.AddDays(-1), expiresAt, null, revokedAt,
            revokedAt is null ? null : "admin", revokedAt is null ? null : "revoked"), token.SecretSha256);

    private static StoreOwnerSource Published()
    {
        var owner = new StoreOwnerSource();
        owner.Publish(NpgsqlDataSource.Create("Host=127.0.0.1;Port=1;Username=u;Database=d"));
        return owner;
    }

    private sealed class Rig
    {
        public Func<IReadOnlyList<PersonalTokenSecretRow>> Rows = () => Array.Empty<PersonalTokenSecretRow>();
        public Exception? Throw;
        public int Loads;
        public List<IReadOnlyDictionary<string, DateTimeOffset>> Touches = new();
        public TestClock Clock = new(T0);
        public CapturingTestLogger Log = new();
        public DarlingPersonalTokenSnapshot Snapshot;
        public StoreOwnerSource Owner = Published();

        public Rig(PersonalTokenPendingRevokes? pending = null)
        {
            Snapshot = new DarlingPersonalTokenSnapshot(
                _ =>
                {
                    Loads++;
                    if (Throw is not null)
                    {
                        throw Throw;
                    }

                    return Task.FromResult(Rows());
                },
                (batch, _) =>
                {
                    Touches.Add(new Dictionary<string, DateTimeOffset>(batch));
                    return Task.CompletedTask;
                },
                Clock)
            {
                Pending = pending,
            };
        }

        public Task Refresh() => Snapshot.RefreshAsync(Owner, Log, CancellationToken.None);
    }

    private sealed class TestClock : TimeProvider
    {
        private DateTimeOffset _now;

        public TestClock(DateTimeOffset start) => _now = start;

        public override DateTimeOffset GetUtcNow() => _now;

        public void Advance(TimeSpan by) => _now += by;
    }

    private static PostgresException MissingTable() =>
        new("relation does not exist", "ERROR", "ERROR", "42P01");

    [Fact]
    public void Check_BeforeFirstRefresh_Unavailable()
    {
        var rig = new Rig();
        var token = PersonalTokenFormat.Mint();

        Assert.Equal(PersonalTokenOutcome.Unavailable, rig.Snapshot.Check(token.Value, T0).Outcome);
    }

    [Fact]
    public async Task Check_LiveToken_Live()
    {
        var rig = new Rig();
        var token = PersonalTokenFormat.Mint();
        rig.Rows = () => new[] { Row(token, T0.AddDays(1)) };
        await rig.Refresh();

        var check = rig.Snapshot.Check(token.Value, T0);

        Assert.Equal(PersonalTokenOutcome.Live, check.Outcome);
        Assert.Equal(token.TokenId, check.Row!.TokenId);
    }

    [Fact]
    public async Task Check_WrongSecretKnownId_WrongSecret()
    {
        var rig = new Rig();
        var token = PersonalTokenFormat.Mint();
        var other = PersonalTokenFormat.Mint();
        rig.Rows = () => new[] { Row(token, T0.AddDays(1)) };
        await rig.Refresh();

        /* The known id with another token's secret. */
        var forged = PersonalTokenFormat.Prefix + token.TokenId + other.Value.Substring(PersonalTokenFormat.Prefix.Length + 16);
        var check = rig.Snapshot.Check(forged, T0);

        Assert.Equal(PersonalTokenOutcome.WrongSecret, check.Outcome);
        Assert.Null(check.Row);
    }

    [Fact]
    public async Task Check_UnknownId_UnknownId()
    {
        var rig = new Rig();
        await rig.Refresh();

        var check = rig.Snapshot.Check(PersonalTokenFormat.Mint().Value, T0);

        Assert.Equal(PersonalTokenOutcome.UnknownId, check.Outcome);
        Assert.Null(check.Row);
    }

    [Fact]
    public async Task Check_NotPersonalShape_NotPersonal()
    {
        var rig = new Rig();
        Assert.Equal(PersonalTokenOutcome.NotPersonal, rig.Snapshot.Check("a-file-token", T0).Outcome);
        Assert.Equal(PersonalTokenOutcome.NotPersonal, rig.Snapshot.Check(null, T0).Outcome);

        await rig.Refresh();
        Assert.Equal(PersonalTokenOutcome.NotPersonal, rig.Snapshot.Check("a-file-token", T0).Outcome);
    }

    [Fact]
    public async Task Check_FullPendingRevoke_RefusesEveryTokenOfThatOwnerAndNoOneElses()
    {
        var pending = new PersonalTokenPendingRevokes();
        var rig = new Rig(pending);
        var reader = PersonalTokenFormat.Mint();
        var oper = PersonalTokenFormat.Mint();
        var other = PersonalTokenFormat.Mint();
        rig.Rows = () => new[]
        {
            Row(reader, T0.AddDays(1), owner: "alice", role: "reader"),
            Row(oper, T0.AddDays(1), owner: "alice", role: "operator"),
            Row(other, T0.AddDays(1), owner: "bob", role: "operator"),
        };
        await rig.Refresh();
        Assert.Equal(PersonalTokenOutcome.Live, rig.Snapshot.Check(reader.Value, T0).Outcome);

        pending.Add("alice", operatorOnly: false, PersonalTokenStore.SignInRefusedReason);

        Assert.Equal(PersonalTokenOutcome.Dead, rig.Snapshot.Check(reader.Value, T0).Outcome);
        Assert.Equal(PersonalTokenOutcome.Dead, rig.Snapshot.Check(oper.Value, T0).Outcome);
        Assert.Equal(PersonalTokenOutcome.Live, rig.Snapshot.Check(other.Value, T0).Outcome);

        /* The drain commits and the entry leaves: the gate now follows the store rows alone. */
        pending.Complete(pending.Snapshot().Single());
        Assert.Equal(PersonalTokenOutcome.Live, rig.Snapshot.Check(reader.Value, T0).Outcome);
    }

    [Fact]
    public async Task Check_OperatorOnlyPendingRevoke_RefusesTheOwnersOperatorTokenAndPassesTheirReader()
    {
        var pending = new PersonalTokenPendingRevokes();
        var rig = new Rig(pending);
        var reader = PersonalTokenFormat.Mint();
        var oper = PersonalTokenFormat.Mint();
        rig.Rows = () => new[]
        {
            Row(reader, T0.AddDays(1), owner: "alice", role: "reader"),
            Row(oper, T0.AddDays(1), owner: "alice", role: "operator"),
        };
        await rig.Refresh();

        pending.Add("alice", operatorOnly: true, PersonalTokenStore.SeatLoweredReason);

        Assert.Equal(PersonalTokenOutcome.Dead, rig.Snapshot.Check(oper.Value, T0).Outcome);
        Assert.Equal(PersonalTokenOutcome.Live, rig.Snapshot.Check(reader.Value, T0).Outcome);
    }

    [Fact]
    public async Task ProcessContainer_GivesTheSnapshotTheQueueTheHostsShare()
    {
        var services = new ServiceCollection();
        DarlingProcessServices.Add(services, stoppedMarkerPath: null);
        /* An in-memory queue in place of the one that saves beside the service logs, so this test writes no file there. */
        var queue = new PersonalTokenPendingRevokes();
        services.AddSingleton(queue);
        await using var provider = services.BuildServiceProvider();

        var snapshot = provider.GetRequiredService<DarlingPersonalTokenSnapshot>();

        Assert.Same(queue, snapshot.Pending);
        var host = new DarlingWebHostService(Microsoft.Extensions.Logging.Abstractions.NullLogger<DarlingWebHostService>.Instance, new WebRuntimeState(),
            new CollectorRuntimeState(), new WebTlsCertificateState(), services: provider);
        Assert.Same(queue, host.PendingPersonalRevokes);
    }

    private static string TempFolder()
    {
        var folder = Path.Combine(Path.GetTempPath(), "pending-revokes-" + Guid.NewGuid().ToString("N"));
        Directory.CreateDirectory(folder);
        return folder;
    }

    [Fact]
    public void PendingRevokes_ANewInstanceOnTheSameFile_HoldsTheQueueTheOldOneSaved()
    {
        var folder = TempFolder();
        try
        {
            var file = Path.Combine(folder, "queue.json");
            var first = new PersonalTokenPendingRevokes(stateFile: file);
            first.Add("alice", operatorOnly: false, PersonalTokenStore.SignInRefusedReason);
            first.Add("bob", operatorOnly: true, PersonalTokenStore.SeatLoweredReason);
            first.Add("carol", operatorOnly: true, PersonalTokenStore.SeatLoweredReason);
            first.Remove("carol");
            first.Complete(first.Snapshot().Single(e => e.OwnerKey == "bob"));
            first.Add("dave", operatorOnly: true, PersonalTokenStore.OperatorsDisabledReason);

            var log = new CapturingTestLogger();
            var second = new PersonalTokenPendingRevokes(stateFile: file, logger: log);

            var entries = second.Snapshot().OrderBy(e => e.OwnerKey, StringComparer.Ordinal).ToList();
            Assert.Equal(new[] { "alice", "dave" }, entries.Select(e => e.OwnerKey));
            Assert.False(entries[0].OperatorOnly);
            Assert.Equal(PersonalTokenStore.SignInRefusedReason, entries[0].Reason);
            Assert.True(entries[1].OperatorOnly);
            Assert.Equal(PersonalTokenStore.OperatorsDisabledReason, entries[1].Reason);
            Assert.True(second.Refuses("alice", "reader"));
            Assert.True(second.Refuses("dave", "operator"));
            Assert.False(second.Refuses("dave", "reader"));
            Assert.False(second.Refuses("bob", "operator"));
            Assert.Equal(0, log.CountAtLevel(LogLevel.Warning));
            Assert.Empty(Directory.GetFiles(folder, "*.tmp"));
        }
        finally
        {
            Directory.Delete(folder, recursive: true);
        }
    }

    [Fact]
    public void PendingRevokes_AnUnreadableFile_StartsEmptyWithOneWarning_AndTheNextChangeSavesAgain()
    {
        var folder = TempFolder();
        try
        {
            var file = Path.Combine(folder, "queue.json");
            File.WriteAllText(file, "{ this is not json");
            var log = new CapturingTestLogger();

            var queue = new PersonalTokenPendingRevokes(stateFile: file, logger: log);

            Assert.Equal(0, queue.Count);
            Assert.Equal(1, log.CountAtLevel(LogLevel.Warning));

            queue.Add("alice", operatorOnly: false, PersonalTokenStore.SignInRefusedReason);
            var reloaded = new PersonalTokenPendingRevokes(stateFile: file);
            Assert.Equal("alice", Assert.Single(reloaded.Snapshot()).OwnerKey);
            Assert.Equal(1, log.CountAtLevel(LogLevel.Warning));
        }
        finally
        {
            Directory.Delete(folder, recursive: true);
        }
    }

    [Fact]
    public void PendingRevokes_NoFileYet_StartsEmptyWithoutAWarning()
    {
        var folder = TempFolder();
        try
        {
            var log = new CapturingTestLogger();

            var queue = new PersonalTokenPendingRevokes(stateFile: Path.Combine(folder, "missing.json"), logger: log);

            Assert.Equal(0, queue.Count);
            Assert.Equal(0, log.CountAtLevel(LogLevel.Warning));
        }
        finally
        {
            Directory.Delete(folder, recursive: true);
        }
    }

    [Fact]
    public async Task Check_Revoked_Dead()
    {
        var rig = new Rig();
        var token = PersonalTokenFormat.Mint();
        rig.Rows = () => new[] { Row(token, T0.AddDays(1), revokedAt: T0.AddHours(-1)) };
        await rig.Refresh();

        var check = rig.Snapshot.Check(token.Value, T0);

        Assert.Equal(PersonalTokenOutcome.Dead, check.Outcome);
        Assert.NotNull(check.Row);
    }

    [Fact]
    public async Task Check_ExpiredAtMatchTime_DeadBeforeNextRefresh()
    {
        var rig = new Rig();
        var token = PersonalTokenFormat.Mint();
        var expires = T0.AddMinutes(10);
        rig.Rows = () => new[] { Row(token, expires) };
        await rig.Refresh();

        Assert.Equal(PersonalTokenOutcome.Live, rig.Snapshot.Check(token.Value, expires.AddTicks(-1)).Outcome);
        Assert.Equal(PersonalTokenOutcome.Dead, rig.Snapshot.Check(token.Value, expires).Outcome);
        Assert.Equal(1, rig.Loads);
    }

    [Fact]
    public async Task Refresh_LoaderThrows_BecomesUnavailable_ThenRecovers()
    {
        var rig = new Rig();
        var token = PersonalTokenFormat.Mint();
        rig.Rows = () => new[] { Row(token, T0.AddDays(1)) };
        await rig.Refresh();
        Assert.Equal(PersonalTokenOutcome.Live, rig.Snapshot.Check(token.Value, T0).Outcome);

        /* The token is revoked in the store, and the next read fails: the last good copy must not be served. */
        rig.Rows = () => new[] { Row(token, T0.AddDays(1), revokedAt: T0) };
        rig.Throw = new InvalidOperationException("store down");
        await rig.Refresh();
        Assert.Equal(PersonalTokenOutcome.Unavailable, rig.Snapshot.Check(token.Value, T0).Outcome);
        Assert.Equal(1, rig.Log.CountAtLevel(LogLevel.Warning));

        rig.Throw = null;
        await rig.Refresh();
        Assert.Equal(PersonalTokenOutcome.Dead, rig.Snapshot.Check(token.Value, T0).Outcome);
    }

    [Fact]
    public async Task Refresh_OtherFailure_WarnsOncePer5Minutes()
    {
        var rig = new Rig();
        rig.Throw = new InvalidOperationException("store down");

        for (var i = 0; i < 5; i++)
        {
            await rig.Refresh();
            rig.Clock.Advance(TimeSpan.FromSeconds(5));
        }

        Assert.Equal(1, rig.Log.CountAtLevel(LogLevel.Warning));

        rig.Clock.Advance(TimeSpan.FromMinutes(5));
        await rig.Refresh();
        Assert.Equal(2, rig.Log.CountAtLevel(LogLevel.Warning));
    }

    [Fact]
    public async Task Refresh_MissingTable_Unavailable_LogsOnce()
    {
        var rig = new Rig();
        var token = PersonalTokenFormat.Mint();
        rig.Throw = MissingTable();

        for (var i = 0; i < 5; i++)
        {
            await rig.Refresh();
            rig.Clock.Advance(TimeSpan.FromSeconds(5));
        }

        Assert.Equal(PersonalTokenOutcome.Unavailable, rig.Snapshot.Check(token.Value, T0).Outcome);
        Assert.Equal(5, rig.Loads);
        Assert.Single(rig.Log.Lines);
        Assert.Equal(1, rig.Log.CountAtLevel(LogLevel.Information));
        Assert.Contains($"has not reached V{PersonalTokenSchema.Version}", rig.Log.Joined);
    }

    [Fact]
    public async Task Refresh_OwnerNotPublished_Unavailable_NoLoad()
    {
        var rig = new Rig();
        var token = PersonalTokenFormat.Mint();
        rig.Rows = () => new[] { Row(token, T0.AddDays(1)) };
        await rig.Refresh();
        Assert.Equal(PersonalTokenOutcome.Live, rig.Snapshot.Check(token.Value, T0).Outcome);
        var loadsBefore = rig.Loads;

        await rig.Snapshot.RefreshAsync(new StoreOwnerSource(), rig.Log, CancellationToken.None);

        Assert.Equal(loadsBefore, rig.Loads);
        Assert.Equal(PersonalTokenOutcome.Unavailable, rig.Snapshot.Check(token.Value, T0).Outcome);
        Assert.Empty(rig.Log.Lines);
    }

    [Fact]
    public async Task Flush_WritesOncePer5MinutesPerToken()
    {
        var rig = new Rig();

        rig.Snapshot.MarkUsed("aaaa", T0);
        rig.Snapshot.MarkUsed("bbbb", T0);
        await rig.Snapshot.FlushLastUsedAsync(rig.Owner, T0, CancellationToken.None);
        Assert.Single(rig.Touches);
        Assert.Equal(2, rig.Touches[0].Count);

        /* Used again inside the window: held back. Nothing pending: no write. */
        rig.Snapshot.MarkUsed("aaaa", T0.AddMinutes(1));
        await rig.Snapshot.FlushLastUsedAsync(rig.Owner, T0.AddMinutes(1), CancellationToken.None);
        await rig.Snapshot.FlushLastUsedAsync(rig.Owner, T0.AddMinutes(4), CancellationToken.None);
        Assert.Single(rig.Touches);

        /* Past 5 minutes the pending use goes out, with its own time, and only that id. */
        await rig.Snapshot.FlushLastUsedAsync(rig.Owner, T0.AddMinutes(5), CancellationToken.None);
        Assert.Equal(2, rig.Touches.Count);
        Assert.Equal(T0.AddMinutes(1), Assert.Single(rig.Touches[1]).Value);
        Assert.True(rig.Touches[1].ContainsKey("aaaa"));

        await rig.Snapshot.FlushLastUsedAsync(rig.Owner, T0.AddMinutes(11), CancellationToken.None);
        Assert.Equal(2, rig.Touches.Count);
    }

    /// <summary>I2: the flush memory is kept only for ids the last load returned, so tokens that are gone do not pile up in it.</summary>
    [Fact]
    public async Task Refresh_ForgetsTheFlushTimeOfATokenTheLoadNoLongerReturns()
    {
        var rig = new Rig();
        var token = PersonalTokenFormat.Mint();
        rig.Rows = () => new[] { Row(token, T0.AddDays(1)) };
        await rig.Refresh();
        rig.Snapshot.MarkUsed(token.TokenId, T0);
        await rig.Snapshot.FlushLastUsedAsync(rig.Owner, T0, CancellationToken.None);
        Assert.Single(rig.Touches);

        /* Still loaded: a use inside five minutes waits. */
        await rig.Refresh();
        rig.Snapshot.MarkUsed(token.TokenId, T0.AddMinutes(1));
        await rig.Snapshot.FlushLastUsedAsync(rig.Owner, T0.AddMinutes(1), CancellationToken.None);
        Assert.Single(rig.Touches);

        /* Gone from the store (purged): its flush time is dropped, so the pending use goes out at once. */
        rig.Rows = () => Array.Empty<PersonalTokenSecretRow>();
        await rig.Refresh();
        await rig.Snapshot.FlushLastUsedAsync(rig.Owner, T0.AddMinutes(2), CancellationToken.None);
        Assert.Equal(2, rig.Touches.Count);
    }

    [Fact]
    public async Task Refresh_ForgetsTheDeadLogTimeOfATokenTheLoadNoLongerReturns()
    {
        var rig = new Rig();
        var kept = PersonalTokenFormat.Mint();
        var gone = PersonalTokenFormat.Mint();
        rig.Rows = () => new[] { Row(kept, T0, revokedAt: T0), Row(gone, T0, revokedAt: T0) };
        await rig.Refresh();
        Assert.True(rig.Snapshot.ShouldLogDead(kept.TokenId, T0));
        Assert.True(rig.Snapshot.ShouldLogDead(gone.TokenId, T0));

        rig.Rows = () => new[] { Row(kept, T0, revokedAt: T0) };
        await rig.Refresh();

        Assert.False(rig.Snapshot.ShouldLogDead(kept.TokenId, T0.AddMinutes(1)));
        Assert.True(rig.Snapshot.ShouldLogDead(gone.TokenId, T0.AddMinutes(1)));
    }

    /// <summary>M8: a lowered lifetime binds at the gate at once, without waiting for the start sweep to cut the stored expiry.</summary>
    [Fact]
    public async Task Check_OlderThanTheLifetimeNowAllowed_Dead()
    {
        var rig = new Rig();
        var token = PersonalTokenFormat.Mint();
        /* Created a day before T0, stored to expire in 29 more days. */
        rig.Rows = () => new[] { Row(token, T0.AddDays(29)) };
        await rig.Refresh();

        Assert.Equal(PersonalTokenOutcome.Live, rig.Snapshot.Check(token.Value, T0, maxLifetimeDays: 2).Outcome);
        Assert.Equal(PersonalTokenOutcome.Dead, rig.Snapshot.Check(token.Value, T0, maxLifetimeDays: 1).Outcome);
        Assert.Equal(PersonalTokenOutcome.Live, rig.Snapshot.Check(token.Value, T0).Outcome);
    }

    [Fact]
    public void ShouldLogDead_OncePerHour()
    {
        var snapshot = new DarlingPersonalTokenSnapshot();

        Assert.True(snapshot.ShouldLogDead("aaaa", T0));
        Assert.False(snapshot.ShouldLogDead("aaaa", T0.AddMinutes(59)));
        Assert.True(snapshot.ShouldLogDead("bbbb", T0.AddMinutes(1)));
        Assert.True(snapshot.ShouldLogDead("aaaa", T0.AddHours(1)));
        Assert.False(snapshot.ShouldLogDead("aaaa", T0.AddHours(1).AddMinutes(1)));
    }

    [Fact]
    public async Task Refresh_Overlapping_TheLaterStartApplyLast()
    {
        var token = PersonalTokenFormat.Mint();
        var live = Row(token, T0.AddDays(1));
        var revoked = Row(token, T0.AddDays(1), revokedAt: T0);
        var gate = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        var started = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        var calls = 0;
        var snapshot = new DarlingPersonalTokenSnapshot(
            async _ =>
            {
                if (Interlocked.Increment(ref calls) == 1)
                {
                    started.SetResult();
                    await gate.Task;
                    return new[] { live };
                }

                return new[] { revoked };
            },
            (_, _) => Task.CompletedTask);
        var owner = Published();

        var slow = snapshot.RefreshAsync(owner, new CapturingTestLogger(), CancellationToken.None);
        await started.Task;
        var later = snapshot.RefreshAsync(owner, new CapturingTestLogger(), CancellationToken.None);
        await Task.Delay(50);
        Assert.False(later.IsCompleted);

        gate.SetResult();
        await Task.WhenAll(slow, later).WaitAsync(TimeSpan.FromSeconds(10));

        Assert.Equal(PersonalTokenOutcome.Dead, snapshot.Check(token.Value, T0).Outcome);
    }

    [Fact]
    public async Task Refresh_Success_ResetsTheLogLimits_SoALaterOutageLogsAgain()
    {
        var rig = new Rig();
        rig.Throw = MissingTable();
        await rig.Refresh();
        rig.Throw = new InvalidOperationException("down");
        await rig.Refresh();
        Assert.Equal(1, rig.Log.CountAtLevel(LogLevel.Information));
        Assert.Equal(1, rig.Log.CountAtLevel(LogLevel.Warning));

        rig.Throw = null;
        await rig.Refresh();
        rig.Throw = MissingTable();
        await rig.Refresh();
        rig.Throw = new InvalidOperationException("down again");
        await rig.Refresh();

        Assert.Equal(2, rig.Log.CountAtLevel(LogLevel.Information));
        Assert.Equal(2, rig.Log.CountAtLevel(LogLevel.Warning));
    }

    [Fact]
    public void PendingRevokes_ForConfig_SavesInTheKeyFolder_AndANewQueueFromTheSameConfigLoadsIt()
    {
        var folder = TempFolder();
        try
        {
            var configPath = Path.Combine(folder, "darling.json");
            var config = DarlingConfig.Parse("{\"postgres\":{\"connectionString\":\"Host=h\"}}");
            var keys = DarlingLogHashKeyFile.DirectoryFor(config, configPath);

            var first = PersonalTokenPendingRevokes.ForConfig(config, configPath, new CapturingTestLogger());
            first.Add("alice", operatorOnly: false, PersonalTokenStore.SignInRefusedReason);

            Assert.True(File.Exists(Path.Combine(keys, PersonalTokenPendingRevokes.StateFileName)));
            var second = PersonalTokenPendingRevokes.ForConfig(config, configPath, new CapturingTestLogger());
            Assert.Equal("alice", Assert.Single(second.Snapshot()).OwnerKey);
        }
        finally
        {
            Directory.Delete(folder, recursive: true);
        }
    }

    [Fact]
    public void PendingRevokes_AFailedSave_IsWrittenByTheNextSaveIfDirty_OnceTheFileCanBeWritten()
    {
        var folder = TempFolder();
        try
        {
            /* The folder the file belongs in is a plain file, so the first save cannot create it. */
            var blocker = Path.Combine(folder, "state");
            File.WriteAllText(blocker, "in the way");
            var file = Path.Combine(blocker, "queue.json");
            var log = new CapturingTestLogger();
            var queue = new PersonalTokenPendingRevokes(stateFile: file, logger: log);

            queue.Add("alice", operatorOnly: false, PersonalTokenStore.SignInRefusedReason);
            queue.SaveIfDirty();
            Assert.False(File.Exists(file));
            Assert.Equal(1, log.CountAtLevel(LogLevel.Warning));

            File.Delete(blocker);
            queue.SaveIfDirty();

            Assert.Equal("alice", Assert.Single(new PersonalTokenPendingRevokes(stateFile: file).Snapshot()).OwnerKey);
            Assert.True(queue.Refuses("alice", "reader"));
        }
        finally
        {
            Directory.Delete(folder, recursive: true);
        }
    }

    [Fact]
    public void PendingRevokes_AFileWithARepeatedKey_StartsEmptyWithOneWarning_AndDoesNotThrow()
    {
        var folder = TempFolder();
        try
        {
            var file = Path.Combine(folder, "queue.json");
            File.WriteAllText(file, "{\"entries\":[],\"entries\":[{\"owner_key\":\"alice\",\"operator_only\":false,\"reason\":\"x\"}]}");
            var log = new CapturingTestLogger();

            var queue = new PersonalTokenPendingRevokes(stateFile: file, logger: log);

            Assert.Equal(0, queue.Count);
            Assert.Equal(1, log.CountAtLevel(LogLevel.Warning));
        }
        finally
        {
            Directory.Delete(folder, recursive: true);
        }
    }

    [Fact]
    public void PendingRevokes_AFileOverTheSizeLimit_StartsEmptyWithOneWarning_WithoutBeingParsed()
    {
        var folder = TempFolder();
        try
        {
            /* Valid JSON with one entry: parsed, it would load that entry. The padding takes it past the limit. */
            var file = Path.Combine(folder, "queue.json");
            File.WriteAllText(file, "{\"entries\":[{\"owner_key\":\"alice\",\"operator_only\":false,\"reason\":\"x\"}]}"
                + new string(' ', (int)PersonalTokenPendingRevokes.MaxStateFileBytes));
            var log = new CapturingTestLogger();

            var queue = new PersonalTokenPendingRevokes(stateFile: file, logger: log);

            Assert.Equal(0, queue.Count);
            Assert.Equal(1, log.CountAtLevel(LogLevel.Warning));
        }
        finally
        {
            Directory.Delete(folder, recursive: true);
        }
    }
}
