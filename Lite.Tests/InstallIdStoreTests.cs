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
using System.Runtime.CompilerServices;
using System.Text.Json;
using System.Threading;
using System.Threading.Tasks;
using PerformanceMonitor.Collectors;
using PerformanceMonitorLite.Database;
using PerformanceMonitorLite.Services;
using Xunit;

namespace Lite.Tests;

/// <summary>
/// #4961: Lite's install id lives in its own file at the data root, <c>install-id.json</c>, beside the machine
/// name and Windows user it was made for. These pin the contract the session names will be built on: the id is
/// made once however many first sweeps ask at once, a bad id or a changed machine or user makes a new one with
/// exactly one Warning that names both, and the rewrite never leaves a second file behind.
///
/// <para>Collection <c>app-logger-statics</c>: the Warning assertions drain the process-wide log buffer, which
/// the other tests that do the same share.</para>
/// </summary>
[Collection("app-logger-statics")]
public sealed class InstallIdStoreTests : IDisposable
{
    private const string MachineA = "HOST-A";
    private const string MachineB = "HOST-B";
    private const string SidA = "S-1-5-21-1000-2000-3000-1001";
    private const string SidB = "S-1-5-21-1000-2000-3000-1002";

    private readonly string _root =
        Path.Combine(Path.GetTempPath(), "lite-installid-tests", Guid.NewGuid().ToString("N"));

    public void Dispose()
    {
        try
        {
            if (Directory.Exists(_root))
            {
                Directory.Delete(_root, recursive: true);
            }
        }
        catch
        {
            /* Best-effort test cleanup. */
        }
    }

    private string NewDir([CallerMemberName] string name = "")
    {
        var dir = Path.Combine(_root, name + "-" + Guid.NewGuid().ToString("N")[..6]);
        Directory.CreateDirectory(dir);
        return dir;
    }

    private static InstallIdStore StoreFor(string dir, string machine = MachineA, string? sid = SidA) =>
        new(dir, machine, sid);

    private static string IdFile(string dir) => Path.Combine(dir, InstallIdStore.FileName);

    private static void WriteFile(string dir, string? id, string? machine, string? sid) =>
        File.WriteAllText(IdFile(dir), JsonSerializer.Serialize(
            new Dictionary<string, string?> { ["id"] = id, ["machine"] = machine, ["userSid"] = sid }));

    private static (string? Id, string? Machine, string? Sid) ReadFile(string dir)
    {
        using var doc = JsonDocument.Parse(File.ReadAllText(IdFile(dir)));
        string? Get(string name) => doc.RootElement.TryGetProperty(name, out var p) ? p.GetString() : null;
        return (Get("id"), Get("machine"), Get("userSid"));
    }

    private static void AssertFile(string dir, string expectedId, string expectedMachine, string expectedSid)
    {
        var (id, machine, sid) = ReadFile(dir);
        Assert.Equal(expectedId, id);
        Assert.Equal(expectedMachine, machine);
        Assert.Equal(expectedSid, sid);
    }

    private static List<string> Warnings(IEnumerable<string> log) =>
        log.Where(l => l.Contains("[InstallId]", StringComparison.Ordinal)
                       && l.Contains("[WARN ]", StringComparison.Ordinal)).ToList();

    private static string[] FilesIn(string dir) =>
        Directory.GetFiles(dir).Select(f => Path.GetFileName(f)!).OrderBy(f => f, StringComparer.Ordinal).ToArray();

    /// <summary>Runs one <c>GetId</c> per caller on its own thread, released together so they genuinely overlap.</summary>
    private static string?[] ResolveTogether(IEnumerable<InstallIdStore> callers)
    {
        var all = callers.ToArray();
        using var gate = new Barrier(all.Length);
        var tasks = all
            .Select(s => Task.Factory.StartNew(
                () =>
                {
                    gate.SignalAndWait();
                    return s.GetId();
                },
                TaskCreationOptions.LongRunning))
            .ToArray();
        Task.WaitAll(tasks);
        return tasks.Select(t => t.Result).ToArray();
    }

    /// <summary>
    /// Test 1, one store: every collector's first sweep asks for the id at the same moment, and they must all get
    /// the one id, from one resolve, with one file left behind.
    /// </summary>
    [Fact]
    public void ParallelFirstResolves_OnOneStore_MakeOneId()
    {
        var dir = NewDir();
        var store = StoreFor(dir);

        var ids = ResolveTogether(Enumerable.Repeat(store, 32));

        Assert.Single(ids.Distinct());
        Assert.True(InstallId.IsValid(ids[0]), $"made '{ids[0]}'");
        Assert.Equal(1, store.ResolveCount);
        Assert.Equal(new[] { InstallIdStore.FileName }, FilesIn(dir));
        Assert.Equal(ids[0], ReadFile(dir).Id);
    }

    /// <summary>
    /// Test 1, two stores: two stores on one empty directory (a second object built for the same data root) race
    /// to make the file. Both must come away with the same id and the directory holds one file.
    /// </summary>
    [Fact]
    public void TwoStoresRacingOnOneEmptyDirectory_AgreeOnOneIdAndLeaveOneFile()
    {
        for (var round = 0; round < 5; round++)
        {
            var dir = NewDir();
            var first = StoreFor(dir);
            var second = StoreFor(dir);
            AppLogger.DrainBufferedLines();

            var ids = ResolveTogether(Enumerable.Repeat(first, 12).Concat(Enumerable.Repeat(second, 12)));

            Assert.Single(ids.Distinct());
            Assert.True(InstallId.IsValid(ids[0]), $"made '{ids[0]}'");
            Assert.Equal(new[] { InstallIdStore.FileName }, FilesIn(dir));
            Assert.Equal(ids[0], ReadFile(dir).Id);
            Assert.Empty(Warnings(AppLogger.DrainBufferedLines()));
        }
    }

    /// <summary>
    /// Another process wins the create in the moment between this store seeing no file and making one. The store
    /// must take the winner's id and leave its file alone, rather than overwrite it with a second id.
    /// </summary>
    [Fact]
    public void ALostCreateRace_AdoptsTheWinnersId_AndLeavesItsFileAlone()
    {
        var dir = NewDir();
        var store = StoreFor(dir);
        store.BeforeCreate = () => WriteFile(dir, "feedface", MachineA, SidA);
        AppLogger.DrainBufferedLines();

        var id = store.GetId();

        Assert.Equal("feedface", id);
        Assert.Equal("feedface", ReadFile(dir).Id);
        Assert.Equal(new[] { InstallIdStore.FileName }, FilesIn(dir));
        Assert.Empty(Warnings(AppLogger.DrainBufferedLines()));
    }

    /// <summary>
    /// The winner has made the file but is still writing it, so it is held exclusively. The loser's re-read must
    /// wait that out instead of reading the lock as "unreadable" and making a second id, which is the
    /// intermittent split two first starts at the same moment would otherwise produce.
    /// </summary>
    [Fact]
    public void ALoserThatFindsTheFileStillBeingWritten_WaitsForTheWinner()
    {
        var dir = NewDir();
        var store = StoreFor(dir);
        store.BeforeCreate = () =>
        {
            var held = new FileStream(IdFile(dir), FileMode.CreateNew, FileAccess.Write, FileShare.None);
            var finish = new Thread(() =>
            {
                Thread.Sleep(75);
                held.Write(JsonSerializer.SerializeToUtf8Bytes(
                    new Dictionary<string, string?> { ["id"] = "feedface", ["machine"] = MachineA, ["userSid"] = SidA }));
                held.Dispose();
            })
            { IsBackground = true };
            finish.Start();
        };
        AppLogger.DrainBufferedLines();

        var id = store.GetId();

        Assert.Equal("feedface", id);
        Assert.Equal("feedface", ReadFile(dir).Id);
        Assert.Empty(Warnings(AppLogger.DrainBufferedLines()));
    }

    /// <summary>
    /// Two processes replace the same bad file at the same moment and must end on one id. The two stores here share the
    /// process-wide resolve lock, so they take turns; the second finds the first's file and keeps its id. One Warning in
    /// total, and one file left behind.
    /// </summary>
    [Fact]
    public void TwoReplacersOfOneBadFile_EndWithTheSameId_AndOneWarning()
    {
        var dir = NewDir();
        WriteFile(dir, "NOT-AN-ID", MachineA, SidA);
        AppLogger.DrainBufferedLines();

        var ids = ResolveTogether(new[] { StoreFor(dir), StoreFor(dir) });
        var warnings = Warnings(AppLogger.DrainBufferedLines());

        var id = Assert.Single(ids.Distinct());
        Assert.True(InstallId.IsValid(id), $"made '{id}'");
        AssertFile(dir, id!, MachineA, SidA);
        Assert.Equal(new[] { InstallIdStore.FileName }, FilesIn(dir));
        Assert.Single(warnings);
    }

    /// <summary>
    /// A replacer that judged the file bad is slow, and another process replaces it first. The slow one must end on that
    /// process's id and leave its file alone: the file at the path is no longer the one it judged, so an id of its own
    /// put over it would leave the two processes with different ids.
    /// </summary>
    [Fact]
    public void ASlowReplacer_AdoptsTheIdAnotherReplacerAlreadyMade_AndLeavesItsFileAlone()
    {
        var dir = NewDir();
        WriteFile(dir, "NOT-AN-ID", MachineA, SidA);
        string? first = null;
        var slow = StoreFor(dir);
        slow.BeforeReplace = () => first = StoreFor(dir).GetId();
        AppLogger.DrainBufferedLines();

        var id = slow.GetId();
        var warnings = Warnings(AppLogger.DrainBufferedLines());

        Assert.True(InstallId.IsValid(first), $"the other replacer made '{first}'");
        Assert.Equal(first, id);
        AssertFile(dir, first!, MachineA, SidA);
        Assert.Equal(new[] { InstallIdStore.FileName }, FilesIn(dir));
        var line = Assert.Single(warnings);
        Assert.Contains(first!, line, StringComparison.Ordinal);
    }

    /// <summary>
    /// Another replacer has already moved the bad file out of the way and has not yet made the new one. This replacer
    /// finds no file at the path, makes the file itself, and the other replacer's own create then finds it and adopts
    /// its id.
    /// </summary>
    [Fact]
    public void AReplacerThatFindsTheBadFileAlreadyMovedAside_MakesTheFile_AndALaterReadFindsOneId()
    {
        var dir = NewDir();
        WriteFile(dir, "NOT-AN-ID", MachineA, SidA);
        var store = StoreFor(dir);
        store.BeforeReplace = () => File.Move(IdFile(dir), IdFile(dir) + ".other-replacer");
        AppLogger.DrainBufferedLines();

        var id = store.GetId();
        var warnings = Warnings(AppLogger.DrainBufferedLines());

        Assert.True(InstallId.IsValid(id), $"made '{id}'");
        AssertFile(dir, id!, MachineA, SidA);
        Assert.Equal(id, StoreFor(dir).GetId());
        Assert.Single(warnings);
    }

    /// <summary>
    /// Test 3, Lite half, bad id: an id that is not eight lowercase hex digits (wrong case, wrong length, not hex,
    /// missing, or not a string) makes a new id and exactly one Warning. The old value is not an id, so the
    /// Warning names it as "invalid" and does not repeat what the file held.
    /// </summary>
    [Theory]
    [InlineData("DEADBEEF")]
    [InlineData("deadbee")]
    [InlineData("deadbeef0")]
    [InlineData("deadbeeg")]
    [InlineData("")]
    [InlineData(null)]
    public void ABadId_MakesANewId_AndOneWarningNamingBoth(string? badId)
    {
        var dir = NewDir();
        WriteFile(dir, badId, MachineA, SidA);
        AppLogger.DrainBufferedLines();

        var id = StoreFor(dir).GetId()!;
        var warnings = Warnings(AppLogger.DrainBufferedLines());

        Assert.True(InstallId.IsValid(id), $"made '{id}'");
        Assert.NotEqual(badId, id);
        var line = Assert.Single(warnings);
        Assert.Contains("invalid", line, StringComparison.Ordinal);
        Assert.Contains(id, line, StringComparison.Ordinal);
        AssertFile(dir, id, MachineA, SidA);
        Assert.Equal(new[] { InstallIdStore.FileName }, FilesIn(dir));
    }

    /// <summary>Test 3, Lite half, machine: the same data folder opened on another machine is another install.</summary>
    [Fact]
    public void AMachineMismatch_MakesANewId_AndOneWarningNamingBoth()
    {
        var dir = NewDir();
        WriteFile(dir, "0123abcd", MachineA, SidA);
        AppLogger.DrainBufferedLines();

        var id = StoreFor(dir, MachineB, SidA).GetId()!;
        var warnings = Warnings(AppLogger.DrainBufferedLines());

        Assert.True(InstallId.IsValid(id), $"made '{id}'");
        Assert.NotEqual("0123abcd", id);
        var line = Assert.Single(warnings);
        Assert.Contains("0123abcd", line, StringComparison.Ordinal);
        Assert.Contains(id, line, StringComparison.Ordinal);
        AssertFile(dir, id, MachineB, SidA);
        Assert.Equal(new[] { InstallIdStore.FileName }, FilesIn(dir));
    }

    /// <summary>Test 3, Lite half, user: the same data folder under another Windows user is another install.</summary>
    [Fact]
    public void AUserSidMismatch_MakesANewId_AndOneWarningNamingBoth()
    {
        var dir = NewDir();
        WriteFile(dir, "0123abcd", MachineA, SidA);
        AppLogger.DrainBufferedLines();

        var id = StoreFor(dir, MachineA, SidB).GetId()!;
        var warnings = Warnings(AppLogger.DrainBufferedLines());

        Assert.True(InstallId.IsValid(id), $"made '{id}'");
        Assert.NotEqual("0123abcd", id);
        var line = Assert.Single(warnings);
        Assert.Contains("0123abcd", line, StringComparison.Ordinal);
        Assert.Contains(id, line, StringComparison.Ordinal);
        AssertFile(dir, id, MachineA, SidB);
        Assert.Equal(new[] { InstallIdStore.FileName }, FilesIn(dir));
    }

    /// <summary>A file that records no machine or user cannot be shown to match this one.</summary>
    [Fact]
    public void ABindingThatIsMissing_IsAMismatch()
    {
        var dir = NewDir();
        WriteFile(dir, "0123abcd", machine: null, sid: null);
        AppLogger.DrainBufferedLines();

        var id = StoreFor(dir).GetId();

        Assert.NotEqual("0123abcd", id);
        Assert.Single(Warnings(AppLogger.DrainBufferedLines()));
    }

    /// <summary>
    /// A file that cannot be read as the id record (damaged, empty, or the wrong shape) is treated like a bad id:
    /// a new id and exactly one Warning.
    /// </summary>
    [Theory]
    [InlineData("this is not json")]
    [InlineData("")]
    [InlineData("[]")]
    [InlineData("null")]
    [InlineData("{\"id\": 12345678}")]
    public void AnUnreadableFile_MakesANewId_AndOneWarning(string content)
    {
        var dir = NewDir();
        File.WriteAllText(IdFile(dir), content);
        AppLogger.DrainBufferedLines();

        var id = StoreFor(dir).GetId()!;
        var warnings = Warnings(AppLogger.DrainBufferedLines());

        Assert.True(InstallId.IsValid(id), $"made '{id}'");
        var line = Assert.Single(warnings);
        Assert.Contains("invalid", line, StringComparison.Ordinal);
        Assert.Contains(id, line, StringComparison.Ordinal);
        AssertFile(dir, id, MachineA, SidA);
        Assert.Equal(new[] { InstallIdStore.FileName }, FilesIn(dir));
    }

    /// <summary>
    /// The ordinary case every start after the first: a good id bound to this machine and user is kept, quietly,
    /// across store objects (a restart), and a change of letter case in the machine name is not a new machine.
    /// </summary>
    [Fact]
    public void AMatchingFile_KeepsTheId_WithoutAWarning()
    {
        var dir = NewDir();
        var made = StoreFor(dir).GetId();
        AppLogger.DrainBufferedLines();

        var again = StoreFor(dir).GetId();
        var recased = StoreFor(dir, MachineA.ToLowerInvariant(), SidA.ToLowerInvariant()).GetId();

        Assert.Equal(made, again);
        Assert.Equal(made, recased);
        Assert.Empty(Warnings(AppLogger.DrainBufferedLines()));
        Assert.Equal(new[] { InstallIdStore.FileName }, FilesIn(dir));
    }

    /// <summary>
    /// "Cached after the first resolve": once a store has answered, it does not look at the disk again, so a sweep
    /// in the middle of a run cannot see the id change under it.
    /// </summary>
    [Fact]
    public void TheIdIsCached_AfterTheFirstResolve()
    {
        var dir = NewDir();
        var store = StoreFor(dir);
        var first = store.GetId();
        File.Delete(IdFile(dir));

        var second = store.GetId();

        Assert.Equal(first, second);
        Assert.Equal(1, store.ResolveCount);
        Assert.False(File.Exists(IdFile(dir)), "the second call went back to the disk");
    }

    /// <summary>
    /// An id that cannot be saved is never used: sessions made under it could not be found again after a restart. The
    /// store answers with no id, says so in one Warning with the error, asks again no sooner than a minute later, and
    /// logs the repeats below Warning until an id is saved.
    /// </summary>
    [Fact]
    public void WhenTheFileCannotBeSaved_NoIdIsUsed_AndTheNextCycleTriesAgain()
    {
        var blocker = Path.Combine(NewDir(), "not-a-folder");
        File.WriteAllText(blocker, "a file where the data folder should be");
        var data = Path.Combine(blocker, "data");
        var clock = new DateTime(2026, 10, 2, 12, 0, 0, DateTimeKind.Utc);
        var store = StoreFor(data);
        store.UtcNowForTests = () => clock;
        AppLogger.DrainBufferedLines();

        Assert.Null(store.GetId());
        var line = Assert.Single(Warnings(AppLogger.DrainBufferedLines()));
        Assert.Contains("could not be saved", line, StringComparison.Ordinal);
        Assert.Contains("next cycle tries again", line, StringComparison.Ordinal);
        Assert.Contains("could not be saved", store.Failure, StringComparison.Ordinal);

        /* Within the minute: no id, and the disk is not touched. */
        clock = clock.AddSeconds(59);
        Assert.Null(store.GetId());
        Assert.Equal(1, store.ResolveCount);

        /* After it: another try, which fails the same way and does not warn again. */
        clock = clock.AddSeconds(2);
        Assert.Null(store.GetId());
        Assert.Equal(2, store.ResolveCount);
        Assert.Empty(Warnings(AppLogger.DrainBufferedLines()));

        /* The folder is usable again: the next cycle after a minute saves an id and uses it. */
        File.Delete(blocker);
        clock = clock.AddSeconds(61);
        var id = store.GetId();
        Assert.True(InstallId.IsValid(id), $"made '{id}'");
        Assert.Equal(id, ReadFile(data).Id);
        Assert.Null(store.Failure);
        Assert.Equal(id, store.GetId());
        Assert.Equal(3, store.ResolveCount);
    }

    /// <summary>
    /// A file that exists but cannot be read (here, held by another process past the wait) is never replaced: an id
    /// made over it would drop the install's sessions from its own view. No id is used, one Warning says why, and the
    /// file is left as it was.
    /// </summary>
    [Fact]
    public void AFileLockedPastTheBudget_IsNeverReplaced_AndNoIdIsUsed()
    {
        var dir = NewDir();
        WriteFile(dir, "0123abcd", MachineA, SidA);
        var before = File.ReadAllText(IdFile(dir));
        var store = StoreFor(dir);
        store.LockedReadBudget = TimeSpan.FromMilliseconds(150);
        AppLogger.DrainBufferedLines();

        string? id;
        using (new FileStream(IdFile(dir), FileMode.Open, FileAccess.Read, FileShare.Delete))
        {
            id = store.GetId();
        }

        Assert.Null(id);
        Assert.Equal(before, File.ReadAllText(IdFile(dir)));
        Assert.Equal(new[] { InstallIdStore.FileName }, FilesIn(dir));
        var line = Assert.Single(Warnings(AppLogger.DrainBufferedLines()));
        Assert.Contains("could not be read", line, StringComparison.Ordinal);
        Assert.Contains("next cycle tries again", line, StringComparison.Ordinal);
        Assert.Contains("could not be read", store.Failure, StringComparison.Ordinal);
    }

    /// <summary>
    /// No id is not cached: calls within a minute answer with none without touching the file, and the first call after
    /// it reads the file once it can be read, and keeps that id.
    /// </summary>
    [Fact]
    public void AFileThatCouldNotBeRead_IsReadAgainOnTheFirstCallAfterAMinute()
    {
        var dir = NewDir();
        WriteFile(dir, "0123abcd", MachineA, SidA);
        var clock = new DateTime(2026, 10, 2, 12, 0, 0, DateTimeKind.Utc);
        var store = StoreFor(dir);
        store.LockedReadBudget = TimeSpan.FromMilliseconds(100);
        store.UtcNowForTests = () => clock;
        var held = new FileStream(IdFile(dir), FileMode.Open, FileAccess.Read, FileShare.Delete);
        try
        {
            Assert.Null(store.GetId());
        }
        finally
        {
            held.Dispose();
        }

        /* Readable now, but the minute has not passed: still no id, and the file is not read. */
        clock = clock.AddSeconds(59);
        Assert.Null(store.GetId());
        Assert.Equal(1, store.ResolveCount);

        clock = clock.AddSeconds(2);
        Assert.Equal("0123abcd", store.GetId());
        Assert.Equal(2, store.ResolveCount);
        Assert.Null(store.Failure);

        Assert.Equal("0123abcd", store.GetId());
        Assert.Equal(2, store.ResolveCount);
    }

    /// <summary>
    /// A new id reaches the final path only by a move from a temp file, so a crash between the write and the move leaves
    /// no file there, and the next start finds a first start rather than a half-written file to replace.
    /// </summary>
    [Fact]
    public void ACrashBetweenTheWriteAndTheMove_LeavesNoFileAtTheFinalPath()
    {
        var dir = NewDir();
        var store = StoreFor(dir);
        string[] atTheCrash = [];
        string? folderAtTheCrash = null;
        store.BeforeMove = () =>
        {
            atTheCrash = FilesIn(dir);
            folderAtTheCrash = NewDir("crash");
            foreach (var file in Directory.GetFiles(dir))
            {
                File.Copy(file, Path.Combine(folderAtTheCrash, Path.GetFileName(file)));
            }

            throw new IOException("the process ended here");
        };
        AppLogger.DrainBufferedLines();

        Assert.Null(store.GetId());

        var temp = Assert.Single(atTheCrash);
        Assert.StartsWith(InstallIdStore.FileName + ".", temp, StringComparison.Ordinal);
        Assert.EndsWith(".tmp", temp, StringComparison.Ordinal);
        Assert.False(File.Exists(IdFile(dir)), "an id was saved");

        /* The next start, from what the crash left on disk: no final file, so a first start, not a replacement. */
        Assert.NotNull(folderAtTheCrash);
        Assert.False(File.Exists(IdFile(folderAtTheCrash)));
        AppLogger.DrainBufferedLines();
        var next = StoreFor(folderAtTheCrash).GetId();
        Assert.True(InstallId.IsValid(next), $"made '{next}'");
        Assert.Equal(next, ReadFile(folderAtTheCrash).Id);
        Assert.Empty(Warnings(AppLogger.DrainBufferedLines()));
    }

    /// <summary>
    /// The production wiring builds the store from the real machine name and the real Windows user, so the binding
    /// it writes is something that can differ: not an empty string that every machine would share.
    /// </summary>
    [Fact]
    public void ForCurrentUser_BindsTheFileToThisMachineAndWindowsUser()
    {
        var dir = NewDir();

        var id = InstallIdStore.ForCurrentUser(dir).GetId();

        var (fileId, machine, sid) = ReadFile(dir);
        Assert.Equal(id, fileId);
        Assert.Equal(Environment.MachineName, machine);
        Assert.StartsWith("S-1-", sid, StringComparison.Ordinal);
    }

    /// <summary>
    /// The collection service reaches the id through the store it was handed, on first use rather than at
    /// construction: nothing is made on disk until a collector asks, and every ask gets the same id.
    /// </summary>
    [Fact]
    public void TheCollectorService_ResolvesTheIdLazilyThroughItsStore()
    {
        var dir = NewDir();
        var configDir = Path.Combine(dir, "config");
        Directory.CreateDirectory(configDir);
        var store = StoreFor(dir);
        using var duckDb = new DuckDbInitializer(Path.Combine(dir, "t.duckdb"));
        var service = new RemoteCollectorService(
            duckDb,
            new ServerManager(configDir),
            new ScheduleManager(configDir),
            logger: null,
            installIdStore: store);

        Assert.False(File.Exists(IdFile(dir)), "the id was made before any collector asked for it");

        var first = service.GetInstallId();
        var second = service.GetInstallId();

        Assert.True(InstallId.IsValid(first), $"made '{first}'");
        Assert.Equal(first, second);
        Assert.Equal(store.GetId(), first);
        Assert.Equal(1, store.ResolveCount);
    }

    /// <summary>A service built without a store (a test host) has no id to give, and says so rather than inventing one.</summary>
    [Fact]
    public void TheCollectorService_WithoutAStore_HasNoId()
    {
        var dir = NewDir();
        using var duckDb = new DuckDbInitializer(Path.Combine(dir, "t.duckdb"));
        var service = new RemoteCollectorService(
            duckDb,
            new ServerManager(dir),
            new ScheduleManager(dir));

        Assert.Null(service.GetInstallId());
    }

    /// <summary>
    /// The wiring that actually builds the store at startup: <c>MainWindow_Loaded</c> hands the collection service
    /// a store for the data root. A store nothing builds is an id nothing can reach, and only the wiring can regress.
    /// </summary>
    [Fact]
    public void MainWindow_HandsTheCollectorServiceAStoreForTheDataRoot()
    {
        var source = File.ReadAllText(Path.Combine(RepoRoot(), "Lite", "MainWindow.xaml.cs"));

        var at = source.IndexOf("_collectorService = new RemoteCollectorService(", StringComparison.Ordinal);
        Assert.True(at >= 0, "MainWindow builds the collection service");
        var end = source.IndexOf(");", at, StringComparison.Ordinal);
        var call = source[at..end];

        Assert.Contains("InstallIdStore.ForCurrentUser(App.DataDirectory)", call, StringComparison.Ordinal);
    }

    private static string RepoRoot([CallerFilePath] string thisFile = "") =>
        Path.GetFullPath(Path.Combine(Path.GetDirectoryName(thisFile)!, ".."));
}
