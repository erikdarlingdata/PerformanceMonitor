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
    private static string[] ResolveTogether(IEnumerable<InstallIdStore> callers)
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

        var id = StoreFor(dir).GetId();
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

        var id = StoreFor(dir, MachineB, SidA).GetId();
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

        var id = StoreFor(dir, MachineA, SidB).GetId();
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

        var id = StoreFor(dir).GetId();
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
    /// A folder the file cannot be made in must not stop collection: the store answers with an id for this run,
    /// the same one every time, and says so in one Warning.
    /// </summary>
    [Fact]
    public void WhenTheFileCannotBeSaved_TheRunStillGetsOneStableId_AndOneWarning()
    {
        var blocker = Path.Combine(NewDir(), "not-a-folder");
        File.WriteAllText(blocker, "a file where the data folder should be");
        var store = StoreFor(Path.Combine(blocker, "data"));
        AppLogger.DrainBufferedLines();

        var first = store.GetId();
        var second = store.GetId();

        Assert.True(InstallId.IsValid(first), $"made '{first}'");
        Assert.Equal(first, second);
        var line = Assert.Single(Warnings(AppLogger.DrainBufferedLines()));
        Assert.Contains(first, line, StringComparison.Ordinal);
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
        var service = new RemoteCollectorService(
            new DuckDbInitializer(Path.Combine(dir, "t.duckdb")),
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
        var service = new RemoteCollectorService(
            new DuckDbInitializer(Path.Combine(dir, "t.duckdb")),
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
