/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using PerformanceMonitor.Common;
using PerformanceMonitor.Darling.Viewer;
using Xunit;

namespace Darling.Tests;

/// <summary>
/// #4789: adding a server in the Darling viewer, alone or in bulk, never overwrites a DIFFERENT server whose
/// <c>server_id</c> matches the new one.
///
/// <para><b>The defect.</b> A new server's id is a 32-bit hash of its identity, so two different identities can
/// hash to one id. Both dialogs wrote with <c>UpsertMonitoredServerAsync</c>, whose <c>ON CONFLICT (server_id) DO
/// UPDATE</c> rewrote whichever server already held the id: that server stopped being monitored and its
/// collected history showed under the new one, with no message. The MCP <c>add_servers</c> tool already
/// refused this case.</para>
///
/// <para><b>What is pinned here</b> is everything that needs no store: the pair of identities that collide, the
/// classifier that tells a collision from the same server again, the messages the dialogs show, and the
/// dialogs' wiring (an Add goes through <see cref="ViewerDataService.AddMonitoredServerAsync"/>, an edit keeps
/// the upsert). The insert-then-read itself runs against a store in the PostgreSQL CI job.</para>
/// </summary>
public sealed class ViewerAddServerIdCollisionTests
{
    /* Two hosts whose storage names hash to the SAME server_id (found by search). The first test asserts it, so
       a change to the hash says so instead of quietly voiding every collision test below. */
    private const string HolderHost = "sql9jocsv";
    private const string CollidingHost = "sqlsvvqew";

    private static MonitoredServerRow Row(
        string host,
        string? database = null,
        bool readOnlyIntent = false,
        string engine = MonitoredServerRow.EngineSqlServer,
        int port = 0,
        string name = "server") => new()
        {
            ServerId = ViewerDataService.ComputeServerId(host, database, readOnlyIntent, engine, port),
            Name = name,
            Host = host,
            Database = database,
            ReadOnlyIntent = readOnlyIntent,
            Engine = engine,
            Port = port,
        };

    /// <summary>The row a store would hand back for the id the candidate is trying to take: the occupant's own
    /// fields, sitting on the candidate's id (that is what makes it the holder).</summary>
    private static MonitoredServerRow Holding(MonitoredServerRow candidate, MonitoredServerRow occupant)
    {
        occupant.ServerId = candidate.ServerId;
        return occupant;
    }

    [Fact]
    public void TheTwoHosts_DeriveOneServerId_ThroughTheViewersOwnDerivation()
    {
        Assert.NotEqual(HolderHost, CollidingHost);
        Assert.NotEqual(
            ServerIdHelper.BuildStorageName(HolderHost, null, false),
            ServerIdHelper.BuildStorageName(CollidingHost, null, false));

        Assert.Equal(
            ViewerDataService.ComputeServerId(HolderHost, null, false),
            ViewerDataService.ComputeServerId(CollidingHost, null, false));
    }

    [Fact]
    public void ADifferentServerAtTheId_Collides()
    {
        var candidate = Row(CollidingHost, name: "the new server");
        var occupant = Holding(candidate, Row(HolderHost, name: "the server that holds the id"));

        Assert.Equal(candidate.ServerId, occupant.ServerId);
        Assert.Equal(MonitoredServerAddOutcome.Collides, ViewerDataService.ClassifyAddAgainstOccupant(candidate, occupant));
    }

    /// <summary>The identity is the dialogs' address check's: host, database, read-only intent, engine and port.
    /// A difference in any one of them, on a row holding the same id, is a different server.</summary>
    [Fact]
    public void ADifferenceInAnyPartOfTheIdentity_IsACollision()
    {
        var cases = new (string What, MonitoredServerRow Candidate, MonitoredServerRow Occupant)[]
        {
            ("host", Row("sql01", "AppDb"), Row("sql02", "AppDb")),
            ("host case", Row("SQL01", "AppDb"), Row("sql01", "AppDb")),
            ("database", Row("sql01", "AppDb"), Row("sql01", "OtherDb")),
            ("a database against none", Row("sql01", "AppDb"), Row("sql01", null)),
            ("none against a database", Row("sql01", null), Row("sql01", "AppDb")),
            ("read-only intent", Row("sql01", "AppDb"), Row("sql01", "AppDb", readOnlyIntent: true)),
            ("engine", Row("shared-host", null), Row("shared-host", null, engine: MonitoredServerRow.EnginePostgres)),
            ("port", Row("pg01", null, engine: MonitoredServerRow.EnginePostgres, port: 5432),
                     Row("pg01", null, engine: MonitoredServerRow.EnginePostgres, port: 5433)),
        };

        foreach (var (what, candidate, occupant) in cases)
        {
            Assert.True(
                ViewerDataService.ClassifyAddAgainstOccupant(candidate, Holding(candidate, occupant)) == MonitoredServerAddOutcome.Collides,
                $"a different {what} at the same id must be a collision");
        }
    }

    /// <summary>The same server again is a duplicate however it is named or configured: the name, credentials and
    /// every other setting are not part of the identity.</summary>
    [Fact]
    public void TheSameServerAgain_IsADuplicate_WhateverItsNameOrSettings()
    {
        var candidate = Row("sql01", "AppDb", name: "the name typed just now");
        var occupant = Holding(candidate, Row("sql01", "AppDb", name: "the name it was saved under"));
        occupant.Auth = ServerStoreCredential.Sql;
        occupant.Username = "someone";
        occupant.MonthlyCostUsd = 250m;
        occupant.IsEnabled = false;
        occupant.EncryptMode = "Optional";

        Assert.Equal(MonitoredServerAddOutcome.Duplicate, ViewerDataService.ClassifyAddAgainstOccupant(candidate, occupant));

        var serverScoped = Row("sql01");
        Assert.Equal(
            MonitoredServerAddOutcome.Duplicate,
            ViewerDataService.ClassifyAddAgainstOccupant(serverScoped, Holding(serverScoped, Row("sql01"))));
    }

    /// <summary>Engine is compared as a KIND, like the address check: a row a <c>darling.json</c> seed stored as
    /// <c>aurora-postgresql</c> is the same engine as the <c>postgres</c> the dialogs write.</summary>
    [Fact]
    public void EveryAcceptedSpellingOfPostgres_IsTheSameEngine()
    {
        var candidate = Row("pg01", null, engine: MonitoredServerRow.EnginePostgres, port: 5433);

        foreach (var spelling in new[] { "postgres", "PostgreSQL", "pg", "aurora-postgresql", "aurora" })
        {
            var occupant = Holding(candidate, Row("pg01", null, engine: spelling, port: 5433));

            Assert.True(
                ViewerDataService.ClassifyAddAgainstOccupant(candidate, occupant) == MonitoredServerAddOutcome.Duplicate,
                $"'{spelling}' is PostgreSQL, so the same host and port is the same server");
        }
    }

    [Fact]
    public void NothingAtTheId_IsNotSaved()
    {
        Assert.Equal(
            MonitoredServerAddOutcome.NotSaved,
            ViewerDataService.ClassifyAddAgainstOccupant(Row("sql01"), occupant: null));
    }

    /// <summary>The classifier only ever explains a write that did not happen: it never answers
    /// <see cref="MonitoredServerAddOutcome.Added"/>.</summary>
    [Fact]
    public void TheClassifier_NeverAnswersAdded()
    {
        var candidate = Row("sql01", "AppDb");
        var occupants = new MonitoredServerRow?[]
        {
            null,
            Holding(candidate, Row("sql01", "AppDb")),
            Holding(candidate, Row("sql02", "AppDb")),
        };

        foreach (var occupant in occupants)
        {
            Assert.NotEqual(MonitoredServerAddOutcome.Added, ViewerDataService.ClassifyAddAgainstOccupant(candidate, occupant));
        }
    }

    [Fact]
    public void TheAddDialog_NamesTheServerItCollidesWith_AndSavesNothing()
    {
        var holder = Row(HolderHost, name: "Payroll");

        var collides = AddServerDialog.DescribeRefusedAdd(
            new MonitoredServerAddResult(MonitoredServerAddOutcome.Collides, holder));
        var duplicate = AddServerDialog.DescribeRefusedAdd(
            new MonitoredServerAddResult(MonitoredServerAddOutcome.Duplicate, holder));
        var notSaved = AddServerDialog.DescribeRefusedAdd(
            new MonitoredServerAddResult(MonitoredServerAddOutcome.NotSaved, null));

        Assert.Contains("'Payroll'", collides, StringComparison.Ordinal);
        Assert.Contains("collides", collides, StringComparison.Ordinal);
        Assert.Contains("nothing was changed", collides, StringComparison.Ordinal);

        Assert.Contains("already monitored", duplicate, StringComparison.Ordinal);
        Assert.DoesNotContain("collides", duplicate, StringComparison.Ordinal);

        Assert.StartsWith("Not saved", notSaved, StringComparison.Ordinal);
    }

    [Fact]
    public void TheBulkDialog_MarksACollidedRow_WithTheServerItCollidesWith()
    {
        Assert.Equal(
            "Not added: its id collides with Payroll",
            AddMultipleServersDialog.DescribeCollision(Row(HolderHost, name: "Payroll")));
    }

    /// <summary>
    /// The Add dialog's write. An Add goes through <see cref="ViewerDataService.AddMonitoredServerAsync"/>; the
    /// upsert stays for an edit, which keeps its row's id when the address changes (#2158) and so updates in place.
    /// The address guard still runs first. Held at the source level because the suite cannot stand up the dialog
    /// with a live store, and a swap back to the upsert is invisible in every other test.
    /// </summary>
    [Fact]
    public void TheAddDialog_AddsThroughTheInsert_AndOnlyAnEditUpserts()
    {
        var source = RepoFile.ReadRepoFile("Darling", "PerformanceMonitor.Darling.Viewer", "AddServerDialog.xaml.cs");

        var guard = source.IndexOf("GetMonitoredServerByAddressAsync(", StringComparison.Ordinal);
        var isAdd = source.IndexOf("if (_originalServerId is null)", StringComparison.Ordinal);
        var add = source.IndexOf("await _dataService.AddMonitoredServerAsync(row);", StringComparison.Ordinal);
        var upsert = source.IndexOf("await _dataService.UpsertMonitoredServerAsync(row);", StringComparison.Ordinal);

        Assert.True(guard >= 0, "the address guard must still be there");
        Assert.True(isAdd > guard, "the Add branch must come after the address guard");
        Assert.True(add > isAdd, "the Add branch must write through AddMonitoredServerAsync");
        Assert.True(upsert > add, "the edit branch must still write through the upsert, after the Add branch");

        Assert.Equal(1, CountOf(source, "AddMonitoredServerAsync(row)"));
        Assert.Equal(1, CountOf(source, "UpsertMonitoredServerAsync(row)"));
    }

    [Fact]
    public void TheBulkDialog_AddsEachRowThroughTheInsert_AndCountsCollisionsApart()
    {
        var source = RepoFile.ReadRepoFile("Darling", "PerformanceMonitor.Darling.Viewer", "AddMultipleServersDialog.xaml.cs");

        Assert.Contains("await _dataService.AddMonitoredServerAsync(row);", source, StringComparison.Ordinal);
        Assert.DoesNotContain("UpsertMonitoredServerAsync(", source, StringComparison.Ordinal);

        Assert.Contains("public int CollidedCount { get; private set; }", source, StringComparison.Ordinal);
        Assert.Contains("CollidedCount = collided;", source, StringComparison.Ordinal);
        Assert.Contains("{collided} collided", source, StringComparison.Ordinal);

        var window = RepoFile.ReadRepoFile("Darling", "PerformanceMonitor.Darling.Viewer", "MainWindow.ServerManagement.cs");
        Assert.Contains("dialog.CollidedCount > 0", window, StringComparison.Ordinal);
    }

    /// <summary>The data method inserts only into a free id, then reads the holder WITHOUT the secret and hands it
    /// to the classifier; it never touches the upsert.</summary>
    [Fact]
    public void TheAddMethod_InsertsIfAbsent_ReadsTheHolderWithoutTheSecret_AndClassifiesIt()
    {
        var source = RepoFile.ReadRepoFile("Darling", "PerformanceMonitor.Darling.Viewer", "ViewerDataService.MonitoredServers.cs");

        var start = source.IndexOf("public async Task<MonitoredServerAddResult> AddMonitoredServerAsync(", StringComparison.Ordinal);
        var end = source.IndexOf("public static MonitoredServerAddOutcome ClassifyAddAgainstOccupant(", StringComparison.Ordinal);
        Assert.True(start >= 0 && end > start, "the Add method and the classifier must both be there, in that order");

        var body = source[start..end];
        /* #5240: the insert-if-absent and the holder read now run on the transaction that holds the identity lock,
           so they are built on that connection (new NpgsqlCommand(sql, connection, transaction)) instead of through
           the data source; the statements are the same ones, and the lock is taken before the insert. */
        Assert.Contains("new NpgsqlCommand(MonitoredServerInsertIfAbsentSql, connection, transaction)", body, StringComparison.Ordinal);
        Assert.Contains("new NpgsqlCommand(MonitoredServerByIdNoSecretSql, connection, transaction)", body, StringComparison.Ordinal);
        Assert.Contains("ClassifyAddAgainstOccupant(row, occupant)", body, StringComparison.Ordinal);
        Assert.DoesNotContain("Upsert", body, StringComparison.Ordinal);
        Assert.True(
            body.IndexOf("TakeIdentityLockAndFindClaimantAsync(", StringComparison.Ordinal) is var lockAt and >= 0
            && lockAt < body.IndexOf("MonitoredServerInsertIfAbsentSql", StringComparison.Ordinal),
            "the add must take the identity lock and re-read the addresses before it inserts");
    }

    private static int CountOf(string source, string needle)
    {
        var count = 0;
        var at = 0;
        while ((at = source.IndexOf(needle, at, StringComparison.Ordinal)) >= 0)
        {
            count++;
            at += needle.Length;
        }

        return count;
    }
}
