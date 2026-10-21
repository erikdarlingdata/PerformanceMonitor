/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using System.Collections.Generic;
using PerformanceMonitor.Collectors;
using Xunit;

namespace Darling.Tests;

/// <summary>
/// #6312: what an Azure SQL Database per-database fan-out of an Extended Events collector reports. In the 3.11.0
/// release test a firewall outage failed every user database with 40615 while master answered "XE session is not
/// running", and the row was logged SESSION_MISSING with master's message; and in steady state master was counted as a
/// failed database on 35 of 40 healthy rows. A fake per-database outcome stands in for the connection the tests
/// cannot make to Azure. Lite carries the same tests against the same shared code.
/// </summary>
public sealed class AzureXeFanoutFailureTests
{
    private const int FirewallRejected = 40615;
    private const int XeSessionNotRunning = 50000;

    private sealed class FakeSqlFailure(string message, params int[] numbers) : Exception(message)
    {
        public int[] Numbers { get; } = numbers;
    }

    private static IEnumerable<int> NumbersOf(Exception ex) => ((FakeSqlFailure)ex).Numbers;

    private static readonly FakeSqlFailure XeMissing =
        new("XE session [PerformanceMonitor_Attention] is not running", XeSessionNotRunning);

    private static FakeSqlFailure Firewall(string database) =>
        new($"Client with IP address '192.0.2.1' is not allowed to access the server ({database})", FirewallRejected);

    /// <summary>The fan-out as both hosts run it: each database's outcome in order, the first error kept the way the
    /// hosts keep it, and the note and all-failed rethrow composed from the same figures.</summary>
    private static (Exception? Rethrown, string? Note, IReadOnlyList<string> Failed) Sweep(
        IEnumerable<string> databases, Func<string, Exception?> outcome)
    {
        var attempted = 0;
        var failedDatabases = new List<string>();
        Exception? firstFailure = null;
        foreach (var database in databases)
        {
            attempted++;
            if (outcome(database) is { } failure)
            {
                failedDatabases.Add(database);
                firstFailure = AzureSweepScope.PreferConnectionFailure(firstFailure, failure, NumbersOf);
            }
        }

        var allFailed = attempted > 0 && failedDatabases.Count == attempted;
        var note = EnumeratedCollectorDriver.BuildPartialFailureNote(
            failedDatabases.Count, attempted, failedDatabases, firstFailure?.Message);
        return (allFailed ? firstFailure : null, note, failedDatabases);
    }

    private static bool SkipsMaster(string collector) => collector switch
    {
        "attention" => AttentionCollector.Instance.SkipsMasterDatabase,
        "blocked_process_report" => BlockedProcessReportCollector.Instance.SkipsMasterDatabase,
        _ => throw new ArgumentOutOfRangeException(nameof(collector)),
    };

    /// <summary>
    /// THE REPORTED CASE. master is listed first and answers XE-missing, both user databases answer 40615. Every database
    /// failed, so the run rethrows the first error kept: it must be the firewall one.
    /// </summary>
    [Fact]
    public void FirewallOutageOnEveryUserDatabaseOutranksMastersMissingSession()
    {
        var (rethrown, _, failed) = Sweep(new[] { "master", "db1", "db2" },
            db => db == "master" ? XeMissing : Firewall(db));

        Assert.Equal(3, failed.Count);
        Assert.NotNull(rethrown);
        Assert.Contains("not allowed to access the server", rethrown!.Message, StringComparison.Ordinal);
        Assert.DoesNotContain("is not running", rethrown.Message, StringComparison.Ordinal);
    }

    /// <summary>The order the databases fail in does not matter: the connection failure wins whichever came first.</summary>
    [Fact]
    public void ConnectionFailureWinsWhicheverDatabaseFailedFirst()
    {
        var (rethrown, _, _) = Sweep(new[] { "db1", "db2", "master" },
            db => db == "db1" ? Firewall(db) : XeMissing);
        Assert.Contains("not allowed to access the server", rethrown!.Message, StringComparison.Ordinal);
    }

    /// <summary>When some databases failed to connect and others did not, the partial note names the connection
    /// error as its first error.</summary>
    [Fact]
    public void PartialNoteNamesTheConnectionFailureNotTheMissingSession()
    {
        var (rethrown, note, failed) = Sweep(new[] { "db1", "db2", "db3" },
            db => db switch { "db1" => XeMissing, "db2" => Firewall(db), _ => null });

        Assert.Null(rethrown);
        Assert.Equal(new[] { "db1", "db2" }, failed);
        Assert.NotNull(note);
        Assert.Contains("first error: Client with IP address", note!, StringComparison.Ordinal);
    }

    /// <summary>A fan-out with no connection failure keeps the first error, so a real missing session still reads as one.</summary>
    [Fact]
    public void WithoutAConnectionFailureTheFirstErrorIsKept()
    {
        var other = new FakeSqlFailure("Invalid object name", 208);
        var (rethrown, _, _) = Sweep(new[] { "db1", "db2" }, db => db == "db1" ? XeMissing : other);
        Assert.Same(XeMissing, rethrown);
    }

    /// <summary>40613 (a paused or unavailable database), a refused login and a dropped connection are connection failures too.</summary>
    [Theory]
    [InlineData(40613)]
    [InlineData(40615)]
    [InlineData(18456)]
    [InlineData(4060)]
    [InlineData(10061)]
    public void TheClassifiersConnectionErrorsOutrankAMissingSession(int number)
    {
        var connection = new FakeSqlFailure("cannot connect", number);
        Assert.Same(connection, AzureSweepScope.PreferConnectionFailure(XeMissing, connection, NumbersOf));
        Assert.Same(connection, AzureSweepScope.PreferConnectionFailure(connection, XeMissing, NumbersOf));
    }

    /// <summary>
    /// STEADY STATE. master is not read, so a healthy logical server's two user databases are two attempts, none failed, and
    /// there is no "1 of 3 database(s) failed" note.
    /// </summary>
    [Theory]
    [InlineData("attention")]
    [InlineData("blocked_process_report")]
    public void MasterIsNotAFailedDatabaseForTheXeCollectors(string collector)
    {
        var listed = new List<string> { "master", "db1", "db2" };
        var read = SkipsMaster(collector) ? AzureSweepScope.WithoutMaster(listed) : listed;

        var (rethrown, note, failed) = Sweep(read, db => db == "master" ? XeMissing : null);

        Assert.Equal(new[] { "db1", "db2" }, read);
        Assert.Empty(failed);
        Assert.Null(rethrown);
        Assert.Null(note);
    }

    /// <summary>Only the collectors whose master read is always a failure leave master out. deadlocks keeps it, because
    /// master is where Azure's server-wide deadlock telemetry blob is read; every other collector reads the list as given.</summary>
    [Fact]
    public void CollectorsThatGetDataFromMasterStillReadIt()
    {
        Assert.False(DeadlocksCollector.Instance.SkipsMasterDatabase);
        Assert.False(QueryStoreCollector.Instance.SkipsMasterDatabase);
    }

    [Fact]
    public void WithoutMasterKeepsTheOtherDatabasesInOrderAndReturnsTheSameListWhenMasterIsAbsent()
    {
        Assert.Equal(new[] { "a", "b" }, AzureSweepScope.WithoutMaster(new List<string> { "a", "MASTER", "b" }));

        var noMaster = new List<string> { "a", "b" };
        Assert.Same(noMaster, AzureSweepScope.WithoutMaster(noMaster));
    }
}
