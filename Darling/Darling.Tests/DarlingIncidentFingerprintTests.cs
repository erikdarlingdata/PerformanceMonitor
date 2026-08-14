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
using PerformanceMonitor.Alerting;
using PerformanceMonitor.Darling.Service.Mcp;
using PerformanceMonitor.Notifications;
using Xunit;

namespace Darling.Tests;

/// <summary>
/// #2159: recomputing the #1140 dedup fingerprint for STORED incident rows, so an operator can paste an alert's
/// Dedup Key into <c>get_deadlocks</c> / <c>get_deadlock_detail</c> / <c>get_blocking</c> and land on that one
/// incident instead of scanning a server+time window and eyeballing which deadlock matched.
///
/// <para><b>The only failure mode worth testing hard is silent disagreement.</b> The key is a SHA-256 over
/// normalized identity members, so a reader that derives those members even slightly differently from the alert
/// path produces a different hash and matches NOTHING — and an empty result is indistinguishable from "that
/// incident is outside the window". So the centrepiece here is parity against the REAL alert entry points
/// (<see cref="AlertContextBuilders.DeadlockIncidents"/> / <see cref="AlertContextBuilders.BlockingIncidents"/>),
/// not against a hand-computed hash: a hand-computed expectation would pin my reimplementation rather than the
/// product's agreement with itself.</para>
/// </summary>
public sealed class DarlingIncidentFingerprintTests
{
    private const string ServerName = "SQLPROD01";
    private static readonly IReadOnlyList<string> NoExclusions = Array.Empty<string>();

    /// <summary>A deadlock graph with two lock resources, the shape the object extractor reads.</summary>
    private static string Graph(string firstObject, string secondObject) => $"""
        <deadlock>
          <victim-list><victimProcess id="process1" /></victim-list>
          <process-list>
            <process id="process1" spid="55"><inputbuf>UPDATE x</inputbuf></process>
            <process id="process2" spid="56"><inputbuf>UPDATE y</inputbuf></process>
          </process-list>
          <resource-list>
            <keylock objectname="{firstObject}" mode="X">
              <owner-list><owner id="process2" mode="X" /></owner-list>
              <waiter-list><waiter id="process1" mode="S" /></waiter-list>
            </keylock>
            <keylock objectname="{secondObject}" mode="X">
              <owner-list><owner id="process1" mode="X" /></owner-list>
              <waiter-list><waiter id="process2" mode="S" /></waiter-list>
            </keylock>
          </resource-list>
        </deadlock>
        """;

    private static DeadlockAlertRow DeadlockRow(string xml) =>
        new() { VictimProcessId = "process1", VictimSqlText = "UPDATE x", DeadlockGraphXml = xml };

    /// <summary>
    /// THE TEST THIS FEATURE RESTS ON. For the same stored graph, the reader's recomputed key must equal the key
    /// the alert path emits — compared against <see cref="AlertContextBuilders.DeadlockIncidents"/> itself, which
    /// is what actually feeds the Dedup Key fact an operator pastes in.
    /// </summary>
    [Fact]
    public void DeadlockKey_MatchesTheAlertPathsKeyForTheSameGraph()
    {
        var xml = Graph("SalesDB.dbo.Orders.PK_Orders", "SalesDB.dbo.LineItems.PK_LineItems");

        var fromAlert = AlertContextBuilders.DeadlockIncidents(ServerName, new[] { DeadlockRow(xml) }, NoExclusions);
        var fromReader = DarlingIncidentFingerprint.DeadlockKeys(ServerName, new[] { xml });

        Assert.Single(fromAlert);
        Assert.Single(fromReader);
        Assert.Equal(fromAlert[0].DedupKey, fromReader[0]);
        /* Guard the vacuous pass: two nulls would satisfy Equal and prove nothing. */
        Assert.False(string.IsNullOrEmpty(fromReader[0]));
    }

    /// <summary>
    /// The blocking twin. Kept separate because the two paths differ in a way that matters: a blocking key comes
    /// from the identity bucket's REPRESENTATIVE and falls back from the contentious object to a query-pair key,
    /// so it is the one with real room to disagree.
    /// </summary>
    [Fact]
    public void BlockingKey_MatchesTheAlertPathsKeyForTheSameRow()
    {
        var row = new BlockedProcessAlertRow
        {
            DatabaseName = "SalesDB",
            ContentiousObject = "SalesDB.dbo.Orders",
            BlockedSqlText = "SELECT * FROM dbo.Orders WHERE id = 1",
            BlockingSqlText = "UPDATE dbo.Orders SET total = 5 WHERE id = 1",
            WaitTimeMs = 41_000,
            LockMode = "X",
        };

        var fromAlert = AlertContextBuilders.BlockingIncidents(ServerName, new[] { row }, NoExclusions);
        var fromReader = DarlingIncidentFingerprint.BlockingKeys(
            ServerName,
            new[]
            {
                new BlockingIncidentGrouper.BlockedEvent(
                    row.DatabaseName, row.ContentiousObject, row.BlockedSqlText, row.BlockingSqlText,
                    row.WaitTimeMs, row.LockMode),
            });

        Assert.Single(fromAlert);
        Assert.Single(fromReader);
        Assert.Equal(fromAlert[0].DedupKey, fromReader[0]);
        Assert.False(string.IsNullOrEmpty(fromReader[0]));
    }

    /// <summary>
    /// The blocking fallback path: no contentious object, so the key comes from the normalized query pair rather
    /// than an object set. Exercised separately because it is a different branch of the grouper and the branch a
    /// reimplementation would most likely get wrong.
    /// </summary>
    [Fact]
    public void BlockingKey_MatchesTheAlertPath_WhenNoContentiousObjectResolved()
    {
        var row = new BlockedProcessAlertRow
        {
            DatabaseName = "SalesDB",
            ContentiousObject = "",
            BlockedSqlText = "SELECT * FROM dbo.Orders WHERE id = 99",
            BlockingSqlText = "UPDATE dbo.Orders SET total = 7 WHERE id = 99",
            WaitTimeMs = 12_000,
            LockMode = "X",
        };

        var fromAlert = AlertContextBuilders.BlockingIncidents(ServerName, new[] { row }, NoExclusions);
        var fromReader = DarlingIncidentFingerprint.BlockingKeys(
            ServerName,
            new[]
            {
                new BlockingIncidentGrouper.BlockedEvent(
                    row.DatabaseName, row.ContentiousObject, row.BlockedSqlText, row.BlockingSqlText,
                    row.WaitTimeMs, row.LockMode),
            });

        Assert.Single(fromAlert);
        Assert.Equal(fromAlert[0].DedupKey, fromReader[0]);
        Assert.False(string.IsNullOrEmpty(fromReader[0]));
    }

    /// <summary>
    /// Several rows, several incidents: every row's key must equal the alert path's key for its OWN objects, and
    /// the two recurrences over the same object set must collapse to one key. Positional correctness is the
    /// contract the callers rely on — they filter their own row list by index.
    /// </summary>
    [Fact]
    public void DeadlockKeys_ArePositionalAndRecurrencesShareAKey()
    {
        var ordersGraph = Graph("SalesDB.dbo.Orders.PK_Orders", "SalesDB.dbo.LineItems.PK_LineItems");
        var swapped = Graph("SalesDB.dbo.LineItems.PK_LineItems", "SalesDB.dbo.Orders.PK_Orders");
        var otherGraph = Graph("SalesDB.dbo.Customers.PK_Customers", "SalesDB.dbo.Addresses.PK_Addresses");

        var keys = DarlingIncidentFingerprint.DeadlockKeys(ServerName, new[] { ordersGraph, otherGraph, swapped });

        Assert.Equal(3, keys.Count);
        /* Same objects in the other order is the SAME incident — the fingerprint sorts its members. */
        Assert.Equal(keys[0], keys[2]);
        Assert.NotEqual(keys[0], keys[1]);

        var fromAlert = AlertContextBuilders.DeadlockIncidents(
            ServerName, new[] { DeadlockRow(otherGraph) }, NoExclusions);
        Assert.Equal(fromAlert[0].DedupKey, keys[1]);
    }

    /// <summary>
    /// THE SCOPE TRAP, pinned. The fingerprint hashes the server name, and the alert path passes the DISPLAY
    /// name while the MCP resolver returns the STORAGE name. If those two strings ever fed the same hash the
    /// filter would appear to work in testing and return nothing in the field for every renamed server — so
    /// assert they genuinely differ, which is what makes <c>FingerprintNameOf</c> load-bearing rather than
    /// decorative.
    /// </summary>
    [Fact]
    public void TheKeyIsScopedToTheServerName_SoDisplayAndStorageNamesDisagree()
    {
        var xml = Graph("SalesDB.dbo.Orders.PK_Orders", "SalesDB.dbo.LineItems.PK_LineItems");

        var underDisplayName = DarlingIncidentFingerprint.DeadlockKeys("Sales Primary", new[] { xml })[0];
        var underStorageName = DarlingIncidentFingerprint.DeadlockKeys("sqlprod01.contoso.com:SalesDB", new[] { xml })[0];

        Assert.NotEqual(underDisplayName, underStorageName);
    }

    /// <summary>
    /// A graph the extractor finds no objects in yields NO key, so it can never match a filter. That is correct
    /// rather than a gap: the alerting layer emits no incident for it either, so there is no key it could be
    /// searched by. Must not throw, and must keep its position.
    /// </summary>
    [Theory]
    [InlineData("")]
    [InlineData(null)]
    [InlineData("<deadlock><resource-list /></deadlock>")]
    [InlineData("not xml at all")]
    public void ARowWithNoExtractableObjects_HasNoKeyAndDoesNotThrow(string? xml)
    {
        var keys = DarlingIncidentFingerprint.DeadlockKeys(ServerName, new[] { xml });

        Assert.Single(keys);
        Assert.Null(keys[0]);
    }

    /// <summary>
    /// Keys arrive by copy-and-paste out of a ticket or an alert card, so surrounding whitespace and upper-casing
    /// must not defeat the filter. The stored form is lowercase hex.
    /// </summary>
    [Theory]
    [InlineData("  ABCDEF0123  ", "abcdef0123")]
    [InlineData("ABCDEF0123", "abcdef0123")]
    [InlineData("abcdef0123", "abcdef0123")]
    [InlineData("\tabcdef0123\n", "abcdef0123")]
    public void APastedKeyIsNormalizedBeforeComparison(string pasted, string expected)
    {
        Assert.Equal(expected, DarlingIncidentFingerprint.NormalizeKey(pasted));
    }

    /// <summary>
    /// Absent, blank and whitespace-only all mean "no filter requested" — the tools must not treat a blank
    /// string as a key that matches nothing, which would turn an omitted argument into an empty result.
    /// </summary>
    [Theory]
    [InlineData(null, true)]
    [InlineData("", true)]
    [InlineData("   ", true)]
    [InlineData("abcdef", false)]
    public void BlankMeansNoFilterRatherThanAKeyThatMatchesNothing(string? dedupKey, bool expected)
    {
        Assert.Equal(expected, DarlingIncidentFingerprint.NoFilter(dedupKey));
    }

    /// <summary>
    /// The no-match message has to be diagnostic, because the three causes are indistinguishable from silence:
    /// wrong window, wrong server, or a server renamed since the alert fired. It must name the rename — that one
    /// is invisible and permanent — and report how many rows were actually examined, which separates "nothing to
    /// match against" from "plenty, none matching".
    /// </summary>
    [Fact]
    public void TheNoMatchMessageExplainsWhyRatherThanJustSayingEmpty()
    {
        var message = DarlingIncidentFingerprint.NoMatchMessage("deadlocks", "  ABCDEF  ", "Sales Primary", 17);

        Assert.Contains("abcdef", message, StringComparison.Ordinal);
        Assert.Contains("Examined 17 deadlocks", message, StringComparison.Ordinal);
        Assert.Contains("Sales Primary", message, StringComparison.Ordinal);
        Assert.Contains("renamed", message, StringComparison.Ordinal);
        Assert.Contains("hours_back", message, StringComparison.Ordinal);
    }

    /// <summary>
    /// <c>FingerprintNameOf</c> reproduces the alert path's choice: the display name when there is one, the
    /// storage name only as the fallback for a registry row written without one.
    /// </summary>
    [Theory]
    [InlineData("host1:SalesDB", "Sales Primary", "Sales Primary")]
    [InlineData("host1:SalesDB", null, "host1:SalesDB")]
    [InlineData("host1:SalesDB", "", "host1:SalesDB")]
    [InlineData("host1:SalesDB", "   ", "host1:SalesDB")]
    public void FingerprintNamePrefersTheDisplayNameTheAlertPathHashes(
        string storageName, string? displayName, string expected)
    {
        var server = new DarlingServerResolver.RegisteredServer(42, storageName, displayName);

        Assert.Equal(expected, DarlingServerResolver.FingerprintNameOf(server));
    }
}
