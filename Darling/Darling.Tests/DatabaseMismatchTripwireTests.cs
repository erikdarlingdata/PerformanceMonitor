/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using System.IO;
using System.Runtime.CompilerServices;
using PerformanceMonitor.Darling.Service;
using Xunit;

namespace Darling.Tests;

/// <summary>
/// #2228: the tripwire that notices a registration is connected to a database it does not name.
///
/// <para><b>The defect.</b> Identity is registration-derived and was never checked against the connection, so a
/// registration whose Initial Catalog is absent, misspelled or overridden lands in a different database and every
/// collected row is stored under that registration's identity while describing somewhere else — indefinitely, and
/// with nothing anywhere saying so. When a sibling registration names that same database, both collect it and its
/// history exists twice under two identities: #2220's field report of byte-identical deadlock graphs under six
/// <c>server_id</c>s, one real incident alerting six times.</para>
///
/// <para><b>Why a connect-time check and not a registry rule.</b> #2158 established that identity must be
/// assigned rather than derived, which fixes the re-key class but cannot touch this one: the two registrations
/// here genuinely differ in configuration, so no amount of care in hashing config can tell that they resolve to
/// one database. Only the server can answer what a connection actually reached, which makes the connect the one
/// place this is knowable.</para>
/// </summary>
public sealed class DatabaseMismatchTripwireTests
{
    /// <summary>THE CASE: registered for one database, landed in another.</summary>
    [Fact]
    public void ARegistrationConnectedElsewhereIsReported()
    {
        var message = DarlingServerConnector.DescribeDatabaseMismatch("Sibling-A", "Source-DB", "Sibling-A");

        Assert.NotNull(message);
        Assert.Contains("Sibling-A", message, StringComparison.Ordinal);
        Assert.Contains("Source-DB", message, StringComparison.Ordinal);
        /* It must say what is actually at stake — mis-attributed rows, and duplication when a sibling names the
           same database — or it reads as a cosmetic naming complaint and gets ignored. */
        Assert.Contains("two identities", message, StringComparison.Ordinal);
        /* And where to fix it, on both surfaces that can set it. */
        Assert.Contains("Initial Catalog", message, StringComparison.Ordinal);
        Assert.Contains("darling.json", message, StringComparison.Ordinal);
    }

    /// <summary>
    /// Agreement is silent. The tripwire runs on every connect of every server, so a false positive is not a
    /// cosmetic problem — it is what trains an operator past the one line that matters.
    /// </summary>
    [Theory]
    [InlineData("SalesDB", "SalesDB")]
    [InlineData("SalesDB", "salesdb")]
    [InlineData("salesdb", "SALESDB")]
    [InlineData(" SalesDB ", "SalesDB")]
    public void AMatchIsSilent_CaseAndWhitespaceInsensitive(string registered, string connected)
    {
        Assert.Null(DarlingServerConnector.DescribeDatabaseMismatch(registered, connected, "srv"));
    }

    /// <summary>
    /// A registration that names NO database is server-scoped by design — it is meant to land wherever the
    /// login defaults and enumerate from there. Comparing it against whatever that default turned out to be
    /// would fire on every correctly-configured server-scoped registration in the fleet, which is the single
    /// most likely way to make this feature useless.
    /// </summary>
    [Theory]
    [InlineData(null, "master")]
    [InlineData("", "master")]
    [InlineData("   ", "SalesDB")]
    public void AServerScopedRegistrationIsNeverATripwire(string? registered, string connected)
    {
        Assert.Null(DarlingServerConnector.DescribeDatabaseMismatch(registered, connected, "srv"));
    }

    /// <summary>
    /// An absent probe answer is silent too: a null <c>connected_database</c> means the probe did not report it
    /// (an older build's row, or a column that came back null), and "unknown" must not be rendered as
    /// "mismatched". Guessing here would fire on every server the moment anything upstream changed shape.
    /// </summary>
    [Theory]
    [InlineData("SalesDB", null)]
    [InlineData("SalesDB", "")]
    public void AnUnknownConnectedDatabaseIsNotAMismatch(string registered, string? connected)
    {
        Assert.Null(DarlingServerConnector.DescribeDatabaseMismatch(registered, connected, "srv"));
    }

    /// <summary>
    /// Both probes ASK for it, and appended so every existing positional read keeps its ordinal — the readers
    /// index by number, so inserting a column mid-list silently shifts five other fields onto the wrong values.
    /// </summary>
    [Fact]
    public void BothEngineProbesAskWhichDatabaseTheyLandedIn()
    {
        Assert.Contains("DB_NAME() AS connected_database", DarlingServerConnector.DetectionQueryText, StringComparison.Ordinal);
        Assert.Contains("current_database() AS connected_database", DarlingServerConnector.PostgresDetectionQueryText, StringComparison.Ordinal);

        /* Last in each list. */
        Assert.EndsWith("connected_database", DarlingServerConnector.DetectionQueryText.TrimEnd(), StringComparison.Ordinal);
        Assert.EndsWith("connected_database", DarlingServerConnector.PostgresDetectionQueryText.TrimEnd(), StringComparison.Ordinal);

        /* No DMV: the SQL Server probe deliberately avoids sys.dm_os_sys_info because an Azure SQL DB
           monitoring login often lacks VIEW DATABASE STATE (#1535), and DB_NAME() keeps that property. */
        Assert.DoesNotContain("sys.dm_os_sys_info", DarlingServerConnector.DetectionQueryText, StringComparison.Ordinal);
    }

    /// <summary>
    /// The worker fires on the TRANSITION, at Error, and reports the recovery too.
    ///
    /// <para>Pinned at the source because reproducing it needs a live server whose connection lands in a
    /// different database than its registration names. Two things would go wrong silently. Logging per connect
    /// instead of per transition buries a standing misconfiguration — it persists until an operator edits the
    /// registration, so it would repeat on every reconnect forever, which is how a tripwire gets trained past.
    /// And omitting the recovery line means an operator who fixes it has only silence as confirmation.</para>
    /// </summary>
    [Fact]
    public void TheWorkerFiresOnTheTransitionAndReportsTheRecovery()
    {
        var source = ReadWorkerSource();

        Assert.Contains("DarlingServerConnector.DescribeDatabaseMismatch(", source, StringComparison.Ordinal);
        Assert.Contains("LastDatabaseMismatchLogged", source, StringComparison.Ordinal);
        /* Error, because nothing clears it on its own and every sweep meanwhile stores mis-attributed rows. */
        Assert.Contains("_logger.LogError(\"[{Server}] {Mismatch}\"", source, StringComparison.Ordinal);
        Assert.Contains("is resolved.", source, StringComparison.Ordinal);
    }

    private static string ReadWorkerSource([CallerFilePath] string thisFile = "")
    {
        var dir = Path.GetDirectoryName(thisFile)!;
        var relative = Path.Combine("Darling", "PerformanceMonitor.Darling.Service", "DarlingWorker.cs");
        while (dir is not null && !File.Exists(Path.Combine(dir, relative)))
        {
            dir = Path.GetDirectoryName(dir);
        }

        Assert.NotNull(dir);
        return File.ReadAllText(Path.Combine(dir!, relative));
    }
}
