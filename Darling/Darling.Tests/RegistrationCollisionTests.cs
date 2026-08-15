/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using System.Collections.Generic;
using PerformanceMonitor.Common;
using PerformanceMonitor.Darling.Service;
using PerformanceMonitor.Darling.Service.Mcp;
using Xunit;

namespace Darling.Tests;

/// <summary>
/// #2280: <c>add_servers</c> refuses a registration whose connection lands in a database another registration
/// already covers — and, in the same method, keys its duplicate gate on the identity the store actually derives.
///
/// <para><b>The defect being prevented.</b> Identity is registration-derived, so N registrations that silently
/// resolve to one database get N identities and N full copies of every collected row. #2220 reported it as
/// byte-identical deadlock graphs under six <c>server_id</c>s, one real incident alerting six times. #2277 added
/// the connect-time tripwire that reports it; this refuses it at the point of creation, which is the only place
/// it can be prevented rather than described.</para>
///
/// <para><b>Why the probe is what makes it possible.</b> No comparison of configuration can decide this — the two
/// registrations genuinely differ, which is why #2158 and #2218 (identity assigned, then widened) could not touch
/// it. Only the server can say which database a connection reached, and <c>add_servers</c> already probes
/// in-process, so the answer is in hand at exactly the moment the decision has to be made.</para>
/// </summary>
public sealed class RegistrationCollisionTests
{
    private static DarlingMcpServerAdminTools.ParsedServerEntry Entry(
        string host, string? database, bool readOnlyIntent = false, string engine = "sqlserver", int port = 0)
    {
        var config = new MonitoredServer
        {
            Name = host,
            Host = host,
            Database = database,
            ReadOnlyIntent = readOnlyIntent,
            Engine = engine,
            Port = port,
            Auth = "integrated",
        };

        var key = ServerIdHelper.BuildStorageName(host, database, readOnlyIntent, engine, port);
        return new DarlingMcpServerAdminTools.ParsedServerEntry(0, host, key, config, null);
    }

    private static HashSet<string> Claimed(params string[] keys) =>
        new(keys, StringComparer.OrdinalIgnoreCase);

    /// <summary>
    /// THE CASE: the entry names one database, its connection lands in another, and that other one is already
    /// monitored. Adding it would give one real database two identities.
    /// </summary>
    [Fact]
    public void ARegistrationLandingOnAnAlreadyMonitoredDatabaseIsRefused()
    {
        var entry = Entry("azure.example.net", "Sibling-A");
        var claimed = Claimed(ServerIdHelper.BuildStorageName("azure.example.net", "Source-DB", false, "sqlserver", 0));

        var reason = DarlingMcpServerAdminTools.ActualIdentityCollision(entry, "Source-DB", claimed);

        Assert.NotNull(reason);
        Assert.Contains("Sibling-A", reason, StringComparison.Ordinal);
        Assert.Contains("Source-DB", reason, StringComparison.Ordinal);
        /* It has to say what the harm is, or it reads as pedantry about naming. */
        Assert.Contains("two identities", reason, StringComparison.Ordinal);
        Assert.Contains("Initial Catalog", reason, StringComparison.Ordinal);
    }

    /// <summary>
    /// An entry that reaches the database it NAMES is the ordinary case and passes — the declared gate has
    /// already ruled on it, so firing here would only re-detect that gate's decision under a worse name.
    /// </summary>
    [Theory]
    [InlineData("SalesDB", "SalesDB")]
    [InlineData("SalesDB", "salesdb")]
    [InlineData("SalesDB", " SalesDB ")]
    public void AnEntryThatReachesWhatItNamesIsAllowed(string declared, string connected)
    {
        var entry = Entry("host1", declared);
        var claimed = Claimed(ServerIdHelper.BuildStorageName("host1", declared, false, "sqlserver", 0));

        Assert.Null(DarlingMcpServerAdminTools.ActualIdentityCollision(entry, connected, claimed));
    }

    /// <summary>
    /// Landing somewhere else is fine as long as nobody else covers it — this refuses a COLLISION, not a
    /// misconfiguration. #2277's tripwire is what reports the latter, at every connect.
    /// </summary>
    [Fact]
    public void LandingElsewhereIsAllowedWhenNobodyElseCoversIt()
    {
        var entry = Entry("host1", "Declared-DB");
        var claimed = Claimed(ServerIdHelper.BuildStorageName("host1", "Declared-DB", false, "sqlserver", 0));

        Assert.Null(DarlingMcpServerAdminTools.ActualIdentityCollision(entry, "Somewhere-Else", claimed));
    }

    /// <summary>
    /// An absent probe answer is UNKNOWN, not colliding. Refusing on a missing value would block registrations
    /// for a reason nobody could act on — and a stub probe returns exactly this.
    /// </summary>
    [Theory]
    [InlineData(null)]
    [InlineData("")]
    [InlineData("   ")]
    public void AnUnknownConnectedDatabaseNeverRefuses(string? connected)
    {
        var entry = Entry("host1", "Declared-DB");
        var claimed = Claimed(ServerIdHelper.BuildStorageName("host1", "Other-DB", false, "sqlserver", 0));

        Assert.Null(DarlingMcpServerAdminTools.ActualIdentityCollision(entry, connected, claimed));
    }

    /// <summary>
    /// THE VALID PAIR THAT MUST STILL BE ALLOWED: a read-only-intent registration alongside a read-write one for
    /// the same database. <c>read_only_intent</c> is part of the identity, so comparing on (host, database) alone
    /// would refuse a legitimate configuration — which is why the check keys on the FULL identity.
    /// </summary>
    [Fact]
    public void AReadOnlyIntentRegistrationDoesNotCollideWithItsReadWriteTwin()
    {
        /* Read-only entry that lands in Source-DB; the read-WRITE registration of Source-DB is already claimed. */
        var entry = Entry("ag-listener", "Sibling-A", readOnlyIntent: true);
        var claimed = Claimed(ServerIdHelper.BuildStorageName("ag-listener", "Source-DB", false, "sqlserver", 0));

        Assert.Null(DarlingMcpServerAdminTools.ActualIdentityCollision(entry, "Source-DB", claimed));

        /* But it DOES collide with another read-only registration of the same database. */
        var roClaimed = Claimed(ServerIdHelper.BuildStorageName("ag-listener", "Source-DB", true, "sqlserver", 0));
        Assert.NotNull(DarlingMcpServerAdminTools.ActualIdentityCollision(entry, "Source-DB", roClaimed));
    }

    /// <summary>
    /// Engine is part of the identity too (#2218), so a PostgreSQL entry landing in a database name that a SQL
    /// Server registration covers is not a collision — they are different instances on one host.
    /// </summary>
    [Fact]
    public void APostgresEntryDoesNotCollideWithASqlServerRegistration()
    {
        var entry = Entry("box01", "declared", engine: "postgres");
        var sqlServerClaim = Claimed(ServerIdHelper.BuildStorageName("box01", "actual", false, "sqlserver", 0));

        Assert.Null(DarlingMcpServerAdminTools.ActualIdentityCollision(entry, "actual", sqlServerClaim));

        /* Another PostgreSQL registration of that database IS a collision. */
        var pgClaim = Claimed(ServerIdHelper.BuildStorageName("box01", "actual", false, "postgres", 0));
        Assert.NotNull(DarlingMcpServerAdminTools.ActualIdentityCollision(entry, "actual", pgClaim));
    }

    /// <summary>
    /// A server-scoped entry (no database named) that lands somewhere already covered is still refused — the
    /// message says "no database" rather than pretending it named one, so the operator can tell which of their
    /// registrations is the vague one.
    /// </summary>
    [Fact]
    public void AServerScopedEntryThatLandsOnACoveredDatabaseIsRefusedAndSaysSo()
    {
        var entry = Entry("host1", null);
        var claimed = Claimed(ServerIdHelper.BuildStorageName("host1", "master", false, "sqlserver", 0));

        var reason = DarlingMcpServerAdminTools.ActualIdentityCollision(entry, "master", claimed);

        Assert.NotNull(reason);
        Assert.Contains("no database", reason, StringComparison.Ordinal);
    }

    /// <summary>
    /// #2218's regression in this method, pinned: the duplicate gate reads <c>engine</c> and <c>port</c>, so it
    /// keys on the identity the store actually derives.
    ///
    /// <para>Without them the gate keys on a NARROWER identity than the product does, and a PostgreSQL instance
    /// on a host that already has a SQL Server registration reads as a duplicate and is refused — a valid pair
    /// rejected because the gate could not see what distinguishes them. It compiles and every existing test
    /// passes, which is why it is worth an explicit pin.</para>
    /// </summary>
    [Fact]
    public void TheDuplicateGateReadsTheFullIdentity()
    {
        Assert.Contains("engine", DarlingMcpServerAdminTools.ExistingServersSql, StringComparison.Ordinal);
        Assert.Contains("port", DarlingMcpServerAdminTools.ExistingServersSql, StringComparison.Ordinal);
        Assert.Contains("read_only_intent", DarlingMcpServerAdminTools.ExistingServersSql, StringComparison.Ordinal);

        /* And the two identities it would compare genuinely differ, so the columns are load-bearing rather than
           decorative. */
        Assert.NotEqual(
            ServerIdHelper.BuildStorageName("box01", null, false, "sqlserver", 0),
            ServerIdHelper.BuildStorageName("box01", null, false, "postgres", 0));
    }
}
