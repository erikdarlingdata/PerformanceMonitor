/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor Lite.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using System.Linq;
using PerformanceMonitor.Common;
using PerformanceMonitorLite.Models;
using PerformanceMonitorLite.Services;
using Xunit;

namespace PerformanceMonitorLite.Tests;

/// <summary>
/// Regression tests for #1506.
///
/// Reported by a user monitoring Azure SQL DB whose public IP rotates daily, so the server's
/// firewall rule stops matching and connections are rejected with error 40615. Restoring the rule
/// brought the server back, but collection stayed broken until Lite was restarted.
///
/// The cause: 40615 (firewall) and 40613 (database temporarily unavailable) were classified as
/// "this login cannot read master" — a verdict that was cached per server and never expired. One
/// unlucky moment during an outage permanently demoted a healthy server to single-database
/// collection, or, when the connection had no explicit database, to collecting nothing at all.
///
/// The fixes pinned here: reachability errors are not permission errors; the verdict expires; and a
/// server coming back from an outage discards it immediately.
///
/// Also #1631, the over-correction. #1506 removed 40615 from the rights list (right) AND from the
/// single-database fallback path (wrong): a client can be firewalled out at the logical server while
/// a DATABASE-level rule keeps its user database fully reachable, which is the #857 case and the
/// configuration Microsoft recommends. "Not a rights denial" and "no fallback" are separate
/// questions, and these tests now pin both independently.
///
/// Only in-memory state is touched, so the service is constructed with null dependencies — the same
/// approach as <see cref="XeSessionHealthTests"/>.
/// </summary>
public class AzureMasterFallbackTests
{
    private const int ServerId = 7;

    private static RemoteCollectorService CreateService() =>
        new(duckDb: null!, serverManager: null!, scheduleManager: null!);

    private static ServerConnection Server(string name = "azure-logical-server") =>
        new() { ServerName = name, DisplayName = name };

    /// <summary>
    /// The #1506 half. 40615 is "client with IP address ... is not allowed to access the server", a
    /// firewall rejection at the logical server, and 40613 is "not currently available, please retry
    /// later" — neither is a statement about this login's RIGHTS, so neither may form a rights verdict.
    ///
    /// Note this is now only half the story: 40615 not being a rights denial does NOT mean the
    /// single-database fallback is wrong for it — see the #1631 pins below, which is the distinction
    /// whose absence caused that regression.
    /// </summary>
    [Theory]
    [InlineData(40615)] // Cannot open server — client IP not allowed (firewall)
    [InlineData(40613)] // Database not currently available — retry later
    public void Reachability_Errors_Are_Not_Master_Access_Denied(int errorNumber)
    {
        Assert.False(SqlErrorClassification.IsMasterAccessDenied(errorNumber));
    }

    /// <summary>
    /// #857 must keep working: a contained user that exists only in a user database really does get
    /// these when it opens master, and single-database fallback is the right answer for them.
    /// </summary>
    [Theory]
    [InlineData(229)]   // Permission denied on object
    [InlineData(230)]   // Permission denied on column
    [InlineData(916)]   // Principal cannot access the database under the current security context
    [InlineData(4060)]  // Cannot open database requested by the login
    [InlineData(18456)] // Login failed for user
    public void Permission_Errors_Are_Still_Master_Access_Denied(int errorNumber)
    {
        Assert.True(SqlErrorClassification.IsMasterAccessDenied(errorNumber));
    }

    /// <summary>
    /// #1631. Azure SQL DB evaluates DATABASE-level IP firewall rules BEFORE server-level ones, and a
    /// client matching a database-level rule is granted a connection to that database even with no
    /// server-level rule permitting it — while master, where server-level rules live, still needs one.
    /// So "40615 opening master, user database perfectly reachable" is a real, Microsoft-recommended
    /// configuration, and the single-database fallback is exactly right for it.
    ///
    /// #1506 correctly stopped reading 40615 as a rights denial but also dropped it from the fallback
    /// path, on the mistaken premise that the same rule blocks the user database too. Every
    /// database-scoped collector then failed outright against a reachable database.
    /// </summary>
    [Fact]
    public void Firewall_Rejection_At_The_Server_Still_Falls_Back_To_The_Database()
    {
        Assert.True(RemoteCollectorService.ShouldFallBackToSingleDatabaseError(40615));
    }

    /// <summary>
    /// Every rights denial still implies the fallback — the fallback set is a superset, never a
    /// replacement, of the #857 list.
    /// </summary>
    [Theory]
    [InlineData(229)]
    [InlineData(230)]
    [InlineData(916)]
    [InlineData(4060)]
    [InlineData(18456)]
    public void Permission_Errors_Also_Fall_Back(int errorNumber)
    {
        Assert.True(RemoteCollectorService.ShouldFallBackToSingleDatabaseError(errorNumber));
    }

    /// <summary>
    /// 40613 stays out of the fallback path: it is transient, so retrying — not degrading to
    /// single-database collection — is the correct response.
    /// </summary>
    [Fact]
    public void Transient_Unavailability_Does_Not_Trigger_The_Fallback()
    {
        Assert.False(RemoteCollectorService.ShouldFallBackToSingleDatabaseError(40613));
    }

    /// <summary>
    /// The invariant that would have caught #1506 at authoring time: no error number may be both
    /// "retry, this is temporary" and "stop, degrade to single-database". 40613 was on both lists.
    /// Asserted against the FALLBACK set, which is the broader of the two give-up sets.
    /// </summary>
    [Fact]
    public void No_Error_Is_Both_Transient_And_Permanently_Fatal()
    {
        var contradictory = RetryHelper.TransientErrorNumbers
            .Where(RemoteCollectorService.ShouldFallBackToSingleDatabaseError)
            .ToList();

        Assert.True(
            contradictory.Count == 0,
            $"Error(s) {string.Join(", ", contradictory)} are treated as retryable-transient AND as a " +
            "reason to degrade to single-database collection. Pick one — see #1506.");
    }

    /// <summary>
    /// The silent half of the bug. On Azure SQL DB with no explicit database, InitialCatalog resolves
    /// to master, and a single-database fallback list of "master" is empty — so every database-scoped
    /// collector iterated nothing, wrote zero rows, and reported SUCCESS. The status bar read
    /// "Collection: Running / Collectors: N OK" while absolutely nothing was being collected.
    /// </summary>
    [Theory]
    [InlineData("master")] // the InitialCatalog default when no database is configured
    [InlineData("")]
    [InlineData(null)]
    public void No_Database_To_Fall_Back_To_Throws_Rather_Than_Collecting_Nothing(string? targetDb)
    {
        var ex = Assert.Throws<InvalidOperationException>(
            () => RemoteCollectorService.FallbackDatabaseList(Server(), targetDb, reason: "master DB inaccessible"));

        /* The message has to tell them what to actually do about it. #5498: the firewall comes first (40615 means
           the client's IP address was not allowed, which a server-level block causes too); setting a Database is
           only the master-only case's fix. */
        var firewall = ex.Message.IndexOf("firewall did not allow this client's IP address", StringComparison.Ordinal);
        var rule = ex.Message.IndexOf("add a firewall rule", StringComparison.Ordinal);
        var setDatabase = ex.Message.IndexOf("set a Database for", StringComparison.Ordinal);

        Assert.True(firewall > 0, ex.Message);
        Assert.True(rule > firewall, ex.Message);
        Assert.Contains("either a server-level rule or a database-level rule in master", ex.Message, StringComparison.Ordinal);
        Assert.True(setDatabase > rule, ex.Message);
        Assert.Contains("can open a user database but not master", ex.Message, StringComparison.Ordinal);
    }

    private const string FirewallMessage =
        "Cannot open server 'example' requested by the login. Client with IP address '192.0.2.1' is not allowed to " +
        "access the server. To enable access, use the Azure Management Portal or run sp_set_firewall_rule on the " +
        "master database to create a firewall rule for this IP address or address range.";

    /// <summary>
    /// #5647: error 40615 is a firewall rejection, which a missing Database setting never causes. The message names
    /// the firewall and the client IP the server reported, then offers setting the Database for a client that a
    /// database-level rule on a user database allows (Azure checks that rule only for that database).
    /// </summary>
    [Theory]
    [InlineData("master")]
    [InlineData("")]
    [InlineData(null)]
    public void Error_40615_Names_The_Firewall_And_Client_Ip_Then_Offers_Setting_The_Database(string? targetDb)
    {
        var ex = Assert.Throws<InvalidOperationException>(
            () => RemoteCollectorService.FallbackDatabaseList(
                Server(), targetDb, reason: "master DB inaccessible (SQL error 40615)",
                errorNumber: 40615, serverMessage: FirewallMessage));

        Assert.StartsWith("master DB inaccessible (SQL error 40615), and this connection has no target database", ex.Message, StringComparison.Ordinal);
        Assert.Contains("The Azure firewall did not allow this client's IP address (192.0.2.1)", ex.Message, StringComparison.Ordinal);
        Assert.Contains("dd a firewall rule", ex.Message, StringComparison.Ordinal);
        Assert.Contains("either a server-level rule or a database-level rule in master", ex.Message, StringComparison.Ordinal);
        Assert.EndsWith(
            "If a database-level rule in a user database already allows this IP, set the Database for 'azure-logical-server' to that database instead.",
            ex.Message, StringComparison.Ordinal);
        Assert.DoesNotContain("can open a user database", ex.Message, StringComparison.Ordinal);
    }

    [Fact]
    public void Error_40615_Without_An_Ip_In_The_Server_Text_Still_Offers_Setting_The_Database()
    {
        var ex = Assert.Throws<InvalidOperationException>(
            () => RemoteCollectorService.FallbackDatabaseList(
                Server(), "master", reason: "master DB inaccessible (SQL error 40615)",
                errorNumber: 40615, serverMessage: "Ein anderer Text ohne Adresse."));

        Assert.Contains("did not allow this client's IP address. Add a firewall rule", ex.Message, StringComparison.Ordinal);
        Assert.EndsWith(
            "If a database-level rule in a user database already allows this IP, set the Database for 'azure-logical-server' to that database instead.",
            ex.Message, StringComparison.Ordinal);
    }

    /// <summary>
    /// A known error number that is not 40615 (4060, 18456) has ruled the firewall out, so the message is the set-a-Database
    /// advice alone. The decision is the error number, not the words: a reason that merely says 40615 gets neither the
    /// firewall-only message nor firewall advice when the number the caller read differs.
    /// </summary>
    [Theory]
    [InlineData(4060)]
    [InlineData(18456)]
    public void A_Known_Non_Firewall_Error_Number_Has_Set_Database_Advice_And_No_Firewall_Text(int errorNumber)
    {
        var ex = Assert.Throws<InvalidOperationException>(
            () => RemoteCollectorService.FallbackDatabaseList(
                Server(), "master", reason: "master DB inaccessible (SQL error 40615)",
                errorNumber: errorNumber, serverMessage: FirewallMessage));

        Assert.DoesNotContain("firewall", ex.Message, StringComparison.OrdinalIgnoreCase);
        Assert.DoesNotContain("40615, the", ex.Message, StringComparison.Ordinal);
        Assert.DoesNotContain("192.0.2.1", ex.Message, StringComparison.Ordinal);
        Assert.EndsWith(
            "If this login can open a user database but not master, set a Database for 'azure-logical-server' " +
            "so database-scoped collectors have something to read.",
            ex.Message, StringComparison.Ordinal);
    }

    /// <summary>With no error number the caller cannot rule the firewall out, so the combined text stays.</summary>
    [Fact]
    public void No_Error_Number_Keeps_The_Firewall_Then_Set_Database_Message()
    {
        var ex = Assert.Throws<InvalidOperationException>(
            () => RemoteCollectorService.FallbackDatabaseList(
                Server(), "master", reason: "master DB inaccessible (SQL error 40615)",
                errorNumber: null, serverMessage: FirewallMessage));

        Assert.Contains(
            "If the error is 40615, the Azure firewall did not allow this client's IP address: " +
            "add a firewall rule for it, either a server-level rule or a database-level rule in master. " +
            "If this login can open a user database but not master, set a Database for 'azure-logical-server' " +
            "so database-scoped collectors have something to read.",
            ex.Message, StringComparison.Ordinal);
        Assert.DoesNotContain("192.0.2.1", ex.Message, StringComparison.Ordinal);
    }

    /// <summary>The client IP is the only thing read out of the server's text. Documentation addresses only.</summary>
    [Theory]
    [InlineData("Client with IP address '192.0.2.1' is not allowed to access the server.", "192.0.2.1")]
    [InlineData("Client with IP address '2001:db8::1' is not allowed to access the server.", "2001:db8::1")]
    public void ExtractClientIp_Reads_An_Ipv4_Or_Ipv6_Address(string message, string expected)
    {
        Assert.Equal(expected, AzureMasterFallbackMessage.ExtractClientIp(message));
    }

    [Theory]
    [InlineData("Client with IP address '<b>x</b>' is not allowed to access the server.")]
    [InlineData("Client with IP address '{0}' is not allowed to access the server.")]
    [InlineData("Ein anderer Text ohne Adresse.")]
    [InlineData("")]
    [InlineData(null)]
    public void ExtractClientIp_Gives_Null_For_Markup_No_Address_Or_No_Text(string? message)
    {
        Assert.Null(AzureMasterFallbackMessage.ExtractClientIp(message));
    }

    /// <summary>
    /// RunCollectorAsync catches InvalidOperationException TWICE: once filtered on
    /// `ex.Message.Contains("MFA authentication cancelled")` — which records the benign status
    /// SKIPPED — and once in the general handler, which records ERROR. If the new throw's wording
    /// ever drifted into matching that filter, the failure would be logged as a routine skip and the
    /// silence this fix exists to break would come straight back.
    /// </summary>
    [Fact]
    public void Throw_Is_Not_Mistaken_For_An_Mfa_Cancellation()
    {
        var ex = Assert.Throws<InvalidOperationException>(
            () => RemoteCollectorService.FallbackDatabaseList(Server(), "master", reason: "master DB inaccessible"));

        Assert.DoesNotContain("MFA authentication cancelled", ex.Message);
    }

    [Fact]
    public void A_Configured_Database_Is_Used_As_The_Fallback()
    {
        var databases = RemoteCollectorService.FallbackDatabaseList(
            Server(), "AdventureWorks", reason: "master DB inaccessible");

        /* The #857 path: master is unreadable, but the connection names a database, so collect there. */
        Assert.Equal(new[] { "AdventureWorks" }, databases);
    }

    [Fact]
    public void Fresh_Verdict_Suppresses_Re_Probing_Master()
    {
        var service = CreateService();

        service.MarkMasterInaccessible(ServerId);

        /* #857: a login that genuinely cannot read master shouldn't retry it every cycle. */
        Assert.True(service.IsMasterProbeThrottled(ServerId));
    }

    [Fact]
    public void Verdict_Expires_So_A_Server_Recovers_Without_A_Restart()
    {
        var service = CreateService();

        service.MarkMasterInaccessible(ServerId, DateTime.UtcNow.AddMinutes(-16));

        /* Past the 15-minute recheck window: probe master again rather than trusting a stale verdict.
           This is the backstop for an outage that never produced a clean offline->online transition. */
        Assert.False(service.IsMasterProbeThrottled(ServerId));
    }

    [Fact]
    public void Server_Returning_From_An_Outage_Discards_The_Verdict_Immediately()
    {
        var service = CreateService();
        var server = Server();
        var serverId = RemoteCollectorService.GetServerId(server);

        service.MarkMasterInaccessible(serverId);
        service.NoteServerOffline(server);
        service.NoteServerOnline(server);

        /* Firewall rule restored, server answers again, verdict dropped — so database-scoped collection
           resumes on the next cycle instead of waiting for an app restart.

           This pins the offline->online RULE, not the wiring: RunDueCollectorsAsync is what calls these
           two on the real edge, and it needs a live ServerManager + ScheduleManager to drive. */
        Assert.False(service.IsMasterProbeThrottled(serverId));
    }

    [Fact]
    public void Verdict_Survives_A_Server_That_Was_Never_Offline()
    {
        var service = CreateService();
        var server = Server();
        var serverId = RemoteCollectorService.GetServerId(server);

        service.MarkMasterInaccessible(serverId);
        service.NoteServerOnline(server);

        /* The #857 steady state: reachable server, login simply lacks master rights. There was no
           outage to invalidate the verdict, so it stands and master is not re-probed every minute. */
        Assert.True(service.IsMasterProbeThrottled(serverId));
    }

    [Fact]
    public void Offline_Tracking_Is_Per_Server()
    {
        var service = CreateService();
        var recovered = Server("recovered-server");
        var stillDenied = Server("still-denied-server");

        var recoveredId = RemoteCollectorService.GetServerId(recovered);
        var stillDeniedId = RemoteCollectorService.GetServerId(stillDenied);

        service.MarkMasterInaccessible(recoveredId);
        service.MarkMasterInaccessible(stillDeniedId);

        service.NoteServerOffline(recovered);
        service.NoteServerOnline(recovered);

        Assert.False(service.IsMasterProbeThrottled(recoveredId));
        Assert.True(service.IsMasterProbeThrottled(stillDeniedId));
    }
}
