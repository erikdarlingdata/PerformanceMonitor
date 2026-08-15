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
using System.Runtime.CompilerServices;
using PerformanceMonitor.Darling.Service;
using PerformanceMonitor.Darling.Viewer;
using Xunit;

namespace Darling.Tests;

/// <summary>
/// #2158: editing a server's address keeps its identity, so its collected history stays attached to it.
///
/// <para><b>The defect.</b> The Add/Edit save re-derived <c>server_id</c> from host/database/read-only-intent on
/// every save. So an operator fixing a hostname typo produced a row under a NEW id and the old row was deleted —
/// which left the registry tidy and every <c>collect.*</c> row keyed to the old id orphaned, with nothing
/// pointing at it. The visible symptom is a server that reads as though it had never been monitored, which is
/// why it went unnoticed: nothing looks broken, the history is simply gone.</para>
///
/// <para><b>Why identity must be assigned rather than derived, argued from consequences.</b> Three issues pull
/// on this and they pull in opposite directions. #2158 says a config edit must NOT change the identity.
/// #2228 says two different configs resolving to one real database must not be two identities — which no
/// config-derived hash can decide, because the configs genuinely differ. #2218 says two instances on one host
/// need distinguishing, which wants MORE fields in the derivation and therefore makes #2158 strictly worse.
/// Only one shape satisfies all three: the identity is allocated once and never recomputed, the derived address
/// is a lookup key rather than the identity, and what the target actually IS comes from the connection. This
/// file pins the first of those three.</para>
/// </summary>
public sealed class ServerIdentitySurvivesAnEditTests
{
    /// <summary>
    /// THE FIX, pinned at the source: the row builder prefers the row's existing identity and only derives one
    /// when there is none (an Add).
    ///
    /// <para>Pinned textually because the alternative is a WPF dialog — reproducing it behaviourally means
    /// standing up <c>AddServerDialog</c> with a live store and a real edit, which the suite cannot do. A
    /// re-derivation here is invisible in every other test and silently discards history, so it is worth
    /// holding at the only level available.</para>
    /// </summary>
    [Fact]
    public void TheEditSaveKeepsTheOriginalIdentity_AndOnlyAddDerivesOne()
    {
        var source = ReadDialogSource();

        Assert.Contains(
            "ServerId = _originalServerId ?? ViewerDataService.ComputeServerId(host, database, readOnlyIntent),",
            source, StringComparison.Ordinal);

        /* And the delete-the-old-identity step is GONE: with the id preserved there is no second row to clean
           up, and leaving the delete in would drop the row that was just written. */
        Assert.DoesNotContain("await _dataService.DeleteMonitoredServerAsync(original);", source, StringComparison.Ordinal);
    }

    /// <summary>
    /// The collision guard survived the change and now asks about the ADDRESS.
    ///
    /// <para>This is the half that would have been easy to lose. The old guard compared a derived id against
    /// the registry, which only works while every row's id equals the hash of its own address — precisely the
    /// invariant this change gives up. Left as it was, an edit could point a second registration at an address
    /// another server already monitors and the guard would never fire, because the derived id it looked up
    /// belongs to nobody. That is #2228's shape reached from the registry side.</para>
    /// </summary>
    [Fact]
    public void TheCollisionGuardChecksTheAddress_NotADerivedId()
    {
        var source = ReadDialogSource();

        Assert.Contains("GetMonitoredServerByAddressAsync(row.Host, row.Database, row.ReadOnlyIntent)", source, StringComparison.Ordinal);
        /* Compared by id afterwards, which is what excludes "collided with myself" on an edit that leaves the
           address alone (a rename, or new credentials). */
        Assert.Contains("occupant.ServerId != row.ServerId", source, StringComparison.Ordinal);
        Assert.DoesNotContain("await _dataService.GetMonitoredServerAsync(row.ServerId) is not null", source, StringComparison.Ordinal);
    }

    /// <summary>
    /// The address lookup matches a NULL database with <c>IS NOT DISTINCT FROM</c>. A plain <c>=</c> never
    /// matches NULL in SQL, so every server-scoped registration — the common case — would read as "address
    /// free" and the guard would pass for all of them.
    /// </summary>
    [Fact]
    public void TheAddressLookupMatchesANullDatabase()
    {
        var sql = ViewerDataService.MonitoredServerByAddressSql;

        Assert.Contains("WHERE host = $1", sql, StringComparison.Ordinal);
        Assert.Contains("database IS NOT DISTINCT FROM $2", sql, StringComparison.Ordinal);
        Assert.Contains("read_only_intent = $3", sql, StringComparison.Ordinal);
        /* Secret-free, so a read-only seat gets an answer rather than 42501 on the column it is denied. */
        Assert.DoesNotContain("encrypted_password", sql, StringComparison.Ordinal);
    }

    /// <summary>
    /// The file-vs-store reconcile no longer calls an edited server "not monitored".
    ///
    /// <para>That log line gives ADVICE — "add them with the Viewer's Add Server dialog" — so being wrong is
    /// worse than being silent: after an address edit the file's derived id matches nothing, and an id-only
    /// comparison would tell the operator to re-add the one server they had just fixed, on every start.</para>
    /// </summary>
    [Fact]
    public void AnEditedServerIsNotReportedAsFileOnly()
    {
        var file = new[] { Server("prod-01", host: "prod-01.old.example.com") };

        /* The store row kept its identity through the edit, so its id is NOT the hash of the file's address. */
        var storeIds = new HashSet<int> { 999_111 };
        var storeNames = new HashSet<string>(StringComparer.OrdinalIgnoreCase) { "prod-01" };

        Assert.Empty(StoreConfigProvider.ServersOnlyInFile(file, storeIds, storeNames));

        /* Without the name arm this is exactly the wrong answer the old comparison gave. */
        Assert.Single(StoreConfigProvider.ServersOnlyInFile(file, storeIds, new HashSet<string>()));
    }

    /// <summary>
    /// A genuinely removed server is STILL reported. The Viewer's Remove hard-deletes the row, so it is absent
    /// under both keys — the name arm must not turn this log line off altogether, which is the obvious way to
    /// over-apply the fix.
    /// </summary>
    [Fact]
    public void ADeletedServerIsStillReportedAsFileOnly()
    {
        var file = new[] { Server("gone-01", host: "gone-01.example.com") };

        var missing = StoreConfigProvider.ServersOnlyInFile(
            file, new HashSet<int> { 999_111 }, new HashSet<string>(StringComparer.OrdinalIgnoreCase) { "someone-else" });

        Assert.Equal(new[] { "gone-01" }, missing);
    }

    /// <summary>
    /// Matching is either-or, not name-only. Nothing enforces display-name uniqueness, so a name-only
    /// comparison would hide a genuinely unmonitored server behind a same-named sibling; and an id match alone
    /// must still be enough, which is the path every unedited server takes.
    /// </summary>
    [Fact]
    public void AnIdMatchAloneIsEnough_AndNameMatchingDoesNotReplaceIt()
    {
        var server = Server("prod-02", host: "prod-02.example.com");

        /* Id present, name absent — the ordinary case for a server nobody has edited. */
        Assert.Empty(StoreConfigProvider.ServersOnlyInFile(
            new[] { server }, new HashSet<int> { server.ServerId }, new HashSet<string>()));

        /* Neither present. */
        Assert.Single(StoreConfigProvider.ServersOnlyInFile(
            new[] { server }, new HashSet<int>(), new HashSet<string>()));
    }

    /// <summary>
    /// Omitting the names keeps the old id-only behaviour, so a caller that has not been updated cannot start
    /// silently suppressing the warning.
    /// </summary>
    [Fact]
    public void WithNoNamesSuppliedTheComparisonIsIdOnly()
    {
        var server = Server("prod-03", host: "prod-03.example.com");

        Assert.Single(StoreConfigProvider.ServersOnlyInFile(new[] { server }, new HashSet<int>()));
        Assert.Empty(StoreConfigProvider.ServersOnlyInFile(new[] { server }, new HashSet<int> { server.ServerId }));
    }

    private static MonitoredServer Server(string name, string host) =>
        new() { Name = name, Host = host, Auth = "integrated" };

    private static string ReadDialogSource([CallerFilePath] string thisFile = "")
    {
        var dir = Path.GetDirectoryName(thisFile)!;
        var relative = Path.Combine("Darling", "PerformanceMonitor.Darling.Viewer", "AddServerDialog.xaml.cs");
        while (dir is not null && !File.Exists(Path.Combine(dir, relative)))
        {
            dir = Path.GetDirectoryName(dir);
        }

        Assert.NotNull(dir);
        return File.ReadAllText(Path.Combine(dir!, relative));
    }
}
