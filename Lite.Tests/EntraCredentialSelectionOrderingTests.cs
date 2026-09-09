/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor Lite.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using System.IO;
using System.Linq;
using System.Runtime.CompilerServices;
using Darling.Tests;
using Xunit;

namespace PerformanceMonitorLite.Tests;

/// <summary>
/// The ordering the credential-selection listener's NARROW WINDOW depends on, pinned because it
/// lives in a file this feature does not otherwise touch.
///
/// <para><c>Azure.Identity</c> reports the selected credential once per credential instance and
/// SqlClient caches that instance in a process-wide static, so the event fires at most once per
/// process — whichever code opens the first <c>ActiveDirectoryDefault</c> connection is the only
/// one that can ever observe it. The listener is attached at
/// <c>ServerManager.CheckConnectionAsync</c> and at the connection dialog's Test button, and NOT at
/// the other <c>SqlConnection</c> sites in Lite, which is only sound because
/// <c>CollectionBackgroundService</c> checks connections before it collects.</para>
///
/// <para><b>A reorder there breaks observability silently</b> — the app would connect identically,
/// the listener would attach identically, and the event would already have been raised and
/// discarded by the collector. There is no failure to notice, which is the definition of something
/// that needs a pin rather than a comment.</para>
/// </summary>
public class EntraCredentialSelectionOrderingTests
{
    [Fact]
    public void TheCollectionLoop_ChecksConnections_BeforeItCollects()
    {
        /* Stripped, and the strip is load-bearing rather than habit. No comment in that file names
           either identifier TODAY, so the strip changes nothing today - but measured: with the loop
           genuinely inverted AND one comment added naming CheckAllConnectionsAsync ahead of the
           collector call, this pin passes on raw text and fails on stripped text. A one-line
           ordering note of exactly that shape is the most natural thing for someone to write while
           reordering this loop, which is the case where the pin has to still work. */
        var source = CSharpSourceWalker.StripCommentsAndStrings(
            ReadRepoFile("Lite/Services/CollectionBackgroundService.cs"));

        var signature = source.IndexOf("ExecuteAsync(CancellationToken stoppingToken)", StringComparison.Ordinal);
        Assert.True(signature >= 0, "CollectionBackgroundService.ExecuteAsync is the collection loop and must exist");

        var brace = source.IndexOf('{', signature);
        Assert.True(brace > signature, "ExecuteAsync must have a body");

        var body = CSharpSourceWalker.BraceBalanced(source, brace);

        var check = body.IndexOf("CheckAllConnectionsAsync", StringComparison.Ordinal);
        var collect = body.IndexOf("RunDueCollectorsAsync", StringComparison.Ordinal);

        Assert.True(check >= 0, "ExecuteAsync must run the connection check; the credential-selection listener is attached inside it");
        Assert.True(collect >= 0, "ExecuteAsync must run the due collectors, or this ordering assertion has nothing to order");

        Assert.True(
            check < collect,
            "CollectionBackgroundService must check connections BEFORE running collectors. The "
                + "credential-selection listener (EntraCredentialSelectionLog) is attached only at the "
                + "connectivity check and the connection dialog, on the grounds that the check gets "
                + "there first — and Azure.Identity raises the selected-credential event at most once "
                + "per process, so if a collector opens the first ActiveDirectoryDefault connection "
                + "the selection is never observable again. Nothing about that failure is visible at "
                + "runtime: the app connects normally and the log is simply missing a line.");
    }

    /// <summary>
    /// And that the connectivity check is still the thing the listener is attached inside — so the
    /// ordering above is about the right method. A rename of the attach site with this pin left
    /// alone would otherwise order two things that no longer matter to each other.
    /// </summary>
    [Fact]
    public void TheConnectivityCheck_IsWhereTheListenerIsAttached()
    {
        var manager = CSharpSourceWalker.StripCommentsAndStrings(
            ReadRepoFile("Lite/Services/ServerManager.cs"));

        var sweep = manager.IndexOf("CheckAllConnectionsAsync", StringComparison.Ordinal);
        Assert.True(sweep >= 0, "ServerManager must still expose the sweep the collection loop calls");

        Assert.Contains("CheckConnectionAsync", manager, StringComparison.Ordinal);
        Assert.Contains("EntraCredentialSelectionLog.Begin", manager, StringComparison.Ordinal);
    }

    private static string ReadRepoFile(string relativePath, [CallerFilePath] string thisFile = "")
    {
        var dir = Path.GetDirectoryName(thisFile)!;
        var parts = relativePath.Split('/');
        while (dir is not null && !File.Exists(Path.Combine(new[] { dir }.Concat(parts).ToArray())))
        {
            dir = Path.GetDirectoryName(dir);
        }

        Assert.NotNull(dir);
        return File.ReadAllText(Path.Combine(new[] { dir! }.Concat(parts).ToArray()));
    }
}
