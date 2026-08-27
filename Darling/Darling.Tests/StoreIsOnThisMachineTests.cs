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
using PerformanceMonitor.Darling.Viewer;
using Xunit;

namespace Darling.Tests;

/// <summary>
/// #2279: the signal behind the Add Server hint about a machine-bound credential.
///
/// <para><b>What it is for.</b> A SQL-auth password is stored as a DPAPI <c>LocalMachine</c> blob, decryptable
/// only on the machine that wrote it — and the SERVICE is what has to decrypt it. So a credential saved from a
/// viewer on another PC can never be used, and the server fails to connect on every sweep afterwards. That is
/// the #2255 field report; #2273 made the resulting failure explain itself, and this warns before it happens.
/// </para>
///
/// <para><b>Why loopback is the signal.</b> The managed deploy builds its store connection on literal
/// <c>127.0.0.1</c> — <c>ViewerSettings</c> mirroring the service's own
/// <c>DarlingManagedPostgres.BuildConnectionString</c> — and the service runs where its managed store runs. So a
/// loopback store means viewer and service share a machine and a saved credential will work. It is a proxy
/// derived from the product's own architecture rather than a guess about deployment.</para>
///
/// <para><b>And why it warns rather than refuses.</b> A non-loopback store does NOT prove the viewer is remote:
/// a bring-your-own store on another host with the service local reads exactly the same. The signal is good
/// enough to decide whether to SAY something and not good enough to decide whether to BLOCK — inverting that
/// would refuse a legitimate first-run Add on the service host, which is worse than the problem.</para>
/// </summary>
public sealed class StoreIsOnThisMachineTests
{
    /// <summary>
    /// THE CASE THAT MUST STAY SILENT: the real managed connection string. This is the single-box deploy the
    /// whole DPAPI design targets and the overwhelmingly common one — a hint that fires for everyone is a hint
    /// nobody reads, so a false positive here would defeat the feature rather than merely annoy.
    /// </summary>
    [Theory]
    [InlineData("Host=127.0.0.1;Port=5641;Username=darling;Password=x;Database=darling")]
    [InlineData("Host=127.0.0.1;Port=5641;Database=darling")]
    [InlineData("Host=localhost;Port=5641;Database=darling")]
    [InlineData("Host=LOCALHOST;Database=darling")]
    [InlineData("Host=::1;Database=darling")]
    [InlineData("Host= 127.0.0.1 ;Database=darling")]
    [InlineData("Server=127.0.0.1;Database=darling")]
    public void ALoopbackStoreIsSilent(string connectionString)
    {
        Assert.True(ViewerDataService.StoreHostIsLoopback(connectionString));
    }

    /// <summary>
    /// An OMITTED host counts as local, because that is what it means — Npgsql defaults to localhost, so a
    /// string with no <c>Host</c> is a local store and warning about it would be wrong.
    /// </summary>
    [Theory]
    [InlineData("Port=5641;Database=darling")]
    [InlineData("Database=darling")]
    [InlineData(";;;")]
    public void AnOmittedHostCountsAsLocal(string connectionString)
    {
        Assert.True(ViewerDataService.StoreHostIsLoopback(connectionString));
    }

    /// <summary>Nothing at all is not evidence of a remote store.</summary>
    [Theory]
    [InlineData(null)]
    [InlineData("")]
    [InlineData("   ")]
    public void AnAbsentConnectionStringIsSilent(string? connectionString)
    {
        Assert.True(ViewerDataService.StoreHostIsLoopback(connectionString));
    }

    /// <summary>THE CASE THAT WARNS: a store on another host, which is how the #2255 report was configured.</summary>
    [Theory]
    [InlineData("Host=prod-sql-use2-monitor-01;Port=5641;Database=darling")]
    [InlineData("Host=10.0.0.10;Port=5432;Database=darling")]
    [InlineData("Host=store.internal.example.com;Database=darling")]
    [InlineData("Server=otherbox;Database=darling")]
    public void ARemoteStoreWarns(string connectionString)
    {
        Assert.False(ViewerDataService.StoreHostIsLoopback(connectionString));
    }

    /// <summary>
    /// A host that merely CONTAINS a loopback spelling is remote. Guards the obvious over-match, which would
    /// silence the hint for exactly the hosts most likely to be typed by hand.
    /// </summary>
    [Theory]
    [InlineData("Host=localhost.evil.example.com;Database=darling")]
    [InlineData("Host=127.0.0.1.example.com;Database=darling")]
    [InlineData("Host=mylocalhost;Database=darling")]
    public void AHostThatMerelyLooksLoopbackStillWarns(string connectionString)
    {
        Assert.False(ViewerDataService.StoreHostIsLoopback(connectionString));
    }

    /// <summary>
    /// An unparseable string fails toward SILENCE. This decides only whether to show a hint, so the wrong
    /// direction is a warning the operator cannot act on attached to a string they cannot fix from here.
    ///
    /// <para>These inputs genuinely throw — verified against <c>DbConnectionStringBuilder</c> rather than
    /// assumed, because a catch for an exception the framework never raises is dead code pretending to be a
    /// safeguard. Note <c>"Host=a;;;=====bad"</c> does NOT throw: it parses as <c>Host=a</c>, which is correctly
    /// treated as remote.</para>
    /// </summary>
    [Theory]
    [InlineData("=")]
    [InlineData("Host")]
    [InlineData("{bad}")]
    [InlineData("Host=a;b")]
    [InlineData("Host==a")]
    public void AnUnparseableConnectionStringIsSilent(string connectionString)
    {
        Assert.True(ViewerDataService.StoreHostIsLoopback(connectionString));
    }

    /// <summary>A string that parses to a real non-loopback host warns, even with trailing junk.</summary>
    [Fact]
    public void AParseableHostWinsOverTrailingJunk()
    {
        Assert.False(ViewerDataService.StoreHostIsLoopback("Host=a;;;=====bad"));
    }

    /// <summary>
    /// The dialog surfaces the hint only for SQL auth on a non-local store, and CLEARS its own message when the
    /// mode changes away — the same self-clearing discipline the Azure arm uses, which is what stops a stale
    /// hint sitting under an unrelated auth mode.
    ///
    /// <para>Pinned at the source: the alternative is standing up a WPF dialog with a live store.</para>
    /// </summary>
    [Fact]
    public void TheDialogShowsTheHintOnlyForSqlAuthOnANonLocalStore()
    {
        var source = ReadDialogSource();

        Assert.Contains("SqlAuthRadio.IsChecked == true && _dataService is { StoreIsOnThisMachine: false }", source, StringComparison.Ordinal);
        Assert.Contains("StatusText.Text = SqlCredentialMachineBoundHint;", source, StringComparison.Ordinal);
        /* Self-clearing, exactly like the Azure message above it. */
        Assert.Contains("else if (StatusText.Text == SqlCredentialMachineBoundHint)", source, StringComparison.Ordinal);
    }

    /// <summary>
    /// The hint has to be actionable, not just alarming: it names all three ways to produce a credential the
    /// service can use, which is what turns it from a warning into an instruction.
    /// </summary>
    [Fact]
    public void TheHintNamesEveryWayToProduceAUsableCredential()
    {
        var source = ReadDialogSource();

        Assert.Contains("viewer on the service's host", source, StringComparison.Ordinal);
        Assert.Contains("--add-server", source, StringComparison.Ordinal);
        Assert.Contains("env:/file:", source, StringComparison.Ordinal);
    }

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
