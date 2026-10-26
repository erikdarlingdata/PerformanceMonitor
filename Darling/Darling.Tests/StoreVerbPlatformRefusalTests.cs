/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using System.IO;
using System.Linq;
using System.Net;
using System.Net.Sockets;
using System.Text.Json;
using System.Threading;
using System.Threading.Tasks;
using PerformanceMonitor.Darling.Service;
using PerformanceMonitor.Darling.Storage;
using Xunit;

namespace Darling.Tests;

/// <summary>
/// #6071: <c>--backfill-rollups</c>, <c>--collapse-legacy-slices</c> and <c>--recompress-plan-dim</c> refused on
/// every non-Windows host with "requires Windows (DPAPI store credential)", although only a MANAGED store's
/// credential is DPAPI. A Linux install with its own PostgreSQL store had no way to clear a retention hold. The
/// decision is one pure function the three verbs call, so it pins here with the platform as an argument
/// (<see cref="OperatingSystem.IsWindows"/> cannot be faked), and the alert and log text name the command form
/// that runs on the service's host.
/// </summary>
public sealed class StoreVerbPlatformRefusalTests
{
    private static readonly string[] Verbs = ["--backfill-rollups", "--collapse-legacy-slices", "--recompress-plan-dim"];

    [Theory]
    [InlineData(true, true, false)]    // Windows + managed: runs
    [InlineData(true, false, false)]   // Windows + bring-your-own: runs
    [InlineData(false, false, false)]  // Linux + bring-your-own: runs (the #6071 case)
    [InlineData(false, true, true)]    // Linux + managed: refuses, because only Windows reads the DPAPI credential
    public void TheDecision_RefusesOnlyAManagedStoreOffWindows(bool isWindows, bool managed, bool refuses)
    {
        foreach (var verb in Verbs)
        {
            var refusal = DarlingCliCommands.ManagedStoreCredentialRefusal(verb, isWindows, managed);
            Assert.Equal(refuses, refusal is not null);
        }
    }

    [Fact]
    public void TheRefusal_NamesTheVerb_TheReason_AndWhatToDo()
    {
        var text = DarlingCliCommands.ManagedStoreCredentialRefusal("--backfill-rollups", isWindows: false, managedStore: true)!;
        Assert.Contains("--backfill-rollups", text, StringComparison.Ordinal);
        Assert.Contains("DPAPI", text, StringComparison.Ordinal);
        Assert.Contains("Windows host", text, StringComparison.Ordinal);
        Assert.Contains("postgres.connectionString", text, StringComparison.Ordinal);
    }

    private static int ClosedPort()
    {
        var listener = new TcpListener(IPAddress.Loopback, 0);
        listener.Start();
        var port = ((IPEndPoint)listener.LocalEndpoint).Port;
        listener.Stop();
        return port;
    }

    /// <summary>The verbs themselves, on whatever host runs the test: a bring-your-own store gets past the platform
    /// check and fails on the connection (nothing listens on the port), never on a Windows message, on any host.</summary>
    [Fact]
    public async Task AByoStore_IsNeverRefusedForThePlatform_ByAnyOfTheThreeVerbs()
    {
        var root = Directory.CreateTempSubdirectory("darling-6071-");
        try
        {
            var path = Path.Combine(root.FullName, "darling.json");
            var connectionString = $"Host=127.0.0.1;Port={ClosedPort()};Username=darling;Password=x;Database=darlingtest;Timeout=2";
            File.WriteAllText(path,
                "{ \"postgres\": { \"connectionString\": " + JsonSerializer.Serialize(connectionString) + " }, "
                + "\"servers\": [ { \"name\": \"SQL2022\", \"host\": \"SQL2022\" } ] }");

            foreach (var verb in Verbs)
            {
                var output = new StringWriter();
                var error = new StringWriter();
                var ct = CancellationToken.None;
                var exit = verb switch
                {
                    "--backfill-rollups" => await DarlingCliCommands.BackfillRollupsAsync(path, dryRun: true, output, error, ct),
                    "--collapse-legacy-slices" => await DarlingCliCommands.CollapseLegacySlicesAsync(path, dryRun: true, output, error, ct),
                    _ => await DarlingCliCommands.RecompressPlanDimAsync(
                        path, dryRun: true, DarlingCliCommands.RecompressVacuumMode.Auto, output, error, ct),
                };

                Assert.Equal(1, exit);
                Assert.Contains("Could not connect to the store", error.ToString(), StringComparison.Ordinal);
                Assert.DoesNotContain("requires Windows", error.ToString(), StringComparison.Ordinal);
                Assert.DoesNotContain("DPAPI", error.ToString(), StringComparison.Ordinal);
            }
        }
        finally
        {
            root.Delete(recursive: true);
        }
    }

    [Fact]
    public void TheVerbForm_IsTheExeOnWindows_AndTheDotnetDllElsewhere()
    {
        Assert.Equal("PerformanceMonitor.Darling.Service.exe --backfill-rollups", OperatorVerbForm.Command("--backfill-rollups", isWindows: true));
        Assert.Equal("dotnet PerformanceMonitor.Darling.Service.dll --backfill-rollups", OperatorVerbForm.Command("--backfill-rollups", isWindows: false));
        Assert.Equal(OperatorVerbForm.Command("--backfill-rollups", OperatingSystem.IsWindows()), OperatorVerbForm.CommandHere("--backfill-rollups"));
    }

    [Fact]
    public void TheRetentionHeldRemedy_NamesTheCommandFormForTheHost()
    {
        var windows = DarlingSelfAlertEvaluator.RetentionHoldRemedy(isWindows: true);
        var linux = DarlingSelfAlertEvaluator.RetentionHoldRemedy(isWindows: false);

        Assert.Contains("PerformanceMonitor.Darling.Service.exe --backfill-rollups", windows, StringComparison.Ordinal);
        Assert.DoesNotContain("dotnet", windows, StringComparison.Ordinal);
        Assert.Contains("dotnet PerformanceMonitor.Darling.Service.dll --backfill-rollups", linux, StringComparison.Ordinal);
        Assert.DoesNotContain(".exe", linux, StringComparison.Ordinal);
    }

    [Fact]
    public void TheHelp_SaysHowToTypeTheVerbsOnLinux_AndThatTheStoreVerbsRunThere()
    {
        var usage = DarlingCliCommands.UsageText();
        Assert.Contains("dotnet PerformanceMonitor.Darling.Service.dll", usage, StringComparison.Ordinal);
        var backfill = usage.Split(Environment.NewLine).First(l => l.Contains("--backfill-rollups [--free-space <size>]  Materialize", StringComparison.Ordinal));
        Assert.Contains("bring-your-own", backfill, StringComparison.Ordinal);
        Assert.All(usage, ch => Assert.True(ch < 128, $"usage text must be ASCII; found U+{(int)ch:X4}"));
    }
}
