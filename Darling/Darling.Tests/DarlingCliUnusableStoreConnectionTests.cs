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
using System.Text.RegularExpressions;
using System.Threading;
using System.Threading.Tasks;
using PerformanceMonitor.Darling.Service;
using Xunit;

namespace Darling.Tests;

/// <summary>
/// Every CLI verb that opens the store says so and exits <c>1</c> when its store setting cannot be used, instead of
/// ending in an unhandled exception (#4744). A <c>postgres.connectionString</c> that Npgsql cannot parse
/// (<c>sslmode=NotARealSslMode</c>), and a managed credential file that cannot be read or unprotected, both throw
/// while the verb builds its connection. <c>--enable-collector</c>, <c>--disable-collector</c> and
/// <c>--check-settings</c> caught that first; <c>--validate-config</c>, the four endpoint toggles,
/// <c>--collapse-legacy-slices</c>, <c>--recompress-plan-dim</c>, <c>--add-server</c> and
/// <c>--backfill-rollups</c> did not. None of this needs a live store: the failure comes before any connection is
/// opened.
/// </summary>
public sealed class DarlingCliUnusableStoreConnectionTests
{
    /// <summary>A string Npgsql cannot parse: it does not know this <c>sslmode</c>.</summary>
    private const string MalformedConnectionString =
        "Host=127.0.0.1;Port=5432;Username=darling;Password=x;Database=darlingtest;sslmode=NotARealSslMode";

    private const string ServerBlock = "\"servers\": [ { \"name\": \"SQL2022\", \"host\": \"SQL2022\" } ]";

    private const string DarlingCliCommandsFile = "DarlingCliCommands.cs";

    private static int GetClosedPort()
    {
        var listener = new TcpListener(IPAddress.Loopback, 0);
        listener.Start();
        var port = ((IPEndPoint)listener.LocalEndpoint).Port;
        listener.Stop();
        return port;
    }

    private static string WriteByoConfig(DirectoryInfo root, string connectionString)
    {
        var path = Path.Combine(root.FullName, "darling.json");
        File.WriteAllText(path,
            "{ \"postgres\": { \"connectionString\": " + JsonSerializer.Serialize(connectionString) + " }, " + ServerBlock + " }");
        return path;
    }

    /// <summary>A managed config whose credential file is there but holds <paramref name="credentialFileContent"/>,
    /// which DPAPI cannot unprotect.</summary>
    private static string WriteManagedConfigWithCredential(DirectoryInfo root, string credentialFileContent)
    {
        var dataDirectory = Path.Combine(root.FullName, "pg");
        File.WriteAllText(DarlingManagedPostgres.CredentialPathFor(dataDirectory), credentialFileContent);
        var path = Path.Combine(root.FullName, "darling.json");
        File.WriteAllText(path,
            "{ \"postgres\": { \"managed\": true, \"port\": " + GetClosedPort()
            + ", \"dataDirectory\": " + JsonSerializer.Serialize(dataDirectory) + " }, " + ServerBlock + " }");
        return path;
    }

    /// <summary>Runs the verb the way <c>Program.cs</c> hands it its arguments. An exception the verb lets escape
    /// is not caught here: it fails the test, which is the crash this class pins.</summary>
    private static async Task<(int Exit, string Output, string Error)> RunVerbAsync(string verb, string configPath)
    {
        var output = new StringWriter();
        var error = new StringWriter();
        var ct = CancellationToken.None;

        var exit = verb switch
        {
            "--validate-config" => await DarlingCliCommands.ValidateConfigAsync(configPath, output, error, ct),
            "--check-settings" => await DarlingCliCommands.CheckSettingsAsync(configPath, json: false, output, error, ct),
            "--enable-mcp" => await DarlingCliCommands.EnableMcpAsync(configPath, output, error, ct),
            "--disable-mcp" => await DarlingCliCommands.DisableMcpAsync(configPath, output, error, ct),
            "--enable-web" => await DarlingCliCommands.EnableWebAsync(configPath, output, error, ct),
            "--disable-web" => await DarlingCliCommands.DisableWebAsync(configPath, output, error, ct),
            "--collapse-legacy-slices" => await DarlingCliCommands.CollapseLegacySlicesAsync(configPath, dryRun: true, output, error, ct),
            "--recompress-plan-dim" => await DarlingCliCommands.RecompressPlanDimAsync(
                configPath, dryRun: true, DarlingCliCommands.RecompressVacuumMode.Auto, output, error, ct),
            "--add-server" => await DarlingCliCommands.AddServerAsync(
                configPath, new StringReader("[ { \"name\": \"sql01\", \"host\": \"sql01\" } ]"), output, error, ct),
            "--backfill-rollups" => await DarlingCliCommands.BackfillRollupsAsync(configPath, dryRun: true, output, error, ct),
            "--enable-collector" => await DarlingCliCommands.ToggleCollectorAsync(
                enable: true, ["wait_stats", "--config", configPath], output, error, ct),
            "--disable-collector" => await DarlingCliCommands.ToggleCollectorAsync(
                enable: false, ["wait_stats", "--config", configPath], output, error, ct),
            _ => throw new ArgumentOutOfRangeException(nameof(verb), verb, "not a verb this class knows"),
        };

        return (exit, output.ToString(), error.ToString());
    }

    /// <summary>A bring-your-own string Npgsql cannot parse, for every verb that can reach it. The four endpoint
    /// toggles are not here: a bring-your-own store stops them at the managed-mode guard, before any connection
    /// string is read, so only the managed credential (below) can fail for them.</summary>
    [Theory]
    [InlineData("--validate-config")]
    [InlineData("--check-settings")]
    [InlineData("--collapse-legacy-slices")]
    [InlineData("--recompress-plan-dim")]
    [InlineData("--add-server")]
    [InlineData("--backfill-rollups")]
    [InlineData("--enable-collector")]
    [InlineData("--disable-collector")]
    public async Task ConnectionStringThatDoesNotParse_ExitsWithOne_AndNamesTheSetting(string verb)
    {
        var root = Directory.CreateTempSubdirectory("darling-cli-4744-badconn-");
        try
        {
            var path = WriteByoConfig(root, MalformedConnectionString);

            var (exit, _, error) = await RunVerbAsync(verb, path);

            Assert.Equal(1, exit);
            Assert.Contains("postgres.connectionString could not be used:", error, StringComparison.Ordinal);
        }
        finally
        {
            root.Delete(recursive: true);
        }
    }

    /// <summary>A managed store whose credential file is there but is not a credential (it is not even base64), for
    /// every verb that reads it. Only Windows reads the credential, so only there is there anything to fail.</summary>
    [Theory]
    [InlineData("--validate-config")]
    [InlineData("--check-settings")]
    [InlineData("--enable-mcp")]
    [InlineData("--disable-mcp")]
    [InlineData("--enable-web")]
    [InlineData("--disable-web")]
    [InlineData("--collapse-legacy-slices")]
    [InlineData("--recompress-plan-dim")]
    [InlineData("--add-server")]
    [InlineData("--backfill-rollups")]
    [InlineData("--enable-collector")]
    [InlineData("--disable-collector")]
    public async Task ManagedCredentialThatCannotBeRead_ExitsWithOne_InsteadOfThrowing(string verb)
    {
        Assert.SkipUnless(OperatingSystem.IsWindows(), "The managed store credential is DPAPI, so only Windows reads it.");
        var root = Directory.CreateTempSubdirectory("darling-cli-4744-badcred-");
        try
        {
            var path = WriteManagedConfigWithCredential(root, "this is not a protected credential");

            var (exit, _, error) = await RunVerbAsync(verb, path);

            Assert.Equal(1, exit);

            /* #4744: it is the stored credential that could not be read, not the connection string, and a managed
               store has no connection string setting to blame. */
            Assert.Contains("The stored store credential could not be read:", error, StringComparison.Ordinal);
            Assert.DoesNotContain("postgres.connectionString", error, StringComparison.Ordinal);
        }
        finally
        {
            root.Delete(recursive: true);
        }
    }

    /// <summary>The other way a managed credential fails: the file is well-formed base64 that this machine's DPAPI
    /// cannot unprotect, which is what a credential written on a different machine looks like from here. The verb
    /// says it is DPAPI on this machine and names the store's own credential file, and it does not use the raw
    /// <c>CryptographicException</c> message an operator reads as the store rejecting a login. It also does not use the
    /// explanation written for a monitored server's saved password: that one blames a Viewer on another PC and tells
    /// the operator to re-add the server, run <c>--add-server</c> or <c>--encrypt-password</c>, or use an
    /// <c>env:</c>/<c>file:</c> reference, and none of those can bring back the store owner's generated password
    /// (#4744).</summary>
    [Theory]
    [InlineData("--validate-config")]
    [InlineData("--check-settings")]
    [InlineData("--enable-mcp")]
    [InlineData("--disable-mcp")]
    [InlineData("--enable-web")]
    [InlineData("--disable-web")]
    [InlineData("--collapse-legacy-slices")]
    [InlineData("--recompress-plan-dim")]
    [InlineData("--add-server")]
    [InlineData("--backfill-rollups")]
    [InlineData("--enable-collector")]
    [InlineData("--disable-collector")]
    public async Task ManagedCredentialThatDpapiCannotUnprotect_IsDescribedAsDpapi_NotAsARawCryptoError(string verb)
    {
        Assert.SkipUnless(OperatingSystem.IsWindows(), "The managed store credential is DPAPI, so only Windows reads it.");
        var root = Directory.CreateTempSubdirectory("darling-cli-4744-dpapi-");
        try
        {
            var path = WriteManagedConfigWithCredential(root, Convert.ToBase64String(new byte[] { 1, 2, 3, 4, 5, 6, 7, 8, 9, 10 }));

            var (exit, _, error) = await RunVerbAsync(verb, path);

            Assert.Equal(1, exit);
            Assert.Contains("The stored store credential could not be read:", error, StringComparison.Ordinal);
            Assert.Contains("DPAPI-decrypt", error, StringComparison.Ordinal);
            Assert.DoesNotContain("Key not valid for use in specified state", error, StringComparison.Ordinal);
            Assert.DoesNotContain("postgres.connectionString", error, StringComparison.Ordinal);

            /* #4744: the store's credential file is named, and none of the monitored-server remedies is offered. */
            Assert.Contains("pg-credential.dpapi", error, StringComparison.Ordinal);
            Assert.DoesNotContain("--add-server", error, StringComparison.Ordinal);
            Assert.DoesNotContain("--encrypt-password", error, StringComparison.Ordinal);
            Assert.DoesNotContain("SQL Server", error, StringComparison.Ordinal);
        }
        finally
        {
            root.Delete(recursive: true);
        }
    }

    /// <summary>The verbs' code is one shared method, and this is the wiring that keeps it that way: the message that
    /// blames the setting is written in one place, every store-opening verb goes through that place, and nothing else
    /// in the file reads the stored credential bare.</summary>
    [Fact]
    public void EveryVerbBuildsItsStoreConnectionThroughTheOneSharedMethod()
    {
        var raw = RepoFile.ReadRepoFile("Darling", "PerformanceMonitor.Darling.Service", DarlingCliCommandsFile);
        var code = CSharpSourceWalker.StripCommentsAndStrings(raw);

        /* The message is a string literal, so it is counted in the raw text; the leading quote keeps a comment that
           quotes the sentence from counting. */
        Assert.Single(Regex.Matches(raw, Regex.Escape("\"postgres.connectionString could not be used")));

        /* Eight verbs open the store through it: --validate-config, --check-settings, the four endpoint toggles (one
           shared body), --collapse-legacy-slices, --recompress-plan-dim, --add-server, --backfill-rollups, and the
           two collector toggles (one shared body). That is eight call sites; a ninth verb that opens the store
           adds a ninth here, and growing this count is the point. */
        var calls = Regex.Matches(code, @"(?<!bool\s)TryBuildStoreConnectionString\(").Count;
        Assert.Equal(8, calls);

        /* The stored credential is read bare in exactly two places: inside that method, and in the firewall verb's
           best-effort read (TryReadEndpointTogglesAsync), which turns a failure into a reason it prints rather than
           into an exit code. A third bare read is a verb that can crash on a credential it cannot unprotect. */
        Assert.Equal(2, Regex.Matches(code, @"TryBuildConnectionStringFromStoredCredential\(").Count);
    }
}
