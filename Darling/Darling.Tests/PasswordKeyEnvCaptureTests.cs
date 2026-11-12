/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using System.Diagnostics;
using System.IO;
using System.Linq;
using System.Security.Cryptography;
using System.Text.Json;
using System.Threading;
using System.Threading.Tasks;
using Microsoft.Extensions.Logging.Abstractions;
using PerformanceMonitor.Darling.Analysis;
using PerformanceMonitor.Darling.Service;
using PerformanceMonitor.Darling.Service.Hosting;
using PerformanceMonitor.Darling.Service.Mcp;
using PerformanceMonitor.Darling.Storage;
using Xunit;
using static Darling.Tests.RepoFile;

namespace Darling.Tests;

/// <summary>
/// #8006 review: a <c>passwordKey: env:NAME</c> variable is read once when the service loads its configuration and then
/// removed from the service's environment (<see cref="DarlingPasswordKeySource.CaptureForService"/>), so no process the
/// service starts afterwards (the managed PostgreSQL, PowerShell) inherits the key.
///
/// <para>In <c>darling-config-env</c> because the tests set and remove process environment variables, and the capture
/// fills a process-wide holder; each uses a variable name of its own and clears it, and the holder, in a
/// <c>finally</c>.</para>
/// </summary>
[Collection("darling-config-env")]
public sealed class PasswordKeyEnvCaptureTests
{
    [Fact]
    public void ACapturedVariable_IsGoneFromTheEnvironmentAndFromChildren_AndTheKeyStillLoads()
    {
        var (pkcs8, keyId) = NewKey();
        var name = UniqueVariable();
        try
        {
            Environment.SetEnvironmentVariable(name, "  " + Convert.ToBase64String(pkcs8) + "\n");

            DarlingPasswordKeySource.CaptureForService(ConfigWith("env:" + name));

            Assert.Null(Environment.GetEnvironmentVariable(name));
            /* What a child process would be started with: ProcessStartInfo copies the current environment. */
            Assert.False(new ProcessStartInfo().Environment.ContainsKey(name));

            /* Twice: the worker's start and a later load in the same process both read the held key. */
            AssertLoaded(DarlingPasswordKeySource.LoadFromSetting("env:" + name, null, NullLogger.Instance), "env:" + name, keyId);
            AssertLoaded(DarlingPasswordKeySource.LoadFromSetting("env:" + name + " ", null, NullLogger.Instance), "env:" + name + " ", keyId);

            /* A second capture, as another host service's load makes, finds the variable gone and keeps the held key. */
            DarlingPasswordKeySource.CaptureForService(ConfigWith("env:" + name));
            AssertLoaded(DarlingPasswordKeySource.LoadFromSetting("env:" + name, null, NullLogger.Instance), "env:" + name, keyId);
        }
        finally
        {
            Environment.SetEnvironmentVariable(name, null);
            DarlingPasswordKeySource.ForgetCaptured(name);
            CryptographicOperations.ZeroMemory(pkcs8);
        }
    }

    [Fact]
    public void WithoutAnEnvReference_CaptureLeavesEveryVariableAlone()
    {
        var name = UniqueVariable();
        var keyFile = Path.Combine(Path.GetTempPath(), Guid.NewGuid().ToString("N") + ".pk");
        try
        {
            Environment.SetEnvironmentVariable(name, "unrelated");

            var configs = new[]
            {
                DarlingConfig.Parse("{\"postgres\":{\"connectionString\":\"Host=h\"}}"),
                ConfigWith(""),
                ConfigWith("   "),
                ConfigWith("file:" + keyFile),
                /* A leading space makes a literal, which LoadFromSetting refuses: it is not a reference to capture. */
                ConfigWith(" env:" + name),
                /* Another variable's reference captures that variable only. */
                ConfigWith("env:" + name + "_OTHER"),
            };

            foreach (var config in configs)
            {
                DarlingPasswordKeySource.CaptureForService(config);
                Assert.Equal("unrelated", Environment.GetEnvironmentVariable(name));
            }
        }
        finally
        {
            Environment.SetEnvironmentVariable(name, null);
            DarlingPasswordKeySource.ForgetCaptured(name);
            DarlingPasswordKeySource.ForgetCaptured(name + "_OTHER");
        }
    }

    [Fact]
    public void CapturingAnUnsetOrBlankVariable_HoldsNothing_AndTheLoadRefusesAsBefore()
    {
        foreach (var value in new[] { null, "", "   " })
        {
            var name = UniqueVariable();
            try
            {
                Environment.SetEnvironmentVariable(name, value);

                DarlingPasswordKeySource.CaptureForService(ConfigWith("env:" + name));

                var load = DarlingPasswordKeySource.LoadFromSetting("env:" + name, null, NullLogger.Instance);
                Assert.True(load.Untrusted);
                Assert.Null(load.Pkcs8);
                Assert.Equal(
                    $"passwordKey: environment variable '{name}' is not set (or empty or blank) — the referenced secret cannot resolve.",
                    load.Refusal);
            }
            finally
            {
                Environment.SetEnvironmentVariable(name, null);
                DarlingPasswordKeySource.ForgetCaptured(name);
            }
        }
    }

    /// <summary>
    /// The capture runs right after each load of darling.json inside the service process and before that path starts
    /// any child: the worker before it builds the managed store (whose pg_ctl and icacls runs inherit the environment),
    /// and the MCP and web hosts, which load the file on their own and run in parallel with the worker, before their
    /// PowerShell firewall probe. Anchored on the load statement and on the call that leads to the first child process,
    /// not on line numbers or surrounding comments, so moving unrelated code does not break it and moving the capture
    /// below the child does.
    /// </summary>
    [Theory]
    [InlineData("DarlingWorker.Bootstrap.cs", "new DarlingManagedPostgres(config.Postgres, _logger)")]
    [InlineData("Mcp/DarlingMcpHostService.cs", "await CheckMcpFirewallAsync(")]
    [InlineData("Mcp/DarlingWebHostService.cs", "await CheckWebFirewallAsync(")]
    public void TheServiceCapturesAfterEachConfigLoad_BeforeItsFirstChildProcess(string file, string firstChild)
    {
        var text = ReadRepoFile(["Darling", "PerformanceMonitor.Darling.Service", .. file.Split('/')]);
        const string load = "config = DarlingConfig.Load();";
        const string capture = "DarlingPasswordKeySource.CaptureForService(config);";

        var loadAt = text.IndexOf(load, StringComparison.Ordinal);
        var captureAt = text.IndexOf(capture, StringComparison.Ordinal);
        var childAt = text.IndexOf(firstChild, StringComparison.Ordinal);
        Assert.True(loadAt >= 0, $"{file}: the config load was not found");
        Assert.True(childAt >= 0, $"{file}: the first child-process call was not found");
        Assert.True(captureAt > loadAt, $"{file}: the passwordKey capture must follow the config load");
        Assert.True(captureAt < childAt, $"{file}: the passwordKey capture must come before {firstChild}");
        Assert.Equal(1, text.Split(load).Length - 1);
    }

    /// <summary>
    /// The runtime order on the path that starts pg_ctl, not only the text order above: the worker's real start, with
    /// a managed store, reaches the store's construction with the variable already gone from its environment and from
    /// what a child would inherit. The store factory records both and throws, so nothing is started and no PostgreSQL is
    /// needed. Windows only, because the worker refuses a managed store anywhere else.
    /// </summary>
    [Fact]
    public async Task TheWorker_HasRemovedTheVariable_WhenItBuildsTheManagedStore()
    {
        Assert.SkipUnless(OperatingSystem.IsWindows(), "postgres.managed requires Windows.");

        var (pkcs8, _) = NewKey();
        var name = UniqueVariable();
        var root = Directory.CreateTempSubdirectory("password-key-env-order-").FullName;
        var previousConfig = Environment.GetEnvironmentVariable("DARLING_CONFIG");
        try
        {
            var configPath = Path.Combine(root, "darling.json");
            await File.WriteAllTextAsync(
                configPath,
                "{\"postgres\":{\"managed\":true,\"port\":55999,\"dataDirectory\":" + JsonSerializer.Serialize(Path.Combine(root, "pg")) +
                    "},\"passwordKey\":" + JsonSerializer.Serialize("env:" + name) + "}",
                TestContext.Current.CancellationToken);
            Environment.SetEnvironmentVariable("DARLING_CONFIG", configPath);
            Environment.SetEnvironmentVariable(name, Convert.ToBase64String(pkcs8));

            var reached = new TaskCompletionSource<(string? Variable, bool Inherited)>(TaskCreationOptions.RunContinuationsAsynchronously);
            var worker = new DarlingWorker(
                NullLogger<DarlingWorker>.Instance, NullLoggerFactory.Instance, new McpRuntimeState(), new WebRuntimeState(),
                new MonitoredServerRegistryState(), new CollectorRuntimeState(), new WebTlsCertificateState(),
                new BaselineCache(), new ReadLatencyAccumulator())
            {
                ManagedPostgresFactoryOverride = (_, _) =>
                {
                    reached.TrySetResult((Environment.GetEnvironmentVariable(name), new ProcessStartInfo().Environment.ContainsKey(name)));
                    throw new InvalidOperationException("the test stops the worker where it would build the managed store");
                },
            };

            using var stop = CancellationTokenSource.CreateLinkedTokenSource(TestContext.Current.CancellationToken);
            await worker.StartAsync(stop.Token);
            try
            {
                var (variable, inherited) = await reached.Task.WaitAsync(TimeSpan.FromMinutes(2), TestContext.Current.CancellationToken);
                /* Not Assert.Null: its failure message would print the value. */
                Assert.True(variable is null, "the passwordKey variable was still in the worker's environment where it builds the managed store");
                Assert.False(inherited, "a child started where the managed store is built would inherit the passwordKey variable");
            }
            finally
            {
                stop.Cancel();
                try
                {
                    await worker.ExecuteTask!.WaitAsync(TimeSpan.FromMinutes(1), TestContext.Current.CancellationToken);
                }
                catch (Exception ex) when (ex is InvalidOperationException or OperationCanceledException)
                {
                    /* The factory's own throw, or the stop: the worker ending is all this waits for. */
                }
            }
        }
        finally
        {
            Environment.SetEnvironmentVariable("DARLING_CONFIG", previousConfig);
            Environment.SetEnvironmentVariable(name, null);
            DarlingPasswordKeySource.ForgetCaptured(name);
            CryptographicOperations.ZeroMemory(pkcs8);
            Directory.Delete(root, recursive: true);
        }
    }

    /// <summary>
    /// The pin above covers every child the service starts only while the processes are started in one file. This
    /// scans the service project's own source files (not the projects it references, and not library code) for the text
    /// <c>new ProcessStartInfo</c> or <c>Process.Start(</c>, and expects them only in <c>DarlingManagedPostgres.cs</c>; a
    /// second file that matches fails here, so its author checks that the capture runs before it. Libraries can start
    /// processes too (Azure.Identity's developer credentials, the AWS SDK's <c>credential_process</c>), but only while
    /// connecting to a monitored server, which is always after a config load and its capture. The CLI verbs never
    /// capture: they are short-lived and start no managed store, and an operator's shell keeps the variable anyway.
    /// </summary>
    [Fact]
    public void InTheServiceProjectsOwnSource_OnlyTheManagedStoreStartsProcesses_AndTheCliNeverCaptures()
    {
        var directory = PathTo("Darling", "PerformanceMonitor.Darling.Service");
        var separator = Path.DirectorySeparatorChar;
        var starters = Directory.EnumerateFiles(directory, "*.cs", SearchOption.AllDirectories)
            .Where(f => !f.Contains($"{separator}obj{separator}", StringComparison.Ordinal)
                && !f.Contains($"{separator}bin{separator}", StringComparison.Ordinal))
            .Where(f =>
            {
                var text = File.ReadAllText(f);
                return text.Contains("new ProcessStartInfo", StringComparison.Ordinal)
                    || text.Contains("Process.Start(", StringComparison.Ordinal);
            })
            .Select(Path.GetFileName)
            .ToList();
        Assert.Equal(new[] { "DarlingManagedPostgres.cs" }, starters);

        foreach (var cli in new[] { "Program.cs", "DarlingCliCommands.cs", "DarlingPasswordKeySelfCheck.cs" })
        {
            Assert.DoesNotContain(
                "CaptureForService",
                ReadRepoFile("Darling", "PerformanceMonitor.Darling.Service", cli),
                StringComparison.Ordinal);
        }
    }

    private static DarlingConfig ConfigWith(string reference) =>
        DarlingConfig.Parse("{\"postgres\":{\"connectionString\":\"Host=h\"},\"passwordKey\":" + JsonSerializer.Serialize(reference) + "}");

    private static (byte[] Pkcs8, string KeyId) NewKey()
    {
        using var key = PasswordPrivateKey.Generate();
        return (key.ExportPkcs8(), key.PublicKey.KeyId);
    }

    private static string UniqueVariable() => "DARLING_TEST_PASSWORD_KEY_" + Guid.NewGuid().ToString("N");

    private static void AssertLoaded(PasswordKeyFileLoad load, string reference, string keyId)
    {
        Assert.Null(load.Refusal);
        Assert.False(load.Untrusted);
        Assert.Equal(reference, load.Path);
        Assert.NotNull(load.Pkcs8);
        try
        {
            using var key = PasswordPrivateKey.FromPkcs8(load.Pkcs8);
            Assert.Equal(keyId, key.PublicKey.KeyId);
        }
        finally
        {
            CryptographicOperations.ZeroMemory(load.Pkcs8);
        }
    }
}
