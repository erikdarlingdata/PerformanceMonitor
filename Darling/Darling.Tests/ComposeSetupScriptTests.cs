/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.IO;
using System.Linq;
using PerformanceMonitor.Darling.Service;
using Xunit;

namespace Darling.Tests;

/// <summary>
/// #6140: a default Linux compose install used to need five secret files in five formats, made by hand.
/// <c>Darling/compose/setup.sh</c> makes them. These tests run the REAL script (copied beside the test binary by
/// the csproj) in a temp folder, so they fail on what a user would see: a missing file, a connection string whose
/// password does not match <c>store_password.txt</c>, a second run that rewrites a secret, a secret printed to the
/// terminal. They skip where no POSIX <c>sh</c> exists.
/// </summary>
public sealed class ComposeSetupScriptTests : IDisposable
{
    private const string SqlPassword = "Sql-Passw0rd-Not-Random!";

    private readonly string _folder = Path.Combine(Path.GetTempPath(), "darling-setup-" + Guid.NewGuid().ToString("N"));

    public ComposeSetupScriptTests()
    {
        Directory.CreateDirectory(_folder);
        var fixtures = Path.Combine(AppContext.BaseDirectory, "Fixtures");
        File.Copy(Path.Combine(fixtures, "compose.setup.sh"), Path.Combine(_folder, "setup.sh"));
        File.Copy(Path.Combine(fixtures, "compose.darling.sample.json"), Path.Combine(_folder, "darling.sample.json"));
    }

    public void Dispose()
    {
        try
        {
            Directory.Delete(_folder, recursive: true);
        }
        catch (IOException)
        {
            /* A scratch folder; the next temp sweep takes it. */
        }
    }

    [Fact]
    public void FirstRun_MakesEveryDefaultFile_AndTheConnectionStringMatchesThePassword()
    {
        var sh = RequireSh();

        var run = Run(sh, _folder, new[] { "--no-prompt" }, SqlPassword);

        Assert.Equal(0, run.ExitCode);
        var password = Read("secrets/store_password.txt");
        Assert.Matches("^[A-Za-z0-9]{32,}$", password);
        Assert.Equal(
            $"Host=store;Port=5432;Username=darling;Password={password};Database=darling",
            Read("secrets/store_connection"));
        Assert.Matches("^[A-Za-z0-9]{40,}$", Read("secrets/web_token.txt"));
        Assert.Matches("^[A-Za-z0-9]{40,}$", Read("secrets/mcp_token.txt"));
        Assert.NotEqual(Read("secrets/web_token.txt"), Read("secrets/mcp_token.txt"));
        Assert.Equal(SqlPassword, Read("secrets/sql_password.txt"));
        Assert.Equal(
            File.ReadAllText(Path.Combine(_folder, "darling.sample.json")),
            File.ReadAllText(Path.Combine(_folder, "darling.json")));

        /* It says where the dashboard is and how to read the token, and prints no secret. */
        Assert.Contains("http://127.0.0.1:5153", run.Output);
        Assert.Contains("cat secrets/web_token.txt", run.Output);
        foreach (var secret in new[] { password, Read("secrets/web_token.txt"), Read("secrets/mcp_token.txt"), SqlPassword })
        {
            Assert.DoesNotContain(secret, run.Output);
        }

        if (!OperatingSystem.IsWindows())
        {
            Assert.Equal(
                UnixFileMode.UserRead | UnixFileMode.UserWrite | UnixFileMode.UserExecute,
                File.GetUnixFileMode(Path.Combine(_folder, "secrets")));
            foreach (var name in new[] { "store_password.txt", "store_connection", "web_token.txt", "mcp_token.txt", "sql_password.txt" })
            {
                Assert.Equal(
                    UnixFileMode.UserRead | UnixFileMode.UserWrite,
                    File.GetUnixFileMode(Path.Combine(_folder, "secrets", name)));
            }
        }
    }

    [Fact]
    public void SecondRun_ChangesNothing_EvenWithAnotherSqlPasswordAndAnEditedConfig()
    {
        var sh = RequireSh();
        Run(sh, _folder, new[] { "--no-prompt" }, SqlPassword);
        File.WriteAllText(Path.Combine(_folder, "darling.json"), "{ \"edited\": true }");
        var before = Snapshot();

        var run = Run(sh, _folder, new[] { "--no-prompt" }, "A-Different-Password");

        Assert.Equal(0, run.ExitCode);
        Assert.Equal(before, Snapshot());
    }

    [Fact]
    public void NoPrompt_WithoutAPassword_WritesNoSqlPasswordFile_AndNamesIt()
    {
        var sh = RequireSh();

        var run = Run(sh, _folder, new[] { "--no-prompt" }, sqlPassword: null);

        Assert.Equal(0, run.ExitCode);
        Assert.False(File.Exists(Path.Combine(_folder, "secrets", "sql_password.txt")));
        Assert.Contains("secrets/sql_password.txt", run.Output);
        Assert.True(File.Exists(Path.Combine(_folder, "secrets", "store_connection")));
    }

    [Fact]
    public void WithoutNoPrompt_AndNoTerminal_ItDoesNotWaitForInput()
    {
        var sh = RequireSh();

        var run = Run(sh, _folder, Array.Empty<string>(), sqlPassword: null);

        Assert.Equal(0, run.ExitCode);
        Assert.False(File.Exists(Path.Combine(_folder, "secrets", "sql_password.txt")));
    }

    [Fact]
    public void WhenOnlyTheStorePasswordExists_TheConnectionStringIsBuiltFromIt()
    {
        var sh = RequireSh();
        Directory.CreateDirectory(Path.Combine(_folder, "secrets"));
        File.WriteAllText(Path.Combine(_folder, "secrets", "store_password.txt"), "ExistingStorePassword123\n");

        Run(sh, _folder, new[] { "--no-prompt" }, SqlPassword);

        Assert.Equal("ExistingStorePassword123", Read("secrets/store_password.txt"));
        Assert.Equal(
            "Host=store;Port=5432;Username=darling;Password=ExistingStorePassword123;Database=darling",
            Read("secrets/store_connection"));
    }

    [Fact]
    public void WhenOnlyTheConnectionStringExists_TheStorePasswordIsTakenFromIt()
    {
        var sh = RequireSh();
        Directory.CreateDirectory(Path.Combine(_folder, "secrets"));
        File.WriteAllText(
            Path.Combine(_folder, "secrets", "store_connection"),
            "Host=store;Port=5432;Username=darling;Password=FromTheConnection99;Database=darling\n");

        Run(sh, _folder, new[] { "--no-prompt" }, SqlPassword);

        Assert.Equal("FromTheConnection99", Read("secrets/store_password.txt"));
    }

    [Fact]
    public void RunFromAnotherDirectory_StillWorksInTheScriptsOwnFolder()
    {
        var sh = RequireSh();
        var elsewhere = Path.Combine(_folder, "elsewhere");
        Directory.CreateDirectory(elsewhere);

        var run = Run(sh, elsewhere, new[] { "--no-prompt" }, SqlPassword, scriptPath: Path.Combine(_folder, "setup.sh").Replace('\\', '/'));

        Assert.Equal(0, run.ExitCode);
        Assert.True(File.Exists(Path.Combine(_folder, "secrets", "store_connection")));
        Assert.False(Directory.Exists(Path.Combine(elsewhere, "secrets")));
    }

    /// <summary>
    /// #6140: the sample ships MCP off, and compose no longer mounts <c>mcp_token</c> for it. The service must
    /// start with <c>mcp.network.token</c> pointing at a file that is not there: the token is read when the
    /// listener starts, not when the configuration loads.
    /// </summary>
    [Fact]
    public void TheSampleConfig_LoadsWithoutTheMcpTokenFile_WhileMcpIsOff()
    {
        var sample = File.ReadAllText(Path.Combine(_folder, "darling.sample.json"))
            .Replace("file:/run/secrets/store_connection", "Host=store;Database=darling", StringComparison.Ordinal);

        var config = DarlingConfig.Parse(sample);

        Assert.False(config.Mcp.Enabled);
        Assert.Equal("file:/run/secrets/mcp_token", config.Mcp.Network!.Token);
    }

    private static string RequireSh()
    {
        var candidates = new List<string> { "sh" };
        if (OperatingSystem.IsWindows())
        {
            candidates.Add(@"C:\Program Files\Git\usr\bin\sh.exe");
            candidates.Add(@"C:\Program Files\Git\bin\sh.exe");
        }

        foreach (var candidate in candidates)
        {
            try
            {
                using var probe = Process.Start(new ProcessStartInfo(candidate, "-c \"exit 0\"")
                {
                    UseShellExecute = false,
                    RedirectStandardOutput = true,
                    RedirectStandardError = true,
                    CreateNoWindow = true,
                });
                if (probe is not null && probe.WaitForExit(15000) && probe.ExitCode == 0)
                {
                    return candidate;
                }
            }
            catch (System.ComponentModel.Win32Exception)
            {
                /* Not on this machine; try the next. */
            }
        }

        Assert.Skip("No POSIX sh on this machine, so the compose setup script cannot be run.");
        return "";
    }

    private static (int ExitCode, string Output) Run(
        string sh, string workingDirectory, string[] args, string? sqlPassword, string scriptPath = "setup.sh")
    {
        var info = new ProcessStartInfo(sh)
        {
            WorkingDirectory = workingDirectory,
            UseShellExecute = false,
            RedirectStandardInput = true,
            RedirectStandardOutput = true,
            RedirectStandardError = true,
            CreateNoWindow = true,
        };
        info.ArgumentList.Add(scriptPath);
        foreach (var arg in args)
        {
            info.ArgumentList.Add(arg);
        }

        info.Environment.Remove("DARLING_SQL_PASSWORD");
        if (sqlPassword is not null)
        {
            info.Environment["DARLING_SQL_PASSWORD"] = sqlPassword;
        }

        using var process = Process.Start(info)!;
        process.StandardInput.Close();
        var stdout = process.StandardOutput.ReadToEndAsync();
        var stderr = process.StandardError.ReadToEndAsync();
        Assert.True(process.WaitForExit(60000), "setup.sh did not finish within a minute.");
        return (process.ExitCode, stdout.Result + stderr.Result);
    }

    private string Read(string relative) =>
        File.ReadAllText(Path.Combine(_folder, relative)).TrimEnd('\r', '\n');

    private Dictionary<string, string> Snapshot() =>
        Directory.GetFiles(_folder, "*", SearchOption.AllDirectories)
            .Where(f => !f.EndsWith("setup.sh", StringComparison.Ordinal))
            .ToDictionary(f => Path.GetRelativePath(_folder, f), File.ReadAllText);
}
