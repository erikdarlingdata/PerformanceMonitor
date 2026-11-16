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
using System.Globalization;
using System.IO;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using Npgsql;
using PerformanceMonitor.Darling.Service;
using Xunit;

namespace Darling.Tests;

/// <summary>
/// #8779: <c>--prepare-provision-roles</c> against a migrated store owned by a <c>CREATEROLE</c> login that is not a superuser,
/// then the runtime's own psql on the three files it wrote, as the chart's Job does. Gated on <c>DARLING_TEST_PGRUNTIME</c> and
/// built on <see cref="ProvisionRolesOwnerRunLiveTests.RunInOwnStoreAsync"/>, with the owner and the database both named
/// <c>app</c>.
/// </summary>
[Collection("darling-config-env")]
public sealed class PrepareProvisionRolesLiveTests
{
    private static readonly Dictionary<string, string> RolePasswords = new()
    {
        ["admin"] = "admin-0123456789abcdef",
        ["viewer"] = "viewer-0123456789abcd",
        ["web"] = "web-0123456789abcdefg",
        ["mcp"] = "mcp-0123456789abcdefg",
    };

    [Fact]
    public async Task PrepareThenPsql_AsCreateRoleOwner_ProvisionsAndVerifiersMatch()
    {
        await RunAsync(async (runtimeRoot, directory, superuser, ownerLogin, ct) =>
        {
            var (args, stdout, stderr) = Prepare(directory, ownerLogin, waitSeconds: 60);
            var exit = await DarlingCliCommands.PrepareProvisionRolesAsync(args, stdout, stderr, ct);

            Assert.True(exit == ProvisionRolesExitCodes.Prepared, $"exit {exit}{Environment.NewLine}{stderr}");
            AssertNoSecrets(directory, stdout.ToString(), stderr.ToString());
            Assert.Contains("owner app in database app", stdout.ToString(), StringComparison.Ordinal);
            var outDir = Path.Combine(directory, "out");
            foreach (var name in new[] { "provision-roles.sql", "pgpass", "libpq.env" })
            {
                Assert.True(File.Exists(Path.Combine(outDir, name)), name);
            }

            var psql = await RunPsqlAsync(runtimeRoot, outDir, ct);
            Assert.True(psql.ExitCode == 0, $"psql exited {psql.ExitCode}.{Environment.NewLine}{psql.Error}");
            foreach (var (role, password) in RolePasswords)
            {
                Assert.Equal("darling-managed", await ComposeStoreRolesLiveTests.RoleMarkerAsync(superuser, role, ct));
                var stored = await ComposeStoreRolesLiveTests.OwnerScalarAsync<string>(
                    superuser, $"SELECT rolpassword FROM pg_authid WHERE rolname = '{role}'", ct);
                Assert.True(ScramSha256Verifier.Verifies(stored, password), $"the stored verifier of {role} does not match its password.");
            }
        });
    }

    [Fact]
    public async Task PrepareThenPsql_StoreWithoutDatabaseSearchPath_StillPrepares()
    {
        await RunAsync(async (runtimeRoot, directory, superuser, ownerLogin, ct) =>
        {
            await ComposeStoreRolesLiveTests.ExecAsync(superuser, "ALTER DATABASE app RESET search_path; ALTER ROLE app RESET search_path", ct);
            var bare = new NpgsqlConnectionStringBuilder(ownerLogin) { SearchPath = null, Pooling = false }.ConnectionString;
            var seen = await ComposeStoreRolesLiveTests.OwnerScalarAsync<string>(bare, "SHOW search_path", ct);
            Assert.DoesNotContain("collect", seen, StringComparison.Ordinal);
            var (args, stdout, stderr) = Prepare(directory, ownerLogin, waitSeconds: 20);

            var exit = await DarlingCliCommands.PrepareProvisionRolesAsync(args, stdout, stderr, ct);

            Assert.True(exit == ProvisionRolesExitCodes.Prepared, $"exit {exit}{Environment.NewLine}{stderr}");
            var psql = await RunPsqlAsync(runtimeRoot, Path.Combine(directory, "out"), ct);
            Assert.True(psql.ExitCode == 0, $"psql exited {psql.ExitCode}.{Environment.NewLine}{psql.Error}");
        });
    }

    [Fact]
    public async Task Prepare_OwnerNameWithAControlCharacter_RefusesNamingTheOwner_AndWritesNothing()
    {
        await RunAsync(async (runtimeRoot, directory, superuser, ownerLogin, ct) =>
        {
            const string password = "TabOwner8779R1";
            await ComposeStoreRolesLiveTests.ExecAsync(superuser, $"CREATE ROLE \"tab	x\" LOGIN NOSUPERUSER CREATEROLE PASSWORD '{password}'; GRANT app TO \"tab	x\"", ct);
            var (args, stdout, stderr) = Prepare(directory, ComposeStoreRolesLiveTests.Login(superuser, "tab	x", password), waitSeconds: 20);

            var exit = await DarlingCliCommands.PrepareProvisionRolesAsync(args, stdout, stderr, ct);

            Assert.Equal(ProvisionRolesExitCodes.Usage, exit);
            Assert.Contains("The owner name contains a control character (U+0009 at position 3)", stderr.ToString(), StringComparison.Ordinal);
            Assert.DoesNotContain('	', stderr.ToString());
            Assert.False(File.Exists(Path.Combine(directory, "out", "provision-roles.sql")));
            AssertNoSecrets(directory, stdout.ToString(), stderr.ToString());
        });
    }

    [Fact]
    public async Task Prepare_UnmigratedStore_TimesOut()
    {
        await RunAsync(async (runtimeRoot, directory, superuser, ownerLogin, ct) =>
        {
            await ComposeStoreRolesLiveTests.ExecAsync(superuser, "DROP TABLE darling_schema_version", ct);
            var (args, stdout, stderr) = Prepare(directory, ownerLogin, waitSeconds: 6);

            var exit = await DarlingCliCommands.PrepareProvisionRolesAsync(args, stdout, stderr, ct);

            Assert.Equal(ProvisionRolesExitCodes.SchemaWaitTimedOut, exit);
            Assert.Contains("schema version none", stderr.ToString(), StringComparison.Ordinal);
            Assert.False(File.Exists(Path.Combine(directory, "out", "provision-roles.sql")));
            AssertNoSecrets(directory, stdout.ToString(), stderr.ToString());
        });
    }

    [Fact]
    public async Task Prepare_SuperuserOwnedFunction_RefusesNamingIt()
    {
        await RunAsync(async (runtimeRoot, directory, superuser, ownerLogin, ct) =>
        {
            var superuserName = new NpgsqlConnectionStringBuilder(superuser).Username!;
            var signature = await ComposeStoreRolesLiveTests.OwnerScalarAsync<string>(
                superuser,
                "SELECT p.oid::regprocedure::text FROM pg_proc p JOIN pg_namespace n ON n.oid = p.pronamespace "
                + "WHERE n.nspname = 'config' AND p.prokind = 'f' ORDER BY p.proname LIMIT 1", ct);
            var function = signature[..signature.IndexOf('(', StringComparison.Ordinal)].Split('.')[^1].Trim('"');
            await ComposeStoreRolesLiveTests.ExecAsync(superuser, $"ALTER ROUTINE {signature} OWNER TO \"{superuserName}\"", ct);
            var (args, stdout, stderr) = Prepare(directory, ownerLogin, waitSeconds: 60);

            var exit = await DarlingCliCommands.PrepareProvisionRolesAsync(args, stdout, stderr, ct);

            Assert.Equal(ProvisionRolesExitCodes.OwnershipRefused, exit);
            Assert.Contains(function, stderr.ToString(), StringComparison.Ordinal);
            Assert.Contains($"(owner {superuserName})", stderr.ToString(), StringComparison.Ordinal);
            Assert.False(File.Exists(Path.Combine(directory, "out", "provision-roles.sql")));
            AssertNoSecrets(directory, stdout.ToString(), stderr.ToString());
        });
    }

    private static Task RunAsync(Func<string, string, string, string, CancellationToken, Task> body) =>
        ProvisionRolesOwnerRunLiveTests.RunInOwnStoreAsync(createRole: true, body, ownerName: "app", databaseName: "app");

    /// <summary>Writes the login and the four role passwords under <paramref name="directory"/> and returns the verb's arguments
    /// (the password form) for the owner the connection string names.</summary>
    private static (string[] Args, StringWriter Stdout, StringWriter Stderr) Prepare(string directory, string ownerLogin, int waitSeconds)
    {
        var owner = new NpgsqlConnectionStringBuilder(ownerLogin);
        var login = Path.Combine(directory, "login");
        File.WriteAllText(login, owner.Password + "\n");
        var roles = Directory.CreateDirectory(Path.Combine(directory, "roles")).FullName;
        foreach (var (role, password) in RolePasswords)
        {
            File.WriteAllText(Path.Combine(roles, role), password + "\n");
        }

        var args = new[]
        {
            "--login", login, "--login-kind", "password", "--passwords", roles, "--out", Path.Combine(directory, "out"),
            "--wait-seconds", waitSeconds.ToString(CultureInfo.InvariantCulture),
            "--host", owner.Host!, "--port", owner.Port.ToString(CultureInfo.InvariantCulture), "--database", owner.Database!,
            "--username", owner.Username!, "--ssl-mode", "Disable",
        };
        return (args, new StringWriter(), new StringWriter());
    }

    private static void AssertNoSecrets(string directory, params string[] outputs)
    {
        var secrets = new List<string>(RolePasswords.Values) { "SCRAM-SHA-256$", File.ReadAllText(Path.Combine(directory, "login")).TrimEnd('\n') };
        foreach (var output in outputs)
        {
            foreach (var secret in secrets)
            {
                Assert.DoesNotContain(secret, output, StringComparison.Ordinal);
            }
        }
    }

    /// <summary>psql as the Job runs it: the libpq environment from <c>libpq.env</c>, <c>PGPASSFILE</c> for the password, and
    /// <c>-X -v ON_ERROR_STOP=1 -c "SET lock_timeout = '120s'" -f provision-roles.sql</c>.</summary>
    private static async Task<(int ExitCode, string Error)> RunPsqlAsync(string runtimeRoot, string outDirectory, CancellationToken ct)
    {
        var psql = Path.Combine(runtimeRoot, "pgsql", "bin", "psql.exe");
        var start = new ProcessStartInfo(psql)
        {
            RedirectStandardOutput = true,
            RedirectStandardError = true,
            UseShellExecute = false,
            CreateNoWindow = true,
        };
        // The Job's script drops every inherited PG* variable first (a hosted Windows runner has PGUSER and PGPASSWORD set, and
        // PGPASSWORD outranks the pgpass file); the same here, so the connection comes from libpq.env and the pgpass file only.
        foreach (var name in start.Environment.Keys.Where(key => key.StartsWith("PG", StringComparison.OrdinalIgnoreCase)).ToList())
        {
            start.Environment.Remove(name);
        }

        foreach (var line in File.ReadAllLines(Path.Combine(outDirectory, "libpq.env")))
        {
            var equals = line.IndexOf('=', StringComparison.Ordinal);
            var value = line[(equals + 1)..];
            start.Environment[line[..equals]] = value.Substring(1, value.Length - 2).Replace("'\\''", "'", StringComparison.Ordinal);
        }

        start.Environment["PGPASSFILE"] = Path.Combine(outDirectory, "pgpass");
        foreach (var argument in new[] { "-X", "-w", "-v", "ON_ERROR_STOP=1", "-c", "SET lock_timeout = '120s'", "-f", Path.Combine(outDirectory, "provision-roles.sql") })
        {
            start.ArgumentList.Add(argument);
        }

        using var process = Process.Start(start)!;
        var error = process.StandardError.ReadToEndAsync(ct);
        var output = process.StandardOutput.ReadToEndAsync(ct);
        using var timeout = CancellationTokenSource.CreateLinkedTokenSource(ct);
        timeout.CancelAfter(TimeSpan.FromMinutes(3));
        try
        {
            await process.WaitForExitAsync(timeout.Token);
        }
        catch (OperationCanceledException)
        {
            process.Kill(entireProcessTree: true);
            throw;
        }

        await output;
        return (process.ExitCode, await error);
    }
}
