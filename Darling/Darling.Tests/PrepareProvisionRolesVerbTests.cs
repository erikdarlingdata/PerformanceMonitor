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
using System.Threading;
using System.Threading.Tasks;
using Npgsql;
using PerformanceMonitor.Darling.Service;
using Xunit;

namespace Darling.Tests;

/// <summary>The argument grammar, the login readers and the file checks of <c>--prepare-provision-roles</c> (#8779). None of
/// these reaches a store: the verb refuses bad input before it connects.</summary>
[Trait("Stage", "Guard")]
public sealed class PrepareProvisionRolesVerbTests : IDisposable
{
    private const string Verifier = "SCRAM-SHA-256$";
    private readonly string _directory = Directory.CreateTempSubdirectory("darling-8779-verb-").FullName;

    public void Dispose() => Directory.Delete(_directory, recursive: true);

    private static string[] PasswordKindArgs(params string[] extra) =>
        new[]
        {
            "--login", "/run/login", "--login-kind", "password", "--passwords", "/run/roles", "--out", "/work", "--wait-seconds", "30",
            "--host", "db", "--port", "5432", "--database", "app", "--username", "app", "--ssl-mode", "Require",
        }.Concat(extra).ToArray();

    private static string[] WithoutOption(string[] args, string option)
    {
        var list = args.ToList();
        var at = list.IndexOf(option);
        list.RemoveRange(at, 2);
        return list.ToArray();
    }

    private static string[] With(string[] args, string option, string value)
    {
        var copy = (string[])args.Clone();
        copy[Array.IndexOf(copy, option) + 1] = value;
        return copy;
    }

    [Fact]
    public void Parse_PasswordKind_RequiresHostOptions()
    {
        Assert.True(DarlingCliCommands.TryParsePrepareProvisionRolesArgs(PasswordKindArgs("--root-certificate", "/ca.crt"), out var parsed, out var error), error);
        Assert.Equal("db", parsed!.Host);
        Assert.Equal(5432, parsed.Port);
        Assert.Equal("app", parsed.Database);
        Assert.Equal("app", parsed.Username);
        Assert.Equal(SslMode.Require, parsed.SslMode);
        Assert.Equal("/ca.crt", parsed.RootCertificate);
        Assert.Equal(30, parsed.WaitSeconds);

        foreach (var option in new[] { "--host", "--port", "--database", "--username", "--ssl-mode" })
        {
            Assert.False(DarlingCliCommands.TryParsePrepareProvisionRolesArgs(WithoutOption(PasswordKindArgs(), option), out _, out error));
            Assert.Contains(option, error, StringComparison.Ordinal);
        }
    }

    [Theory]
    [InlineData("a,b")]
    [InlineData("a.example,b.example")]
    public void Parse_PasswordKind_RefusesAHostList(string host)
    {
        Assert.False(DarlingCliCommands.TryParsePrepareProvisionRolesArgs(With(PasswordKindArgs(), "--host", host), out _, out var error));
        Assert.Contains("--host", error, StringComparison.Ordinal);
        Assert.Contains("one host", error, StringComparison.Ordinal);
    }

    [Fact]
    public void Parse_ConnectionStringKind_RefusesHostOptions()
    {
        var baseArgs = new[] { "--login", "/run/login", "--login-kind", "connection-string", "--passwords", "/run/roles", "--out", "/work", "--wait-seconds", "0" };
        Assert.True(DarlingCliCommands.TryParsePrepareProvisionRolesArgs(baseArgs, out var parsed, out var error), error);
        Assert.Null(parsed!.Host);

        foreach (var (option, value) in new[]
        {
            ("--host", "db"), ("--port", "5432"), ("--database", "app"), ("--username", "app"), ("--ssl-mode", "Require"), ("--root-certificate", "/ca.crt"),
        })
        {
            Assert.False(DarlingCliCommands.TryParsePrepareProvisionRolesArgs(baseArgs.Concat(new[] { option, value }).ToArray(), out _, out error));
            Assert.Contains(option, error, StringComparison.Ordinal);
        }
    }

    [Fact]
    public void Parse_RefusesUnknownFlags()
    {
        Assert.False(DarlingCliCommands.TryParsePrepareProvisionRolesArgs(PasswordKindArgs("--verbose", "yes"), out _, out var error));
        Assert.Contains("--verbose", error, StringComparison.Ordinal);
        Assert.False(DarlingCliCommands.TryParsePrepareProvisionRolesArgs(PasswordKindArgs("stray"), out _, out error));
        Assert.False(DarlingCliCommands.TryParsePrepareProvisionRolesArgs(PasswordKindArgs("--out", "/other"), out _, out error));
        Assert.Contains("twice", error, StringComparison.Ordinal);
        Assert.False(DarlingCliCommands.TryParsePrepareProvisionRolesArgs(With(PasswordKindArgs(), "--ssl-mode", "Sometimes"), out _, out error));
        Assert.Contains("--ssl-mode", error, StringComparison.Ordinal);
    }

    [Theory]
    [InlineData("abc")]
    [InlineData("-5")]
    [InlineData("1.5")]
    [InlineData("")]
    public void Parse_RefusesNonNumericWait(string wait)
    {
        Assert.False(DarlingCliCommands.TryParsePrepareProvisionRolesArgs(With(PasswordKindArgs(), "--wait-seconds", wait), out _, out var error));
        Assert.NotNull(error);
    }

    [Fact]
    public void Usage_IsAscii()
    {
        Assert.All(DarlingCliCommands.PrepareProvisionRolesUsageText(), c => Assert.True(c < 128, $"non-ASCII character U+{(int)c:X4}"));
    }

    [Fact]
    public void KnownVerb_IsRecognised_ByTheDispatcherClassifier()
    {
        Assert.True(DarlingCliCommands.IsPrepareProvisionRolesVerb("--PREPARE-PROVISION-ROLES"));
        Assert.False(DarlingCliCommands.IsPrepareProvisionRolesVerb("--prepare"));
        Assert.True(DarlingCliCommands.IsKnownVerb("--prepare-provision-roles"));
        Assert.Equal(StartupAction.RunKnownVerb, DarlingCliCommands.ClassifyStartupArgs(new[] { "--prepare-provision-roles" }));
    }

    [Theory]
    [InlineData("Root Certificate=/tmp/x.crt")]
    [InlineData("SSL Certificate=/tmp/x.crt")]
    public void ConnectionStringFile_Refuses_OtherRootCertificate_And_ClientCertificate(string extra)
    {
        var login = DarlingCliCommands.TryReadProvisionRolesLogin(
            ConnectionStringArgs(), $"Host=h;Database=d;Username=u;Password=p-0123456789abcdef;{extra}\n", out var error);

        Assert.Null(login);
        Assert.NotNull(error);
        Assert.DoesNotContain("p-0123456789abcdef", error, StringComparison.Ordinal);
    }

    [Theory]
    [InlineData("VerifyFull")]
    [InlineData("VerifyCA")]
    public void ConnectionStringFile_Refuses_VerifyModesWithoutARootCertificate(string mode)
    {
        /* Npgsql would check the OS trust store, but libpq's verify-ca and verify-full read sslrootcert and psql exits 2 without it. */
        var login = DarlingCliCommands.TryReadProvisionRolesLogin(
            ConnectionStringArgs(), $"Host=h;Database=d;Username=u;Password=p-0123456789abcdef;SSL Mode={mode}\n", out var error);

        Assert.Null(login);
        Assert.Contains("Root Certificate", error, StringComparison.Ordinal);
        Assert.Contains("/etc/darling/store-ca/ca.crt", error, StringComparison.Ordinal);
        Assert.DoesNotContain("p-0123456789abcdef", error, StringComparison.Ordinal);
        Assert.DoesNotContain("Host=h", error, StringComparison.Ordinal);
    }

    [Theory]
    [InlineData("Require")]
    [InlineData("Prefer")]
    public void ConnectionStringFile_AcceptsOtherModesWithoutARootCertificate(string mode)
    {
        var login = DarlingCliCommands.TryReadProvisionRolesLogin(
            ConnectionStringArgs(), $"Host=h;Database=d;Username=u;Password=p-0123456789abcdef;SSL Mode={mode}\n", out var error);

        Assert.True(login is not null, error);
    }

    [Fact]
    public void ConnectionStringFile_Accepts_TheChartStoreCa_AndCarriesItIntoTheLibpqEnv()
    {
        var login = DarlingCliCommands.TryReadProvisionRolesLogin(
            ConnectionStringArgs(), "Host=h;Database=d;Username=u;Password=p-0123456789abcdef;SSL Mode=VerifyFull;Root Certificate=/etc/darling/store-ca/ca.crt\n", out var error);

        Assert.True(login is not null, error);
        Assert.Equal("/etc/darling/store-ca/ca.crt", login!.RootCertificate);
        var env = ProvisionRolesRenderer.LibpqEnvFile(
            login.Host, login.Port, login.Database, login.Username, ProvisionRolesRenderer.ToLibpqSslMode(login.SslMode), login.RootCertificate);
        Assert.Contains("PGSSLROOTCERT='/etc/darling/store-ca/ca.crt'\n", env, StringComparison.Ordinal);
        Assert.Contains("PGSSLMODE='verify-full'\n", env, StringComparison.Ordinal);
    }

    [Fact]
    public void ConnectionStringFile_QuotedPassword()
    {
        var login = DarlingCliCommands.TryReadProvisionRolesLogin(ConnectionStringArgs(), "Host=h;Database=d;Username=u;Password='a;b''c'\n", out var error);

        Assert.True(login is not null, error);
        Assert.Equal("a;b'c", login!.Password);
        Assert.Equal("h:5432:d:u:a;b'c", ProvisionRolesRenderer.PgPassLine(login.Host, login.Port, login.Database, login.Username, login.Password));
        var env = ProvisionRolesRenderer.LibpqEnvFile(
            login.Host, login.Port, login.Database, login.Username, ProvisionRolesRenderer.ToLibpqSslMode(login.SslMode), login.RootCertificate);
        Assert.Contains("PGUSER='u'\n", env, StringComparison.Ordinal);
        Assert.DoesNotContain("a;b", env, StringComparison.Ordinal);
    }

    [Fact]
    public async Task RolePasswordFiles_MissingOrBad_ExitUsage_NamingTheRole()
    {
        var login = Path.Combine(_directory, "login");
        File.WriteAllText(login, "OwnerPw-0123456789\n");
        var roles = Directory.CreateDirectory(Path.Combine(_directory, "roles")).FullName;
        File.WriteAllText(Path.Combine(roles, "admin"), "admin-0123456789abcdef\n");
        File.WriteAllText(Path.Combine(roles, "viewer"), "short\n");
        File.WriteAllText(Path.Combine(roles, "web"), "web-0123456789abcdefg\r\n");

        var stdout = new StringWriter();
        var stderr = new StringWriter();
        var args = With(With(With(PasswordKindArgs(), "--login", login), "--passwords", roles), "--out", Path.Combine(_directory, "out"));
        var exit = await DarlingCliCommands.PrepareProvisionRolesAsync(args, stdout, stderr, CancellationToken.None);

        Assert.Equal(ProvisionRolesExitCodes.Usage, exit);
        var text = stderr.ToString();
        Assert.Contains("role viewer", text, StringComparison.Ordinal);
        Assert.Contains("role web", text, StringComparison.Ordinal);
        Assert.Contains("carriage return", text, StringComparison.Ordinal);
        Assert.Contains("role mcp", text, StringComparison.Ordinal);
        Assert.DoesNotContain("role admin", text, StringComparison.Ordinal);
        foreach (var secret in new[] { "admin-0123456789abcdef", "web-0123456789abcdefg", "OwnerPw-0123456789", Verifier })
        {
            Assert.DoesNotContain(secret, text, StringComparison.Ordinal);
            Assert.DoesNotContain(secret, stdout.ToString(), StringComparison.Ordinal);
        }

        Assert.False(Directory.Exists(Path.Combine(_directory, "out")));
    }

    [Fact]
    public async Task MissingLoginFile_ExitsUsage()
    {
        var stderr = new StringWriter();
        var exit = await DarlingCliCommands.PrepareProvisionRolesAsync(
            With(PasswordKindArgs(), "--login", Path.Combine(_directory, "nope")), new StringWriter(), stderr, CancellationToken.None);

        Assert.Equal(ProvisionRolesExitCodes.Usage, exit);
        Assert.Contains("login file", stderr.ToString(), StringComparison.Ordinal);
    }

    [Fact]
    public void StoreConnectionString_CarriesTheStoreSearchPath_ForBothLoginKinds()
    {
        Assert.True(DarlingCliCommands.TryParsePrepareProvisionRolesArgs(PasswordKindArgs(), out var passwordArgs, out var error), error);
        var passwordLogin = DarlingCliCommands.TryReadProvisionRolesLogin(passwordArgs!, "OwnerPw-0123456789\n", out error);
        var stringLogin = DarlingCliCommands.TryReadProvisionRolesLogin(ConnectionStringArgs(), "Host=h;Database=d;Username=u;Password=p-0123456789abcdef\n", out error);

        foreach (var login in new[] { passwordLogin, stringLogin })
        {
            var built = new NpgsqlConnectionStringBuilder(DarlingCliCommands.BuildProvisionRolesConnectionString(login!));
            Assert.Equal("collect,config,public", built.SearchPath);
            Assert.Equal(10, built.Timeout);
        }
    }

    [Fact]
    public void StoreConnectionString_KeepsAnOperatorSuppliedSearchPath()
    {
        var login = DarlingCliCommands.TryReadProvisionRolesLogin(
            ConnectionStringArgs(), "Host=h;Database=d;Username=u;Password=p-0123456789abcdef;Search Path=mine,public\n", out var error);

        Assert.True(login is not null, error);
        var built = new NpgsqlConnectionStringBuilder(DarlingCliCommands.BuildProvisionRolesConnectionString(login!));
        Assert.Equal("mine,public", built.SearchPath);
    }

    private static DarlingCliCommands.PrepareProvisionRolesArgs ConnectionStringArgs() =>
        new("/run/login", DarlingCliCommands.ProvisionRolesLoginKind.ConnectionString, "/run/roles", "/work", 0, null, null, null, null, null, null);
}
