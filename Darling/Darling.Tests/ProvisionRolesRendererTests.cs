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
using System.Linq;
using System.Text.RegularExpressions;
using Npgsql;
using PerformanceMonitor.Darling.Service;
using Xunit;

namespace Darling.Tests;

/// <summary>The pure renderer behind <c>--prepare-provision-roles</c> (#8779): the shipped <c>provision-roles.sql</c> with
/// the BEFORE RUNNING substitutions and nothing else.</summary>
[Trait("Stage", "Guard")]
public sealed class ProvisionRolesRendererTests
{
    private static readonly string Shipped =
        File.ReadAllText(Path.Combine(AppContext.BaseDirectory, "Fixtures", "provision-roles.sql"));

    private static readonly Dictionary<string, string> Passwords = new()
    {
        ["admin"] = "admin-0123456789abcdef",
        ["viewer"] = "viewer-0123456789abcd",
        ["web"] = "web-0123456789abcdefg",
        ["mcp"] = "mcp-0123456789abcdefg",
    };

    private static int Count(string text, string needle)
    {
        var count = 0;
        var index = 0;
        while ((index = text.IndexOf(needle, index, StringComparison.Ordinal)) >= 0)
        {
            count++;
            index += needle.Length;
        }

        return count;
    }

    [Fact]
    public void Render_ReplacesEachSiteItsExpectedCount_AndNoPlaceholderRemains()
    {
        foreach (var (text, expected) in ProvisionRolesRenderer.Sites)
        {
            Assert.Equal(expected, Count(Shipped, text));
        }

        var rendered = ProvisionRolesRenderer.Render(Shipped, "app", "app", Passwords);

        foreach (var role in Passwords.Keys)
        {
            Assert.DoesNotContain("CHANGE_ME_" + role.ToUpperInvariant() + "_PASSWORD", rendered, StringComparison.Ordinal);
        }

        var alterRoles = Regex.Matches(rendered, @"^ALTER ROLE (?<role>admin|viewer|web|mcp)\s+LOGIN PASSWORD '(?<secret>[^']*)';", RegexOptions.Multiline);
        Assert.Equal(4, alterRoles.Count);
        foreach (Match m in alterRoles)
        {
            Assert.True(ScramSha256Verifier.Verifies(m.Groups["secret"].Value, Passwords[m.Groups["role"].Value]), m.Groups["role"].Value);
        }

        Assert.Equal(5, Count(rendered, "FOR ROLE \"app\" IN SCHEMA"));
        Assert.Equal(3, Count(rendered, "ON DATABASE \"app\""));
        Assert.Equal(1, Count(rendered, "ALTER DATABASE \"app\" SET search_path"));
    }

    [Fact]
    public void Render_ChangesOnlyTheSiteLines()
    {
        var rendered = ProvisionRolesRenderer.Render(Shipped, "app", "app", Passwords);

        var before = Shipped.Split('\n');
        var after = rendered.Split('\n');
        Assert.Equal(before.Length, after.Length);

        var differing = Enumerable.Range(0, before.Length).Where(i => before[i] != after[i]).ToHashSet();
        var siteLines = Enumerable.Range(0, before.Length)
            .Where(i => ProvisionRolesRenderer.Sites.Any(s => before[i].Contains(s.Text, StringComparison.Ordinal)))
            .ToHashSet();

        Assert.NotEmpty(siteLines);
        Assert.True(siteLines.SetEquals(differing), "differing: " + string.Join(",", differing.OrderBy(i => i)) + " sites: " + string.Join(",", siteLines.OrderBy(i => i)));
    }

    [Theory]
    [InlineData("darling_alert_sources")]
    [InlineData("darling_saved_stop")]
    [InlineData("darling-managed")]
    [InlineData("darling.json")]
    [InlineData("GRANT pg_read_all_stats TO darling;")]
    [InlineData("GRANT CONNECT ON DATABASE darling after this.")]
    public void Render_LeavesNamesThatMustNotChange(string name)
    {
        var rendered = ProvisionRolesRenderer.Render(Shipped, "app", "app", Passwords);

        Assert.True(Count(Shipped, name) > 0, "the shipped script no longer contains " + name);
        Assert.Equal(Count(Shipped, name), Count(rendered, name));
    }

    [Fact]
    public void Render_QuotesIdentifiers()
    {
        var rendered = ProvisionRolesRenderer.Render(Shipped, "Sql-Monitor", "app\"x", Passwords);

        Assert.Contains("FOR ROLE \"Sql-Monitor\" IN SCHEMA", rendered, StringComparison.Ordinal);
        Assert.Contains("ON DATABASE \"app\"\"x\"", rendered, StringComparison.Ordinal);
    }

    [Theory]
    [InlineData("owner", "\n")]
    [InlineData("owner", "\r")]
    [InlineData("owner", "\t")]
    [InlineData("owner", "\0")]
    [InlineData("database", "\n")]
    [InlineData("database", "\r")]
    [InlineData("database", "\t")]
    [InlineData("database", "\0")]
    public void Render_RefusesAControlCharacterInTheOwnerOrDatabase_NamingWhichAndNeverTheValue(string which, string control)
    {
        var bad = "app" + control + "\\! echo run";
        var owner = which == "owner" ? bad : "app";
        var database = which == "database" ? bad : "app";

        var ex = Assert.Throws<ProvisionRolesRenderException>(() => ProvisionRolesRenderer.Render(Shipped, owner, database, Passwords));

        Assert.Contains($"The {which} name contains a control character (U+{(int)control[0]:X4} at position 3)", ex.Message, StringComparison.Ordinal);
        Assert.DoesNotContain("echo run", ex.Message, StringComparison.Ordinal);
        Assert.DoesNotContain(control, ex.Message, StringComparison.Ordinal);
    }

    [Fact]
    public void Render_StillAcceptsUnusualButPrintableNames()
    {
        var rendered = ProvisionRolesRenderer.Render(Shipped, "o wn;er-e", "d\"b -- x", Passwords);

        Assert.Contains("FOR ROLE \"o wn;er-e\" IN SCHEMA", rendered, StringComparison.Ordinal);
        Assert.Contains("ON DATABASE \"d\"\"b -- x\"", rendered, StringComparison.Ordinal);
    }

    [Theory]
    [InlineData(false)]
    [InlineData(true)]
    public void Render_Refuses_WhenASiteIsMissingOrDuplicated(bool duplicate)
    {
        const string site = "GRANT CONNECT ON DATABASE darling TO mcp;";
        var script = duplicate ? Shipped.Replace(site, site + "\n" + site, StringComparison.Ordinal) : Shipped.Replace(site, "", StringComparison.Ordinal);

        var ex = Assert.Throws<ProvisionRolesRenderException>(() => ProvisionRolesRenderer.Render(script, "app", "app", Passwords));

        Assert.Contains(site, ex.Message, StringComparison.Ordinal);
        Assert.Contains(duplicate ? "expected 1, found 2" : "expected 1, found 0", ex.Message, StringComparison.Ordinal);
    }

    [Fact]
    public void Render_KeepsCrlf()
    {
        var crlf = Shipped.Replace("\r\n", "\n", StringComparison.Ordinal).Replace("\n", "\r\n", StringComparison.Ordinal);

        var rendered = ProvisionRolesRenderer.Render(crlf, "app", "app", Passwords);

        Assert.Equal(5, Count(rendered, "FOR ROLE \"app\" IN SCHEMA"));
        Assert.Equal(3, Count(rendered, "ON DATABASE \"app\""));
        Assert.DoesNotContain("CHANGE_ME_MCP_PASSWORD", rendered, StringComparison.Ordinal);
        Assert.Contains("\r\n", rendered, StringComparison.Ordinal);
        Assert.Equal(Count(rendered, "\n"), Count(rendered, "\r\n"));
    }

    [Fact]
    public void CheckRolePassword_Accepts_AndDropsOneTrailingNewline()
    {
        Assert.Null(ProvisionRolesRenderer.CheckRolePassword("web", "0123456789abcdef\n", out var password));
        Assert.Equal("0123456789abcdef", password);
    }

    [Theory]
    [InlineData("", "empty")]
    [InlineData("0123456789abcde", "shorter than 16")]
    [InlineData("abc;0123456789abcdef", "outside")]
    [InlineData("0123456789abcdef\r\n", "carriage return")]
    public void CheckRolePassword_Refuses_NamingTheRole_NeverTheValue(string raw, string expectedText)
    {
        var refusal = ProvisionRolesRenderer.CheckRolePassword("mcp", raw, out _);

        Assert.NotNull(refusal);
        Assert.Contains(expectedText, refusal, StringComparison.Ordinal);
        Assert.Contains("mcp", refusal, StringComparison.Ordinal);
        if (raw.Length > 0)
        {
            Assert.DoesNotContain(raw.TrimEnd('\r', '\n'), refusal, StringComparison.Ordinal);
        }
    }

    [Fact]
    public void PgPassLine_EscapesColonAndBackslash()
    {
        Assert.Equal("fe80\\:\\:1:5432:a\\\\b:app:pw", ProvisionRolesRenderer.PgPassLine("fe80::1", 5432, "a\\b", "app", "pw"));
    }

    [Fact]
    public void LibpqEnvFile_QuotesValues()
    {
        var withCert = ProvisionRolesRenderer.LibpqEnvFile("db", 5432, "app", "o'brien", "verify-full", "/etc/ca.crt");
        Assert.Contains("PGUSER='o'\\''brien'\n", withCert, StringComparison.Ordinal);
        Assert.Contains("PGSSLROOTCERT='/etc/ca.crt'\n", withCert, StringComparison.Ordinal);
        Assert.Contains("PGAPPNAME='darling-provision-roles'\n", withCert, StringComparison.Ordinal);

        var without = ProvisionRolesRenderer.LibpqEnvFile("db", 5432, "app", "app", "require", null);
        Assert.DoesNotContain("PGSSLROOTCERT", without, StringComparison.Ordinal);
    }

    [Theory]
    [InlineData(SslMode.Disable, "disable")]
    [InlineData(SslMode.Allow, "allow")]
    [InlineData(SslMode.Prefer, "prefer")]
    [InlineData(SslMode.Require, "require")]
    [InlineData(SslMode.VerifyCA, "verify-ca")]
    [InlineData(SslMode.VerifyFull, "verify-full")]
    public void ToLibpqSslMode_MapsEveryMode(SslMode mode, string expected)
    {
        Assert.Equal(expected, ProvisionRolesRenderer.ToLibpqSslMode(mode));
    }
}
