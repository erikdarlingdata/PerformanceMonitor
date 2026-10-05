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
using System.Net;
using System.Net.Http;
using System.Runtime.CompilerServices;
using System.Text;
using System.Text.RegularExpressions;
using System.Threading.Tasks;
using Microsoft.AspNetCore.Builder;
using Microsoft.AspNetCore.Hosting;
using Microsoft.AspNetCore.Http;
using Microsoft.AspNetCore.TestHost;
using Microsoft.Extensions.Logging;
using Npgsql;
using PerformanceMonitor.Darling.Service;
using PerformanceMonitor.Darling.Service.Hosting;
using Xunit;

namespace Darling.Tests;

/// <summary>
/// The web add-server route accepts an <c>env:</c> / <c>file:</c> password only when the operator allowed it, and
/// never for something the service owns.
/// </summary>
public sealed class ServerAddSecretReferencePolicyTests
{
    private static readonly string Root = Path.GetFullPath(Path.Combine(Path.GetTempPath(), "darling-secrets"));
    private static readonly string Owned = Path.Combine(Root, "own");

    private static string? Check(string? password, params string[] allowed) =>
        DarlingWebSecretReferencePolicy.Refusal(
            password, allowed, new[] { Owned }, new[] { "DARLING_CONFIG" });

    [Theory]
    [InlineData("file:/etc/passwd")]
    [InlineData("env:HOME")]
    public void AnEmptyAllowlist_RefusesBothReferenceKinds(string password)
    {
        Assert.Equal(DarlingWebSecretReferencePolicy.RefusalText, Check(password));
    }

    [Fact]
    public void AnAllowedPrefix_Passes()
    {
        Assert.Null(Check("file:" + Path.Combine(Root, "ok", "pw"), "file:" + Path.Combine(Root, "ok")));
        Assert.Null(Check("env:SQLADD_PROD", "env:SQLADD_"));
    }

    [Fact]
    public void ASiblingPrefix_DotDotAndServiceOwnedTargets_AreRefused()
    {
        var allowed = "file:" + Path.Combine(Root, "ok");
        var refusals = new[]
        {
            Check("file:" + Path.Combine(Root, "okX", "pw"), allowed),
            Check("file:" + Path.Combine(Root, "ok", "..", "own", "darling.json"), allowed),
            Check("file:" + Path.Combine(Root, "ok", "..", "elsewhere"), allowed),
            Check("file:relative/pw", allowed),
            Check("file:" + Path.Combine(Owned, "darling.json"), "file:" + Root),
            Check("env:DARLING_CONFIG", "env:DARLING_"),
        };

        Assert.All(refusals, r => Assert.Equal(DarlingWebSecretReferencePolicy.RefusalText, r));
    }

    [Theory]
    [InlineData(null)]
    [InlineData("")]
    [InlineData("   ")]
    [InlineData("hunter2")]
    [InlineData("a:file:b")]
    public void ALiteralOrEmptyValue_IsNotThePoliciesToDecide(string? password)
    {
        Assert.Null(Check(password));
    }

    [Fact]
    public void ConfigEntries_MustBeFileOrEnv_AndFilePathsAbsolute()
    {
        DarlingConfig Cfg(params string[] entries)
        {
            var c = new DarlingConfig();
            c.Web.ServerAddSecretReferences = entries.ToList();
            return c;
        }

        Assert.Empty(new DarlingConfig().Web.ServerAddSecretReferences);
        Assert.Contains(Cfg("/run/x").Validate(), p => p.Contains("serverAddSecretReferences", StringComparison.Ordinal));
        Assert.Contains(Cfg("file:relative").Validate(), p => p.Contains("absolute", StringComparison.Ordinal));
        Assert.DoesNotContain(Cfg("file:" + Root, "env:X_").Validate(), p => p.Contains("serverAddSecretReferences", StringComparison.Ordinal));
    }

    [Fact]
    public void TheScope_OwnsTheConfigDirectoryAndTheFilesTheConfigNames()
    {
        var cfg = new DarlingConfig();
        cfg.Web.Network = new WebNetworkConfig { Tls = new WebTlsConfig { CertPath = Path.Combine(Root, "tls", "c.pem") } };
        cfg.Postgres.ConnectionString = "file:" + Path.Combine(Root, "pg", "cs");
        cfg.Smtp.Password = "env:SMTP_PW_VAR";
        var scope = DarlingWebSecretReferencePolicy.ScopeFor(cfg, Path.Combine(Root, "cfg", "darling.json"));

        Assert.Contains(Path.Combine(Root, "cfg"), scope.ServiceOwnedPaths);
        Assert.Contains(Path.Combine(Root, "tls", "c.pem"), scope.ServiceOwnedPaths);
        Assert.Contains(Path.Combine(Root, "pg", "cs"), scope.ServiceOwnedPaths);
        Assert.Contains("SMTP_PW_VAR", scope.ServiceOwnedEnvNames);
    }

    [Fact]
    public void EveryEnvironmentVariableTheServiceReads_IsInTheOwnedSet()
    {
        var repo = Path.GetFullPath(Path.Combine(Path.GetDirectoryName(ThisFile())!, ".."));
        var missing = new List<string>();
        foreach (var dir in new[] { "PerformanceMonitor.Darling.Service", "PerformanceMonitor.Darling.Storage" })
        {
            foreach (var file in Directory.EnumerateFiles(Path.Combine(repo, dir), "*.cs", SearchOption.AllDirectories)
                .Where(f => !f.Contains(Path.DirectorySeparatorChar + "obj" + Path.DirectorySeparatorChar, StringComparison.Ordinal)))
            {
                var text = File.ReadAllText(file);
                foreach (Match m in Regex.Matches(text, "GetEnvironmentVariable\\(\\s*\"([^\"]+)\""))
                {
                    if (!DarlingWebSecretReferencePolicy.ServiceEnvNames.Contains(m.Groups[1].Value))
                    {
                        missing.Add(m.Groups[1].Value);
                    }
                }

                /* A constant-named read: the constant's literal must be listed too. */
                foreach (Match m in Regex.Matches(text, "GetEnvironmentVariable\\(\\s*([A-Z][A-Za-z0-9_]*)\\s*[,)]"))
                {
                    var lit = Regex.Match(text, "\\b" + m.Groups[1].Value + "\\s*=\\s*\"([^\"]+)\"");
                    if (lit.Success && !DarlingWebSecretReferencePolicy.ServiceEnvNames.Contains(lit.Groups[1].Value))
                    {
                        missing.Add(lit.Groups[1].Value);
                    }
                }
            }
        }

        Assert.Empty(missing.Distinct());
    }

    [Fact]
    public async Task TheRoute_RefusesAFileReference_WithTheDefaultScope_AndNeverReachesTheCore()
    {
        var builder = WebApplication.CreateBuilder();
        builder.WebHost.UseTestServer();
        builder.Logging.ClearProviders();
        await using var app = builder.Build();
        app.Use(async (context, next) =>
        {
            context.Items[DarlingWebSeat.HttpContextItemKey] = new DarlingWebSeat("alice", true);
            await next(context);
        });
        var calls = 0;
        using var source = NpgsqlDataSource.Create("Host=localhost;Database=x");
        DarlingWebEndpoints.MapServers(app, source, app.Logger, _ =>
        {
            calls++;
            return Task.FromResult("{}");
        });
        await app.StartAsync(TestContext.Current.CancellationToken);

        using var request = new HttpRequestMessage(HttpMethod.Post, "/api/servers")
        {
            Content = new StringContent(
                "[{\"host\":\"sql01\",\"auth\":\"SQL\",\"username\":\"monitor\",\"password\":\"file:/etc/hostname\"}]",
                Encoding.UTF8, "application/json"),
        };
        using var response = await app.GetTestClient().SendAsync(request, TestContext.Current.CancellationToken);
        var body = await response.Content.ReadAsStringAsync(TestContext.Current.CancellationToken);

        Assert.Equal(HttpStatusCode.BadRequest, response.StatusCode);
        Assert.Contains("web.serverAddSecretReferences", body, StringComparison.Ordinal);
        Assert.Equal(0, calls);
    }

    private static string ThisFile([CallerFilePath] string path = "") => path;
}
