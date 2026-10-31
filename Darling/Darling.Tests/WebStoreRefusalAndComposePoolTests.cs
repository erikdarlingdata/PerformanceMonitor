/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using System.IO;
using System.Net;
using System.Net.Sockets;
using System.Text;
using System.Threading;
using System.Threading.Tasks;
using Microsoft.AspNetCore.Builder;
using Microsoft.AspNetCore.Http;
using Microsoft.AspNetCore.TestHost;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Logging;
using Npgsql;
using PerformanceMonitor.Darling.Service.Mcp;
using PerformanceMonitor.Darling.Service;
using PerformanceMonitor.Darling.Service.Hosting;
using Xunit;

namespace Darling.Tests;

/// <summary>
/// Two behaviours of the web host's store login split, neither needing a store. A write the store refuses for privilege (SQLSTATE 42501,
/// what the read-only <c>viewer</c> login gets when the web login's credential is missing and the host falls back to it) answers with the
/// 403 sentence of <see cref="DarlingWebStoreWrites"/> and not a 500; a read keeps its 500. And <c>/api/compose/run</c> connects through the
/// read-only pool it is given, not the write pool.
/// </summary>
public sealed class WebStoreRefusalAndComposePoolTests
{
    private const string PermissionSentence = "Error during create_mute_rule: 42501: permission denied for table config_mute_rules";

    private static async Task<(int Status, string Body)> SendAsync(string method, string path, bool managedStore)
    {
        var ct = TestContext.Current.CancellationToken;
        var builder = WebApplication.CreateBuilder(new WebApplicationOptions());
        builder.WebHost.UseTestServer();
        builder.Logging.ClearProviders();
        await using var app = builder.Build();
        var logger = new CapturingTestLogger();
        app.UseMiddleware<DarlingWebFailureObserver>((ILogger)logger, new DarlingHttpRefusalLog(TimeSpan.FromMinutes(1), 16), managedStore);
        app.Map("/api/store-refusal", () => DarlingWebEndpoints.ServerErrorResult(PermissionSentence, "/api/store-refusal", logger, 1, CancellationToken.None));
        await app.StartAsync(ct);
        using var server = app.GetTestServer();
        var context = await server.SendAsync(request =>
        {
            request.Request.Method = method;
            request.Request.Path = path;
            request.Request.Headers.Host = "localhost";
        }, ct);
        var text = await new StreamReader(context.Response.Body).ReadToEndAsync(ct);
        await app.StopAsync(ct);
        return (context.Response.StatusCode, text);
    }

    [Theory]
    [InlineData("POST", true)]
    [InlineData("PUT", false)]
    [InlineData("DELETE", true)]
    public async Task AWriteTheStoreRefusesForPrivilege_Answers403WithTheStoreSentence(string method, bool managedStore)
    {
        var (status, body) = await SendAsync(method, "/api/store-refusal", managedStore);

        Assert.Equal(StatusCodes.Status403Forbidden, status);
        var error = System.Text.Json.JsonDocument.Parse(body).RootElement.GetProperty("error").GetString();
        Assert.Equal(DarlingWebStoreWrites.Refused("make this change", managedStore, webLoginWrite: true), error);
        if (managedStore)
        {
            Assert.Contains("lists only", error, StringComparison.Ordinal);
        }
        else
        {
            // A store you bring: the sentence names the web role, never the admin login.
            Assert.Contains("postgres.webConnectionString at the web role", error, StringComparison.Ordinal);
            Assert.DoesNotContain("admin", error, StringComparison.OrdinalIgnoreCase);
        }
        Assert.DoesNotContain("42501", body, StringComparison.Ordinal);
    }

    [Fact]
    public async Task AReadTheStoreRefusesForPrivilege_KeepsItsServerError()
    {
        var (status, body) = await SendAsync("GET", "/api/store-refusal", managedStore: true);

        Assert.Equal(StatusCodes.Status500InternalServerError, status);
        Assert.DoesNotContain("lists only", body, StringComparison.Ordinal);
    }

    private sealed class CountingListener : IDisposable
    {
        private readonly TcpListener _listener = new(IPAddress.Loopback, 0);
        private readonly CancellationTokenSource _stop = new();
        private int _accepted;

        public CountingListener()
        {
            _listener.Start();
            _ = Task.Run(async () =>
            {
                try
                {
                    while (!_stop.IsCancellationRequested)
                    {
                        using var client = await _listener.AcceptTcpClientAsync(_stop.Token);
                        Interlocked.Increment(ref _accepted);
                    }
                }
                catch (OperationCanceledException)
                {
                }
                catch (ObjectDisposedException)
                {
                }
            });
        }

        public int Accepted => Volatile.Read(ref _accepted);

        public string ConnectionString =>
            $"Host=127.0.0.1;Port={((IPEndPoint)_listener.LocalEndpoint).Port};Username=x;Password=x;Database=x;Timeout=1;Pooling=false";

        public void Dispose()
        {
            _stop.Cancel();
            _listener.Stop();
            _stop.Dispose();
        }
    }

    [Fact]
    public async Task TheComposeRun_ConnectsThroughTheReadPool_NeverTheWritePool()
    {
        using var write = new CountingListener();
        using var read = new CountingListener();
        await using var writeStore = NpgsqlDataSource.Create(write.ConnectionString);
        await using var readStore = NpgsqlDataSource.Create(read.ConnectionString);
        var ct = TestContext.Current.CancellationToken;

        var builder = WebApplication.CreateBuilder(new WebApplicationOptions());
        builder.WebHost.UseTestServer();
        builder.Logging.ClearProviders();
        builder.Services.AddSingleton(writeStore);
        await using var app = builder.Build();
        DarlingWebEndpoints.MapAll(app, writeStore, new CollectorRuntimeState(), new CapturingTestLogger(), composeReadPostgres: readStore);
        await app.StartAsync(ct);
        using var server = app.GetTestServer();

        await server.SendAsync(request =>
        {
            request.Request.Method = "POST";
            request.Request.Path = "/api/compose/run";
            request.Request.Headers.Host = "localhost";
            request.Request.ContentType = "application/json";
            request.Request.Body = new MemoryStream(Encoding.UTF8.GetBytes(
                "{\"panel\":{\"source\":\"query_stats\",\"measure\":\"query_worker_us\",\"aggregate\":\"sum\",\"timeBucket\":\"day\",\"viz\":\"line\"}}"));
        }, ct);

        Assert.True(read.Accepted > 0, "the compose run never reached the read pool");
        Assert.Equal(0, write.Accepted);
        await app.StopAsync(ct);
    }
}
