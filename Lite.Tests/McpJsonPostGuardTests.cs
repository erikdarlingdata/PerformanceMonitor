/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor Lite.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using System.IO;
using System.Net.Http;
using System.Text;
using System.Threading.Tasks;
using Microsoft.AspNetCore.Builder;
using Microsoft.AspNetCore.Hosting;
using Microsoft.AspNetCore.Http;
using Microsoft.AspNetCore.TestHost;
using PerformanceMonitor.Common;
using PerformanceMonitorLite.Mcp;
using Xunit;

namespace PerformanceMonitorLite.Tests;

/// <summary>
/// Lite's MCP host answers 415 to a POST whose Content-Type is not JSON, before <c>MapMcp</c>, the same as
/// Darling's MCP host. This pins 415 for every non-JSON type in the matrix, and the install order: Host guard,
/// this guard, <c>MapMcp</c>. The middleware is run through a real pipeline with a terminal handler that stands
/// in for <c>MapMcp</c> and counts the requests it receives.
/// </summary>
[Trait("Stage", "Guard")]
public sealed class McpJsonPostGuardTests
{
    private static async Task<(int Status, int Reached)> PostAsync(string method, Action<HttpRequestMessage>? shape)
    {
        var reached = 0;
        var builder = WebApplication.CreateBuilder();
        builder.WebHost.UseTestServer();
        await using var app = builder.Build();
        McpHostService.UseJsonPostGuard(app);
        app.Run(context =>
        {
            reached++;
            context.Response.StatusCode = StatusCodes.Status200OK;
            return Task.CompletedTask;
        });
        await app.StartAsync();

        using var client = app.GetTestClient();
        using var request = new HttpRequestMessage(new HttpMethod(method), "/");
        shape?.Invoke(request);
        using var response = await client.SendAsync(request);
        return ((int)response.StatusCode, reached);
    }

    private static void Body(HttpRequestMessage request, string contentType, string text = "{\"jsonrpc\":\"2.0\"}")
    {
        request.Content = new ByteArrayContent(Encoding.UTF8.GetBytes(text));
        if (contentType.Length == 0)
        {
            request.Content.Headers.Remove("Content-Type");
        }
        else
        {
            request.Content.Headers.TryAddWithoutValidation("Content-Type", contentType);
        }
    }

    [Theory]
    [InlineData("")]
    [InlineData("application/problem+json")]
    [InlineData("text/plain")]
    [InlineData("text/plain;charset=UTF-8")]
    [InlineData("application/x-www-form-urlencoded")]
    [InlineData("multipart/form-data; boundary=x")]
    [InlineData("application/jsonx")]
    public async Task APostThatIsNotJson_Gets415_AndNeverReachesTheHandler(string contentType)
    {
        var (status, reached) = await PostAsync("POST", r => Body(r, contentType));

        Assert.Equal(StatusCodes.Status415UnsupportedMediaType, status);
        Assert.Equal(0, reached);
    }

    [Theory]
    [InlineData("application/json")]
    [InlineData("application/json; charset=utf-8")]
    [InlineData("Application/JSON;charset=UTF-8")]
    public async Task APostThatIsJson_ReachesTheHandler(string contentType)
    {
        var (status, reached) = await PostAsync("POST", r => Body(r, contentType));

        Assert.Equal(StatusCodes.Status200OK, status);
        Assert.Equal(1, reached);
    }

    [Theory]
    [InlineData("GET")]
    [InlineData("DELETE")]
    public async Task ARequestThatIsNotAPost_IsNotChecked(string method)
    {
        var (status, reached) = await PostAsync(method, shape: null);

        Assert.Equal(StatusCodes.Status200OK, status);
        Assert.Equal(1, reached);
    }

    [Theory]
    [InlineData(null, false)]
    [InlineData("", false)]
    [InlineData("application/json", true)]
    [InlineData("application/json; charset=utf-8", true)]
    [InlineData("APPLICATION/JSON", true)]
    [InlineData(" application/json ;charset=utf-8", true)]
    [InlineData("application/problem+json", false)]
    [InlineData("text/plain", false)]
    public void IsJson_Matrix(string? contentType, bool expected)
        => Assert.Equal(expected, JsonContentType.IsJson(contentType));
}
