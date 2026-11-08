/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

#nullable enable

using System;
using System.IO;
using System.Linq;
using System.Reflection;
using System.Text.Json;
using System.Text.Json.Nodes;
using System.Threading;
using Microsoft.AspNetCore.Http;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Logging.Abstractions;
using PerformanceMonitor.Darling.Service;
using PerformanceMonitor.Darling.Storage;
using Xunit;

namespace Darling.Tests;

/// <summary>
/// #8323: a saved notebook cell the store stops at its statement timeout answers a plain sentence of the product's own, naming
/// the window and the server set the cell was reading, with the engine's text behind <c>detail</c>. A panel author's own
/// query error (#4283) still answers the engine's text at the top, with no detail.
/// </summary>
public sealed class ComposeStatementTimeoutAnswerTests
{
    private static IResult Answer(ComposeRunOutcome outcome, JsonObject body)
    {
        var method = typeof(DarlingWebEndpoints)
            .GetMethods(BindingFlags.Static | BindingFlags.NonPublic | BindingFlags.Public)
            .FirstOrDefault(m => m.Name == "ComposeRunFailureResult" && m.GetParameters().Any(p => p.ParameterType == typeof(JsonObject)));
        if (method is null)
        {
            Assert.Fail("ComposeRunFailureResult never sees the run's body, so a statement timeout answers the engine's sentence and says nothing about the window or the servers.");
        }

        var parameters = method!.GetParameters();
        var args = new object?[parameters.Length];
        for (var i = 0; i < parameters.Length; i++)
        {
            args[i] = parameters[i].ParameterType == typeof(ComposeRunOutcome) ? outcome
                : parameters[i].ParameterType == typeof(JsonObject) ? body
                : parameters[i].ParameterType == typeof(string) ? "/api/compose/run"
                : parameters[i].ParameterType == typeof(long) ? 65_000L
                : parameters[i].ParameterType == typeof(CancellationToken) ? CancellationToken.None
                : parameters[i].ParameterType.IsAssignableFrom(typeof(NullLogger)) ? NullLogger.Instance
                : parameters[i].HasDefaultValue ? parameters[i].DefaultValue
                : null;
        }

        return (IResult)method.Invoke(null, args)!;
    }

    private static JsonDocument ResultBody(IResult result, out int statusCode)
    {
        var services = new ServiceCollection();
        services.AddOptions();
        services.AddLogging();
        var context = new DefaultHttpContext { RequestServices = services.BuildServiceProvider() };
        var stream = new MemoryStream();
        context.Response.Body = stream;

        result.ExecuteAsync(context).GetAwaiter().GetResult();

        statusCode = context.Response.StatusCode;
        stream.Position = 0;
        return JsonDocument.Parse(stream);
    }

    private static ComposeRunOutcome StatementTimeout() =>
        ComposeRunOutcome.AuthorQueryError(ComposePanelRunner.StatementTimeoutText, QueryStoreComposeStamp.StatementTimeoutSqlState);

    [Fact]
    public void AStatementTimeout_AnswersAPlainSentence_NamingTheWindowAndTheServers_WithTheEngineTextBehindDetail()
    {
        var body = new JsonObject { ["panel"] = new JsonObject(), ["hours"] = 24, ["server"] = new JsonArray("a", "b", "c") };

        using var json = ResultBody(Answer(StatementTimeout(), body), out var statusCode);

        Assert.Equal(StatusCodes.Status400BadRequest, statusCode);
        var error = json.RootElement.GetProperty("error").GetString()!;
        Assert.Contains("statement timeout", error, StringComparison.Ordinal);
        Assert.Contains("24 hours", error, StringComparison.Ordinal);
        Assert.Contains("3 servers", error, StringComparison.Ordinal);
        Assert.DoesNotContain("canceling statement", error, StringComparison.Ordinal);
        Assert.Equal(ComposePanelRunner.StatementTimeoutText, json.RootElement.GetProperty("detail").GetString());
    }

    [Fact]
    public void AStatementTimeout_OverAnExplicitWindow_NamesItsEnds_AndEveryServerWhenNoneIsNamed()
    {
        var body = new JsonObject
        {
            ["panel"] = new JsonObject(),
            ["windowStart"] = "2026-01-01T00:00:00Z",
            ["windowEnd"] = "2026-01-02T06:30:00Z",
        };

        using var json = ResultBody(Answer(StatementTimeout(), body), out var statusCode);

        Assert.Equal(StatusCodes.Status400BadRequest, statusCode);
        var error = json.RootElement.GetProperty("error").GetString()!;
        Assert.Contains("2026-01-01 00:00", error, StringComparison.Ordinal);
        Assert.Contains("2026-01-02 06:30", error, StringComparison.Ordinal);
        Assert.Contains("every server", error, StringComparison.Ordinal);
        Assert.Equal(ComposePanelRunner.StatementTimeoutText, json.RootElement.GetProperty("detail").GetString());
    }

    [Fact]
    public void AStatementTimeout_WithOneServer_SaysOneServer()
    {
        var body = new JsonObject { ["panel"] = new JsonObject(), ["server"] = "orders" };

        using var json = ResultBody(Answer(StatementTimeout(), body), out _);

        var error = json.RootElement.GetProperty("error").GetString()!;
        Assert.Contains("1 server", error, StringComparison.Ordinal);
        Assert.DoesNotContain("1 servers", error, StringComparison.Ordinal);
        Assert.Contains("24 hours", error, StringComparison.Ordinal);
    }

    /// <summary>#4283: the author's own syntax error is theirs to fix, so the engine's text stays the answer and nothing sits behind it.</summary>
    [Fact]
    public void ASyntaxError_StillAnswersTheEnginesText_WithNoDetail()
    {
        var outcome = ComposeRunOutcome.AuthorQueryError("Query failed: syntax error at or near \"selct\"", "42601");
        var body = new JsonObject { ["panel"] = new JsonObject(), ["hours"] = 24 };

        using var json = ResultBody(Answer(outcome, body), out var statusCode);

        Assert.Equal(StatusCodes.Status400BadRequest, statusCode);
        Assert.Equal("Query failed: syntax error at or near \"selct\"", json.RootElement.GetProperty("error").GetString());
        Assert.False(json.RootElement.TryGetProperty("detail", out _));
    }
}
