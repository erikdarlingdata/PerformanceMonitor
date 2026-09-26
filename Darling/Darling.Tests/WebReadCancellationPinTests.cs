/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using System.Collections.Generic;
using System.Linq;
using System.Reflection;
using System.Threading;
using System.Threading.Tasks;
using Microsoft.AspNetCore.Http;
using ModelContextProtocol.Server;
using Npgsql;
using PerformanceMonitor.Darling.Analysis;
using PerformanceMonitor.Darling.Service;
using PerformanceMonitor.Darling.Service.Mcp;
using Xunit;

namespace Darling.Tests;

/// <summary>
/// #4203: a superseded or abandoned <c>/api/read/{tool}</c> request must stop its store query, not just stop
/// answering it. Both halves of the ratchet read the CODE, never a hand-written tool list — the same shape
/// <see cref="LiveCleanupConversionRatchetTests"/> uses for its own conversion sweep — and the one hand-kept
/// list, <see cref="DarlingWebEndpoints.CancellationAllowlist"/>, may only shrink: a PR converts a tool's
/// dispatch entry AND its <c>[McpServerTool]</c> method together and removes the name here, never adds one.
///
/// <para><b>Half 1</b> proves the DISPATCH TABLE: every entry not on the allowlist is invoked with an
/// already-cancelled <see cref="HttpContext.RequestAborted"/> and a data source pointed at a closed local
/// port, and must throw <see cref="OperationCanceledException"/> — not <see cref="NpgsqlException"/>, which is
/// what an untokened call produces when it actually dials the dead port instead of observing the token first.
/// <b>Half 2</b> proves the TOOL METHODS: reflection over every <c>[McpServerTool]</c> method that takes an
/// <see cref="NpgsqlDataSource"/> asserts it also takes a <see cref="CancellationToken"/>, unless allowlisted.
/// A tool could pass either check alone — a dispatch entry that passes a token to a method with nowhere to put
/// it, or a method with a token parameter nothing ever fills in — so both run.</para>
/// </summary>
public sealed class WebReadCancellationPinTests
{
    /// <summary>
    /// A data source that never dials. Port 1 is never listening, and — exactly as
    /// <c>DarlingAnalysisBudgetTests.DeadStore</c> documents for the same connection string — an
    /// ALREADY-cancelled token is observed before the first socket call, so every case here cancels rather
    /// than racing a real connection attempt.
    /// </summary>
    private static NpgsqlDataSource DeadStore() =>
        new NpgsqlDataSourceBuilder("Host=127.0.0.1;Port=1;Username=none;Password=none;Database=none;Timeout=2")
            .Build();

    /// <summary>
    /// An <see cref="HttpContext"/> whose request is already aborted, carrying query values for the few
    /// required text parameters (<c>get_query_trend</c>'s <c>query_hash</c> / <c>database_name</c>, and
    /// <c>get_wait_trend</c>'s <c>wait_type</c>) so a dispatch entry with a synchronous "is this parameter
    /// present" gate still reaches its store call instead of returning a missing-parameter envelope with
    /// nothing cancelled.
    /// </summary>
    private static HttpContext CancelledRequest()
    {
        var context = new DefaultHttpContext();
        context.Request.QueryString = new QueryString("?query_hash=deadbeef&database_name=probe&counter_name=x&wait_type=CXPACKET");
        context.RequestAborted = new CancellationToken(canceled: true);
        return context;
    }

    /* ── half 1: the dispatch table ── */

    public static TheoryData<string> ConvertedDispatchEntries()
    {
        var data = new TheoryData<string>();
        foreach (var name in DarlingWebEndpoints.BuildReadDispatch().Keys
                     .Except(DarlingWebEndpoints.CancellationAllowlist)
                     .OrderBy(n => n, StringComparer.Ordinal))
        {
            data.Add(name);
        }

        return data;
    }

    [Theory]
    [MemberData(nameof(ConvertedDispatchEntries))]
    public async Task AConvertedReadEndpoint_ObservesCancellation_InsteadOfDialingTheStore(string name)
    {
        /* #4203 get_store_host: BuildReadDispatch's postgresConfig defaults to null for a bare caller (this
           test's own doc comment on BuildReadDispatch), and the tool short-circuits to an "unavailable"
           envelope before touching the store when that happens — never reaching the cancellation check this
           theory proves. A non-null PostgresConfig here reaches past that guard into the real gather path,
           the same way the rest of this theory's entries reach their own store call. */
        var dispatch = DarlingWebEndpoints.BuildReadDispatch(postgresConfig: new PostgresConfig { ConnectionString = "Host=127.0.0.1;Port=1;Username=none;Password=none;Database=none;Timeout=2" });
        await using var store = DeadStore();
        var analysis = new DarlingAnalysisService(store);
        var context = CancelledRequest();

        var handler = dispatch[name];

        /* OperationCanceledException, specifically: an unconverted entry reaches Npgsql's own connection
           attempt against the dead port and throws NpgsqlException instead, which is the exact regression
           this proves absent — the token reaching the connection open, not merely existing on a signature. */
        await Assert.ThrowsAsync<OperationCanceledException>(() => handler(context, store, analysis));
    }

    [Fact]
    public void ConvertedDispatchEntries_IsNotVacuous()
    {
        Assert.True(
            DarlingWebEndpoints.BuildReadDispatch().Keys.Except(DarlingWebEndpoints.CancellationAllowlist).Any(),
            "every dispatch entry is on the allowlist, so the theory above ran zero cases — a green run over "
            + "nothing is indistinguishable from a converted tree.");
    }

    /* ── half 2: the tool methods ── */

    /// <summary>
    /// Every <c>[McpServerTool]</c> method that takes an <see cref="NpgsqlDataSource"/> AND is on the
    /// <c>/api/read/*</c> surface (<see cref="DarlingWebEndpoints.BuildReadDispatch"/>'s keys — the catalog
    /// minus <see cref="DarlingWebEndpoints.ExcludedToolNames"/>, the same scope
    /// <c>DarlingWebEndpointsTests</c> pins). #4203 is about the READ surface an abandoned request can leave
    /// running; a write tool (<c>create_custom_view</c>, <c>mute_analysis_finding</c>, ...) is a different
    /// question this issue never raised, and scanning past that scope reported them as offenders with no
    /// dispatch entry to convert and no allowlist to shrink them off of.
    /// </summary>
    private static IEnumerable<(string Name, MethodInfo Method)> ReflectStoreReadingToolMethods()
    {
        var readSurface = DarlingWebEndpoints.BuildReadDispatch().Keys.ToHashSet(StringComparer.Ordinal);
        var assembly = typeof(DarlingMcpTools).Assembly;
        foreach (var type in assembly.GetTypes())
        {
            if (type.GetCustomAttribute<McpServerToolTypeAttribute>() is null)
            {
                continue;
            }

            foreach (var method in type.GetMethods(
                         BindingFlags.Public | BindingFlags.NonPublic | BindingFlags.Static | BindingFlags.Instance))
            {
                var attr = method.GetCustomAttribute<McpServerToolAttribute>();
                if (attr?.Name is not { Length: > 0 } name || !readSurface.Contains(name))
                {
                    continue;
                }

                if (method.GetParameters().Any(p => p.ParameterType == typeof(NpgsqlDataSource)))
                {
                    yield return (name, method);
                }
            }
        }
    }

    [Fact]
    public void EveryStoreReadingToolMethod_TakesACancellationToken_UnlessAllowlisted()
    {
        var offenders = ReflectStoreReadingToolMethods()
            .Where(t => !DarlingWebEndpoints.CancellationAllowlist.Contains(t.Name))
            .Where(t => !t.Method.GetParameters().Any(p => p.ParameterType == typeof(CancellationToken)))
            .Select(t => t.Name)
            .OrderBy(n => n, StringComparer.Ordinal)
            .ToArray();

        Assert.True(offenders.Length == 0,
            "these [McpServerTool] methods take an NpgsqlDataSource but no CancellationToken, and are not in "
            + "DarlingWebEndpoints.CancellationAllowlist: " + string.Join(", ", offenders));
    }

    [Fact]
    public void ReflectStoreReadingToolMethods_IsNotVacuous()
    {
        Assert.True(ReflectStoreReadingToolMethods().Any(),
            "no [McpServerTool] method takes an NpgsqlDataSource — the reflection scan found nothing, so the "
            + "assertion above held over an empty set.");
    }

    /* ── the allowlist itself ── */

    [Fact]
    public void CancellationAllowlist_NamesOnlyRealReadTools()
    {
        var catalog = DarlingWebEndpoints.BuildReadDispatch().Keys.ToHashSet(StringComparer.Ordinal);
        foreach (var name in DarlingWebEndpoints.CancellationAllowlist)
        {
            Assert.Contains(name, catalog);
        }
    }

    /// <summary>
    /// A name stays on the allowlist only while its tool method has nowhere to put a token. Once BOTH halves
    /// convert — the method gains a <see cref="CancellationToken"/> parameter and its dispatch entry passes
    /// <c>RequestAborted</c> — the entry is stale and must be removed, the discipline
    /// <see cref="LiveCleanupConversionRatchetTests"/> holds for its own ceiling.
    /// </summary>
    [Fact]
    public void CancellationAllowlist_HasNoStaleEntry()
    {
        var toolMethods = ReflectStoreReadingToolMethods()
            .ToDictionary(t => t.Name, t => t.Method, StringComparer.Ordinal);

        var stale = DarlingWebEndpoints.CancellationAllowlist
            .Where(name => toolMethods.TryGetValue(name, out var method)
                           && method.GetParameters().Any(p => p.ParameterType == typeof(CancellationToken)))
            .OrderBy(n => n, StringComparer.Ordinal)
            .ToArray();

        Assert.True(stale.Length == 0,
            "these allowlist entries already have a CancellationToken-taking tool method and must be removed "
            + "from DarlingWebEndpoints.CancellationAllowlist: " + string.Join(", ", stale));
    }
}
