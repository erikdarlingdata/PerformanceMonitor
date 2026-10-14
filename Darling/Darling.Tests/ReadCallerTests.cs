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
using System.Text.RegularExpressions;
using PerformanceMonitor.Darling.Service;
using Xunit;

namespace Darling.Tests;

/// <summary>
/// Pins the read caller (#5793): the MCP caller string (client info name and version, else the User-Agent's first product
/// token, else <c>unknown</c>, held to 80 characters), the web caller (a salted hash of the session key that two reads of one
/// session share, two sessions do not, and that is neither the key nor a prefix of it), the accumulator's distinct-caller
/// count and its cap, and a name scan that fails if any recorder input or stored column is named like personal data.
/// </summary>
public sealed class ReadCallerTests
{
    [Fact]
    public void AnMcpClientWithANameAndVersion_IsStoredAsNameSpaceVersion()
    {
        Assert.Equal("claude-desktop 1.2.3", ReadCaller.OfMcpClient("claude-desktop", "1.2.3", null));
        Assert.Equal("claude-desktop 1.2.3", ReadCaller.OfMcpClient("  claude-desktop ", " 1.2.3 ", null));
        Assert.Equal("claude-desktop", ReadCaller.OfMcpClient("claude-desktop", null, null));
    }

    [Fact]
    public void ALongClientNameAndVersion_IsTrimmedTo80Characters()
    {
        var caller = ReadCaller.OfMcpClient(new string('n', 70), "1.2.3.4.5.6.7.8.9", null);
        Assert.Equal(ReadCaller.MaxLength, caller.Length);
        Assert.Equal(80, ReadCaller.MaxLength);
        Assert.StartsWith(new string('n', 70) + " 1.2.3.4.", caller, StringComparison.Ordinal);

        var agent = ReadCaller.OfMcpClient(null, null, "x/" + new string('9', 200));
        Assert.Equal(80, agent.Length);
        Assert.StartsWith("ua:x/999", agent, StringComparison.Ordinal);
    }

    [Fact]
    public void AClientWithNoName_IsUnknown()
    {
        Assert.Equal("unknown", ReadCaller.OfMcpClient(null, null, null));
        Assert.Equal("unknown", ReadCaller.OfMcpClient("", "1.0", ""));
        Assert.Equal("unknown", ReadCaller.OfMcpClient("   ", "1.0", "   "));
        Assert.Equal("unknown", ReadCaller.OfMcpClient(null, null, "(compatible)"));
    }

    [Fact]
    public void ClientInfo_WinsOverTheUserAgent()
    {
        Assert.Equal("claude-desktop 1.2.3", ReadCaller.OfMcpClient("claude-desktop", "1.2.3", "node/22.3.0"));
    }

    [Fact]
    public void TheUserAgentFallback_KeepsOnlyTheFirstProductToken()
    {
        Assert.Equal("ua:node/22.3.0", ReadCaller.OfMcpClient(null, null, "node/22.3.0 (win32; x64) undici/6.0"));
        Assert.Equal("ua:python-httpx/0.27.0", ReadCaller.OfMcpClient(null, null, "python-httpx/0.27.0"));
        Assert.Equal("ua:Claude-User/1.0", ReadCaller.OfMcpClient("", "", "  Claude-User/1.0 extra/2"));
        Assert.Equal("ua:curl", ReadCaller.OfMcpClient(null, null, "curl(8.0)"));
        Assert.Null(ReadCaller.FirstProductToken(null));
        Assert.Null(ReadCaller.FirstProductToken("(only a comment)"));
    }

    [Fact]
    public void TwoReadsInOneWebSession_ShareTheHash_AndTwoSessionsDiffer()
    {
        const string sessionA = "1767225600.c2Vzc2lvbi1h.QUJDREVGR0hJSktMTU5PUFFSU1RVVldYWVo";
        const string sessionB = "1767225600.c2Vzc2lvbi1i.YWJjZGVmZ2hpamtsbW5vcHFyc3R1dnd4eXo";

        var first = ReadCaller.OfWebSession(sessionA);
        var second = ReadCaller.OfWebSession(sessionA);
        var other = ReadCaller.OfWebSession(sessionB);

        Assert.Equal(first, second);
        Assert.NotEqual(first, other);
        Assert.Matches("^web:[0-9a-f]{12}$", first);
        Assert.Matches("^web:[0-9a-f]{12}$", other);
    }

    [Fact]
    public void TheWebHash_IsNotTheSessionKey_NorAPrefixOfIt_AndDependsOnTheSalt()
    {
        const string sessionKey = "1767225600.QUJDREVGR0hJSktMTU5PUFFSU1RVVldYWVo";
        var caller = ReadCaller.OfWebSession(sessionKey);
        var hash = caller["web:".Length..];

        Assert.NotEqual(sessionKey, caller);
        Assert.NotEqual(sessionKey, hash);
        Assert.False(sessionKey.StartsWith(hash, StringComparison.OrdinalIgnoreCase), "the stored hash is a prefix of the session key");
        Assert.DoesNotContain(hash, sessionKey, StringComparison.OrdinalIgnoreCase);
        Assert.DoesNotContain(sessionKey, caller, StringComparison.Ordinal);

        /* Keyed by the salt: the same key under two salts gives two values, so the stored value names no session once the
           process (and its salt) is gone. */
        var saltA = new byte[32];
        var saltB = Enumerable.Repeat((byte)7, 32).ToArray();
        Assert.NotEqual(ReadCaller.OfWebSession(sessionKey, saltA), ReadCaller.OfWebSession(sessionKey, saltB));
    }

    [Fact]
    public void AWebReadWithNoSession_IsWebNone()
    {
        Assert.Equal("web:none", ReadCaller.OfWebSession(null));
        Assert.Equal("web:none", ReadCaller.OfWebSession(""));
    }

    [Fact]
    public void TheAccumulator_CountsDistinctCallersPerKey_AndANullCallerAddsNone()
    {
        var accumulator = new ReadLatencyAccumulator();
        accumulator.Record(ReadSurface.Mcp, "get_wait_stats", ReadOutcome.Ok, 10, ReadWindowBand.OneHour, 0, 0, "claude-desktop 1.2.3");
        accumulator.Record(ReadSurface.Mcp, "get_wait_stats", ReadOutcome.Ok, 10, ReadWindowBand.OneHour, 0, 0, "claude-desktop 1.2.3");
        accumulator.Record(ReadSurface.Mcp, "get_wait_stats", ReadOutcome.Ok, 10, ReadWindowBand.OneHour, 0, 0, "ua:node/22.3.0");
        accumulator.Record(ReadSurface.Mcp, "get_wait_stats", ReadOutcome.Ok, 10, ReadWindowBand.OneHour, 0, 0, null);
        accumulator.Record(ReadSurface.Mcp, "get_wait_stats", ReadOutcome.Ok, 10);
        accumulator.Record(ReadSurface.Web, "get_wait_stats", ReadOutcome.Ok, 10, ReadWindowBand.OneHour, 0, 0, "web:0123456789ab");

        var drained = accumulator.Drain();
        var mcp = Assert.Single(drained, r => r.Surface == ReadSurface.Mcp && r.WindowBand == ReadWindowBand.OneHour);
        Assert.Equal(2, mcp.DistinctCallers);
        Assert.Equal(4, mcp.Count);
        Assert.Equal(0, Assert.Single(drained, r => r.Surface == ReadSurface.Mcp && r.WindowBand == ReadWindowBand.None).DistinctCallers);
        Assert.Equal(1, Assert.Single(drained, r => r.Surface == ReadSurface.Web).DistinctCallers);

        /* The drain resets the hour: the next hour's count starts from nothing. */
        accumulator.Record(ReadSurface.Mcp, "get_wait_stats", ReadOutcome.Ok, 10, ReadWindowBand.OneHour, 0, 0, "ua:node/22.3.0");
        Assert.Equal(1, Assert.Single(accumulator.Drain()).DistinctCallers);
    }

    [Fact]
    public void TheDistinctCallerSet_StopsAtItsCap()
    {
        var accumulator = new ReadLatencyAccumulator();
        for (var i = 0; i < ReadLatencyAccumulator.MaxDistinctCallers + 250; i++)
        {
            accumulator.Record(ReadSurface.Web, "get_blocking", ReadOutcome.Ok, 5, ReadWindowBand.OneDay, 0, 0, "web:" + i.ToString("x12", System.Globalization.CultureInfo.InvariantCulture));
        }

        var row = Assert.Single(accumulator.Drain());
        Assert.Equal(1_000, ReadLatencyAccumulator.MaxDistinctCallers);
        Assert.Equal(ReadLatencyAccumulator.MaxDistinctCallers, row.DistinctCallers);
        Assert.Equal(ReadLatencyAccumulator.MaxDistinctCallers + 250, row.Count);
    }

    /* A name that reads like personal or credential data. Compared after lower-casing and dropping '_' and '-'. */
    private static readonly Regex s_personalDataName = new("cookie|token|authoriz|header|bearer|password|remote|address|username|useragent|login", RegexOptions.CultureInvariant);

    /* The one exemption: the MCP caller's fallback reads the User-Agent product token (#5793), and only ReadCaller sees it. */
    private static readonly HashSet<string> s_allowed = new(StringComparer.Ordinal) { "OfMcpClient.userAgent", "FirstProductToken.userAgent" };

    internal static IReadOnlyList<string> PersonalDataNames(IEnumerable<string> names) =>
        names.Where(n => !s_allowed.Contains(n))
             .Where(n => s_personalDataName.IsMatch(n[(n.LastIndexOf('.') + 1)..].ToLowerInvariant().Replace("_", "", StringComparison.Ordinal).Replace("-", "", StringComparison.Ordinal)))
             .ToList();

    /// <summary>Every input the recorder takes (the accumulator's Record overloads, the slow-read Offer and Build, the record
    /// and drained-row fields, and the caller helpers) and every column the two flush statements write, by name: none may be
    /// named like a cookie, token, authorization, header, remote address or user name. The User-Agent product token is the
    /// one allowed name, and only on the caller helpers.</summary>
    [Fact]
    public void NoRecorderInput_OrStoredColumn_IsNamedLikePersonalData()
    {
        var names = new List<string>();
        void AddParameters(Type type, string method)
        {
            var found = type.GetMethods(BindingFlags.Public | BindingFlags.NonPublic | BindingFlags.Static | BindingFlags.Instance)
                .Where(m => m.Name == method).ToList();
            Assert.NotEmpty(found);
            names.AddRange(found.SelectMany(m => m.GetParameters()).Select(p => method + "." + p.Name));
        }

        AddParameters(typeof(ReadLatencyAccumulator), "Record");
        AddParameters(typeof(SlowReadLog), "Offer");
        AddParameters(typeof(SlowReadLog), "Build");
        AddParameters(typeof(ReadCaller), "OfMcpClient");
        AddParameters(typeof(ReadCaller), "FirstProductToken");
        AddParameters(typeof(ReadCaller), "OfWebSession");
        names.AddRange(typeof(SlowReadRecord).GetProperties().Select(p => "SlowReadRecord." + p.Name));
        names.AddRange(typeof(ReadLatencyAccumulator).GetNestedType("DrainedLatency", BindingFlags.NonPublic)!.GetProperties().Select(p => "DrainedLatency." + p.Name));

        names.AddRange(InsertColumns(SlowReadLog.InsertSql).Select(c => "slow_reads." + c));
        var accumulatorSource = RepoFile.ReadRepoFile("Darling", "PerformanceMonitor.Darling.Service", "ReadLatencyAccumulator.cs");
        names.AddRange(InsertColumns(accumulatorSource[accumulatorSource.IndexOf("INSERT INTO collect.read_latency", StringComparison.Ordinal)..]).Select(c => "read_latency." + c));

        Assert.Contains("slow_reads.caller", names);
        Assert.Contains("read_latency.distinct_callers", names);
        Assert.Contains("OfMcpClient.userAgent", names);
        Assert.Empty(PersonalDataNames(names));
    }

    [Fact]
    public void TheNameScan_CatchesThePersonalDataShapes_AndAllowsOnlyTheCallerHelpersUserAgent()
    {
        Assert.Equal(
            new[] { "Record.cookie", "Record.authToken", "Record.Authorization", "x.request_headers", "x.remote_ip_address", "x.user_name", "Offer.userAgent" },
            PersonalDataNames(new[] { "Record.cookie", "Record.authToken", "Record.Authorization", "x.request_headers", "x.remote_ip_address", "x.user_name", "Offer.userAgent", "OfMcpClient.userAgent", "Record.caller", "x.distinct_callers" }));
    }

    private static IEnumerable<string> InsertColumns(string sql)
    {
        var open = sql.IndexOf('(');
        var close = sql.IndexOf(')', open);
        return sql[(open + 1)..close].Split(',', StringSplitOptions.TrimEntries | StringSplitOptions.RemoveEmptyEntries);
    }
}
