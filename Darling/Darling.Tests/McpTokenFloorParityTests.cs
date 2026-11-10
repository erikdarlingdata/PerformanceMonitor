using System;
using System.Reflection;
using Microsoft.Extensions.Logging.Abstractions;
using PerformanceMonitor.Darling.Service;
using PerformanceMonitor.Darling.Service.Hosting;
using PerformanceMonitor.Darling.Service.Mcp;
using Xunit;

namespace Darling.Tests;

/// <summary>
/// #8462: the floor a bearer token clears before it serves the write and investigation tools (at least 16 characters,
/// at least 6 different ones) is one rule, <c>PerformanceMonitor.Common.McpTokenFloor</c>, and Darling's resolvers
/// (<see cref="DarlingHostBinding.DescribeTokenProblem"/>, <see cref="DarlingMcpWritePolicy.ResolveForStart"/>) decide
/// exactly what it decides at the boundary inputs: 15 and 16 characters, 5 and 6 different characters. Lite's twin,
/// <c>McpTokenFloorParityTests</c> in Lite.Tests, runs the same inputs through Lite's resolvers, so the two apps cannot
/// drift apart without one of the two failing.
/// </summary>
public sealed class McpTokenFloorParityTests
{
    private const string Token = "0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef";

    /// <summary>The boundary inputs and what the floor decides for each.</summary>
    public static TheoryData<string?, bool> Boundary => new()
    {
        { null, false },
        { "", false },
        { "                ", false },
        /* 15 characters, all different: one short of the length floor. */
        { "abcdefghijklmno", false },
        /* 16 characters, all different: exactly the length floor. */
        { "abcdefghijklmnop", true },
        /* 16 characters, 5 different: one short of the distinct-character floor. */
        { "abcdeabcdeabcdea", false },
        /* 16 characters, 6 different: exactly the distinct-character floor. */
        { "abcdefabcdefabcd", true },
        /* 32 copies of one letter: length alone is not enough. */
        { "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa", false },
        { Token, true },
    };

    [Theory]
    [MemberData(nameof(Boundary))]
    public void DarlingResolvers_DecideWhatTheSharedFloorDecides(string? token, bool usable)
    {
        Assert.Equal(usable, SharedFloorClears(token));
        Assert.Equal(usable, DarlingHostBinding.DescribeTokenProblem(token) is null);

        var writes = DarlingMcpWritePolicy.ResolveForStart(new McpConfig { AllowWrites = true }, token ?? "", out _);
        Assert.Equal(usable, writes.Writes);

        var investigations = DarlingMcpWritePolicy.ResolveForStart(new McpConfig { AllowInvestigations = true }, token ?? "", out _);
        Assert.Equal(usable, investigations.Investigations);
    }

    [Fact]
    public void DarlingConstants_AreTheSharedFloorsConstants()
    {
        Assert.Equal(DarlingHostBinding.MinimumTokenLength, SharedFloorConstant("MinimumLength"));
        Assert.Equal(DarlingHostBinding.MinimumDistinctTokenCharacters, SharedFloorConstant("MinimumDistinctCharacters"));
    }

    /// <summary>
    /// In Darling, <c>mcp.allowTokenlessLoopback</c> applies only to a start with NO token configured. A configured
    /// token is still resolved, so the loopback gate compares against it, and the write tools are served when
    /// <c>mcp.allowWrites</c> asks for them. With no token configured the opt-out serves reads only.
    /// </summary>
    [Fact]
    public void TokenlessLoopbackOptOut_WithAConfiguredToken_StillGatesAndServesTheWrites()
    {
        var mcp = new McpConfig
        {
            AllowWrites = true,
            AllowTokenlessLoopback = true,
            Network = new McpNetworkConfig { Token = Token },
        };

        var resolved = DarlingMcpHostService.ResolveLoopbackOnlyToken(mcp.Network, mcp.AllowTokenlessLoopback, NullLogger.Instance);
        Assert.False(resolved.Refuse);
        Assert.False(resolved.MissingToken);
        Assert.Null(resolved.TokenProblem);
        Assert.Equal(Token, resolved.Token);

        var effective = DarlingMcpWritePolicy.ResolveForStart(mcp, resolved.Token, out var problem);
        Assert.Null(problem);
        Assert.True(effective.Writes);
        Assert.True(effective.Offers("mute_analysis_finding"));

        var none = DarlingMcpHostService.ResolveLoopbackOnlyToken(null, allowTokenlessLoopback: true, NullLogger.Instance);
        Assert.False(none.Refuse);
        Assert.Equal("", none.Token);
        var readsOnly = DarlingMcpWritePolicy.ResolveForStart(
            new McpConfig { AllowWrites = true, AllowTokenlessLoopback = true }, none.Token, out _);
        Assert.False(readsOnly.Writes);
        Assert.False(readsOnly.Offers("mute_analysis_finding"));
    }

    /* The shared floor lives in PerformanceMonitor.Common, which Lite and Darling both reference. It is reached by
       name so this class also fails, with a message, when the rule is spelled in each app instead of once. */
    private static readonly Type? SharedFloor =
        typeof(PerformanceMonitor.Common.CountText).Assembly.GetType("PerformanceMonitor.Common.McpTokenFloor");

    private static bool SharedFloorClears(string? token)
    {
        Assert.True(SharedFloor is not null,
            "PerformanceMonitor.Common.McpTokenFloor is missing: the token floor must be one constant and predicate in Common that both apps call");
        var clears = SharedFloor!.GetMethod("Clears", BindingFlags.Static | BindingFlags.Public | BindingFlags.NonPublic, [typeof(string)]);
        Assert.True(clears is not null, "McpTokenFloor.Clears(string?) is missing");
        return (bool)clears!.Invoke(null, [token])!;
    }

    private static int SharedFloorConstant(string name)
    {
        Assert.True(SharedFloor is not null,
            "PerformanceMonitor.Common.McpTokenFloor is missing: the token floor must be one constant and predicate in Common that both apps call");
        var field = SharedFloor!.GetField(name, BindingFlags.Static | BindingFlags.Public | BindingFlags.NonPublic);
        Assert.True(field is not null, $"McpTokenFloor.{name} is missing");
        return (int)field!.GetRawConstantValue()!;
    }
}
