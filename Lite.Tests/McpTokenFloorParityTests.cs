using System;
using System.Reflection;
using PerformanceMonitorLite.Mcp;
using Xunit;

namespace PerformanceMonitorLite.Tests;

/// <summary>
/// #8462: the floor a bearer token clears before it serves the write and investigation tools (at least 16 characters,
/// at least 6 different ones) is one rule, <c>PerformanceMonitor.Common.McpTokenFloor</c>, and Lite's resolvers
/// (<see cref="McpTokenStore.IsUsableToken"/>, <see cref="McpHostService.DecideServing"/>) decide exactly what it decides
/// at the boundary inputs: 15 and 16 characters, 5 and 6 different characters. Darling's twin,
/// <c>McpTokenFloorParityTests</c> in Darling.Tests, runs the same inputs through Darling's resolvers, so the two apps
/// cannot drift apart without one of the two failing.
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
    public void LiteResolvers_DecideWhatTheSharedFloorDecides(string? token, bool usable)
    {
        Assert.Equal(usable, SharedFloorClears(token));
        Assert.Equal(usable, McpTokenStore.IsUsableToken(token));

        if (string.IsNullOrEmpty(token))
        {
            /* With the opt-out off, a start with no token at all is refused before the floor is looked at. */
            Assert.Throws<ArgumentException>(() => McpHostService.DecideServing(token, allowTokenlessLoopback: false, allowWrites: true, allowInvestigations: false));
            return;
        }

        var writes = McpHostService.DecideServing(token, allowTokenlessLoopback: false, allowWrites: true, allowInvestigations: false);
        Assert.Equal(usable, writes.Writes);

        var investigations = McpHostService.DecideServing(token, allowTokenlessLoopback: false, allowWrites: false, allowInvestigations: true);
        Assert.Equal(usable, investigations.Investigations);
    }

    [Fact]
    public void LiteConstants_AreTheSharedFloorsConstants()
    {
        Assert.Equal(McpTokenStore.MinimumTokenLength, SharedFloorConstant("MinimumLength"));
        Assert.Equal(McpTokenStore.MinimumDistinctTokenCharacters, SharedFloorConstant("MinimumDistinctCharacters"));
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
