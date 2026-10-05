/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using System.Net;
using System.Net.Sockets;
using System.Text.Json;
using PerformanceMonitor.Darling.Service;
using PerformanceMonitor.Darling.Service.Hosting;
using Xunit;

namespace Darling.Tests;

/// <summary>
/// #5288: <see cref="CidrAllowList"/>, the ONE parser behind <c>mcp.network.allowFrom</c> and
/// <c>web.network.allowFrom</c> (one CIDR or a list), and <see cref="CidrListJsonConverter"/>, which lets
/// darling.json spell the list as a string or an array of strings. The gate and the bind ladder are pinned
/// beside their own suites (<see cref="DarlingHostBindingTests"/>, <see cref="DarlingAllowFromListGateTests"/>);
/// this class pins the type's own contract: how text parses, what the canonical text is, what the type's
/// DEFAULT does (nothing is admitted), and the converter's accepted and refused shapes.
/// </summary>
public sealed class DarlingCidrAllowListTests
{
    private static CidrAllowList Parsed(string text)
    {
        Assert.True(CidrAllowList.TryParse(text, out var list), $"'{text}' should parse");
        return list;
    }

    /* ---- parse: one CIDR is exactly what it was before the list existed ---- */

    [Theory]
    [InlineData("192.168.1.0/24")]
    [InlineData(" 192.168.1.0/24 ")]
    [InlineData("2001:db8::/32")]
    [InlineData("203.0.113.9/32")]
    [InlineData("0.0.0.0/0")]      // allow-all IPv4 is a legal entry the operator writes on purpose
    public void TryParse_OneCidr_IsOneEntry_AndItsTextIsWhatIPNetworkGave(string text)
    {
        var list = Parsed(text);

        Assert.Equal(1, list.Count);
        Assert.Equal(IPNetwork.Parse(text.Trim()).ToString(), list.ToString());
    }

    [Fact]
    public void TryParse_CommaString_KeepsEveryEntryInOrder()
    {
        var list = Parsed("10.8.0.0/16,192.168.1.5/32");

        Assert.Equal(2, list.Count);
        Assert.Equal("10.8.0.0/16", list.Entries[0].ToString());
        Assert.Equal("192.168.1.5/32", list.Entries[1].ToString());
        Assert.Equal("10.8.0.0/16,192.168.1.5/32", list.ToString());
    }

    [Fact]
    public void TryParse_WhitespaceAroundEntries_IsTrimmed()
        => Assert.Equal("10.8.0.0/16,192.168.1.5/32", Parsed("  10.8.0.0/16 ,\t192.168.1.5/32  ").ToString());

    [Fact]
    public void TryParse_MixedFamilies_ParseHere_TheFamilyRuleBelongsToTheBindLadder()
    {
        /* The parser does not know the listen address, so it cannot apply the family rule. It reports each
           entry's family, and ResolveBind refuses a list with a wrong-family entry. */
        var list = Parsed("10.8.0.0/16,2001:db8::/32");

        Assert.Equal(2, list.Count);
        Assert.False(list.AllInFamily(AddressFamily.InterNetwork));
        Assert.False(list.AllInFamily(AddressFamily.InterNetworkV6));
        Assert.True(Parsed("10.8.0.0/16,192.168.1.0/24").AllInFamily(AddressFamily.InterNetwork));
        Assert.True(Parsed("2001:db8::/32,fd00::/8").AllInFamily(AddressFamily.InterNetworkV6));
    }

    /* ---- parse: what is refused ---- */

    [Theory]
    [InlineData("192.168.1.0/24,")]               // a trailing comma leaves an empty entry
    [InlineData(",192.168.1.0/24")]
    [InlineData("192.168.1.0/24,,10.8.0.0/16")]
    [InlineData("192.168.1.0/24, ")]
    [InlineData(" , ")]
    [InlineData(",")]
    public void TryParse_EmptyEntry_IsRefused_NotSkipped(string text)
    {
        Assert.False(CidrAllowList.TryParse(text, out var list));
        Assert.Equal("", list.ToString());
    }

    [Theory]
    [InlineData(null)]
    [InlineData("")]
    [InlineData("   ")]
    public void TryParse_NullEmptyOrBlank_IsRefused(string? text)
        => Assert.False(CidrAllowList.TryParse(text, out _));

    [Theory]
    [InlineData("not-a-cidr")]
    [InlineData("192.168.1.0")]                    // an address with no prefix: CIDR form only
    [InlineData("192.168.1.0/33")]                 // impossible IPv4 prefix length
    [InlineData("2001:db8::/129")]
    [InlineData("192.168.1.0/24,garbage")]         // ONE bad entry refuses the whole list
    [InlineData("192.168.1.0/24,10.8.0.0")]
    [InlineData("192.168.1.0/24;10.8.0.0/16")]     // the separator is a comma, nothing else
    [InlineData("192.168.1.0/24 10.8.0.0/16")]
    public void TryParse_NotACidr_IsRefused(string text)
        => Assert.False(CidrAllowList.TryParse(text, out _));

    [Fact]
    public void TryParse_Duplicates_AreDropped_AndFirstSeenOrderIsKept()
    {
        Assert.Equal("10.0.0.0/8,192.168.0.0/16", Parsed("10.0.0.0/8,192.168.0.0/16,10.0.0.0/8").ToString());
        Assert.Equal("192.168.0.0/16,10.0.0.0/8", Parsed("192.168.0.0/16,10.0.0.0/8,192.168.0.0/16").ToString());
        Assert.Equal(1, Parsed("10.0.0.0/8, 10.0.0.0/8").Count);
    }

    [Fact]
    public void TryParse_HostBits_AreMasked_AndTheCanonicalTextShowsIt()
    {
        /* IPNetwork.TryParse masks host bits instead of refusing them, and the list keeps accepting them (the
           pre-list behavior). What changed is visibility: ToString() is what the start line prints, so a
           masked entry reads as the range that is actually enforced. */
        Assert.Equal("192.168.1.0/24", Parsed("192.168.1.5/24").ToString());
        Assert.Equal("10.0.0.0/8", Parsed("10.1.2.3/8").ToString());

        /* A masked entry that collapses onto an existing one is a duplicate. */
        Assert.Equal("10.0.0.0/8", Parsed("10.1.2.3/8,10.0.0.0/8").ToString());
    }

    /* ---- an address written any way but four plain decimal numbers is refused, not read as another address ---- */

    [Fact]
    public void TryParse_NonCanonicalIPv4_Premise_IPNetworkReadsEachSpellingAsADifferentRange()
    {
        /* Why the refusal below exists: IPNetwork.TryParse follows inet_aton, so a leading zero is octal, 0x is
           hex and a short form is zero-padded. Each of these parses, as a range other than the one it reads as. */
        Assert.Equal("192.168.8.0/24", IPNetwork.Parse("192.168.010.0/24").ToString());
        Assert.Equal("8.0.0.0/8", IPNetwork.Parse("010.0.0.0/8").ToString());
        Assert.Equal("0.0.0.0/8", IPNetwork.Parse("10/8").ToString());
        Assert.Equal("10.0.0.0/16", IPNetwork.Parse("10.1/16").ToString());
        Assert.Equal("10.0.0.0/8", IPNetwork.Parse("0x0A.0.0.0/8").ToString());
    }

    [Theory]
    [InlineData("010.0.0.0/8")]            // a leading zero reads as octal (010 = 8)
    [InlineData("192.168.010.0/24")]
    [InlineData("10/8")]                   // a short form is zero-padded
    [InlineData("10.1/16")]
    [InlineData("0x0A.0.0.0/8")]           // 0x reads as hex
    [InlineData("1.2.3.04/32")]
    public void TryParse_NonCanonicalIPv4_IsRefused(string text)
    {
        Assert.False(CidrAllowList.TryParse(text, out var list));
        Assert.Equal("", list.ToString());

        /* Refused wherever it sits in a list: one bad entry refuses the whole list. */
        Assert.False(CidrAllowList.TryParse($"10.8.0.0/16,{text}", out _));
        Assert.False(CidrAllowList.TryParse($"{text},10.8.0.0/16", out _));
    }

    [Theory]
    [InlineData("0.0.0.0/0")]
    [InlineData("10.0.0.0/8")]
    [InlineData("192.168.1.5/24")]         // host bits are masked afterwards, never refused
    [InlineData("255.255.255.255/32")]
    [InlineData("100.64.0.0/10")]
    [InlineData("1.2.3.0/24")]
    public void TryParse_PlainDecimalIPv4_IsAccepted(string text)
        => Assert.True(CidrAllowList.TryParse(text, out _));

    [Theory]
    [InlineData("fe80::1%5/64")]
    [InlineData("fe80::1%eth0/64")]
    [InlineData("fe80::%5/64")]            // IPNetwork even keeps this zone in its own text
    [InlineData("fe80::1%x';calc;'/64")]   // a zone is free text, so it must not survive the parse
    [InlineData("fe80::1%a b/64")]
    [InlineData("fe80::1%$(calc)/64")]
    [InlineData("10.8.0.0/16,fe80::1%5/64")]
    public void TryParse_IPv6ZoneIndex_IsRefused(string text)
    {
        Assert.False(CidrAllowList.TryParse(text, out var list));
        Assert.Equal("", list.ToString());
    }

    [Theory]
    [InlineData("64:ff9b::192.0.2.33/96", "64:ff9b::/96")]
    [InlineData("64:ff9b::1.2.3.4/96,10.8.0.0/16", "64:ff9b::/96,10.8.0.0/16")]
    public void TryParse_IPv6DottedTail_FourPlainNumbers_IsAccepted(string text, string expected)
        => Assert.Equal(expected, Parsed(text).ToString());

    [Theory]
    [InlineData("64:ff9b::192.0.2.033/96")]   // the dotted tail follows the same rule as an IPv4 entry
    [InlineData("64:ff9b::1.2.3.04/96")]
    public void TryParse_IPv6DottedTail_WithALeadingZero_IsRefused(string text)
        => Assert.False(CidrAllowList.TryParse(text, out _));

    /* ---- F6: an IPv4-mapped IPv6 entry is refused ---- */

    [Fact]
    public void CidrAllowList_MappedEntry_PremiseHolds_IPNetworkParsesItAndItMatchesNoIPv4Address()
    {
        /* Why the refusal below exists: IPNetwork happily parses a mapped base address, and such a network
           matches NO IPv4 address, because IsRemoteAddressAllowed maps a mapped REMOTE to IPv4 before it
           tests the list. On an IPv6 listen the entry would pass the family rule and then never match:
           silent dead config. */
        Assert.True(IPNetwork.TryParse("::ffff:10.0.0.0/104", out var network));
        Assert.True(network.BaseAddress.IsIPv4MappedToIPv6);
        Assert.False(network.Contains(IPAddress.Parse("10.0.0.5")));
    }

    [Theory]
    [InlineData("::ffff:10.0.0.0/104")]
    [InlineData("::ffff:192.168.1.5/128")]
    [InlineData("::ffff:0:0/96")]
    [InlineData("10.8.0.0/16,::ffff:10.0.0.0/104")]   // refused wherever it sits in the list
    [InlineData("::ffff:10.0.0.0/104,10.8.0.0/16")]
    [InlineData("2001:db8::/32,::ffff:10.0.0.0/104")]
    public void CidrAllowList_MappedEntry_IsRefused(string text)
        => Assert.False(CidrAllowList.TryParse(text, out _));

    /* ---- Parse, Contains, the IPNetwork conversion ---- */

    [Fact]
    public void Parse_ValidText_ReturnsTheList_InvalidText_ThrowsFormatException()
    {
        Assert.Equal("10.8.0.0/16,192.168.1.0/24", CidrAllowList.Parse("10.8.0.0/16, 192.168.1.0/24").ToString());
        Assert.Throws<FormatException>(() => CidrAllowList.Parse("10.8.0.0/16,"));
        Assert.Throws<FormatException>(() => CidrAllowList.Parse(""));
    }

    [Theory]
    [InlineData("192.168.1.50", true)]   // first entry
    [InlineData("10.8.3.4", true)]       // second entry
    [InlineData("203.0.113.9", true)]    // a /32
    [InlineData("203.0.113.10", false)]  // the neighbor of the /32
    [InlineData("192.168.2.1", false)]
    [InlineData("10.9.0.1", false)]
    [InlineData("2001:db8::1", false)]   // a native IPv6 address against IPv4 entries
    public void Contains_IsTrueForAnyEntry(string address, bool expected)
        => Assert.Equal(expected, Parsed("192.168.1.0/24,10.8.0.0/16,203.0.113.9/32").Contains(IPAddress.Parse(address)));

    [Fact]
    public void Contains_NullAddress_IsFalse()
        => Assert.False(Parsed("192.168.1.0/24").Contains(null));

    [Fact]
    public void ImplicitConversion_FromAnIPNetwork_IsAOneEntryList()
    {
        CidrAllowList list = IPNetwork.Parse("192.168.1.0/24");

        Assert.Equal(1, list.Count);
        Assert.Equal("192.168.1.0/24", list.ToString());
        Assert.True(list.Contains(IPAddress.Parse("192.168.1.50")));
        Assert.False(list.Contains(IPAddress.Parse("192.168.2.1")));
    }

    /* ---- F7: default(CidrAllowList) admits nobody ---- */

    [Fact]
    public void CidrAllowList_Default_RefusesEveryNonLoopbackRemote()
    {
        CidrAllowList none = default;

        foreach (var remote in new[]
        {
            "10.0.0.5", "192.168.1.50", "172.16.0.1", "8.8.8.8", "0.0.0.0", "255.255.255.255",
            "2001:db8::1", "fe80::1", "::ffff:10.0.0.5", "::ffff:192.168.1.50",
        })
        {
            var address = IPAddress.Parse(remote);
            Assert.False(none.Contains(address), $"default list must not contain {remote}");
            Assert.False(DarlingHostBinding.IsRemoteAddressAllowed(address, none), $"default list must refuse {remote}");
        }

        /* Loopback is the host's own exemption, taken before the list is asked — not a list entry. */
        Assert.True(DarlingHostBinding.IsRemoteAddressAllowed(IPAddress.Loopback, none));
        Assert.True(DarlingHostBinding.IsRemoteAddressAllowed(IPAddress.IPv6Loopback, none));
        Assert.True(DarlingHostBinding.IsRemoteAddressAllowed(IPAddress.Parse("::ffff:127.0.0.1"), none));

        /* An unverifiable origin fails closed, and an empty list is never "all in family". */
        Assert.False(DarlingHostBinding.IsRemoteAddressAllowed(null, none));
        Assert.False(none.AllInFamily(AddressFamily.InterNetwork));
        Assert.False(none.AllInFamily(AddressFamily.InterNetworkV6));

        /* The reason the type has its own default: default(IPNetwork) is 0.0.0.0/0, which admits every IPv4
           address. If this ever stops being true the F7 note on CidrAllowList needs revisiting. */
        Assert.True(default(IPNetwork).Contains(IPAddress.Parse("10.0.0.5")));
    }

    [Fact]
    public void CidrAllowList_Default_ToStringIsEmpty()
    {
        CidrAllowList none = default;

        Assert.Equal("", none.ToString());
        Assert.Equal(0, none.Count);
        Assert.Empty(none.Entries);
    }

    /* ---- the converter: a string or an array of strings, through the real config load ---- */

    private static string ConfigJson(string section, string allowFromJson)
        => @"{ ""postgres"": { ""managed"": true }, ""servers"": [ { ""host"": ""SQL2022"" } ], """ + section
            + @""": { ""network"": { ""listen"": ""192.168.1.205"", ""allowFrom"": " + allowFromJson + @" } } }";

    private static string? McpAllowFrom(string allowFromJson)
        => DarlingConfig.Parse(ConfigJson("mcp", allowFromJson)).Mcp.Network!.AllowFrom;

    private static string? WebAllowFrom(string allowFromJson)
        => DarlingConfig.Parse(ConfigJson("web", allowFromJson)).Web.Network!.AllowFrom;

    [Theory]
    [InlineData(@"""192.168.1.0/24""", "192.168.1.0/24")]
    [InlineData(@"""10.8.0.0/16, 192.168.1.5/32""", "10.8.0.0/16, 192.168.1.5/32")]   // a string is never normalized
    [InlineData(@"""""", "")]
    public void Converter_String_LoadsAsWritten_OnBothListeners(string json, string expected)
    {
        Assert.Equal(expected, McpAllowFrom(json));
        Assert.Equal(expected, WebAllowFrom(json));
    }

    [Theory]
    [InlineData(@"[""10.8.0.0/16"", ""192.168.1.5/32""]", "10.8.0.0/16,192.168.1.5/32")]
    [InlineData(@"[ ""10.8.0.0/16"" , ""192.168.1.5/32"" , ""2001:db8::/32"" ]", "10.8.0.0/16,192.168.1.5/32,2001:db8::/32")]
    [InlineData(@"[""10.8.0.0/16""]", "10.8.0.0/16")]
    [InlineData(@"[]", "")]
    public void Converter_Array_IsJoinedWithCommas_NoSpaces_OnBothListeners(string json, string expected)
    {
        Assert.Equal(expected, McpAllowFrom(json));
        Assert.Equal(expected, WebAllowFrom(json));
    }

    [Fact]
    public void Converter_ArrayThroughTheParser_GivesTheCanonicalList()
    {
        var joined = McpAllowFrom(@"[""10.8.0.0/16"", ""192.168.1.5/24"", ""10.8.0.0/16""]");

        Assert.Equal("10.8.0.0/16,192.168.1.5/24,10.8.0.0/16", joined);
        Assert.Equal("10.8.0.0/16,192.168.1.0/24", Parsed(joined!).ToString());
    }

    [Theory]
    [InlineData(@"[""10.0.0.0/8"", """"]")]       // an empty element joins to a trailing comma
    [InlineData(@"[""10.0.0.0/8"", "" ""]")]
    [InlineData(@"[""""]")]
    [InlineData(@"[]")]
    [InlineData(@"[""10.0.0.0/8"", ""not-a-cidr""]")]
    public void Converter_BadContent_LoadsAndTheParserRefusesIt_SoTheLadderDegradesTheListener(string json)
    {
        /* Shape is the converter's, content is the parser's. A typo in an optional, default-off endpoint must
           not fail the whole config load (D-validate); it degrades that listener to loopback-only, Critical. */
        var joined = McpAllowFrom(json);

        Assert.False(CidrAllowList.TryParse(joined, out _));
        Assert.Equal(
            DarlingHostBinding.BindReason.AllowFromInvalid,
            DarlingHostBinding.ResolveBind("192.168.1.205", joined, tokenPresent: true, networkConfigured: true, managed: true).Reason);
    }

    [Fact]
    public void Converter_NumberElement_FailsLoad()
    {
        /* F12: today a non-string allowFrom fails the load with a JsonException; the converter keeps that for
           an array element that is not a string. */
        Assert.ThrowsAny<JsonException>(() => McpAllowFrom(@"[""10.0.0.0/8"", 5]"));
        Assert.ThrowsAny<JsonException>(() => WebAllowFrom(@"[""10.0.0.0/8"", 5]"));
    }

    [Theory]
    [InlineData(@"[""10.0.0.0/8"", null]")]                            // a null element is not a string either
    [InlineData(@"[""10.0.0.0/8"", true]")]
    [InlineData(@"[[""10.0.0.0/8""]]")]                                // a nested array
    [InlineData(@"[""10.0.0.0/8"", { ""cidr"": ""10.0.0.0/8"" }]")]
    [InlineData(@"5")]                                                  // a bare number
    [InlineData(@"true")]
    [InlineData(@"{ ""cidr"": ""10.0.0.0/8"" }")]
    public void Converter_AnyOtherShape_FailsLoad(string json)
    {
        Assert.ThrowsAny<JsonException>(() => McpAllowFrom(json));
        Assert.ThrowsAny<JsonException>(() => WebAllowFrom(json));
    }

    [Fact]
    public void Converter_NullToken_IsNull()
    {
        Assert.Null(McpAllowFrom("null"));
        Assert.Null(WebAllowFrom("null"));
    }

    [Fact]
    public void Converter_Write_EmitsTheStringAsHeld()
    {
        Assert.Contains(
            @"""allowFrom"":""10.8.0.0/16,192.168.1.5/32""",
            JsonSerializer.Serialize(new McpNetworkConfig { AllowFrom = "10.8.0.0/16,192.168.1.5/32" }));
        Assert.Contains(@"""allowFrom"":null", JsonSerializer.Serialize(new WebNetworkConfig()));
    }

    [Fact]
    public void Converter_IsNotOnThePostgresStoreAllowFrom_AnArrayStillFailsLoad_AStringStillLoads()
    {
        /* postgres.network.allowFrom feeds pg_hba and the store reconcile (a separate subsystem with its own
           symmetric-removal rules) and stays exactly as it was: a string only. */
        const string Template = @"{ ""postgres"": { ""managed"": true, ""network"": { ""listen"": ""192.168.1.205"", ""allowFrom"": {0} } },
            ""servers"": [ { ""host"": ""SQL2022"" } ] }";

        Assert.Equal(
            "192.168.1.0/24",
            DarlingConfig.Parse(Template.Replace("{0}", @"""192.168.1.0/24""")).Postgres.Network!.AllowFrom);
        Assert.ThrowsAny<JsonException>(
            () => DarlingConfig.Parse(Template.Replace("{0}", @"[""192.168.1.0/24""]")));
    }
}
