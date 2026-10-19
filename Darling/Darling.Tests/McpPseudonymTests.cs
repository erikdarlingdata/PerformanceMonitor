/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using System.Collections.Generic;
using System.Text.Json.Nodes;
using PerformanceMonitor.Common;
using Xunit;

namespace Darling.Tests;

/// <summary>
/// #6066: under <c>metadataOnly</c> a login or host gets one pseudonym per store, <c>login#</c> or <c>host#</c> plus 8 hex from
/// a keyed hash, the same in every answer. These pin the format, the stability, the collision suffix, the "never masked
/// twice" rule and the no-key fallback. The Lite twin of the shared vector is <c>PseudonymVectorTests</c> in Lite.Tests.
/// </summary>
public sealed class McpPseudonymTests
{
    /* The shared vector (rule 14): the key is the 32 bytes 0x00 to 0x1f. Lite.Tests carries the same three lines. */
    [Theory]
    [InlineData("login", "CORP\\Alice", "login#5f484746")]
    [InlineData("login", "sa", "login#8f799a85")]
    [InlineData("host", "AppBox", "host#863ab665")]
    public void TheSharedVector_GivesTheSameThreePseudonymsInBothApps(string kind, string name, string expected)
    {
        Assert.Equal(expected, PseudonymTestKey.Key.Pseudonym(kind, name));
        string field = kind == "login" ? "login_name" : "host_name";
        Assert.Equal(expected, McpDataDetailMasker.MaskField(field, name, McpDataDetail.MetadataOnly, PseudonymTestKey.Key));
    }

    [Fact]
    public void ALoginAndAHostWithTheSameText_GetDifferentPseudonyms()
    {
        Assert.NotEqual(PseudonymTestKey.Key.Pseudonym("login", "sa"), PseudonymTestKey.Key.Pseudonym("host", "sa").Replace("host#", "login#", StringComparison.Ordinal));
    }

    [Fact]
    public void TheSameName_GetsTheSamePseudonym_FromTwoMaskersWithTheSameKey_AndADifferentOneFromAnotherKey()
    {
        var first = McpDataDetailMasker.ForAnswer(McpDataDetail.MetadataOnly, PseudonymTestKey.Key);
        var second = McpDataDetailMasker.ForAnswer(McpDataDetail.MetadataOnly, new PseudonymKey(new byte[32]));
        var third = McpDataDetailMasker.ForAnswer(McpDataDetail.MetadataOnly, PseudonymTestKey.Key);
        string a = first.Field("login_name", "CORP\\alice")!;
        Assert.Equal(a, third.Field("login_name", "corp\\ALICE "));
        Assert.NotEqual(a, second.Field("login_name", "CORP\\alice"));
    }

    /* Found once with a birthday search under the fixed test key: HMAC-SHA256 of "login\n" + "user<n>" for n = 0, 1, 2 ...; the
       first repeat of the first 4 bytes is user24019 and user106390 (after 106,390 hashes). Hard-coded here so the test does not search. */
    private const string CollidingA = "user24019";
    private const string CollidingB = "user106390";

    [Fact]
    public void TwoNamesThatHashToOnePseudonym_InOneAnswer_GetASuffix_AndNeverShareOne()
    {
        string shared = PseudonymTestKey.Login(CollidingA);
        Assert.Equal(shared, PseudonymTestKey.Login(CollidingB));

        var masker = McpDataDetailMasker.ForAnswer(McpDataDetail.MetadataOnly, PseudonymTestKey.Key);
        Assert.Equal(shared, masker.Field("login_name", CollidingA));
        Assert.Equal(shared + "-2", masker.Field("login_name", CollidingB));
        /* Each name keeps its pseudonym for the rest of the answer. */
        Assert.Equal(shared, masker.Field("blocking_login_name", CollidingA));
        Assert.Equal(shared + "-2", masker.Field("blocking_login_name", CollidingB));
        /* A third colliding name takes the next free suffix. */
        Assert.NotEqual(shared + "-2", masker.Field("login_name", "someone-else-" + shared));
    }

    [Fact]
    public void APseudonym_IsNeverMaskedAgain()
    {
        var masker = McpDataDetailMasker.ForAnswer(McpDataDetail.MetadataOnly, PseudonymTestKey.Key);
        foreach (string already in new[] { "login#5f484746", "login#db2b3896-2", "login#12", "host#a06e9ea5", "host#3" })
        {
            string key = already.StartsWith("login", StringComparison.Ordinal) ? "login_name" : "host_name";
            Assert.Equal(already, masker.Field(key, already));
        }

        /* A brief stored at a weaker level and re-masked: the pseudonyms it already holds survive. */
        string once = McpDataDetailMasker.MaskJson("{\"login_name\":\"sa\"}", McpDataDetail.MetadataOnly, key: PseudonymTestKey.Key);
        Assert.Equal(once, McpDataDetailMasker.MaskJson(once, McpDataDetail.MetadataOnly, key: PseudonymTestKey.Key));
        Assert.Same(once, McpDataDetailMasker.MaskJson(once, McpDataDetail.MetadataOnly, key: PseudonymTestKey.Key));

        /* Text that only starts like one is still a name. */
        Assert.Equal(PseudonymTestKey.Login("login#5f48474"), masker.Field("login_name", "login#5f48474"));
    }

    [Fact]
    public void WithNoKey_AnswersAreNumberedPerAnswer_AndTheNameIsNeverShown()
    {
        PseudonymKeyNotice.Reset();
        var warnings = new List<string>();
        PseudonymKeyNotice.Sink = warnings.Add;
        try
        {
            var masker = McpDataDetailMasker.ForAnswer(McpDataDetail.MetadataOnly, null);
            Assert.Equal("login#1", masker.Field("login_name", "CORP\\alice"));
            Assert.Equal("login#2", masker.Field("login_name", "bob"));
            Assert.Equal("login#1", masker.Field("login_name", "CORP\\alice"));
            Assert.Equal("host#1", masker.Field("host_name", "APP01"));
            Assert.Equal("login#1", McpDataDetailMasker.ForAnswer(McpDataDetail.MetadataOnly).Field("login_name", "bob"));
            string json = McpDataDetailMasker.MaskJson("{\"login_name\":\"CORP\\\\alice\",\"host_name\":\"APP01\"}", McpDataDetail.MetadataOnly);
            Assert.DoesNotContain("alice", json, StringComparison.Ordinal);
            Assert.DoesNotContain("APP01", json, StringComparison.Ordinal);

            /* The warning is logged once per process, names no login and carries no key. */
            string warning = Assert.Single(warnings);
            Assert.DoesNotContain("alice", warning, StringComparison.Ordinal);
            Assert.DoesNotContain("PseudonymKey", warning, StringComparison.Ordinal);

            /* The levels that keep logins and hosts never warn and never change. */
            warnings.Clear();
            PseudonymKeyNotice.Reset();
            Assert.Equal("alice", McpDataDetailMasker.ForAnswer(McpDataDetail.MaskLiterals).Field("login_name", "alice"));
            Assert.Equal("alice", McpDataDetailMasker.ForAnswer(McpDataDetail.Full).Field("login_name", "alice"));
            Assert.Empty(warnings);
        }
        finally
        {
            PseudonymKeyNotice.Sink = null;
            PseudonymKeyNotice.Reset();
        }
    }

    [Fact]
    public void TheKey_CannotBeReadBackFromItsText()
    {
        string text = PseudonymTestKey.Key.ToString();
        Assert.Equal("PseudonymKey", text);
        Assert.DoesNotContain(Convert.ToHexStringLower(PseudonymTestKey.Bytes), text, StringComparison.Ordinal);
        Assert.Throws<ArgumentException>(() => new PseudonymKey(new byte[31]));
    }

    [Fact]
    public void ABriefComposedWithTheKey_ShowsTheKeyedPseudonym_ForALoginThatAppearsInTwoPlaces()
    {
        var node = new JsonObject { ["login_name"] = "CORP\\alice", ["note"] = null };
        var masker = McpDataDetailMasker.ForAnswer(McpDataDetail.MetadataOnly, PseudonymTestKey.Key);
        string json = masker.Json(node.ToJsonString());
        Assert.Contains(PseudonymTestKey.Login("CORP\\alice"), json, StringComparison.Ordinal);
    }
}
