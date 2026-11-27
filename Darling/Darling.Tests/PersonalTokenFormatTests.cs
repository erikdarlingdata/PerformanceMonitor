/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using System.Security.Cryptography;
using System.Text;
using PerformanceMonitor.Darling.Service.Mcp;
using Xunit;

namespace Darling.Tests;

/// <summary>#8737: the <c>dpt1_</c> personal MCP token: minted from random bytes, parsed back to the id and the secret's hash.</summary>
public sealed class PersonalTokenFormatTests
{
    private const string GoodId = "0123456789abcdef";
    private static readonly string GoodSecret = new('A', 43);

    [Fact]
    public void Mint_ValueParsesBackToSameIdAndHash()
    {
        var minted = PersonalTokenFormat.Mint();

        Assert.True(PersonalTokenFormat.TryParse(minted.Value, out var tokenId, out var hash));
        Assert.Equal(minted.TokenId, tokenId);
        Assert.Equal(minted.SecretSha256, hash);
    }

    [Fact]
    public void Mint_ValueIs65CharsWithPrefix()
    {
        var minted = PersonalTokenFormat.Mint();

        Assert.Equal(65, minted.Value.Length);
        Assert.StartsWith(PersonalTokenFormat.Prefix, minted.Value, StringComparison.Ordinal);
    }

    [Fact]
    public void Mint_TwoMintsDiffer()
    {
        var first = PersonalTokenFormat.Mint();
        var second = PersonalTokenFormat.Mint();

        Assert.NotEqual(first.Value, second.Value);
        Assert.NotEqual(first.TokenId, second.TokenId);
    }

    [Fact]
    public void Mint_HashIsTheSha256OfTheSecretText()
    {
        var minted = PersonalTokenFormat.Mint();
        var secret = minted.Value.Substring(PersonalTokenFormat.Prefix.Length + minted.TokenId.Length + 1);

        Assert.Equal(SHA256.HashData(Encoding.ASCII.GetBytes(secret)), minted.SecretSha256);
        Assert.Equal(minted.SecretSha256, PersonalTokenFormat.HashSecret(secret));
    }

    [Theory]
    [InlineData(null)]
    [InlineData("")]
    [InlineData("dpt2_0123456789abcdef_AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA")]
    [InlineData("dpt1_0123456789ABCDEF_AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA")]
    [InlineData("dpt1_0123456789abcde_AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA")]
    [InlineData("dpt1_0123456789abcdef_AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA")]
    [InlineData("dpt1_0123456789abcdef_AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA+")]
    [InlineData("dpt1_0123456789abcdef_AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA=")]
    [InlineData("abcdefghijklmnopqrstuvwxyz0123456789ABCD")]
    public void TryParse_RejectsAMalformedValue(string? value)
    {
        Assert.False(PersonalTokenFormat.TryParse(value, out var tokenId, out var hash));
        Assert.Equal(string.Empty, tokenId);
        Assert.Empty(hash);
    }

    [Fact]
    public void TryParse_SecretContainingUnderscoreAndDash_Parses()
    {
        var secret = "_-" + new string('A', 41);
        var value = PersonalTokenFormat.Prefix + GoodId + "_" + secret;

        Assert.True(PersonalTokenFormat.TryParse(value, out var tokenId, out var hash));
        Assert.Equal(GoodId, tokenId);
        Assert.Equal(PersonalTokenFormat.HashSecret(secret), hash);
    }

    [Fact]
    public void TryParse_WellFormedHandBuiltValue_Parses()
    {
        Assert.True(PersonalTokenFormat.TryParse(PersonalTokenFormat.Prefix + GoodId + "_" + GoodSecret, out var tokenId, out _));
        Assert.Equal(GoodId, tokenId);
    }

    /// <summary>M3: the record's printed form names the token id only, so an interpolation in a log line or an assertion cannot leak the value.</summary>
    [Fact]
    public void Minted_ToString_NeverCarriesTheValueOrTheSecret()
    {
        var minted = PersonalTokenFormat.Mint();
        var secret = minted.Value[(PersonalTokenFormat.Prefix.Length + minted.TokenId.Length + 1)..];

        var printed = minted.ToString();

        Assert.DoesNotContain(minted.Value, printed, StringComparison.Ordinal);
        Assert.DoesNotContain(secret, printed, StringComparison.Ordinal);
        Assert.DoesNotContain(Convert.ToHexString(minted.SecretSha256), printed, StringComparison.OrdinalIgnoreCase);
        Assert.Contains(minted.TokenId, printed, StringComparison.Ordinal);
        Assert.DoesNotContain(minted.Value, $"{minted}", StringComparison.Ordinal);
    }
}
