/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using System.Buffers.Text;
using System.Security.Cryptography;
using System.Text;

namespace PerformanceMonitor.Darling.Service.Mcp;

/// <summary>A freshly minted personal token: the <see cref="Value"/> shown to its owner once, and what the store keeps. Its printed form
/// (<c>ToString</c>, and so any interpolation) names the <see cref="TokenId"/> only, never the value or the hash.</summary>
internal sealed record MintedToken(string Value, string TokenId, byte[] SecretSha256)
{
    private bool PrintMembers(StringBuilder builder)
    {
        builder.Append("TokenId = ").Append(TokenId);
        return true;
    }
}

/// <summary>
/// The personal MCP token (#8737): <c>dpt1_</c> + 16 lowercase hex characters (the token id, 8 random bytes) + <c>_</c> + 43 base64url
/// characters without padding (the secret, 32 random bytes), 65 characters in all. The store keeps the id and the SHA-256 of the secret's
/// text, never the secret. The secret may hold <c>_</c> and <c>-</c>, so a value is split on its first two <c>_</c> only.
/// </summary>
internal static class PersonalTokenFormat
{
    /// <summary>What every personal token starts with.</summary>
    public const string Prefix = "dpt1_";

    private const int IdBytes = 8;
    private const int SecretBytes = 32;
    private const int IdLength = IdBytes * 2;
    private const int SecretLength = 43;

    /// <summary>Mints a token from the system's random number generator.</summary>
    public static MintedToken Mint()
    {
        var tokenId = Convert.ToHexString(RandomNumberGenerator.GetBytes(IdBytes)).ToLowerInvariant();
        var secret = Base64Url.EncodeToString(RandomNumberGenerator.GetBytes(SecretBytes));
        return new MintedToken(Prefix + tokenId + "_" + secret, tokenId, HashSecret(secret));
    }

    /// <summary>The SHA-256 of the secret's ASCII text.</summary>
    public static byte[] HashSecret(string secret) => SHA256.HashData(Encoding.ASCII.GetBytes(secret));

    /// <summary>Splits a presented value into its token id and the hash of its secret. False, with an empty id and hash, for anything
    /// that is not shaped like a minted token; the secret is never returned.</summary>
    public static bool TryParse(string? value, out string tokenId, out byte[] secretSha256)
    {
        tokenId = string.Empty;
        secretSha256 = Array.Empty<byte>();

        if (value is null || value.Length != Prefix.Length + IdLength + 1 + SecretLength
            || !value.StartsWith(Prefix, StringComparison.Ordinal)
            || value[Prefix.Length + IdLength] != '_')
        {
            return false;
        }

        var id = value.Substring(Prefix.Length, IdLength);
        var secret = value.Substring(Prefix.Length + IdLength + 1);
        foreach (var c in id)
        {
            if (!(c is >= '0' and <= '9' or >= 'a' and <= 'f'))
            {
                return false;
            }
        }

        foreach (var c in secret)
        {
            if (!(c is >= 'A' and <= 'Z' or >= 'a' and <= 'z' or >= '0' and <= '9' or '-' or '_'))
            {
                return false;
            }
        }

        tokenId = id;
        secretSha256 = HashSecret(secret);
        return true;
    }
}
