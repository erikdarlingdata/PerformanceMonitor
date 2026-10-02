/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using System.Globalization;
using System.Security.Cryptography;
using System.Text;
using System.Text.RegularExpressions;

namespace PerformanceMonitor.Darling.Service;

/// <summary>
/// A PostgreSQL SCRAM-SHA-256 password verifier, computed client-side (#3910), so role provisioning sends
/// <c>PASSWORD 'SCRAM-SHA-256$...'</c> instead of the password itself. PostgreSQL stores a string already in
/// that format as-is, whatever <c>password_encryption</c> says, which is exactly what psql's <c>\password</c>
/// does; the password never appears in any statement, so no surface that records statement text (the server
/// log's <c>STATEMENT:</c> lines, <c>log_statement</c>, <c>auto_explain</c>, <c>pg_stat_statements</c> with
/// utility tracking on) can capture it. A leaked verifier is not a login: the generated passwords are 32
/// random alphanumerics (~190 bits), far beyond a dictionary.
///
/// <para>The format and derivation are RFC 5802 / RFC 7677 as PostgreSQL implements them
/// (<c>scram_build_secret</c>): <c>SCRAM-SHA-256$&lt;iterations&gt;:&lt;salt&gt;$&lt;StoredKey&gt;:&lt;ServerKey&gt;</c>,
/// base64 with padding, where SaltedPassword = PBKDF2-HMAC-SHA-256(password, salt, iterations),
/// StoredKey = SHA-256(HMAC(SaltedPassword, "Client Key")) and ServerKey = HMAC(SaltedPassword, "Server Key").
/// PostgreSQL applies SASLprep to the password first; that is the identity on ASCII alphanumerics, the only
/// passwords this is given (<see cref="DarlingManagedPostgres.GeneratePassword"/>).</para>
/// </summary>
internal static class ScramSha256Verifier
{
    /// <summary>PostgreSQL's default <c>scram_iterations</c>.</summary>
    public const int DefaultIterations = 4096;

    /// <summary>PostgreSQL's <c>SCRAM_DEFAULT_SALT_LEN</c>.</summary>
    public const int SaltLength = 16;

    private const string Prefix = "SCRAM-SHA-256$";

    private static readonly Regex s_shape = new(
        @"^SCRAM-SHA-256\$(?<iterations>[0-9]{1,9}):(?<salt>[A-Za-z0-9+/]+={0,2})\$(?<stored>[A-Za-z0-9+/]+={0,2}):(?<server>[A-Za-z0-9+/]+={0,2})$",
        RegexOptions.Compiled | RegexOptions.CultureInvariant);

    /// <summary>A verifier for <paramref name="password"/> with a fresh random salt and PostgreSQL's default
    /// iteration count.</summary>
    public static string Create(string password) =>
        Compute(password, RandomNumberGenerator.GetBytes(SaltLength), DefaultIterations);

    /// <summary>The verifier for <paramref name="password"/> under the given salt and iteration count.</summary>
    public static string Compute(string password, byte[] salt, int iterations)
    {
        ArgumentNullException.ThrowIfNull(password);
        ArgumentNullException.ThrowIfNull(salt);
        ArgumentOutOfRangeException.ThrowIfLessThan(iterations, 1);

        var (storedKey, serverKey) = DeriveKeys(password, salt, iterations);
        return Prefix
            + iterations.ToString(CultureInfo.InvariantCulture) + ":" + Convert.ToBase64String(salt)
            + "$" + Convert.ToBase64String(storedKey) + ":" + Convert.ToBase64String(serverKey);
    }

    /// <summary>StoredKey and ServerKey, the two halves a server keeps.</summary>
    internal static (byte[] StoredKey, byte[] ServerKey) DeriveKeys(string password, byte[] salt, int iterations)
    {
        var saltedPassword = Rfc2898DeriveBytes.Pbkdf2(Encoding.UTF8.GetBytes(password), salt, iterations, HashAlgorithmName.SHA256, 32);
        var clientKey = HMACSHA256.HashData(saltedPassword, "Client Key"u8);
        var serverKey = HMACSHA256.HashData(saltedPassword, "Server Key"u8);
        return (SHA256.HashData(clientKey), serverKey);
    }

    /// <summary>Whether <paramref name="text"/> has the shape PostgreSQL recognizes as a SCRAM-SHA-256
    /// verifier (and therefore stores as-is). The shape admits no quote, so it is also what makes
    /// interpolating one into a <c>PASSWORD '...'</c> literal escaping-safe.</summary>
    public static bool IsVerifier(string? text) => text is not null && s_shape.IsMatch(text);

    /// <summary>
    /// Whether the verifier PostgreSQL holds (<c>pg_authid.rolpassword</c>) accepts <paramref name="password"/>:
    /// the keys are re-derived under the stored salt and iteration count and compared in constant time. False
    /// for anything that is not a SCRAM-SHA-256 verifier (an MD5 hash, a missing password), which provisioning
    /// then re-asserts.
    /// </summary>
    public static bool Verifies(string? storedSecret, string password)
    {
        ArgumentNullException.ThrowIfNull(password);
        if (storedSecret is null)
        {
            return false;
        }

        var match = s_shape.Match(storedSecret);
        if (!match.Success
            || !int.TryParse(match.Groups["iterations"].Value, NumberStyles.None, CultureInfo.InvariantCulture, out var iterations)
            || iterations < 1)
        {
            return false;
        }

        byte[] salt;
        byte[] storedKey;
        byte[] serverKey;
        try
        {
            salt = Convert.FromBase64String(match.Groups["salt"].Value);
            storedKey = Convert.FromBase64String(match.Groups["stored"].Value);
            serverKey = Convert.FromBase64String(match.Groups["server"].Value);
        }
        catch (FormatException)
        {
            return false;
        }

        var (expectedStored, expectedServer) = DeriveKeys(password, salt, iterations);
        return CryptographicOperations.FixedTimeEquals(expectedStored, storedKey)
            & CryptographicOperations.FixedTimeEquals(expectedServer, serverKey);
    }
}
