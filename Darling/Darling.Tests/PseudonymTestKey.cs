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
using PerformanceMonitor.Common;

namespace Darling.Tests;

/// <summary>
/// The fixed key the pseudonym tests use (#6066): the 32 bytes 0x00 to 0x1f. <see cref="Login"/> and <see cref="Host"/> compute
/// the expected pseudonym here, with the formula written out again rather than through <see cref="PseudonymKey"/>, so a test
/// that compares them checks the production code against an independent copy of the rule.
/// </summary>
internal static class PseudonymTestKey
{
    public static readonly byte[] Bytes = Enumerable32();

    public static readonly PseudonymKey Key = new(Bytes);

    private static byte[] Enumerable32()
    {
        var bytes = new byte[32];
        for (int i = 0; i < bytes.Length; i++) bytes[i] = (byte)i;
        return bytes;
    }

    public static string Login(string name) => Pseudonym("login", name);

    public static string Host(string name) => Pseudonym("host", name);

    private static string Pseudonym(string kind, string name)
    {
        byte[] mac = HMACSHA256.HashData(Bytes, Encoding.UTF8.GetBytes(kind + "\n" + name.Trim().ToLowerInvariant()));
        return kind + "#" + Convert.ToHexStringLower(mac.AsSpan(0, 4));
    }
}
