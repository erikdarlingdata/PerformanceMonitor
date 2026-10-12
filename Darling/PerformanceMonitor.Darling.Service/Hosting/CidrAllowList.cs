/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using System.Collections.Generic;
using System.Net;
using System.Net.Sockets;
using System.Text.Json;
using System.Text.Json.Serialization;

namespace PerformanceMonitor.Darling.Service.Hosting;

/// <summary>
/// #5288: the parsed form of <c>mcp.network.allowFrom</c> / <c>web.network.allowFrom</c> — one CIDR or a LIST
/// of them. The config property stays a <c>string?</c> (every consumer keeps compiling, and a JSON array is
/// joined with <c>,</c> by <see cref="CidrListJsonConverter"/>), and THIS type is the one parser: the bind
/// ladder (<see cref="DarlingHostBinding.ResolveBind"/>), the in-app gate
/// (<see cref="DarlingHostBinding.IsRemoteAddressAllowed"/>) and both hosts' start-up all read the list through
/// <see cref="TryParse"/>, so what the ladder validates is exactly what the gate enforces.
///
/// <para>The text is split on <c>,</c> and each entry is trimmed. An EMPTY entry (a trailing comma, <c>,,</c>,
/// a blank array element) is refused rather than skipped: a hand-edit that leaves one behind is a typo worth
/// failing loudly on, and a silently narrower or wider list is the wrong way to find out. Each entry must be
/// CIDR form (<c>/32</c> or <c>/128</c> for one address, exactly as before the list existed), parsed by
/// <c>IPNetwork.TryParse</c>, which MASKS host bits rather than refusing them:
/// <c>192.168.1.5/24</c> is accepted as <c>192.168.1.0/24</c>, and <see cref="ToString"/> (and so the start
/// line) shows the masked form, which is how a masked entry becomes visible. Duplicates (after masking) are
/// dropped and the first-seen order is kept.</para>
///
/// <para><b>An address must be spelled the plain way</b> (<see cref="IsPlainCidrText"/>): an IPv4 address, or the
/// dotted tail of an IPv6 one, is exactly four decimal numbers 0-255 with no leading zero, and an IPv6 zone
/// index (<c>%</c>) is refused, so the range written is the range enforced and opened in the firewall.
/// <c>IPNetwork.TryParse</c> alone also takes the inet_aton spellings (<c>192.168.010.0/24</c> reads as
/// <c>192.168.8.0/24</c> because a leading zero is octal, <c>0x0A.0.0.0/8</c> as <c>10.0.0.0/8</c>, <c>10/8</c>
/// as <c>0.0.0.0/8</c>); those are refused. The text is checked as written, BEFORE masking, so
/// <c>192.168.1.5/24</c> still parses.</para>
///
/// <para><b>An IPv4-mapped IPv6 entry (<c>::ffff:10.0.0.0/104</c>) is refused.</b> <c>IPNetwork.Parse</c>
/// accepts it, but <see cref="DarlingHostBinding.IsRemoteAddressAllowed"/> maps a mapped REMOTE to IPv4 before
/// it tests the list, so a mapped entry can never match anything — dead config the family rule exists to
/// refuse loudly.</para>
///
/// <para><b><c>default(CidrAllowList)</c> is EMPTY and admits nobody</b> (<see cref="Contains"/> is false,
/// <see cref="ToString"/> is empty). That is the opposite of <c>default(IPNetwork)</c>, which is
/// <c>0.0.0.0/0</c> and admits every IPv4 address, so the implicit conversion from <see cref="IPNetwork"/>
/// below carries a REAL network only: no <c>IPNetwork</c> default may reach it, which is why every former
/// <c>IPNetwork allowedCidr = default</c> is now <c>CidrAllowList allowedCidr = default</c>. Loopback is not a
/// list matter: <see cref="DarlingHostBinding.IsRemoteAddressAllowed"/> admits it before it asks the list.</para>
/// </summary>
internal readonly struct CidrAllowList
{
    /// <summary>Null for <c>default</c>; otherwise at least one entry (the parser never builds an empty list).</summary>
    private readonly IPNetwork[]? _entries;

    private CidrAllowList(IPNetwork[] entries) => _entries = entries;

    /// <summary>The entries, masked and de-duplicated, in the order written. Empty for <c>default</c>.</summary>
    internal IReadOnlyList<IPNetwork> Entries => _entries ?? Array.Empty<IPNetwork>();

    /// <summary>How many entries the list holds (0 for <c>default</c>).</summary>
    internal int Count => _entries?.Length ?? 0;

    /// <summary>
    /// Parses <paramref name="text"/> (one CIDR, or CIDRs separated by commas) per the rules on the type.
    /// Null, empty, whitespace-only, an empty entry, a non-CIDR entry (including a bare address with no prefix),
    /// an address not spelled the plain way (<see cref="IsPlainCidrText"/>) and an IPv4-mapped IPv6 entry all
    /// return false with <paramref name="list"/> left at <c>default</c>. Never throws.
    /// </summary>
    internal static bool TryParse(string? text, out CidrAllowList list)
    {
        list = default;
        if (string.IsNullOrWhiteSpace(text))
        {
            return false;
        }

        var entries = new List<IPNetwork>();
        var seen = new HashSet<string>(StringComparer.Ordinal);
        foreach (var raw in text.Split(','))
        {
            var entry = raw.Trim();
            if (entry.Length == 0
                || !IPNetwork.TryParse(entry, out var network)
                || network.BaseAddress.IsIPv4MappedToIPv6
                || !IsPlainCidrText(entry))
            {
                return false;
            }

            if (seen.Add(network.ToString()))
            {
                entries.Add(network);
            }
        }

        list = new CidrAllowList(entries.ToArray());
        return true;
    }

    /// <summary>
    /// #5288: true when the ONE entry <paramref name="cidr"/> (<c>address/prefix</c>, already trimmed) spells its
    /// address the plain way. The text before the LAST <c>/</c> is checked as written, BEFORE any masking, so
    /// <c>192.168.1.5/24</c> is plain (the parser masks it to <c>192.168.1.0/24</c> afterwards). An IPv4 address,
    /// or the dotted tail of an IPv6 address (<c>64:ff9b::192.0.2.33/96</c>), must be exactly four decimal
    /// numbers 0-255 with no leading zero. An IPv6 address with no dotted tail is hex groups only and has no
    /// second reading. Any <c>%</c> (an IPv6 zone index) is false.
    ///
    /// <para>This is shared by <see cref="TryParse"/> and the store's single-CIDR check
    /// (<c>DarlingManagedPostgres.ResolveNetworkExposure</c>, which feeds <c>pg_hba.conf</c>). It does not
    /// replace <c>IPNetwork.TryParse</c>: call it on an entry that parser already accepted. Pure; never throws.</para>
    /// </summary>
    internal static bool IsPlainCidrText(string cidr)
    {
        if (cidr.Contains('%', StringComparison.Ordinal))
        {
            return false;
        }

        var slash = cidr.LastIndexOf('/');
        if (slash <= 0)
        {
            return false;
        }

        var address = cidr[..slash];
        var colon = address.LastIndexOf(':');
        if (colon < 0)
        {
            return IsPlainDottedQuad(address);
        }

        /* IPv6: only a dotted tail has a decimal spelling to check, and it can only sit after the last colon. */
        var tail = address[(colon + 1)..];
        return !address.AsSpan(0, colon).Contains('.')
            && (!tail.Contains('.', StringComparison.Ordinal) || IsPlainDottedQuad(tail));
    }

    /// <summary>Exactly four decimal numbers 0-255 joined by <c>.</c>, each one to three ASCII digits with no
    /// leading zero (a lone <c>0</c> is fine).</summary>
    private static bool IsPlainDottedQuad(string text)
    {
        var parts = text.Split('.');
        if (parts.Length != 4)
        {
            return false;
        }

        foreach (var part in parts)
        {
            if (part.Length is 0 or > 3 || (part.Length > 1 && part[0] == '0'))
            {
                return false;
            }

            var value = 0;
            foreach (var c in part)
            {
                if (c is < '0' or > '9')
                {
                    return false;
                }

                value = (value * 10) + (c - '0');
            }

            if (value > 255)
            {
                return false;
            }
        }

        return true;
    }

    /// <summary>
    /// <see cref="TryParse"/> for a caller that has ALREADY validated the text (the hosts, after
    /// <see cref="DarlingHostBinding.ResolveBind"/> answered network mode): throws
    /// <see cref="FormatException"/> instead of returning false.
    /// </summary>
    internal static CidrAllowList Parse(string text)
        => TryParse(text, out var list)
            ? list
            : throw new FormatException($"'{text}' is not a valid CIDR list (one CIDR, or CIDRs separated by commas).");

    /// <summary>True when <paramref name="address"/> falls inside ANY entry. False for <c>default</c> and for a
    /// null address (fail closed). Pure range membership: loopback and IPv4-mapped unwrapping are
    /// <see cref="DarlingHostBinding.IsRemoteAddressAllowed"/>'s job, in one place.</summary>
    internal bool Contains(IPAddress? address)
    {
        if (address is null || _entries is null)
        {
            return false;
        }

        foreach (var entry in _entries)
        {
            if (entry.Contains(address))
            {
                return true;
            }
        }

        return false;
    }

    /// <summary>True when EVERY entry is of <paramref name="family"/> — the bind ladder's family rule. An empty
    /// (<c>default</c>) list has no entry to be of any family, so it is false: it must not pass as valid.</summary>
    internal bool AllInFamily(AddressFamily family)
    {
        if (_entries is null)
        {
            return false;
        }

        foreach (var entry in _entries)
        {
            if (entry.BaseAddress.AddressFamily != family)
            {
                return false;
            }
        }

        return true;
    }

    /// <summary>The canonical text: each entry as <c>base/prefix</c> (host bits masked), joined by <c>,</c> with
    /// no spaces — so ONE entry is byte-for-byte what <see cref="IPNetwork.ToString"/> gave before the list
    /// existed. Empty for <c>default</c>.</summary>
    public override string ToString()
        => _entries is null ? "" : string.Join(",", _entries);

    /// <summary>One network as a one-entry list, so a caller (a test, the firewall check) that holds an
    /// <see cref="IPNetwork"/> keeps compiling. A REAL network only: <c>default(IPNetwork)</c> is
    /// <c>0.0.0.0/0</c> (allow every IPv4 address) and must never be converted — see the type remarks.</summary>
    public static implicit operator CidrAllowList(IPNetwork network) => new([network]);
}

/// <summary>
/// #5288: reads <c>allowFrom</c> from darling.json as a JSON string (one CIDR, or CIDRs separated by commas) OR a
/// JSON array of strings, and hands the config property one <c>string?</c> either way: an array is joined with
/// <c>,</c> (no spaces). The converter validates SHAPE only — it never parses a CIDR, so
/// <see cref="CidrAllowList.TryParse"/> stays the one parser and a bad entry degrades the listener through the
/// bind ladder (<c>AllowFromInvalid</c>, Critical, loopback-only) instead of failing the whole config load.
///
/// <para>Any other shape throws <see cref="JsonException"/>, which is what a non-string <c>allowFrom</c> already
/// did: a number or object, or an array holding a non-string element (a number, <c>null</c>, a nested array).
/// A JSON <c>null</c> is a null <c>allowFrom</c>, as before.</para>
/// </summary>
internal sealed class CidrListJsonConverter : JsonConverter<string?>
{
    public override string? Read(ref Utf8JsonReader reader, Type typeToConvert, JsonSerializerOptions options)
    {
        switch (reader.TokenType)
        {
            case JsonTokenType.Null:
                return null;

            case JsonTokenType.String:
                return reader.GetString();

            case JsonTokenType.StartArray:
            {
                var parts = new List<string>();
                while (reader.Read())
                {
                    if (reader.TokenType == JsonTokenType.EndArray)
                    {
                        return string.Join(",", parts);
                    }

                    if (reader.TokenType != JsonTokenType.String)
                    {
                        throw new JsonException(
                            $"allowFrom array elements must be strings (CIDRs); found {reader.TokenType}.");
                    }

                    parts.Add(reader.GetString()!);
                }

                throw new JsonException("allowFrom array is not terminated.");
            }

            default:
                throw new JsonException(
                    $"allowFrom must be a string (one CIDR, or CIDRs separated by commas) or an array of strings; found {reader.TokenType}.");
        }
    }

    public override void Write(Utf8JsonWriter writer, string? value, JsonSerializerOptions options)
    {
        if (value is null)
        {
            writer.WriteNullValue();
        }
        else
        {
            writer.WriteStringValue(value);
        }
    }
}
