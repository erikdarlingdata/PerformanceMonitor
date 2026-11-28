/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using System.Buffers.Binary;
using System.Buffers.Text;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Security.Cryptography;
using System.Text.Json;

namespace PerformanceMonitor.Common;

/// <summary>
/// The <c>requestState</c> a confirmation question carries (#9562), so a yes counts only for the exact question this process asked.
/// The client echoes the state when it sends the call again, and the call runs only when the state
/// <list type="bullet">
/// <item>was issued by this process, for this tool and these arguments (an HMAC-SHA256 under a random 32-byte key made at start,
/// never written down, logged or returned, so a state altered by one byte, or issued for another tool or other arguments, fails);</item>
/// <item>is not older than <see cref="MaxAge"/>;</item>
/// <item>has not been answered already: each state carries a random nonce, and the nonce is spent by the first answer, so the same
/// call replayed with the same state and a yes is asked again instead of running twice.</item>
/// </list>
/// The spent nonces are held in memory only until their state is too old to be accepted anyway. A restart drops the key and the set,
/// so every question pending at that moment is asked again, which is the safe side.
///
/// <para>A state is <c>base64url(issued-at, nonce, mac)</c>. The arguments are hashed as canonical JSON (properties sorted at every
/// level), so the order a client sends them in does not matter. The answer itself travels in <c>inputResponses</c>, not in the
/// arguments, so no argument is left out of the hash.</para>
/// </summary>
public sealed class McpConfirmationStates
{
    /// <summary>How long a state stays good after the question was asked.</summary>
    public static readonly TimeSpan MaxAge = TimeSpan.FromMinutes(10);

    private const int NonceBytes = 16;
    private const int StampBytes = 8;
    private const int MacBytes = 32;

    /// <summary>The instance the hosts use.</summary>
    public static McpConfirmationStates Shared { get; } = new();

    private readonly object _gate = new();
    private readonly byte[] _key = RandomNumberGenerator.GetBytes(32);
    private readonly TimeProvider _clock;
    private readonly Dictionary<string, long> _spent = new(StringComparer.Ordinal);

    /// <summary>A state that passed every check but the single use: <see cref="TrySpend"/> decides that.</summary>
    public readonly record struct Ticket(string Nonce, long IssuedAtMs);

    /// <summary>A separate instance with its own key and clock, for a test.</summary>
    public McpConfirmationStates(TimeProvider? clock = null) => _clock = clock ?? TimeProvider.System;

    /// <summary>Issues a state for <paramref name="tool"/> called with <paramref name="arguments"/>.</summary>
    public string Issue(string tool, IDictionary<string, JsonElement> arguments)
    {
        var header = new byte[StampBytes + NonceBytes];
        BinaryPrimitives.WriteInt64BigEndian(header, _clock.GetUtcNow().ToUnixTimeMilliseconds());
        RandomNumberGenerator.Fill(header.AsSpan(StampBytes));

        var state = new byte[header.Length + MacBytes];
        header.CopyTo(state, 0);
        MacOf(header, tool, arguments).CopyTo(state, header.Length);
        return Base64Url.EncodeToString(state);
    }

    /// <summary>
    /// The ticket for <paramref name="state"/> when it was issued by this process for exactly this tool and these arguments less than
    /// <see cref="MaxAge"/> ago, otherwise null. It does not spend the state.
    /// </summary>
    public Ticket? Check(string tool, IDictionary<string, JsonElement> arguments, string? state)
    {
        if (string.IsNullOrEmpty(state) || !Base64Url.IsValid(state))
        {
            return null;
        }

        var raw = Base64Url.DecodeFromChars(state);
        if (raw.Length != StampBytes + NonceBytes + MacBytes)
        {
            return null;
        }

        var header = raw.AsSpan(0, StampBytes + NonceBytes).ToArray();
        if (!CryptographicOperations.FixedTimeEquals(raw.AsSpan(header.Length), MacOf(header, tool, arguments)))
        {
            return null;
        }

        var issuedAtMs = BinaryPrimitives.ReadInt64BigEndian(header);
        if (_clock.GetUtcNow().ToUnixTimeMilliseconds() - issuedAtMs >= (long)MaxAge.TotalMilliseconds)
        {
            return null;
        }

        return new Ticket(Convert.ToHexString(header, StampBytes, NonceBytes), issuedAtMs);
    }

    /// <summary>True once for a ticket: the first answer spends its nonce, and every later answer with the same state gets false.</summary>
    public bool TrySpend(Ticket ticket)
    {
        lock (_gate)
        {
            var oldest = _clock.GetUtcNow().ToUnixTimeMilliseconds() - (long)MaxAge.TotalMilliseconds;
            foreach (var expired in _spent.Where(kv => kv.Value <= oldest).Select(kv => kv.Key).ToList())
            {
                _spent.Remove(expired);
            }

            return _spent.TryAdd(ticket.Nonce, ticket.IssuedAtMs);
        }
    }

    private byte[] MacOf(byte[] header, string tool, IDictionary<string, JsonElement> arguments)
    {
        using var buffer = new MemoryStream();
        buffer.Write(header);
        using (var writer = new Utf8JsonWriter(buffer))
        {
            writer.WriteStartObject();
            writer.WriteString("tool", tool);
            writer.WritePropertyName("arguments");
            writer.WriteStartObject();
            foreach (var argument in arguments.OrderBy(a => a.Key, StringComparer.Ordinal))
            {
                writer.WritePropertyName(argument.Key);
                WriteCanonical(writer, argument.Value);
            }

            writer.WriteEndObject();
            writer.WriteEndObject();
        }

        return HMACSHA256.HashData(_key, buffer.ToArray());
    }

    private static void WriteCanonical(Utf8JsonWriter writer, JsonElement value)
    {
        switch (value.ValueKind)
        {
            case JsonValueKind.Object:
                writer.WriteStartObject();
                foreach (var property in value.EnumerateObject().OrderBy(p => p.Name, StringComparer.Ordinal))
                {
                    writer.WritePropertyName(property.Name);
                    WriteCanonical(writer, property.Value);
                }

                writer.WriteEndObject();
                break;
            case JsonValueKind.Array:
                writer.WriteStartArray();
                foreach (var item in value.EnumerateArray())
                {
                    WriteCanonical(writer, item);
                }

                writer.WriteEndArray();
                break;
            default:
                value.WriteTo(writer);
                break;
        }
    }
}
