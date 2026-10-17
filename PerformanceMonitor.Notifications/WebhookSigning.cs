/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using System.Collections.Generic;
using System.Globalization;
using System.Security.Cryptography;
using System.Text;

namespace PerformanceMonitor.Notifications;

/// <summary>
/// The signing keys of one generic webhook destination (#5998): the current key, and the previous key while the rotation
/// overlap lasts. In the store each value is sealed; after the reader opens them they hold the key text
/// (<c>whsec_</c> and base64). <see cref="ToString"/> never prints a key, so a route or settings object that is logged
/// does not carry one.
/// </summary>
/// <param name="Key">The current key, or empty for none.</param>
/// <param name="PreviousKey">The key the current one replaced, or empty for none.</param>
/// <param name="PreviousUntilUtc">When the previous key stops being sent, in UTC, or null. The store writes it on the
/// database's clock.</param>
/// <param name="DatabaseClockAhead">How far the database's clock was ahead of the reader's when the row was read (zero when
/// the two agree or the reader never measured it). A window end is compared on the database's clock, the clock that wrote
/// it, so skew between the two machines does not move the 7-day cutoff (#5998).</param>
public sealed record WebhookSigningKeys(string Key, string PreviousKey, DateTime? PreviousUntilUtc, TimeSpan DatabaseClockAhead = default)
{
    /// <summary>No key: the destination is sent unsigned (D4: signing is on once a key exists).</summary>
    public static readonly WebhookSigningKeys None = new("", "", null);

    /// <summary>Whether a current key is set.</summary>
    public bool HasKey => !string.IsNullOrWhiteSpace(Key);

    /// <summary>
    /// The keys a send at <paramref name="nowUtc"/> signs with: the current key, then the previous key while
    /// <paramref name="nowUtc"/> is before <see cref="PreviousUntilUtc"/>. Empty when no current key is set. After the
    /// overlap the previous key is gone: it signs nothing, even while it is still stored.
    /// </summary>
    public IReadOnlyList<string> LiveKeys(DateTime nowUtc)
    {
        if (!HasKey)
        {
            return Array.Empty<string>();
        }

        if (PreviousInWindow(nowUtc))
        {
            return new[] { Key, PreviousKey };
        }

        return new[] { Key };
    }

    /// <summary>
    /// Whether a previous key is set and its window is still open at <paramref name="nowUtc"/> (the reader's clock): the
    /// comparison is made on the database's clock, which wrote the window end.
    /// </summary>
    public bool PreviousInWindow(DateTime nowUtc) =>
        !string.IsNullOrWhiteSpace(PreviousKey) && PreviousUntilUtc is { } until && nowUtc + DatabaseClockAhead < until;

    /// <summary>Equal when the keys and the window end are equal. The clock offset is a measurement that differs a little on every
    /// read of the same row, so two reads of an unchanged row must still compare equal.</summary>
    public bool Equals(WebhookSigningKeys? other) =>
        other is not null
        && string.Equals(Key, other.Key, StringComparison.Ordinal)
        && string.Equals(PreviousKey, other.PreviousKey, StringComparison.Ordinal)
        && PreviousUntilUtc == other.PreviousUntilUtc;

    /// <inheritdoc />
    public override int GetHashCode() => HashCode.Combine(Key, PreviousKey, PreviousUntilUtc);

    /// <summary>Never prints a key.</summary>
    public override string ToString() =>
        "WebhookSigningKeys { HasKey = " + HasKey.ToString(CultureInfo.InvariantCulture)
        + ", HasPreviousKey = " + (!string.IsNullOrWhiteSpace(PreviousKey)).ToString(CultureInfo.InvariantCulture)
        + ", PreviousUntilUtc = " + (PreviousUntilUtc?.ToString("o", CultureInfo.InvariantCulture) ?? "none") + " }";
}

/// <summary>What one signed send carries (#5998): the keys that sign it, its message id and its timestamp.
/// <see cref="ToString"/> prints no key.</summary>
internal sealed record WebhookSendSigning(IReadOnlyList<string> Keys, string MessageId, long Timestamp)
{
    public override string ToString() => "WebhookSendSigning { Keys = " + Keys.Count.ToString(CultureInfo.InvariantCulture) + ", MessageId = " + MessageId + " }";
}

/// <summary>
/// Standard Webhooks signing (#5998, decision D1): the <c>webhook-id</c>, <c>webhook-timestamp</c> and
/// <c>webhook-signature</c> headers. The signature is an HMAC-SHA256 over <c>id.timestamp.body</c>, keyed with the bytes
/// the key's base64 holds, written as <c>v1,</c> and the base64 of the hash. During a rotation both keys sign, and the
/// header carries both signatures separated by a space (decision D3). The receiver checks the timestamp window and
/// dedupes on the id (docs/signed-webhooks.md); the sender only signs.
/// </summary>
public static class WebhookSigning
{
    /// <summary>The header that carries the message id. A receiver dedupes on it.</summary>
    public const string IdHeader = "webhook-id";

    /// <summary>The header that carries the send time, in whole seconds since 1970-01-01 UTC.</summary>
    public const string TimestampHeader = "webhook-timestamp";

    /// <summary>The header that carries the signatures.</summary>
    public const string SignatureHeader = "webhook-signature";

    /// <summary>The prefix of a key's text, as Standard Webhooks writes a secret.</summary>
    public const string KeyPrefix = "whsec_";

    /// <summary>The version prefix of one signature.</summary>
    public const string SignatureVersion = "v1";

    /// <summary>How many random bytes a new key holds. Standard Webhooks asks for 24 to 64.</summary>
    public const int KeyBytes = 32;

    /// <summary>The fewest bytes a key may hold, as Standard Webhooks asks.</summary>
    public const int MinKeyBytes = 24;

    /// <summary>How many days the previous key keeps signing after a rotation (decision D3). The store write that rotates
    /// a key reads this number, and the receiver docs state it.</summary>
    public const int RotationOverlapDays = 7;

    /// <summary><see cref="RotationOverlapDays"/> as a span.</summary>
    public static readonly TimeSpan RotationOverlap = TimeSpan.FromDays(RotationOverlapDays);

    /// <summary>A new random key: <see cref="KeyPrefix"/> and the base64 of <see cref="KeyBytes"/> random bytes.</summary>
    public static string NewKey() => KeyPrefix + Convert.ToBase64String(RandomNumberGenerator.GetBytes(KeyBytes));

    /// <summary>A new message id: <c>msg_</c> and 32 hex digits.</summary>
    public static string NewMessageId() => "msg_" + Guid.NewGuid().ToString("N");

    /// <summary>Whether <paramref name="key"/> is key text this signer can use: <see cref="KeyPrefix"/> (optional) and
    /// the base64 of at least <see cref="MinKeyBytes"/> bytes.</summary>
    public static bool TryDecodeKey(string? key, out byte[] bytes)
    {
        bytes = Array.Empty<byte>();
        var text = key?.Trim() ?? "";
        if (text.StartsWith(KeyPrefix, StringComparison.Ordinal))
        {
            text = text[KeyPrefix.Length..];
        }

        try
        {
            bytes = Convert.FromBase64String(text);
        }
        catch (FormatException)
        {
            bytes = Array.Empty<byte>();
            return false;
        }

        return bytes.Length >= MinKeyBytes;
    }

    /// <summary>The seconds since 1970-01-01 UTC of <paramref name="utc"/>, as the timestamp header writes it.</summary>
    public static long UnixSeconds(DateTime utc) =>
        new DateTimeOffset(DateTime.SpecifyKind(utc, DateTimeKind.Utc)).ToUnixTimeSeconds();

    /// <summary>
    /// The <c>webhook-signature</c> value for <paramref name="body"/>: one <c>v1,</c> signature per key, in the order
    /// given, separated by a space. Throws <see cref="ArgumentException"/> when a key is not key text; the message names
    /// no key.
    /// </summary>
    public static string Sign(IReadOnlyList<string> keys, string messageId, long timestamp, ReadOnlySpan<byte> body)
    {
        ArgumentNullException.ThrowIfNull(keys);
        var prefix = Encoding.UTF8.GetBytes(messageId + "." + timestamp.ToString(CultureInfo.InvariantCulture) + ".");
        var signed = new byte[prefix.Length + body.Length];
        prefix.CopyTo(signed, 0);
        body.CopyTo(signed.AsSpan(prefix.Length));

        var parts = new List<string>(keys.Count);
        foreach (var key in keys)
        {
            if (!TryDecodeKey(key, out var secret))
            {
                throw new ArgumentException("A webhook signing key is not valid key text.", nameof(keys));
            }

            parts.Add(SignatureVersion + "," + Convert.ToBase64String(HMACSHA256.HashData(secret, signed)));
        }

        return string.Join(' ', parts);
    }
}
