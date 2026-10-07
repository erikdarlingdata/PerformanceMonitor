/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using System.Buffers.Binary;
using System.Collections.Generic;
using System.Globalization;
using System.IO;
using System.Security.Cryptography;
using System.Text;

namespace PerformanceMonitor.Darling.Storage;

/// <summary>
/// What a sealed password belongs to (#5366): a purpose and the ordered connection fields the password is used with.
/// Both go into the sealed value's associated data, so a value opens only for the purpose and the fields it was sealed
/// for. The field set and order are part of the stored format: a migration that rewrites a bound column breaks every
/// value sealed over the old text, so a change here or to a bound column needs a plan for the values already stored.
/// </summary>
public sealed class PasswordBinding
{
    private const string FormatLabel = "PerformanceMonitor.Darling.sealed.v1";

    /// <summary>The purpose of a SQL login password or a service-principal secret (and of its connection test).</summary>
    public const string ServerPurpose = "server";

    /// <summary>The purpose of a remediation login password.</summary>
    public const string RemediationPurpose = "remediation";

    /// <summary>The purpose of the mail-server password.</summary>
    public const string SmtpPurpose = "smtp";

    /// <summary>The purpose of a webhook URL, the PagerDuty routing key and the generic webhook's headers.</summary>
    public const string WebhookPurpose = "webhook";

    /// <summary>The webhook slots, in the order the notification row lists its columns: the Teams URL, the Slack URL,
    /// the generic URL, the PagerDuty routing key and the generic headers.</summary>
    public static readonly IReadOnlyList<string> WebhookSlots = new[]
    {
        "teams", "slack", "generic", "pagerduty", "generic_headers",
    };

    /// <summary>The row id of the notification settings row, for <see cref="ForWebhook"/>.</summary>
    public const string WebhookSettingsRow = "notification";

    /// <summary>The row id of a notification route, for <see cref="ForWebhook"/>.</summary>
    public static string WebhookRouteRow(int routeId) => "route:" + Number(routeId);

    /// <summary>The names of the server purpose's fields, in the order <see cref="ForServer"/> binds them. The census
    /// checks these against <see cref="ServerConnectionIdentity"/>.</summary>
    internal static readonly IReadOnlyList<string> ServerFieldNames = new[]
    {
        nameof(ServerConnectionIdentity.Host), nameof(ServerConnectionIdentity.Port),
        nameof(ServerConnectionIdentity.Database), nameof(ServerConnectionIdentity.ReadOnlyIntent),
        nameof(ServerConnectionIdentity.Auth), nameof(ServerConnectionIdentity.Username),
        nameof(ServerConnectionIdentity.EncryptMode), nameof(ServerConnectionIdentity.TrustServerCertificate),
        nameof(ServerConnectionIdentity.MultiSubnetFailover), nameof(ServerConnectionIdentity.Engine),
    };

    /// <summary>The names of the remediation purpose's fields, in the order <see cref="ForRemediation"/> binds them. The
    /// census checks the remediation connection builder against these.</summary>
    internal static readonly IReadOnlyList<string> RemediationFieldNames = new[]
    {
        nameof(ServerConnectionIdentity.Host), nameof(ServerConnectionIdentity.Database),
        nameof(ServerConnectionIdentity.EncryptMode), nameof(ServerConnectionIdentity.TrustServerCertificate),
        nameof(ServerConnectionIdentity.MultiSubnetFailover), "RemediationUsername",
        nameof(ServerConnectionIdentity.Engine),
    };

    private static readonly Dictionary<string, int> FieldCounts = new Dictionary<string, int>(StringComparer.Ordinal)
    {
        [ServerPurpose] = 10,
        [RemediationPurpose] = 7,
        [SmtpPurpose] = 4,
        [WebhookPurpose] = 4,
    };

    private PasswordBinding(string purpose, string?[] fields)
    {
        if (!FieldCounts.TryGetValue(purpose, out var expected) || fields.Length != expected)
        {
            throw new InvalidOperationException("A password binding has a fixed number of fields for each purpose.");
        }

        Purpose = purpose;
        Fields = Array.AsReadOnly(fields);
    }

    /// <summary>The purpose the password is sealed for.</summary>
    public string Purpose { get; }

    /// <summary>The bound field values, in order. Null and empty bind the same.</summary>
    public IReadOnlyList<string?> Fields { get; }

    /// <summary>
    /// The binding for a server's SQL password or service-principal secret: host, port, database, read-only intent,
    /// authentication, username, encrypt mode, certificate trust, multi-subnet failover and engine. The server id is not
    /// bound, so a row keeps its password when it is renumbered.
    /// </summary>
    public static PasswordBinding ForServer(ServerConnectionIdentity c) => new(ServerPurpose, new string?[]
    {
        c.Host, Number(c.Port), c.Database, Flag(c.ReadOnlyIntent), Lower(c.Auth), c.Username,
        Lower(c.EncryptMode), Flag(c.TrustServerCertificate), Flag(c.MultiSubnetFailover), Lower(c.Engine),
    });

    /// <summary>The binding for a server's remediation password: host, database, encrypt mode, certificate trust,
    /// multi-subnet failover, the remediation username and engine.</summary>
    public static PasswordBinding ForRemediation(ServerConnectionIdentity c, string? remediationUsername) =>
        new(RemediationPurpose, new string?[]
        {
            c.Host, c.Database, Lower(c.EncryptMode), Flag(c.TrustServerCertificate), Flag(c.MultiSubnetFailover),
            remediationUsername, Lower(c.Engine),
        });

    /// <summary>The binding for the mail-server password: host, port, whether SSL is used, and the username.</summary>
    public static PasswordBinding ForSmtp(string? host, int port, bool useSsl, string? username) =>
        new(SmtpPurpose, new string?[] { host, Number(port), Flag(useSsl), username });

    /// <summary>
    /// The binding for a webhook value: its slot (one of <see cref="WebhookSlots"/>), the row it is stored in
    /// (<see cref="WebhookSettingsRow"/> or <see cref="WebhookRouteRow"/>), the proxy text of its channel, and, for the
    /// generic headers, the stored text of the same row's generic URL (empty for every other slot). A value opens only
    /// in the slot and row it was sealed for, and a changed proxy or generic URL needs the value again.
    /// </summary>
    public static PasswordBinding ForWebhook(string slot, string row, string? proxy, string? boundUrl)
    {
        var known = false;
        foreach (var name in WebhookSlots)
        {
            known |= string.Equals(name, slot, StringComparison.Ordinal);
        }

        if (!known)
        {
            throw new ArgumentException("A webhook binding names one of the webhook slots.", nameof(slot));
        }

        return new(WebhookPurpose, new string?[] { slot, row, proxy, boundUrl });
    }

    /// <summary>The associated data for a value sealed under <paramref name="keyId"/>: the format label, the key id, the
    /// purpose and each field, each as a 4-byte big-endian length and its UTF-8 bytes.</summary>
    public byte[] EncodeAad(string keyId) => Encode(keyId);

    /// <summary>
    /// SHA-256 of the same encoding with an empty key id. A value that is not sealed (a legacy one) is pinned to its row
    /// by this hash, because it has no key id.
    /// </summary>
    public byte[] LegacyPinHash() => SHA256.HashData(Encode(""));

    private byte[] Encode(string keyId)
    {
        using var buffer = new MemoryStream();
        Append(buffer, FormatLabel);
        Append(buffer, keyId);
        Append(buffer, Purpose);
        foreach (var field in Fields)
        {
            Append(buffer, field);
        }

        return buffer.ToArray();
    }

    private static void Append(MemoryStream buffer, string? text)
    {
        byte[] bytes;
        try
        {
            bytes = PasswordSeal.StrictUtf8.GetBytes(text ?? "");
        }
        catch (EncoderFallbackException)
        {
            // Never echo the value: the message names no field text and carries no inner exception.
            throw new ArgumentException("A password binding field is not valid text.");
        }

        Span<byte> length = stackalloc byte[4];
        BinaryPrimitives.WriteInt32BigEndian(length, bytes.Length);
        buffer.Write(length);
        buffer.Write(bytes);
    }

    private static string Number(int value) => value.ToString(CultureInfo.InvariantCulture);

    private static string Flag(bool value) => value ? "1" : "0";

    private static string? Lower(string? value) => value?.ToLowerInvariant();
}
