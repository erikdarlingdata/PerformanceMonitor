/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using System.Text;

namespace PerformanceMonitor.Darling.Storage;

/// <summary>
/// The SMTP password rules the Viewer's Settings window and the MCP <c>update_notification_channels</c> tool share (#6392): what text
/// can be stored, the sentences a refusal carries, and the seal for the host, port, SSL flag and user name the password is bound to.
/// One copy here; the Viewer's <c>ViewerSmtpSeal</c> and <c>ViewerPasswordSealer</c> point at it.
/// </summary>
public static class SmtpPasswordSealing
{
    private static readonly UTF8Encoding s_strictUtf8 = new(false, true);

    /// <summary>The refusal for a change to the host, port, SSL flag or user name when no password was given.</summary>
    public const string ReenterText = "Changing how mail is sent needs the SMTP password again.";

    /// <summary>The refusal for a password that is not valid text (a lone surrogate).</summary>
    public const string PasswordCharactersText = "The password contains characters that cannot be stored.";

    /// <summary>The refusal for a password longer than a sealed value can hold.</summary>
    public const string PasswordTooLongText = "The password is longer than can be stored.";

    /// <summary>The refusal for a connection field that is not valid text, naming the field.</summary>
    public static string FieldCharactersText(string field) => $"The {field} contains characters that cannot be stored.";

    /// <summary>Whether <paramref name="text"/> can be stored (it holds no lone surrogate).</summary>
    public static bool IsValidText(string? text)
    {
        if (text is null)
        {
            return true;
        }

        try
        {
            _ = s_strictUtf8.GetByteCount(text);
            return true;
        }
        catch (EncoderFallbackException)
        {
            return false;
        }
    }

    /// <summary>The name of the first bound field that is not valid text, or null when both are.</summary>
    public static string? FirstInvalidBoundField(string? host, string? username)
    {
        if (!IsValidText(host))
        {
            return "mail server";
        }

        return IsValidText(username) ? null : "SMTP user name";
    }

    /// <summary>
    /// Seals the SMTP password for the host, port, SSL flag and user name it is stored with, with <paramref name="seal"/> (the key's seal).
    /// Throws <see cref="NotificationRouteRefusedException"/> (kind Value) for text that cannot be stored; the sentence never carries a value.
    /// </summary>
    public static string Seal(
        string plaintext, string? host, int port, bool useSsl, string? username, Func<string, PasswordBinding, string> seal)
    {
        ArgumentNullException.ThrowIfNull(plaintext);
        ArgumentNullException.ThrowIfNull(seal);

        if (!IsValidText(plaintext))
        {
            throw new NotificationRouteRefusedException(PasswordCharactersText, NotificationRouteRefusalKind.Value);
        }

        if (FirstInvalidBoundField(host, username) is { } invalidField)
        {
            throw new NotificationRouteRefusedException(FieldCharactersText(invalidField), NotificationRouteRefusalKind.Value);
        }

        try
        {
            return seal(plaintext, PasswordBinding.ForSmtp(host, port, useSsl, username));
        }
        catch (Exception ex) when (ex is PasswordSealException or ArgumentException)
        {
            throw new NotificationRouteRefusedException(
                s_strictUtf8.GetByteCount(plaintext) > PasswordSeal.MaxPlaintextBytes ? PasswordTooLongText : PasswordCharactersText,
                NotificationRouteRefusalKind.Value);
        }
    }
}
