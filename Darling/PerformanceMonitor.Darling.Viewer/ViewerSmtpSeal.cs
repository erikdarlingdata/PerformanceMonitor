/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using System.Text;
using PerformanceMonitor.Darling.Storage;

namespace PerformanceMonitor.Darling.Viewer;

/// <summary>
/// The SMTP half of <see cref="ViewerPasswordSealer"/> (#5366): seals the mail-server password for the host, port, SSL
/// flag and user name it is stored with, the same four values the service builds its binding from when it reads the row.
/// </summary>
public sealed partial class ViewerPasswordSealer
{
    /// <summary>
    /// Seals the SMTP password for the settings it is stored with. The four values must be exactly the ones written to
    /// the notification row. Throws <see cref="ViewerPasswordRefusedException"/> when the password or a bound field is
    /// not valid text; the sentence never carries a value.
    /// </summary>
    public string SealSmtp(string plaintext, string? host, int port, bool useSsl, string? username)
    {
        ArgumentNullException.ThrowIfNull(plaintext);

        if (!ViewerSmtpSeal.IsValidText(plaintext))
        {
            throw new ViewerPasswordRefusedException(PasswordCharactersText);
        }

        var invalidField = ViewerSmtpSeal.FirstInvalidBoundField(host, username);
        if (invalidField is not null)
        {
            throw new ViewerPasswordRefusedException(FieldCharactersText(invalidField));
        }

        try
        {
            return PasswordSeal.Seal(plaintext, key, PasswordBinding.ForSmtp(host, port, useSsl, username));
        }
        catch (Exception ex) when (ex is PasswordSealException or ArgumentException)
        {
            throw new ViewerPasswordRefusedException(
                Encoding.UTF8.GetByteCount(plaintext) > PasswordSeal.MaxPlaintextBytes ? PasswordTooLongText : PasswordCharactersText);
        }
    }
}

/// <summary>What a Settings save does with the SMTP password.</summary>
public enum ViewerSmtpPasswordAction
{
    /// <summary>Store what the row already holds (nothing, or the value that was loaded).</summary>
    Keep,

    /// <summary>Seal the password that was typed.</summary>
    Seal,

    /// <summary>The save is refused: how mail is sent changed and no password was typed.</summary>
    Refuse,
}

/// <summary>The decisions around the sealed SMTP password in the Settings window (#5366), kept apart from the window so they can be tested.</summary>
public static class ViewerSmtpSeal
{
    private static readonly UTF8Encoding s_strictUtf8 = new(false, true);

    /// <summary>The refusal for a change to the host, port, SSL flag or user name when no password was typed.</summary>
    public const string ReenterText = "Changing how mail is sent needs the SMTP password again.";

    /// <summary>What the status line says next to a blank password box when a password is already saved.</summary>
    public const string SavedText = "Saved. Leave blank to keep it.";

    /// <summary>What a test send says when the only password there is is a saved one this window cannot read.</summary>
    public const string SavedCannotTestText = "The saved SMTP password cannot be read here. Type the password to send a test email.";

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

    /// <summary>Whether the values a password is sealed for differ between the stored row and the edited one: host, port, SSL flag, user name.</summary>
    public static bool BindingChanged(NotificationRow stored, NotificationRow edited)
    {
        ArgumentNullException.ThrowIfNull(stored);
        ArgumentNullException.ThrowIfNull(edited);

        var was = PasswordBinding.ForSmtp(stored.SmtpHost, stored.SmtpPort, stored.SmtpUseSsl, stored.SmtpUsername);
        var now = PasswordBinding.ForSmtp(edited.SmtpHost, edited.SmtpPort, edited.SmtpUseSsl, edited.SmtpUsername);
        for (var i = 0; i < was.Fields.Count; i++)
        {
            if (!string.Equals(was.Fields[i] ?? "", now.Fields[i] ?? "", StringComparison.Ordinal))
            {
                return true;
            }
        }

        return false;
    }

    /// <summary>
    /// What to do with the SMTP password on a save. <paramref name="stored"/> is the row the window loaded;
    /// <paramref name="storedPlain"/> is the password the box was pre-filled with (only for a value this machine could read),
    /// or null. A typed password is sealed; a blank one keeps the stored value, unless the host, port, SSL flag or user
    /// name changed, which needs the password again.
    /// </summary>
    public static ViewerSmtpPasswordAction Decide(
        NotificationRow stored, string? storedPlain, NotificationRow edited, string? typed)
    {
        ArgumentNullException.ThrowIfNull(stored);
        ArgumentNullException.ThrowIfNull(edited);
        typed ??= "";

        var changed = BindingChanged(stored, edited);
        if (typed.Length > 0)
        {
            /* The box was pre-filled with a value this machine could read and left alone: that is a blank for a value that stays. */
            if (!changed && storedPlain is not null && string.Equals(typed, storedPlain, StringComparison.Ordinal))
            {
                return ViewerSmtpPasswordAction.Keep;
            }

            return ViewerSmtpPasswordAction.Seal;
        }

        return changed && !string.IsNullOrEmpty(stored.SmtpEncryptedPassword)
            ? ViewerSmtpPasswordAction.Refuse
            : ViewerSmtpPasswordAction.Keep;
    }
}
