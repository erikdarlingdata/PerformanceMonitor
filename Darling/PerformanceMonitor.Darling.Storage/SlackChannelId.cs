/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using System.Text.RegularExpressions;

namespace PerformanceMonitor.Darling.Storage;

/// <summary>
/// The one copy of the Slack channel id shape (#7656): a route's bot channel (<c>slack_bot_channel</c>, V252) is stored as the channel's
/// id, never its name, because names change and the bot posts by id. An id starts with <c>C</c> (a public or private channel) or <c>G</c>
/// (a legacy private group), followed by capital letters and digits. Every writer of the column (the Viewer's route dialog, the MCP route
/// tools, the web route form) refuses any other shape with <see cref="RefusalText"/>; the store functions hold the same rule. Blank is
/// none and is always accepted.
/// </summary>
public static class SlackChannelId
{
    /// <summary>The longest id accepted. Slack ids are shorter; the bound keeps a pasted paragraph out of the column.</summary>
    public const int MaxLength = 32;

    /// <summary>The sentence a value of the wrong shape is refused with. It never carries the value.</summary>
    public const string RefusalText =
        "The Slack bot channel is a channel id: C or G, then capital letters and digits (for example C0123ABCDEF), or blank for none. Not the channel's name.";

    private static readonly Regex s_shape = new("^[CG][A-Z0-9]{1,31}$", RegexOptions.CultureInvariant, TimeSpan.FromSeconds(1));

    /// <summary>The value as stored: trimmed, with null as blank.</summary>
    public static string Normalize(string? text) => text?.Trim() ?? "";

    /// <summary>Whether <paramref name="text"/>, trimmed, is a channel id. Blank is not one.</summary>
    public static bool IsValid(string? text)
    {
        var id = Normalize(text);
        return id.Length > 0 && id.Length <= MaxLength && s_shape.IsMatch(id);
    }

    /// <summary>The refusal for a bot channel value, or null when it is blank (none) or a channel id.</summary>
    public static string? Refusal(string? text) =>
        Normalize(text).Length == 0 || IsValid(text) ? null : RefusalText;
}
