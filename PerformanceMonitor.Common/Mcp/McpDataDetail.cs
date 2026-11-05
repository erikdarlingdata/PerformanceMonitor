/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;

namespace PerformanceMonitor.Common;

/// <summary>
/// How much collected data the MCP tools hand out (#5632, <c>mcp.dataDetail</c>). The field table that says what each
/// level does to which field is <see cref="McpDataDetailKeys"/>; the masker is <see cref="McpDataDetailMasker"/>.
/// </summary>
public enum McpDataDetail
{
    /// <summary>Everything the tools read, as today. The default.</summary>
    Full = 0,

    /// <summary>Statement text, error text and plans keep their shape with every literal replaced; parameter values
    /// are replaced. Logins and host names are kept.</summary>
    MaskLiterals = 1,

    /// <summary>Statement text, error text, plans, input buffers and parameter values are withheld; logins and host
    /// names are masked. Object and database names, counts, durations, waits, times and program names are kept.</summary>
    MetadataOnly = 2,
}

/// <summary>Reads the <c>mcp.dataDetail</c> value: <c>full</c>, <c>maskLiterals</c> or <c>metadataOnly</c>, ignoring
/// case and surrounding blanks. A blank value is <c>full</c>; anything else is <c>metadataOnly</c>, the strictest
/// level, because a typo must not hand out more than the operator asked for. The caller logs it once.</summary>
public static class McpDataDetailModes
{
    /// <summary>The setting's name in <c>darling.json</c>, for log lines.</summary>
    public const string Setting = "mcp.dataDetail";

    /// <summary>The shipped value.</summary>
    public const string DefaultValue = "full";

    /// <returns>True when <paramref name="value"/> named a level (or was blank); false when it was unrecognized and
    /// the level is <see cref="McpDataDetail.MetadataOnly"/>.</returns>
    public static bool TryParse(string? value, out McpDataDetail mode)
    {
        var text = value?.Trim();
        if (string.IsNullOrEmpty(text) || string.Equals(text, "full", StringComparison.OrdinalIgnoreCase))
        {
            mode = McpDataDetail.Full;
            return true;
        }

        if (string.Equals(text, "maskLiterals", StringComparison.OrdinalIgnoreCase))
        {
            mode = McpDataDetail.MaskLiterals;
            return true;
        }

        if (string.Equals(text, "metadataOnly", StringComparison.OrdinalIgnoreCase))
        {
            mode = McpDataDetail.MetadataOnly;
            return true;
        }

        mode = McpDataDetail.MetadataOnly;
        return false;
    }

    /// <summary>The level <paramref name="value"/> runs as: <see cref="TryParse"/>'s level, without the flag.</summary>
    public static McpDataDetail Resolve(string? value)
    {
        TryParse(value, out McpDataDetail mode);
        return mode;
    }

    /// <summary>The setting's own spelling of <paramref name="mode"/>.</summary>
    public static string Word(McpDataDetail mode) => mode switch
    {
        McpDataDetail.MaskLiterals => "maskLiterals",
        McpDataDetail.MetadataOnly => "metadataOnly",
        _ => "full",
    };
}

/// <summary>
/// The level an MCP host serves, registered in the host's services so that <see cref="McpDataDetailFilter"/> reads it from each
/// request (#5632). Lite registers one singleton from its config. Darling registers it per request, from <c>mcp.dataDetail</c> and the
/// calling token's own level (#7023), so a Darling instance is never shared between requests; a test host that registered a singleton
/// can still change <see cref="Mode"/> between calls to drive one host through every level.
/// </summary>
public sealed class McpDataDetailSetting
{
    /// <summary>Creates the setting at <paramref name="mode"/>.</summary>
    public McpDataDetailSetting(McpDataDetail mode) => Mode = mode;

    /// <summary>The level the host serves.</summary>
    public McpDataDetail Mode { get; internal set; }
}
