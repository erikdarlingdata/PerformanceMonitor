/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using System.Collections.Generic;
using Microsoft.Extensions.Logging;
using PerformanceMonitor.Common;

namespace PerformanceMonitor.Darling.Service.Mcp;

/// <summary>What a service that serves personal MCP tokens (#8737) applies: the limits after clamping, the strongest role an admin's
/// token may take, and the data level it can only narrow.</summary>
internal sealed record PersonalTokenSettings(int MaxLifetimeDays, int MaxTokensPerPerson, string AdminRole, McpDataDetail? DataDetail, McpDataDetail ServiceDetail = McpDataDetail.Full)
{
    /// <summary>The level a personal token is served: the stricter of the service's own level and the personal tokens' <c>dataDetail</c>.</summary>
    public McpDataDetail ServedDetail => DataDetail is { } own && own > ServiceDetail ? own : ServiceDetail;

    /// <summary>True when an admin may mint an <c>operator</c> token.</summary>
    public bool AdminMayMintOperator => AdminRole == McpTokenEntry.OperatorRole;
}

/// <summary>
/// Decides, from <c>darling.json</c> alone, whether personal MCP tokens are served and with which settings (#8737). Never serves wider than
/// asked: an out-of-range number is clamped, an unknown <c>adminRole</c> becomes <c>reader</c>, and an unparseable <c>dataDetail</c> keeps
/// the feature off (the #7023 rule). A start that cannot serve it names why in one warning.
/// </summary>
internal static class DarlingPersonalTokenSettings
{
    private const string Setting = "mcp.personalTokens";
    private const int MinLifetimeDays = 1;
    private const int MaxLifetimeDaysLimit = 365;
    private const int MinTokensPerPerson = 1;
    private const int MaxTokensPerPersonLimit = 50;

    /// <summary>The settings to serve, or null when the feature is off or cannot be served; every reason is logged once as a warning.</summary>
    public static PersonalTokenSettings? Resolve(DarlingConfig config, ILogger? logger)
    {
        var (settings, problems) = Evaluate(config);
        if (logger is not null)
        {
            foreach (var problem in problems)
            {
                logger.LogWarning("{Problem}", problem);
            }
        }

        return settings;
    }

    /// <summary>The reasons <see cref="Resolve"/> warns about, as text for <c>--validate-config</c>: empty when the feature is off or
    /// is served exactly as written.</summary>
    public static IReadOnlyList<string> Problems(DarlingConfig config) => Evaluate(config).Problems;

    private static (PersonalTokenSettings? Settings, IReadOnlyList<string> Problems) Evaluate(DarlingConfig config)
    {
        var block = config.Mcp?.PersonalTokens;
        if (block is null || !block.Enabled)
        {
            return (null, Array.Empty<string>());
        }

        var missing = new List<string>();
        if (config.IsFrontDoor)
        {
            missing.Add("this service is a front door (frontDoor.enabled)");
        }

        if (config.Web?.Network?.Oidc is not { IsConfigured: true })
        {
            missing.Add("web.network.oidc is not configured");
        }

        if (config.Web?.Enabled != true)
        {
            missing.Add("web.enabled is false in darling.json");
        }

        if (config.Mcp!.AllowTokenlessLoopback)
        {
            missing.Add("mcp.allowTokenlessLoopback is on");
        }

        if (missing.Count > 0)
        {
            return (null, new[] { $"{Setting}.enabled is on but personal MCP tokens are not served: {string.Join("; ", missing)}." });
        }

        McpDataDetail? dataDetail = null;
        if (!string.IsNullOrWhiteSpace(block.DataDetail))
        {
            if (!McpDataDetailModes.TryParse(block.DataDetail, out var parsed))
            {
                return (null, new[] { $"{Setting}.dataDetail is not full, maskLiterals or metadataOnly, so personal MCP tokens are not served." });
            }

            dataDetail = parsed;
        }

        var problems = new List<string>();
        var lifetime = Clamp(block.MaxLifetimeDays, MinLifetimeDays, MaxLifetimeDaysLimit, "maxLifetimeDays", problems);
        var perPerson = Clamp(block.MaxTokensPerPerson, MinTokensPerPerson, MaxTokensPerPersonLimit, "maxTokensPerPerson", problems);

        var adminRole = McpTokenEntry.ReaderRole;
        if (string.Equals(block.AdminRole, McpTokenEntry.OperatorRole, StringComparison.Ordinal))
        {
            adminRole = McpTokenEntry.OperatorRole;
        }
        else if (!string.Equals(block.AdminRole, McpTokenEntry.ReaderRole, StringComparison.Ordinal))
        {
            problems.Add($"{Setting}.adminRole is not operator or reader; admins are served reader.");
        }

        return (new PersonalTokenSettings(lifetime, perPerson, adminRole, dataDetail, McpDataDetailModes.Resolve(config.Mcp.DataDetail)), problems);
    }

    private static int Clamp(int value, int min, int max, string name, List<string> problems)
    {
        if (value >= min && value <= max)
        {
            return value;
        }

        var served = Math.Clamp(value, min, max);
        problems.Add($"{Setting}.{name} is {value}, outside {min} to {max}; {served} is served.");
        return served;
    }
}
