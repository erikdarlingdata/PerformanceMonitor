/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using System.Globalization;
using PerformanceMonitor.Darling.Storage;

namespace PerformanceMonitor.Darling.Viewer;

/// <summary>
/// The three <c>config.config_service</c> settings that darling.json only seeds on the first start (#9638): how many days plan XML
/// is kept (<c>plan_content_retention_days</c>), the statement timeout of the composed reads (<c>compose_statement_timeout_seconds</c>)
/// and how plan XML is stored (<c>plan_xml_compression</c>). The Settings window reads and checks them here, so what a typed value may
/// be is one definition a test can run without a window. The allowed values are <see cref="ServiceFlagLimits"/>, the same ones the
/// <c>update_service_flags</c> MCP tool refuses on and the service applies when it reads the columns.
/// </summary>
internal static class ServiceSeedSettings
{
    /// <summary>The default for <c>plan_content_retention_days</c>.</summary>
    public const int DefaultRetentionDays = 21;

    /// <summary>The default for <c>compose_statement_timeout_seconds</c>.</summary>
    public const int DefaultTimeoutSeconds = ServiceFlagLimits.ComposeStatementTimeoutSecondsDefault;

    /// <summary>The default for <c>plan_xml_compression</c>.</summary>
    public const string DefaultCompression = ServiceFlagLimits.PlanXmlCompressionGzip;

    /// <summary>The values the plan storage list offers, which are the only ones the column's CHECK constraint accepts.</summary>
    public static readonly string[] CompressionChoices = [ServiceFlagLimits.PlanXmlCompressionGzip, ServiceFlagLimits.PlanXmlCompressionNone];

    /// <summary>The field names <see cref="TryRead"/> reports for a refusal.</summary>
    public const string RetentionDaysField = "retentionDays";
    public const string TimeoutSecondsField = "timeoutSeconds";
    public const string CompressionField = "compression";

    /// <summary>The help line under the three fields.</summary>
    public static readonly string HelpText =
        $"Plan retention is how many days plan XML is kept ({ServiceFlagLimits.PlanContentRetentionDaysMin}-{ServiceFlagLimits.PlanContentRetentionDaysMax}, or 0 for off, default {DefaultRetentionDays}). A change takes effect at the next purge. "
        + $"Query timeout bounds each read the Darling service composes for the web dashboard and MCP ({ServiceFlagLimits.ComposeStatementTimeoutSecondsMin}-{ServiceFlagLimits.ComposeStatementTimeoutSecondsMax} seconds, default {DefaultTimeoutSeconds}). "
        + "The service re-applies it to the viewer and mcp roles when it reloads settings. Only roles you provisioned by hand need ALTER ROLE ... SET statement_timeout. "
        + $"Plan storage is {ServiceFlagLimits.PlanXmlCompressionGzip} (compressed, the default) or {ServiceFlagLimits.PlanXmlCompressionNone} (plain text, larger, readable by direct SQL). A change applies to plans written from then on, and plans already stored stay readable. "
        + "darling.json only seeds these three on the first start.";

    /// <summary>
    /// The retention the service uses for a stored value: 0 or less is off, anything else is held to the range. The window shows this
    /// rather than a hand-edited raw value, so a store holding 3 shows 7 and saving without a change does not trip the range check.
    /// </summary>
    public static int EffectiveRetentionDays(int stored) =>
        ServiceFlagLimits.ClampPlanContentRetentionDays(stored);

    /// <summary>The timeout the service uses for a stored value: 0 or less is the default, anything else is held to the range.</summary>
    public static int EffectiveTimeoutSeconds(int stored) =>
        ServiceFlagLimits.ClampComposeStatementTimeoutSeconds(stored);

    /// <summary>The storage form the service uses for a stored value: its own spelling when it is one of the two, otherwise gzip.</summary>
    public static string EffectiveCompression(string? stored) =>
        ServiceFlagLimits.NormalizePlanXmlCompression(stored) ?? DefaultCompression;

    /// <summary>
    /// Reads the three fields as typed. Returns null when all three are fine and the values are in the out parameters. Returns the
    /// refusal text of the first bad one otherwise, with the name of the field it belongs to in <paramref name="badField"/>.
    /// </summary>
    public static string? TryRead(
        string? retentionText, string? timeoutText, string? compression,
        out int retentionDays, out int timeoutSeconds, out string plan, out string? badField)
    {
        retentionDays = DefaultRetentionDays;
        timeoutSeconds = DefaultTimeoutSeconds;
        plan = DefaultCompression;
        badField = null;

        if (!int.TryParse(retentionText?.Trim(), NumberStyles.Integer, CultureInfo.CurrentCulture, out var days)
            || ServiceFlagLimits.PlanContentRetentionDaysError(days) is not null)
        {
            badField = RetentionDaysField;
            return ServiceFlagLimits.PlanContentRetentionDaysMessage;
        }

        if (!int.TryParse(timeoutText?.Trim(), NumberStyles.Integer, CultureInfo.CurrentCulture, out var seconds)
            || ServiceFlagLimits.ComposeStatementTimeoutSecondsError(seconds) is not null)
        {
            badField = TimeoutSecondsField;
            return ServiceFlagLimits.ComposeStatementTimeoutSecondsMessage;
        }

        if (ServiceFlagLimits.NormalizePlanXmlCompression(compression) is not { } normalized)
        {
            badField = CompressionField;
            return ServiceFlagLimits.PlanXmlCompressionMessage;
        }

        retentionDays = days;
        timeoutSeconds = seconds;
        plan = normalized;
        return null;
    }
}
