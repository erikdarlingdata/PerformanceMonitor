/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using System.Globalization;

namespace PerformanceMonitor.Darling.Viewer;

/// <summary>
/// The three <c>config.config_service</c> settings that darling.json only seeds on the first start (#9638): how many days plan XML
/// is kept (<c>plan_content_retention_days</c>), the statement timeout of the composed reads (<c>compose_statement_timeout_seconds</c>)
/// and how plan XML is stored (<c>plan_xml_compression</c>). The Settings window reads and checks them here, so the allowed values
/// are one definition a test can run without a window. The bounds are the ones the service applies when it reads the columns, so a
/// value this class accepts is a value the service uses as written.
/// </summary>
internal static class ServiceSeedSettings
{
    /// <summary>The shortest plan retention that is on. 0 turns the extra plan retention off.</summary>
    public const int RetentionDaysMin = 7;

    /// <summary>The longest plan retention.</summary>
    public const int RetentionDaysMax = 365;

    /// <summary>The shortest statement timeout of the composed reads, in seconds.</summary>
    public const int TimeoutSecondsMin = 5;

    /// <summary>The longest statement timeout of the composed reads, in seconds.</summary>
    public const int TimeoutSecondsMax = 600;

    /// <summary>The default for <c>plan_content_retention_days</c>.</summary>
    public const int DefaultRetentionDays = 21;

    /// <summary>The default for <c>compose_statement_timeout_seconds</c>.</summary>
    public const int DefaultTimeoutSeconds = 60;

    /// <summary>The default for <c>plan_xml_compression</c>.</summary>
    public const string DefaultCompression = "gzip";

    /// <summary>Plan XML is compressed with gzip. The default.</summary>
    public const string CompressionGzip = "gzip";

    /// <summary>Plan XML is stored as text, so a direct SQL reader can read it as it is.</summary>
    public const string CompressionNone = "none";

    /// <summary>The values the plan storage list offers, which are the only ones the column's CHECK constraint accepts.</summary>
    public static readonly string[] CompressionChoices = [CompressionGzip, CompressionNone];

    public static readonly string RetentionDaysMessage =
        $"Plan retention must be 0 (off) or between {RetentionDaysMin} and {RetentionDaysMax} days.";

    public static readonly string TimeoutSecondsMessage =
        $"Query timeout must be between {TimeoutSecondsMin} and {TimeoutSecondsMax} seconds.";

    public static readonly string CompressionMessage =
        $"Plan storage must be {CompressionGzip} or {CompressionNone}.";

    /// <summary>The help line under the three fields.</summary>
    public static readonly string HelpText =
        $"Plan retention is how many days plan XML is kept ({RetentionDaysMin}-{RetentionDaysMax}, or 0 to keep it only as long as the query history it belongs to, default {DefaultRetentionDays}). "
        + $"Query timeout bounds each read the Darling service composes for the web dashboard and MCP ({TimeoutSecondsMin}-{TimeoutSecondsMax} seconds, default {DefaultTimeoutSeconds}). "
        + "It reaches the database roles when the service next starts. "
        + $"Plan storage is {CompressionGzip} (compressed, the default) or {CompressionNone} (plain text, larger, readable by direct SQL). "
        + "darling.json only seeds these three on the first start.";

    /// <summary>The field names <see cref="TryRead"/> reports for a refusal.</summary>
    public const string RetentionDaysField = "retentionDays";
    public const string TimeoutSecondsField = "timeoutSeconds";
    public const string CompressionField = "compression";

    /// <summary>The refusal for a retention value outside its range, or null.</summary>
    public static string? RetentionDaysError(int days) =>
        days == 0 || days is >= RetentionDaysMin and <= RetentionDaysMax ? null : RetentionDaysMessage;

    /// <summary>The refusal for a timeout outside its range, or null.</summary>
    public static string? TimeoutSecondsError(int seconds) =>
        seconds is >= TimeoutSecondsMin and <= TimeoutSecondsMax ? null : TimeoutSecondsMessage;

    /// <summary>The refusal for a plan storage value the column does not accept, or null.</summary>
    public static string? CompressionError(string? value) =>
        value is CompressionGzip or CompressionNone ? null : CompressionMessage;

    /// <summary>
    /// The retention the service uses for a stored value: 0 or less is off, anything else is held to the range. The window shows this
    /// rather than a hand-edited raw value, so a store holding 3 shows 7 and saving without a change does not trip the range check.
    /// </summary>
    public static int EffectiveRetentionDays(int stored) =>
        stored <= 0 ? 0 : Math.Clamp(stored, RetentionDaysMin, RetentionDaysMax);

    /// <summary>The timeout the service uses for a stored value: 0 or less is the default, anything else is held to the range.</summary>
    public static int EffectiveTimeoutSeconds(int stored) =>
        Math.Clamp(stored <= 0 ? DefaultTimeoutSeconds : stored, TimeoutSecondsMin, TimeoutSecondsMax);

    /// <summary>The storage form the service uses for a stored value: 'none' in any case, otherwise gzip.</summary>
    public static string EffectiveCompression(string? stored) =>
        string.Equals(stored?.Trim(), CompressionNone, StringComparison.OrdinalIgnoreCase) ? CompressionNone : CompressionGzip;

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

        if (!int.TryParse(retentionText?.Trim(), NumberStyles.Integer, CultureInfo.CurrentCulture, out var days) || RetentionDaysError(days) is not null)
        {
            badField = RetentionDaysField;
            return RetentionDaysMessage;
        }

        if (!int.TryParse(timeoutText?.Trim(), NumberStyles.Integer, CultureInfo.CurrentCulture, out var seconds) || TimeoutSecondsError(seconds) is not null)
        {
            badField = TimeoutSecondsField;
            return TimeoutSecondsMessage;
        }

        if (CompressionError(compression) is { } compressionError)
        {
            badField = CompressionField;
            return compressionError;
        }

        retentionDays = days;
        timeoutSeconds = seconds;
        plan = compression!;
        return null;
    }
}
