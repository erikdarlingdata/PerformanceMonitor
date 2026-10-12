/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System.Collections.Generic;
using System.Globalization;

namespace PerformanceMonitorLite.Services;

/// <summary>
/// Validates the Settings window's two Runnable Queue boxes (#5641) as a PAIR. The rules are the ones
/// <c>RunnableQueueEvaluator.ClampBars</c> applies when the engine reads the settings (each bar a whole number of at
/// least 1, Critical never below Warning), but the window refuses a bad pair with a message instead of clamping it
/// silently, so the stored bars are the bars the operator typed. Both bars change together or neither does: a
/// half-applied pair would save a Critical the Warning no longer sits under. Pure, so the rules are testable without
/// a WPF window.
/// </summary>
internal static class RunnableQueueBarsInput
{
    internal const string WarningInvalidMessage =
        "Runnable queue warning bar must be a whole number of at least 1 (runnable tasks per scheduler).";

    internal const string CriticalInvalidMessage =
        "Runnable queue critical bar must be a whole number of at least 1 (runnable tasks per scheduler).";

    internal const string CriticalBelowWarningMessage =
        "Runnable queue critical bar must be at least the warning bar.";

    /// <summary>
    /// Parses and checks the two boxes. <paramref name="errors"/> receives one message per problem; the returned
    /// bars are meaningful only when it comes back empty.
    /// </summary>
    internal static (int Warning, int Critical) Validate(string? warningText, string? criticalText, List<string> errors)
    {
        var warningOk = int.TryParse(warningText, NumberStyles.Integer, CultureInfo.CurrentCulture, out var warning) && warning >= 1;
        var criticalOk = int.TryParse(criticalText, NumberStyles.Integer, CultureInfo.CurrentCulture, out var critical) && critical >= 1;

        if (!warningOk)
        {
            errors.Add(WarningInvalidMessage);
        }
        if (!criticalOk)
        {
            errors.Add(CriticalInvalidMessage);
        }
        if (warningOk && criticalOk && critical < warning)
        {
            errors.Add(CriticalBelowWarningMessage);
        }

        return (warning, critical);
    }
}
