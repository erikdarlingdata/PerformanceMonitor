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
/// The ONE rule for whether a <c>sys.configurations</c> row's configured value matches its in-use value (#5633 item 9).
/// Authored once here because four consumers compute the flag (Darling's <c>get_server_config</c>, Lite's twin, the
/// Darling viewer's Configuration tab and Lite's), and a rule written four times drifts.
///
/// <para><b>Configured 0 and in use 16 for <c>min server memory (MB)</c> is a match.</b> SQL Server's floor for that
/// setting is 16 MB: a configured 0 (the default) is run as 16, so <c>value</c> 0 beside <c>value_in_use</c> 16 is the
/// engine behaving normally, not a pending change. Plain equality flagged every default install as "not matching" and
/// sent people to look for a restart that was never needed. Nothing else is special-cased: any other pair that differs
/// still reports a mismatch.</para>
/// </summary>
public static class ConfigValuesMatch
{
    /// <summary>The setting whose configured 0 runs as <see cref="MinServerMemoryFloorMb"/>.</summary>
    public const string MinServerMemoryName = "min server memory (MB)";

    /// <summary>The smallest in-use value SQL Server runs <c>min server memory (MB)</c> at, in MB.</summary>
    public const long MinServerMemoryFloorMb = 16;

    /// <summary>True when <paramref name="valueConfigured"/> and <paramref name="valueInUse"/> are the same effective setting.</summary>
    public static bool Matches(string? configurationName, long valueConfigured, long valueInUse)
    {
        if (valueConfigured == valueInUse)
        {
            return true;
        }

        return valueConfigured == 0
            && valueInUse == MinServerMemoryFloorMb
            && string.Equals(configurationName, MinServerMemoryName, StringComparison.OrdinalIgnoreCase);
    }
}
