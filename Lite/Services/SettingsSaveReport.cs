/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor Lite.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

namespace PerformanceMonitorLite.Services;

/// <summary>
/// What the Settings window's Save button is entitled to say (#2433).
/// </summary>
public enum SettingsSaveOutcome
{
    /// <summary>settings.json could not be written. Nothing on the page reached disk.</summary>
    NothingWritten,

    /// <summary>The document was written, but a writer rejected some of what was typed and has already
    /// said so in its own dialog naming the values it refused.</summary>
    WrittenWithObjections,

    /// <summary>Everything reached disk and every writer was happy.</summary>
    Saved,

    /// <summary>As <see cref="Saved"/>, plus the MCP server's enablement or port changed, which the
    /// running process cannot pick up.</summary>
    SavedAndMcpNeedsRestart
}

/// <summary>
/// The rule the Settings window's toast follows, kept as a pure function so it can be pinned without a UI
/// thread (#2433).
///
/// <para>The window used to say "Settings saved." whenever nothing objected, and "nothing objected" was
/// answered by three bools that all meant "the boxes validated" — a different question from whether a byte
/// reached disk, which nothing was in a position to answer at all. The ten writers each rewrote the whole
/// document behind their own catch, so "SMTP saved, alerts did not" was a reachable state and no single
/// sentence could have described it honestly.</para>
///
/// <para>The fix folded the ten rewrites into one write, which removes that state instead of describing
/// it, and this is what is left: one ordering rule over four inputs. The rule worth writing down is that
/// <see cref="SettingsSaveOutcome.NothingWritten"/> outranks the validation objections. A writer that
/// rejected a value has already raised its own dialog about that value; what the user has NOT been told,
/// and cannot find out anywhere else, is that none of it was saved.</para>
/// </summary>
public static class SettingsSaveReport
{
    /// <param name="documentWritten">Whether the one write of settings.json succeeded.</param>
    /// <param name="mcpChanged">Whether the MCP enablement or port changed, needing a restart.</param>
    /// <param name="alertsValid">Whether every alert box validated.</param>
    /// <param name="mcpValid">Whether the MCP port validated and could be bound.</param>
    /// <param name="webhooksValid">Whether the generic webhook configuration validated.</param>
    public static SettingsSaveOutcome Classify(
        bool documentWritten,
        bool mcpChanged,
        bool alertsValid,
        bool mcpValid,
        bool webhooksValid)
    {
        if (!documentWritten)
        {
            return SettingsSaveOutcome.NothingWritten;
        }

        if (!alertsValid || !mcpValid || !webhooksValid)
        {
            return SettingsSaveOutcome.WrittenWithObjections;
        }

        return mcpChanged ? SettingsSaveOutcome.SavedAndMcpNeedsRestart : SettingsSaveOutcome.Saved;
    }
}
