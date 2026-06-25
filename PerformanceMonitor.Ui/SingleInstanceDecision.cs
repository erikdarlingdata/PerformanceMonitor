/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;

namespace PerformanceMonitor.Ui
{
    /// <summary>
    /// What a launching instance should do when it finds the single-instance mutex already held
    /// (version-aware upgrade handoff). Pure decision over already-measured inputs — no Win32, no
    /// processes — so it is fully unit-testable; the messy measurement lives in
    /// <see cref="IProcessInspector"/> and the orchestration in <see cref="SingleInstanceCoordinator"/>.
    /// </summary>
    public enum HandoffAction
    {
        /// <summary>The running instance is an older build at an integrity level we can reach — close it and take over.</summary>
        TakeOver,

        /// <summary>Genuine "already running" (same/newer build), or we can't determine enough to safely act — surface the running instance and exit.</summary>
        Surface,

        /// <summary>Running instance is older but at a higher integrity level we can't signal/kill, and we can elevate same-user — offer "Restart as administrator".</summary>
        IntegrityErrorWithRunas,

        /// <summary>Running instance is older but at a higher integrity level we can't signal/kill, and we cannot elevate same-user — show a manual-close message.</summary>
        IntegrityErrorManualOnly,
    }

    /// <summary>
    /// The version-aware single-instance handoff decision (#single-instance-upgrade-handoff plan).
    /// </summary>
    public static class SingleInstanceDecision
    {
        /// <param name="ourVersion">This (launching) build's release version, or null if it couldn't be resolved.</param>
        /// <param name="otherVersion">The running instance's release version read from its on-disk exe, or null if unreadable.</param>
        /// <param name="ourIntegrityLevel">This process's mandatory-integrity RID (e.g. 0x3000 = High).</param>
        /// <param name="otherIntegrityLevel">The running instance's integrity RID, or null if it couldn't be measured.</param>
        /// <param name="canElevateSameUser">Whether the current user could elevate in place (split-token admin / already elevated).</param>
        public static HandoffAction Decide(
            Version? ourVersion,
            Version? otherVersion,
            int ourIntegrityLevel,
            int? otherIntegrityLevel,
            bool canElevateSameUser)
        {
            /* Can't read either version → can't tell if this is an upgrade → behave as today (surface). */
            if (ourVersion is null || otherVersion is null)
            {
                return HandoffAction.Surface;
            }

            /* Same or newer running instance → genuine "already running" (incl. a plain double-launch),
               or an older build trying to evict a newer one. Never take over. */
            if (otherVersion.CompareTo(ourVersion) >= 0)
            {
                return HandoffAction.Surface;
            }

            /* The running instance is OLDER — a real takeover is warranted. Can we reach it?
               We need its integrity level to know whether our signal/kill can land. If we couldn't
               measure it (e.g. token read denied), don't guess — fall back to surface. */
            if (otherIntegrityLevel is null)
            {
                return HandoffAction.Surface;
            }

            /* Older and at an integrity level at or below ours → we can signal/kill it → take over. */
            if (otherIntegrityLevel.Value <= ourIntegrityLevel)
            {
                return HandoffAction.TakeOver;
            }

            /* Older but higher integrity than us → our exit signal and Kill() are both blocked.
               Surface a clear error; offer elevation only if it would stay same-user. */
            return canElevateSameUser
                ? HandoffAction.IntegrityErrorWithRunas
                : HandoffAction.IntegrityErrorManualOnly;
        }

        /// <summary>
        /// Parses a ProductVersion / InformationalVersion string to its numeric core for comparison.
        /// Strips any SemVer pre-release (<c>-rc1</c>) or build (<c>+sha</c>) suffix and parses the
        /// leading <c>X.Y[.Z[.W]]</c> with <see cref="Version"/> — deliberately dependency-free.
        /// Consequence: two builds that share the same numeric <c>&lt;Version&gt;</c> (e.g. two nightlies,
        /// or rc1→rc2) compare equal → "surface", which is also the right call for a normal double-launch.
        /// Real release upgrades bump <c>&lt;Version&gt;</c>, so the upgrade case is covered. Returns null
        /// if there is no parseable numeric core.
        /// </summary>
        public static Version? ParseProductVersion(string? raw)
        {
            if (string.IsNullOrWhiteSpace(raw))
            {
                return null;
            }

            var core = raw.Trim();

            var plus = core.IndexOf('+');
            if (plus >= 0)
            {
                core = core.Substring(0, plus);
            }

            var dash = core.IndexOf('-');
            if (dash >= 0)
            {
                core = core.Substring(0, dash);
            }

            return Version.TryParse(core, out var version) ? version : null;
        }
    }
}
