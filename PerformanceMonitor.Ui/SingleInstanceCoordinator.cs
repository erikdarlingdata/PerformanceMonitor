/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using System.Collections.Generic;
using System.ComponentModel;
using System.Diagnostics;
using System.Linq;
using System.Reflection;
using System.Threading;

namespace PerformanceMonitor.Ui
{
    /// <summary>Command-line flag the elevated relaunch carries so it skips the "close it?" prompt.</summary>
    public static class HandoffArgs
    {
        public const string AutoConfirm = "--upgrade-takeover";
    }

    /// <summary>App-supplied dialogs for the handoff. Implemented per app (WPF MessageBox), kept out of
    /// the coordinator so the orchestration stays testable and the UI stays in the app.</summary>
    public interface IHandoffPrompts
    {
        /// <summary>"A previous version is still running. Close it and continue?" — true = proceed.</summary>
        bool ConfirmCloseAndContinue(string oldVersion, string newVersion);

        /// <summary>"…is running as administrator. Restart as administrator to continue?" — true = elevate.</summary>
        bool ConfirmRestartAsAdmin(string oldVersion);

        /// <summary>Info-only: an elevated previous version must be closed from its tray icon.</summary>
        void ShowMustCloseElevatedManually(string oldVersion);
    }

    /// <summary>Per-app configuration for <see cref="SingleInstanceCoordinator"/>.</summary>
    public sealed class SingleInstanceOptions
    {
        /// <summary>Constant per-app mutex name (e.g. <c>PerformanceMonitorLite_SingleInstance</c>).</summary>
        public string MutexName { get; set; } = string.Empty;

        /// <summary>Process name without extension (e.g. <c>PerformanceMonitorLite</c>).</summary>
        public string ProcessName { get; set; } = string.Empty;

        /// <summary>Shared named event the owning instance listens on for an "exit for upgrade" request.</summary>
        public string ExitEventName { get; set; } = string.Empty;

        /// <summary>Run in the NEW instance to tell the running instance to surface (Lite: signal the show
        /// event; Dashboard: broadcast <c>WM_SHOWMONITOR</c>).</summary>
        public Action SurfaceRunningInstance { get; set; } = static () => { };

        /// <summary>Run in the OWNING instance when an exit-for-upgrade request arrives — the app's real
        /// shutdown path (Lite: <c>Application.Shutdown()</c>; Dashboard: <c>MainWindow.ExitApplication()</c>).</summary>
        public Action GracefulSelfExit { get; set; } = static () => { };

        /// <summary>App dialogs.</summary>
        public IHandoffPrompts Prompts { get; set; } = null!;

        /// <summary>True when launched by an elevated relaunch (<see cref="HandoffArgs.AutoConfirm"/>) — skip the close prompt.</summary>
        public bool AutoConfirm { get; set; }

        /// <summary>Injectable for tests; defaults to <see cref="Win32ProcessInspector"/>.</summary>
        public IProcessInspector? Inspector { get; set; }

        /// <summary>Optional diagnostic logging.</summary>
        public Action<string>? Log { get; set; }
    }

    /// <summary>
    /// Version-aware single-instance enforcement with upgrade handoff. Replaces the old inline
    /// "mutex held → surface + exit" block: when a NEWER build launches over an OLDER running one
    /// (the post-upgrade case), it closes the old one and takes over instead of being handed back
    /// the stale in-memory version. See plans/single-instance-upgrade-handoff.md.
    ///
    /// Lifetime: the owning process keeps the instance (it holds the mutex + the exit listener) and
    /// disposes it on exit.
    /// </summary>
    public sealed class SingleInstanceCoordinator : IDisposable
    {
        private const int GracefulWaitWithListenerMs = 20000; // old instance's own clean shutdown can take ~10s+
        private const int GracefulWaitNoListenerMs = 3000;    // no listener yet → it may be mid-startup
        private const int PostKillWaitMs = 3000;
        private const int MutexAcquireTimeoutMs = 5000;

        private readonly SingleInstanceOptions _options;
        private readonly IProcessInspector _inspector;
        private Mutex? _mutex;
        private bool _ownsMutex;
        private SingleInstanceSignal? _exitSignal;
        private bool _disposed;

        public SingleInstanceCoordinator(SingleInstanceOptions options)
        {
            _options = options ?? throw new ArgumentNullException(nameof(options));
            if (string.IsNullOrEmpty(options.MutexName) || string.IsNullOrEmpty(options.ProcessName) || string.IsNullOrEmpty(options.ExitEventName))
                throw new ArgumentException("MutexName, ProcessName, and ExitEventName are required.", nameof(options));
            if (options.Prompts is null)
                throw new ArgumentException("Prompts is required.", nameof(options));
            _inspector = options.Inspector ?? new Win32ProcessInspector();
        }

        /// <summary>
        /// True if this instance should proceed with startup (it now owns the single-instance slot).
        /// False if the caller should <c>Shutdown()</c> immediately (it surfaced an existing instance,
        /// raised an integrity error, declined, or relaunched elevated).
        /// </summary>
        public bool TryBecomeOwner()
        {
            try
            {
                _mutex = new Mutex(true, _options.MutexName, out var createdNew);
                if (createdNew)
                {
                    BecomeOwner();
                    return true;
                }
            }
            catch (UnauthorizedAccessException)
            {
                /* The named mutex was created by a higher-integrity (elevated) instance; a non-elevated
                   process is denied write access to it (mandatory NO_WRITE_UP). That IS the elevated-old
                   case — fall through to the handoff, which measures the integrity mismatch and raises
                   the actionable error (+ "Restart as administrator"). The handoff null-guards the
                   absent mutex; after elevation the equal-integrity relaunch acquires it normally. */
                _mutex = null;
            }
            catch (System.IO.IOException)
            {
                _mutex = null;
            }

            /* Another instance holds the mutex — decide whether to surface it (today's behavior) or
               take over (it's a stale older build from before an upgrade). */
            return Handoff();
        }

        private bool Handoff()
        {
            var others = FindOtherInstances();
            if (others.Count == 0)
            {
                /* Mutex held but no peer process found — a race or an abandoned mutex. Try to grab it. */
                return TryAcquireAfterRelease() || SurfaceAndExit();
            }

            try
            {
                var ourVersion = CurrentReleaseVersion();
                var ourIntegrity = _inspector.CurrentIntegrityLevel();
                var otherVersion = _inspector.GetReleaseVersion(others[0].Id);
                var otherIntegrity = _inspector.GetIntegrityLevel(others[0].Id);
                var canElevate = _inspector.CanElevateSameUser();

                var action = SingleInstanceDecision.Decide(ourVersion, otherVersion, ourIntegrity, otherIntegrity, canElevate);
                _options.Log?.Invoke($"Handoff: ours={ourVersion} other={otherVersion} ourIL={ourIntegrity} otherIL={otherIntegrity} canElevate={canElevate} -> {action}");

                var oldText = otherVersion?.ToString() ?? "unknown";
                var newText = ourVersion?.ToString() ?? "unknown";

                switch (action)
                {
                    case HandoffAction.TakeOver:
                        if (!_options.AutoConfirm && !_options.Prompts.ConfirmCloseAndContinue(oldText, newText))
                            return SurfaceAndExit();
                        return TakeOverFrom(others) || SurfaceAndExit();

                    case HandoffAction.IntegrityErrorWithRunas:
                        if (_options.Prompts.ConfirmRestartAsAdmin(oldText))
                            RelaunchElevated();
                        return false; // either relaunched, or declined — exit without surfacing

                    case HandoffAction.IntegrityErrorManualOnly:
                        _options.Prompts.ShowMustCloseElevatedManually(oldText);
                        return false;

                    case HandoffAction.Surface:
                    default:
                        return SurfaceAndExit();
                }
            }
            finally
            {
                foreach (var proc in others)
                {
                    proc.Dispose();
                }
            }
        }

        private bool TakeOverFrom(List<Process> others)
        {
            /* Ask the old instance(s) to shut down gracefully (it runs its real exit path: flush DuckDB,
               dispose tray, release the mutex). The shared exit event broadcasts to whoever is listening. */
            var delivered = SingleInstanceSignal.TrySignal(_options.ExitEventName);
            var budget = delivered ? GracefulWaitWithListenerMs : GracefulWaitNoListenerMs;

            var stillRunning = WaitForExit(others, budget);
            if (stillRunning.Count > 0)
            {
                if (!delivered)
                {
                    /* No listener existed → the old instance may be mid-startup (e.g. building the DuckDB
                       schema). Killing then is riskier than a stale alert; bail to surface instead. */
                    return false;
                }

                /* It had a listener (past startup) but didn't exit in time — force-kill as a last resort. */
                foreach (var proc in stillRunning)
                {
                    TryKill(proc);
                }
                WaitForExit(stillRunning, PostKillWaitMs);
            }

            return TryAcquireAfterRelease();
        }

        private bool TryAcquireAfterRelease()
        {
            if (_mutex is null)
                return false;
            try
            {
                if (_mutex.WaitOne(MutexAcquireTimeoutMs))
                {
                    BecomeOwner();
                    return true;
                }
            }
            catch (AbandonedMutexException)
            {
                /* Previous owner died without releasing — we now hold it. */
                BecomeOwner();
                return true;
            }
            return false;
        }

        private void BecomeOwner()
        {
            _ownsMutex = true;
        }

        /// <summary>
        /// Opens the "exit for upgrade" channel — the app calls this once it is past its risky startup
        /// (DuckDB initialized, MCP bound, etc.). Deferring it here (rather than in <see cref="BecomeOwner"/>)
        /// preserves the safety rule that a newer build won't disturb an instance that is still
        /// initializing: while no listener exists, a launching newer build sees <c>TrySignal == false</c>
        /// and surfaces instead of signaling/killing us mid-init. No-op if we don't own the slot or the
        /// channel is already open. Idempotent and thread-safe to call from the UI thread.
        /// </summary>
        public void EnableUpgradeHandoff()
        {
            if (!_ownsMutex || _exitSignal is not null || _disposed)
                return;
            /* Listen for a future newer build asking us to step aside. */
            _exitSignal = new SingleInstanceSignal(_options.ExitEventName, _options.GracefulSelfExit);
        }

        private bool SurfaceAndExit()
        {
            try { _options.SurfaceRunningInstance(); }
            catch (Exception ex) { _options.Log?.Invoke($"Surface failed: {ex.Message}"); }
            return false;
        }

        private void RelaunchElevated()
        {
            try
            {
                var exe = Environment.ProcessPath;
                if (string.IsNullOrEmpty(exe))
                    return;

                Process.Start(new ProcessStartInfo
                {
                    FileName = exe,
                    Arguments = HandoffArgs.AutoConfirm,
                    UseShellExecute = true,
                    Verb = "runas",
                });
            }
            catch (Win32Exception ex) when (ex.NativeErrorCode == 1223) // ERROR_CANCELLED — user dismissed UAC
            {
                _options.Log?.Invoke("Elevation cancelled by user.");
            }
            catch (Exception ex)
            {
                _options.Log?.Invoke($"Elevation failed: {ex.Message}");
            }
        }

        private List<Process> FindOtherInstances()
        {
            var self = Environment.ProcessId;
            var result = new List<Process>();
            try
            {
                foreach (var proc in Process.GetProcessesByName(_options.ProcessName))
                {
                    if (proc.Id == self)
                    {
                        proc.Dispose();
                        continue;
                    }
                    result.Add(proc);
                }
            }
            catch (Exception ex)
            {
                _options.Log?.Invoke($"Enumerate instances failed: {ex.Message}");
            }
            return result;
        }

        private static List<Process> WaitForExit(List<Process> procs, int totalBudgetMs)
        {
            var deadline = Environment.TickCount64 + totalBudgetMs;
            var stillRunning = new List<Process>();
            foreach (var proc in procs)
            {
                var remaining = (int)Math.Max(0, deadline - Environment.TickCount64);
                try
                {
                    if (!proc.WaitForExit(remaining))
                        stillRunning.Add(proc);
                }
                catch
                {
                    /* Process already gone / inaccessible — treat as exited. */
                }
            }
            return stillRunning;
        }

        private void TryKill(Process proc)
        {
            try { proc.Kill(); }
            catch (Exception ex) { _options.Log?.Invoke($"Kill failed: {ex.Message}"); }
        }

        private static Version? CurrentReleaseVersion()
        {
            var informational = Assembly.GetEntryAssembly()
                ?.GetCustomAttribute<AssemblyInformationalVersionAttribute>()
                ?.InformationalVersion;
            return SingleInstanceDecision.ParseProductVersion(informational)
                ?? Assembly.GetEntryAssembly()?.GetName().Version;
        }

        public void Dispose()
        {
            if (_disposed)
                return;
            _disposed = true;

            _exitSignal?.Dispose();
            if (_mutex is not null)
            {
                if (_ownsMutex)
                {
                    try { _mutex.ReleaseMutex(); } catch { /* not held / abandoned */ }
                }
                _mutex.Dispose();
            }
        }
    }
}
