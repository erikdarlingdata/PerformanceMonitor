/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using System.Runtime.InteropServices;
using System.Threading;
using System.Windows;

namespace PerformanceMonitor.Ui;

/// <summary>
/// Guarded clipboard reads for the shared Plan Viewer paste paths (Lite, the Darling viewer, and the
/// deprecated Full Dashboard all route through here). Windows serializes clipboard access, so
/// <see cref="Clipboard.GetText()"/> throws <see cref="COMException"/> (<c>CLIPBRD_E_CANT_OPEN</c>,
/// 0x800401D0) whenever another process momentarily holds the clipboard - a clipboard manager (Ditto,
/// ClipboardFusion, Windows Clipboard History), Office, a browser, or an RDP / locked-desktop session. That
/// is a routine transient condition, so a bare call takes the whole app down for no good reason (#2833).
/// This wraps the read in a short bounded retry and returns <c>false</c> on persistent failure instead of
/// throwing, letting callers show a graceful notice (button paths) or simply no-op (Ctrl+V paths).
/// </summary>
public static class ClipboardText
{
    /// <summary>
    /// Attempts to read the clipboard's text, retrying briefly if the clipboard cannot be opened.
    /// Returns <c>true</c> with the clipboard text - which may still be empty or whitespace, so callers keep
    /// their own "no text" handling - when the read succeeds; returns <c>false</c> with an empty string when
    /// the clipboard could not be opened after the bounded retries. Only the clipboard-open failure family
    /// (<see cref="COMException"/> / <see cref="ExternalException"/>) is swallowed; any other exception
    /// propagates.
    /// </summary>
    public static bool TryRead(out string text)
    {
        // CLIPBRD_E_CANT_OPEN is transient: another process holds the clipboard for a few milliseconds. Retry
        // a handful of times, ~25 ms apart, before giving up - a worst-case ~175 ms pause only on the rare
        // failure path, versus the crash we are replacing.
        const int maxAttempts = 8;
        const int retryDelayMs = 25;

        for (var attempt = 1; attempt <= maxAttempts; attempt++)
        {
            try
            {
                text = Clipboard.GetText();
                return true;
            }
            catch (ExternalException)
            {
                // COMException (the CLIPBRD_E_CANT_OPEN we care about) derives from ExternalException, so this
                // one catch covers both. Give up after the final attempt instead of letting it crash the app.
                if (attempt == maxAttempts)
                {
                    break;
                }

                Thread.Sleep(retryDelayMs);
            }
        }

        text = string.Empty;
        return false;
    }
}
