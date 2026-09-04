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
using System.Threading.Tasks;
using System.Windows;

namespace PerformanceMonitor.Ui;

/// <summary>
/// Guarded clipboard reads for the shared Plan Viewer paste paths (Lite, the Darling viewer, and the
/// deprecated Full Dashboard all route through here). Windows serializes clipboard access, so
/// <see cref="Clipboard.GetText()"/> throws <see cref="COMException"/> (<c>CLIPBRD_E_CANT_OPEN</c>,
/// 0x800401D0) whenever another process momentarily holds the clipboard - a clipboard manager (Ditto,
/// ClipboardFusion, Windows Clipboard History), Office, a browser, or an RDP / locked-desktop session. That
/// is a routine transient condition, so a bare call takes the whole app down for no good reason (#2833).
/// This wraps the read in a short bounded retry and returns failure on persistent inability to open instead
/// of throwing, letting callers show a graceful notice (button paths) or simply no-op (Ctrl+V paths).
///
/// Two variants share one guarded read-attempt helper: the synchronous <see cref="TryRead"/> (for any
/// non-async caller) sleeps the calling thread between attempts, while <see cref="TryReadAsync"/> awaits
/// <see cref="Task.Delay(int)"/> for the same backoff so a UI-thread caller keeps its WPF message pump
/// responsive on the rare failure path instead of freezing for up to the worst-case retry span (#2837).
/// </summary>
public static class ClipboardText
{
    // CLIPBRD_E_CANT_OPEN is transient: another process holds the clipboard for a few milliseconds. Retry a
    // handful of times, ~25 ms apart, before giving up - a worst-case ~175 ms span only on the rare failure
    // path, versus the crash we are replacing. TryRead spends that span in Thread.Sleep (fine for a non-UI
    // caller); TryReadAsync spends it awaiting Task.Delay so the UI message pump keeps running (#2837).
    private const int MaxAttempts = 8;
    private const int RetryDelayMs = 25;

    /// <summary>
    /// Synchronously attempts to read the clipboard's text, retrying briefly if the clipboard cannot be opened.
    /// Returns <c>true</c> with the clipboard text - which may still be empty or whitespace, so callers keep
    /// their own "no text" handling - when the read succeeds; returns <c>false</c> with an empty string when
    /// the clipboard could not be opened after the bounded retries. Only the clipboard-open failure family
    /// (<see cref="COMException"/> / <see cref="ExternalException"/>) is swallowed; any other exception
    /// propagates. Blocks the calling thread with <see cref="Thread.Sleep(int)"/> between attempts, so a
    /// UI-thread caller should prefer <see cref="TryReadAsync"/>.
    /// </summary>
    public static bool TryRead(out string text)
    {
        for (var attempt = 1; attempt <= MaxAttempts; attempt++)
        {
            if (TryReadOnce(out text))
            {
                return true;
            }

            if (attempt < MaxAttempts)
            {
                Thread.Sleep(RetryDelayMs);
            }
        }

        text = string.Empty;
        return false;
    }

    /// <summary>
    /// Async sibling of <see cref="TryRead"/> for UI-thread callers: the identical bounded-retry read, but the
    /// backoff between attempts <c>await</c>s <see cref="Task.Delay(int)"/> instead of blocking the thread with
    /// <see cref="Thread.Sleep(int)"/>, so the WPF message pump stays responsive on the rare
    /// clipboard-can't-open path (#2837). Returns <c>Ok = true</c> with the clipboard text - which may still be
    /// empty or whitespace, so callers keep their own "no text" handling - on success; <c>Ok = false</c> with
    /// an empty string when the clipboard could not be opened after the bounded retries. Only the
    /// clipboard-open failure family (<see cref="COMException"/> / <see cref="ExternalException"/>) is
    /// swallowed; any other exception propagates. A read that succeeds on the first attempt completes
    /// synchronously (awaiting an already-completed task does not yield), so a caller that sets
    /// <c>e.Handled</c> after the await still does so within the event dispatch on the common path.
    /// </summary>
    public static async Task<(bool Ok, string Text)> TryReadAsync()
    {
        for (var attempt = 1; attempt <= MaxAttempts; attempt++)
        {
            if (TryReadOnce(out var text))
            {
                return (true, text);
            }

            if (attempt < MaxAttempts)
            {
                // No ConfigureAwait(false): the next attempt calls Clipboard.GetText(), which must run on the
                // STA UI thread, so we deliberately resume on the captured (UI) SynchronizationContext.
                await Task.Delay(RetryDelayMs);
            }
        }

        return (false, string.Empty);
    }

    /// <summary>
    /// One guarded <see cref="Clipboard.GetText()"/> attempt, shared by <see cref="TryRead"/> and
    /// <see cref="TryReadAsync"/>: returns <c>true</c> with the text on success, or <c>false</c> with an empty
    /// string when the clipboard-open failure family (<see cref="COMException"/> / <see cref="ExternalException"/>)
    /// is thrown - a transient <c>CLIPBRD_E_CANT_OPEN</c> the caller retries. Any other exception propagates.
    /// </summary>
    private static bool TryReadOnce(out string text)
    {
        try
        {
            text = Clipboard.GetText();
            return true;
        }
        catch (ExternalException)
        {
            // COMException (the CLIPBRD_E_CANT_OPEN we care about) derives from ExternalException, so this one
            // catch covers both.
            text = string.Empty;
            return false;
        }
    }
}
