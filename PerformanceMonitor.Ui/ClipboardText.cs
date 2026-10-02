/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using System.Diagnostics;
using System.Runtime.InteropServices;
using System.Threading;
using System.Threading.Tasks;
using System.Windows;

namespace PerformanceMonitor.Ui;

/// <summary>
/// Guarded clipboard reads and writes for the shared Plan Viewer paste/copy paths (Lite, the Darling
/// viewer, and the deprecated Full Dashboard all route through here). Windows serializes clipboard
/// access, so <see cref="Clipboard.GetText()"/> / <see cref="Clipboard.SetText(string)"/> throw <see
/// cref="COMException"/> (<c>CLIPBRD_E_CANT_OPEN</c>, 0x800401D0) whenever another process momentarily
/// holds the clipboard - a clipboard manager (Ditto, ClipboardFusion, Windows Clipboard History), Office,
/// a browser, or an RDP / locked-desktop session. That is a routine transient condition, so a bare call
/// takes the whole app down for no good reason (reads: #2833; writes: #4582). This wraps each read/write
/// in a short bounded retry and returns failure on persistent inability to open instead of throwing,
/// letting callers show a graceful notice (button paths) or simply no-op (Ctrl+V paths).
///
/// <para><b>The worst case is seconds, not milliseconds (#4624).</b> Every retry attempt calls straight
/// into a WPF <see cref="Clipboard"/> method, and WPF runs its own internal OLE retry loop before that call
/// gives up and throws - measured at roughly 1.1 s per failed call, not the handful of milliseconds an
/// earlier version of this class assumed. Nothing here can see or shorten that internal loop; the only
/// lever this class has is whether to make ANOTHER attempt once one returns. So the bound is two numbers,
/// not one: a small attempt count, and a <c>Stopwatch</c> time budget checked between attempts - once the
/// budget is already spent, the next attempt is skipped even if the attempt count has not run out. The
/// budget can only skip an attempt, never cut one short, so the worst case is the budget plus one attempt;
/// at the measured ~1.1 s per call it is about 2.3 s. See the constants below for the numbers and why.</para>
///
/// Two read variants share one guarded read-attempt helper: the synchronous <see cref="TryRead"/> (for any
/// non-async caller) sleeps the calling thread between attempts, while <see cref="TryReadAsync"/> awaits
/// <see cref="Task.Delay(int)"/> for the same backoff so a UI-thread caller keeps its WPF message pump
/// responsive BETWEEN attempts (#2837) - each attempt itself still blocks the UI thread for WPF's own
/// ~1.1 s, since that block happens inside the underlying <see cref="Clipboard"/> call, not in the gap
/// <see cref="Task.Delay(int)"/> covers.
/// <see cref="TrySetText"/> is the write sibling: every call site today is a synchronous button/menu-item
/// handler, so it sleeps like <see cref="TryRead"/> rather than needing an async twin.
/// </summary>
public static class ClipboardText
{
    // CLIPBRD_E_CANT_OPEN is transient, but every attempt still pays WPF's own internal OLE retry before it
    // throws - about 1.1 s per failed call, measured in #4624. Two attempts, ~25 ms apart, bound the honest
    // typical worst case to roughly 2.3 s (2 * ~1.1 s + one 25 ms gap). TryRead spends the gap in
    // Thread.Sleep (fine for a non-UI caller); TryReadAsync spends it awaiting Task.Delay so the UI message
    // pump keeps running between attempts (#2837) - the attempt itself still blocks either way.
    private const int MaxAttempts = 2;
    private const int RetryDelayMs = 25;

    // The backstop for a call slower than the ~1.1 s #4624 measured: checked between attempts, never
    // mid-attempt (a WPF call already in flight can't be interrupted), so a first attempt that alone spends
    // the budget is not retried. An attempt that ends just inside the budget is still followed by one more,
    // so the worst case is this budget plus one attempt, not the budget itself.
    private static readonly TimeSpan RetryBudget = TimeSpan.FromSeconds(2.5);

    // The backoff between attempts, swapped out by tests so a pin can assert the delay the product REQUESTS
    // (rather than timing it, which thread-pool starvation makes noise). Default to the real Thread.Sleep /
    // Task.Delay. Same role as ReadOnce / WriteOnce below.
    internal static Action<int> SleepBackoff { get; set; } = Thread.Sleep;
    internal static Func<int, Task> DelayBackoff { get; set; } = static ms => Task.Delay(ms);

    // Shared by every retry loop below: true while there is both an attempt and a time budget left to
    // spend on one more try. Checked between attempts, so a budget already spent skips straight to the
    // failure return instead of starting another ~1.1 s wait.
    private static bool HasRetryBudget(int attempt, Stopwatch stopwatch) =>
        attempt < MaxAttempts && stopwatch.Elapsed < RetryBudget;

    /// <summary>
    /// Synchronously attempts to read the clipboard's text, retrying briefly if the clipboard cannot be opened.
    /// Returns <c>true</c> with the clipboard text - which may still be empty or whitespace, so callers keep
    /// their own "no text" handling - when the read succeeds; returns <c>false</c> with an empty string when
    /// the clipboard could not be opened after the bounded retries. Only the clipboard-open failure family
    /// (<see cref="COMException"/> / <see cref="ExternalException"/>) is swallowed; any other exception
    /// propagates. Blocks the calling thread with <see cref="Thread.Sleep(int)"/> between attempts, so a
    /// UI-thread caller should prefer <see cref="TryReadAsync"/>. This is the synchronous entry point, kept
    /// for any non-async / non-UI-thread caller.
    /// </summary>
    public static bool TryRead(out string text)
    {
        var stopwatch = Stopwatch.StartNew();

        for (var attempt = 1; attempt <= MaxAttempts; attempt++)
        {
            if (TryReadOnce(out text))
            {
                return true;
            }

            if (!HasRetryBudget(attempt, stopwatch))
            {
                break;
            }

            SleepBackoff(RetryDelayMs);
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
    /// synchronously (awaiting an already-completed task does not yield), but the retry path DOES yield -- so
    /// a key handler that must suppress the gesture sets <c>e.Handled</c> BEFORE awaiting this, claiming it
    /// during event dispatch; setting it after would let the key finish routing unsuppressed on retry (#2837).
    /// </summary>
    public static async Task<(bool Ok, string Text)> TryReadAsync()
    {
        var stopwatch = Stopwatch.StartNew();

        for (var attempt = 1; attempt <= MaxAttempts; attempt++)
        {
            if (TryReadOnce(out var text))
            {
                return (true, text);
            }

            if (!HasRetryBudget(attempt, stopwatch))
            {
                break;
            }

            // No ConfigureAwait(false): the next attempt calls Clipboard.GetText(), which must run on the
            // STA UI thread, so we deliberately resume on the captured (UI) SynchronizationContext.
            await DelayBackoff(RetryDelayMs);
        }

        return (false, string.Empty);
    }

    // The actual read, swapped out by tests so a pin can force the clipboard-open failure - with a
    // controlled delay standing in for WPF's own ~1.1 s internal retry (#4624) - without a real busy
    // clipboard. Defaults to the real WPF call. Matches WriteOnce's role for TrySetText below.
    internal static Func<string> ReadOnce { get; set; } = Clipboard.GetText;

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
            text = ReadOnce();
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

    /// <summary>
    /// Attempts to write <paramref name="text"/> to the clipboard, retrying briefly if the clipboard cannot
    /// be opened (the same <c>CLIPBRD_E_CANT_OPEN</c> family <see cref="TryRead"/> guards against, on the
    /// write side - #4582: an unguarded <see cref="Clipboard.SetText(string)"/> / <see
    /// cref="Clipboard.SetDataObject(object, bool)"/> throws and crashes the app when another process holds
    /// the clipboard). Returns <c>true</c> on success, <c>false</c> when the clipboard could not be opened
    /// after the bounded retries. Only the clipboard-open failure family is swallowed; any other exception
    /// propagates. Blocks the calling thread with <see cref="Thread.Sleep(int)"/> between attempts, matching
    /// <see cref="TryRead"/> - every existing call site is a synchronous button/menu-item handler, not an
    /// async one, so there is no UI-thread-freeze concern to trade off here the way <see cref="TryReadAsync"/>
    /// does for reads.
    /// </summary>
    public static bool TrySetText(string text)
    {
        var stopwatch = Stopwatch.StartNew();

        for (var attempt = 1; attempt <= MaxAttempts; attempt++)
        {
            if (TrySetTextOnce(text))
            {
                return true;
            }

            if (!HasRetryBudget(attempt, stopwatch))
            {
                break;
            }

            SleepBackoff(RetryDelayMs);
        }

        return false;
    }

    // The actual write, swapped out by tests so a pin can force the clipboard-open failure without a real
    // busy clipboard (COMException/ExternalException isn't something a test can provoke on demand). Defaults
    // to the real WPF call.
    internal static Action<string> WriteOnce { get; set; } = Clipboard.SetText;

    /// <summary>
    /// One guarded write attempt, shared by <see cref="TrySetText"/>: returns <c>true</c> on success, or
    /// <c>false</c> when the clipboard-open failure family (<see cref="COMException"/> /
    /// <see cref="ExternalException"/>) is thrown - a transient <c>CLIPBRD_E_CANT_OPEN</c> the caller
    /// retries. Any other exception propagates.
    /// </summary>
    private static bool TrySetTextOnce(string text)
    {
        try
        {
            WriteOnce(text);
            return true;
        }
        catch (ExternalException)
        {
            return false;
        }
    }

    /// <summary>
    /// Guarded sibling of <see cref="TrySetText"/> for <see cref="Clipboard.SetDataObject(object, bool)"/>
    /// call sites (a plain string via <c>SetDataObject</c> instead of <c>SetText</c> - matches the existing
    /// call sites' convention of avoiding WPF's <c>Clipboard.Flush()</c> - and the chart bitmap copy, which
    /// has no text equivalent). Same bounded retry and failure family as <see cref="TrySetText"/>.
    /// </summary>
    public static bool TrySetDataObject(object data, bool copy = false)
    {
        var stopwatch = Stopwatch.StartNew();

        for (var attempt = 1; attempt <= MaxAttempts; attempt++)
        {
            if (TrySetDataObjectOnce(data, copy))
            {
                return true;
            }

            if (!HasRetryBudget(attempt, stopwatch))
            {
                break;
            }

            SleepBackoff(RetryDelayMs);
        }

        return false;
    }

    // Swapped out by tests, matching WriteOnce's role for TrySetText.
    internal static Action<object, bool> WriteDataObjectOnce { get; set; } = Clipboard.SetDataObject;

    private static bool TrySetDataObjectOnce(object data, bool copy)
    {
        try
        {
            WriteDataObjectOnce(data, copy);
            return true;
        }
        catch (ExternalException)
        {
            return false;
        }
    }
}
