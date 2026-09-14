/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor Lite.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using System.Diagnostics;
using System.Windows;
using PerformanceMonitorLite.Services;

namespace PerformanceMonitorLite.Windows;

/// <summary>
/// Shows the code and URL for one device-code sign-in while the connection attempt is still open,
/// and gives the user a way out of it.
///
/// <para><b>Why a window and not the status line in the dialog.</b> The code has to be legible,
/// selectable and reachable from every site that can open a device-code connection — Test Connection
/// in the Add/Edit dialog and the Manage Servers connectivity check — and only one of those has a
/// status line at all. A window owned by whatever is in front covers both with one implementation,
/// and matches how the provider registration already works: installed once, process-wide.</para>
///
/// <para><b>What was rejected.</b> A <c>MessageBox</c>: it is modal on the calling thread, and that
/// thread is the driver's token acquisition, so the sign-in would not start polling until the box
/// was dismissed — and dismissing it is also the only way to stop reading the code. A code shown in
/// the Add Server dialog's <c>StatusText</c>: not selectable, one line, and absent on the Manage
/// Servers path. Embedding a browser: the mode exists precisely so that no in-process sign-in
/// surface is needed.</para>
///
/// <para>Lifetime is the attempt's, not the window's:
/// <see cref="EntraDeviceCodeAttempt.Finished"/> closes it when the connection ends for any reason,
/// and closing it cancels the attempt. Both directions are wired, so neither can outlive the
/// other.</para>
/// </summary>
public partial class EntraDeviceCodeWindow : Window
{
    private readonly EntraDeviceCodeAttempt _attempt;
    private bool _closingBecauseAttemptFinished;
    private bool _closed;

    /// <summary>
    /// Binds the window to one attempt. The challenge is read here rather than bound, because it is
    /// set once by the driver's callback before this is constructed and never changes afterwards.
    /// </summary>
    public EntraDeviceCodeWindow(EntraDeviceCodeAttempt attempt)
    {
        ArgumentNullException.ThrowIfNull(attempt);

        InitializeComponent();

        _attempt = attempt;

        var challenge = attempt.Challenge;
        UserCodeBox.Text = challenge?.UserCode ?? string.Empty;
        VerificationUrlBox.Text = challenge?.VerificationUrl ?? string.Empty;

        attempt.Finished += OnAttemptFinished;

        Loaded += (_, _) =>
        {
            UserCodeBox.Focus();
            UserCodeBox.SelectAll();
        };
    }

    /// <summary>
    /// Closes the window from the attempt's side. Marshaled, because the attempt ends on whichever
    /// thread completed the connection; flagged, so <see cref="Window_Closing"/> can tell this close
    /// apart from the user giving up and does not cancel an attempt that already succeeded.
    ///
    /// <para><b>Guarded, so the second close is never issued rather than merely tolerated.</b> There
    /// is one ordering that reaches here after the window is already gone: the attempt ends on a
    /// background thread and queues the <c>BeginInvoke</c> below, and the user then closes the window
    /// on the dispatcher before that delegate is pumped — <see cref="Window_Closing"/>'s unsubscribe
    /// cannot recall an invocation already queued.
    ///
    /// <para>Measured, that second <c>Close()</c> is harmless today:
    /// <c>Window.InternalClose</c> answers <c>if (_disposed) return;</c> straight after its
    /// <c>VerifyNotClosing</c>, which throws only while a close is IN FLIGHT or on an invalid
    /// composition target — and a closed window has neither, its source window being null
    /// (<c>PresentationFramework</c> 10.0.10). The flag is here anyway, because that is a fact about
    /// undocumented internals in a framework this app does not version, and no test can reach the
    /// race to notice if it changes. Not making the call costs one field and depends on nothing.</para>
    /// </summary>
    private void OnAttemptFinished()
    {
        if (!Dispatcher.CheckAccess())
        {
            Dispatcher.BeginInvoke(new Action(OnAttemptFinished));
            return;
        }

        if (_closed)
        {
            return;
        }

        _closingBecauseAttemptFinished = true;
        Close();
    }

    private void Window_Closing(object sender, System.ComponentModel.CancelEventArgs e)
    {
        _closed = true;
        _attempt.Finished -= OnAttemptFinished;

        if (!_closingBecauseAttemptFinished)
        {
            /* The user closed this window while the sign-in was still open, by the Cancel button, the
               title-bar X or Escape. All three mean the same thing and all three arrive here, which
               is why the cancellation lives in Closing rather than in the button handler. */
            _attempt.Cancel();
        }
    }

    private void Cancel_Click(object sender, RoutedEventArgs e) => Close();

    private void Window_KeyDown(object sender, System.Windows.Input.KeyEventArgs e)
    {
        if (e.Key == System.Windows.Input.Key.Escape)
        {
            Close();
        }
    }

    private void CopyCode_Click(object sender, RoutedEventArgs e)
    {
        try
        {
            Clipboard.SetText(UserCodeBox.Text);
        }
        catch (Exception ex)
        {
            /* The clipboard is a shared OS resource another process can hold, and a locked-down
               desktop can refuse it outright. The code is still on screen and selectable, so this is
               an inconvenience rather than a dead end - and a throw here would take down the window
               the user is reading the code off. */
            AppLogger.Warn(
                EntraDeviceCodeAuth.LogSource,
                $"Could not copy the device code to the clipboard: {ex.Message}");
        }
    }

    private void OpenPage_Click(object sender, RoutedEventArgs e)
    {
        var url = VerificationUrlBox.Text;

        /* Only ever the tenant's own https verification URL, and checked rather than trusted: this
           string arrives from the token endpoint and is handed to the shell, so anything but an
           absolute http(s) URI is refused instead of launched. The code is still readable and the URL
           still selectable if this declines. */
        if (!Uri.TryCreate(url, UriKind.Absolute, out var parsed) ||
            (parsed.Scheme != Uri.UriSchemeHttps && parsed.Scheme != Uri.UriSchemeHttp))
        {
            AppLogger.Warn(
                EntraDeviceCodeAuth.LogSource,
                "The sign-in page was not opened: the verification URL the tenant returned is not an "
                    + "absolute http or https address.");
            return;
        }

        try
        {
            Process.Start(new ProcessStartInfo(parsed.AbsoluteUri) { UseShellExecute = true });
        }
        catch (Exception ex)
        {
            AppLogger.Warn(
                EntraDeviceCodeAuth.LogSource,
                $"Could not open the sign-in page in a browser: {ex.Message}");
        }
    }
}
