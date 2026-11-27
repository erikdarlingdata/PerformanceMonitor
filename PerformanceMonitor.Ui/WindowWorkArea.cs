/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using System.Runtime.InteropServices;
using System.Windows;
using System.Windows.Interop;

namespace PerformanceMonitor.Ui
{
    /// <summary>
    /// The work area of the monitor a window is actually ON, and the clamp that keeps a growing
    /// <c>SizeToContent</c> dialog inside it (#1891).
    ///
    /// <para><b>Why not <see cref="SystemParameters.WorkArea"/>.</b> That property always reports the PRIMARY
    /// monitor, whatever monitor the window is on. #1828/#1829 fixed a dialog whose footer grew off the
    /// bottom of the screen by clamping against it — which is correct on one monitor and wrong on a second
    /// one that is shorter than the primary, where the dialog is allowed to grow to the primary's height and
    /// the footer goes off-screen again. Screen-overflow correctness was the whole point of that fix, so the
    /// remaining hole is closed here rather than left to the common case.</para>
    ///
    /// <para><b>Win32 rather than WinForms.</b> <c>System.Windows.Forms.Screen.FromHandle</c> would answer the
    /// same question, but neither app references WinForms today and turning <c>UseWindowsForms</c> on for one
    /// lookup drags a second UI framework into both. Two P/Invokes are the lighter dependency, and this
    /// project already carries Win32 interop (<see cref="Win32ProcessInspector"/>).</para>
    ///
    /// <para><b>Fails to the old behaviour, never throws.</b> Before a window is shown it has no HWND, and
    /// <c>MonitorFromWindow</c> on <see cref="IntPtr.Zero"/> answers about the wrong thing — so every path
    /// that cannot get a real answer returns <see cref="SystemParameters.WorkArea"/>, which is exactly what
    /// the code did before this existed. A dialog can therefore only be MORE correct than it was.</para>
    /// </summary>
    public static class WindowWorkArea
    {
        private const int MonitorDefaultToNearest = 0x00000002;

        [DllImport("user32.dll", SetLastError = true)]
        private static extern IntPtr MonitorFromWindow(IntPtr hwnd, int dwFlags);

        [DllImport("user32.dll", CharSet = CharSet.Unicode, SetLastError = true)]
        [return: MarshalAs(UnmanagedType.Bool)]
        private static extern bool GetMonitorInfoW(IntPtr hMonitor, ref MonitorInfo lpmi);

        [StructLayout(LayoutKind.Sequential)]
        private struct Rect32
        {
            public int Left;
            public int Top;
            public int Right;
            public int Bottom;
        }

        [StructLayout(LayoutKind.Sequential)]
        private struct MonitorInfo
        {
            public int cbSize;
            public Rect32 rcMonitor;
            public Rect32 rcWork;
            public int dwFlags;
        }

        /// <summary>
        /// The work area — screen minus taskbar and appbars — of the monitor <paramref name="window"/> is on,
        /// in WPF device-independent units, or <see cref="SystemParameters.WorkArea"/> when that cannot be
        /// determined (no HWND yet, or either Win32 call fails).
        ///
        /// <para>The DIP conversion is not optional. <c>GetMonitorInfo</c> answers in PHYSICAL PIXELS while
        /// <c>Top</c>/<c>MaxHeight</c> are DIPs, so on a 150%-scaled monitor an unconverted 2160-pixel work
        /// area would let a dialog claim 2160 DIPs = 3240 physical pixels of height. That is a worse bug than
        /// the one being fixed, which is why the transform comes off the window's OWN
        /// <see cref="PresentationSource"/> rather than any process-wide DPI.</para>
        /// </summary>
        public static Rect ForWindow(Window window)
        {
            if (window is null)
            {
                return SystemParameters.WorkArea;
            }

            try
            {
                var hwnd = new WindowInteropHelper(window).Handle;
                if (hwnd == IntPtr.Zero)
                {
                    return SystemParameters.WorkArea;
                }

                var monitor = MonitorFromWindow(hwnd, MonitorDefaultToNearest);
                if (monitor == IntPtr.Zero)
                {
                    return SystemParameters.WorkArea;
                }

                var info = new MonitorInfo { cbSize = Marshal.SizeOf<MonitorInfo>() };
                if (!GetMonitorInfoW(monitor, ref info))
                {
                    return SystemParameters.WorkArea;
                }

                var source = PresentationSource.FromVisual(window);
                var transform = source?.CompositionTarget?.TransformFromDevice;
                if (transform is null)
                {
                    return SystemParameters.WorkArea;
                }

                var topLeft = transform.Value.Transform(new Point(info.rcWork.Left, info.rcWork.Top));
                var bottomRight = transform.Value.Transform(new Point(info.rcWork.Right, info.rcWork.Bottom));
                var work = new Rect(topLeft, bottomRight);

                /* A zero-area answer is not an answer. Rather than hand back something that would clamp every
                   dialog to nothing, fall back like every other failure above. */
                return work.Width > 0 && work.Height > 0 ? work : SystemParameters.WorkArea;
            }
            catch (Exception)
            {
                /* Interop on a window being torn down can throw; a cosmetic clamp must never take the app
                   with it. The pre-#1891 behaviour is the floor. */
                return SystemParameters.WorkArea;
            }
        }

        /// <summary>
        /// Keeps <paramref name="window"/> inside its own monitor's work area: caps its height, and when
        /// top-anchored <c>SizeToContent</c> growth has pushed the bottom edge past the work area, pulls the
        /// window up so the footer stays visible.
        ///
        /// <para>Shared rather than duplicated in each dialog on purpose — the two Add Server dialogs are
        /// twins, and Lite/Viewer drift in exactly this kind of window-behaviour detail is the recurring
        /// complaint. One body means they cannot disagree.</para>
        /// </summary>
        public static void Clamp(Window window)
        {
            if (window is null)
            {
                return;
            }

            var workArea = ForWindow(window);

            /* Guarded so re-clamping on every LocationChanged during a drag is a no-op once settled: assigning
               MaxHeight can itself resize a SizeToContent window, which would re-enter this method. */
            if (Math.Abs(window.MaxHeight - workArea.Height) > 0.5)
            {
                window.MaxHeight = workArea.Height;
            }

            var top = ClampTop(window.Top, window.ActualHeight, workArea);
            if (!double.IsNaN(top) && Math.Abs(window.Top - top) > 0.5)
            {
                window.Top = top;
            }
        }

        /// <summary>
        /// Where the window's top edge belongs given its height and work area: unchanged while the bottom edge
        /// fits, otherwise pulled up just enough to fit — and never above the work area's top in either case,
        /// since a dialog taller than the screen must lose its bottom rather than its title bar and the controls
        /// under it. The fits case needs the floor too (#9093): <c>CenterOwner</c> places a window by its
        /// DECLARED height before <see cref="Clamp"/> shrinks <c>MaxHeight</c>, so a window can start above the
        /// work area and then fit once capped.
        ///
        /// <para>Split out from <see cref="Clamp"/> because it is the whole decision and the only part worth
        /// testing: a WPF <see cref="Window"/> needs an STA thread and an <c>Application</c>, while this is
        /// arithmetic.</para>
        /// </summary>
        internal static double ClampTop(double top, double actualHeight, Rect workArea) =>
            Math.Max(
                workArea.Top,
                top + actualHeight > workArea.Bottom
                    ? workArea.Bottom - actualHeight
                    : top);
    }
}
