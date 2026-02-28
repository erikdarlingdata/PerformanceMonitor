/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using System.Runtime.InteropServices;

namespace PerformanceMonitorDashboard.Helpers
{
    internal static class NativeMethods
    {
        public const int HWND_BROADCAST = 0xFFFF;

        public static readonly int WM_SHOWMONITOR = RegisterWindowMessage("WM_SHOWMONITOR_DASHBOARD");

        [DllImport("user32.dll", SetLastError = true, CharSet = CharSet.Unicode)]
        private static extern int RegisterWindowMessage(string lpString);

        [DllImport("user32.dll", SetLastError = true)]
        [return: MarshalAs(UnmanagedType.Bool)]
        private static extern bool PostMessage(IntPtr hWnd, int Msg, IntPtr wParam, IntPtr lParam);

        [DllImport("shell32.dll", SetLastError = true)]
        private static extern void SetCurrentProcessExplicitAppUserModelID([MarshalAs(UnmanagedType.LPWStr)] string appId);

        public static void BroadcastShowMessage()
        {
            PostMessage((IntPtr)HWND_BROADCAST, WM_SHOWMONITOR, IntPtr.Zero, IntPtr.Zero);
        }

        public static void SetAppUserModelId(string appId)
        {
            SetCurrentProcessExplicitAppUserModelID(appId);
        }
    }
}
