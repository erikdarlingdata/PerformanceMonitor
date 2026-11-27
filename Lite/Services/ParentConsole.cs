/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor Lite.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using System.IO;
using System.Runtime.InteropServices;

namespace PerformanceMonitorLite.Services;

/// <summary>
/// #9185: Lite is a WinExe, so Windows gives it no console. A command-line switch that prints something (<c>--print-mcp-token</c>)
/// joins the console of the process that started it. A redirected standard handle (<c>&gt; file</c>, a pipe) is left as it is: the
/// handle the parent passed in is the one written to. Nothing here allocates a console window of its own.
/// </summary>
internal static class ParentConsole
{
    private const int AttachParentProcess = -1;
    private const int StdInputHandle = -10;
    private const int StdOutputHandle = -11;
    private const int StdErrorHandle = -12;
    private const uint FileTypeUnknown = 0;

    [DllImport("kernel32.dll", SetLastError = true)]
    private static extern bool AttachConsole(int processId);

    [DllImport("kernel32.dll", SetLastError = true)]
    private static extern IntPtr GetStdHandle(int standardHandle);

    [DllImport("kernel32.dll", SetLastError = true)]
    private static extern bool SetStdHandle(int standardHandle, IntPtr handle);

    [DllImport("kernel32.dll", SetLastError = true)]
    private static extern uint GetFileType(IntPtr handle);

    /// <summary>
    /// Joins the parent's console, if there is one, and opens standard output and standard error as they stand. A GUI process's
    /// <c>Console.Out</c> was bound to the null device at start, so both are opened again here.
    /// </summary>
    /// <param name="output">Standard output. Null when the process has nowhere to write: no parent console and no redirect.</param>
    /// <param name="error">Standard error. Never null: it is a writer that discards when there is no standard error.</param>
    /// <returns>True when standard output can be written to.</returns>
    internal static bool TryOpen(out TextWriter? output, out TextWriter error)
    {
        /* AttachConsole replaces every standard handle with the console's, a redirected one included (measured: a file handle
           from "> file" becomes a console handle, and the token lands on the screen instead of in the file). So a handle that
           was usable before the attach is put back after it. One that was not usable (standard error when only standard output
           is redirected) keeps the console's. The attach fails with no parent console (started from Explorer or a scheduled
           task) or when one is already attached, and neither matters here, so its result is not read. */
        var inBefore = GetStdHandle(StdInputHandle);
        var outBefore = GetStdHandle(StdOutputHandle);
        var errBefore = GetStdHandle(StdErrorHandle);
        _ = AttachConsole(AttachParentProcess);
        Restore(StdInputHandle, inBefore);
        Restore(StdOutputHandle, outBefore);
        Restore(StdErrorHandle, errBefore);

        /* UTF-8 without a BOM: a token redirected to a file must be the token and nothing else. */
        error = HasHandle(StdErrorHandle) ? new StreamWriter(Console.OpenStandardError()) { AutoFlush = true } : TextWriter.Null;
        if (!HasHandle(StdOutputHandle))
        {
            output = null;
            return false;
        }

        output = new StreamWriter(Console.OpenStandardOutput()) { AutoFlush = true };
        return true;
    }

    /// <summary>
    /// #9237: standard input as it stands after <see cref="TryOpen"/>, for a switch that reads a piped JSON array or a piped
    /// password. <paramref name="redirected"/> is true when it is a pipe or a file (nobody is typing), false at a console.
    /// </summary>
    internal static TextReader OpenInput(out bool redirected)
    {
        redirected = Console.IsInputRedirected;
        return HasHandle(StdInputHandle)
            ? new StreamReader(Console.OpenStandardInput(), new System.Text.UTF8Encoding(false))
            : TextReader.Null;
    }

    /// <summary>
    /// #9753: standard input as BYTES, for a piped password. A text reader would take a byte order mark as the first character
    /// of the password, decode UTF-16 into noise and drop a second line without a word, so the password is read raw and checked
    /// by <see cref="PerformanceMonitor.Common.StrictStdinPassword"/>. A process with no standard input gets an empty stream.
    /// </summary>
    internal static Stream OpenInputBytes() => HasHandle(StdInputHandle) ? Console.OpenStandardInput() : Stream.Null;

    private static bool HasHandle(int standardHandle) => IsUsable(GetStdHandle(standardHandle));

    private static bool IsUsable(IntPtr handle) =>
        handle != IntPtr.Zero && handle != new IntPtr(-1) && GetFileType(handle) != FileTypeUnknown;

    private static void Restore(int standardHandle, IntPtr before)
    {
        if (IsUsable(before))
        {
            _ = SetStdHandle(standardHandle, before);
        }
    }
}
