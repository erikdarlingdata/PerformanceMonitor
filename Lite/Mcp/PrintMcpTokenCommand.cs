/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor Lite.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System.IO;
using PerformanceMonitorLite.Services;

namespace PerformanceMonitorLite.Mcp;

/// <summary>
/// #9185: <c>PerformanceMonitorLite.exe --print-mcp-token</c> prints the stored MCP token to standard output and exits, like
/// Darling's <c>--print-mcp-token</c>. The token is kept in the Credential Manager of the Windows user that runs Lite, and a
/// headless install runs as a user nobody signs in as, so there is no Settings window to read it from. The switch is run as that
/// user. It only reads: it never makes, replaces or saves a token, and it starts nothing (no window, no tray icon, no data folder,
/// no single-instance lock, no log file), so it does not disturb a Lite that is running. The token goes to standard output
/// and nowhere else: not the log, not standard error, not a message.
/// </summary>
internal static class PrintMcpTokenCommand
{
    /// <summary>The token was printed.</summary>
    internal const int ExitPrinted = 0;

    /// <summary>No token is stored, so none was printed and none was made.</summary>
    internal const int ExitNoToken = 1;

    /// <summary>The Credential Manager entry could not be read (or is empty), so nothing was printed. A token may well exist.</summary>
    internal const int ExitReadFailed = 2;

    /// <summary>The process has no standard output to write to (no parent console and no redirect), so the vault was not read.</summary>
    internal const int ExitNoOutput = 3;

    /// <summary>The process run: joins the parent's console, then <see cref="Run"/> over the app's Credential Manager store.</summary>
    internal static int RunProcess()
    {
        if (!ParentConsole.TryOpen(out var output, out var error) || output is null)
        {
            return ExitNoOutput;
        }

        return Run(McpTokenStore.ForApp(), output, error);
    }

    /// <summary>Reads the stored token and prints it. Returns <see cref="ExitPrinted"/>, <see cref="ExitNoToken"/> or
    /// <see cref="ExitReadFailed"/>. Standard output gets the token and a newline, or nothing. Standard error gets one plain line
    /// when no token was printed.</summary>
    internal static int Run(McpTokenStore store, TextWriter output, TextWriter error)
    {
        var token = store.Read(out var failure);
        if (token is not null)
        {
            output.WriteLine(token);
            output.Flush();
            return ExitPrinted;
        }

        if (failure is not null)
        {
            error.WriteLine($"The MCP token was not printed: {failure}.");
            return ExitReadFailed;
        }

        error.WriteLine("No MCP token is stored for this Windows user. Turn MCP on and start Lite as this user once, and it makes one.");
        return ExitNoToken;
    }
}
