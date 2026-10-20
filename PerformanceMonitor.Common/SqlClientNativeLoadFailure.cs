/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;

namespace PerformanceMonitor.Common;

/// <summary>
/// Recognises the failure where SqlClient's native network library (SNI) cannot load, and explains it (#6183).
/// Windows cannot load a DLL whose full path is about 260 characters or longer, so an install in a deep folder makes
/// every SQL Server connection throw "The type initializer for 'Microsoft.Data.SqlClient.TdsParser' threw an
/// exception", which does not say why. Darling's connect log, <c>--validate-config</c>, the <c>add_servers</c>
/// probe, and Lite's connection test and connect log all call <see cref="Describe"/>, so the wording lives here once.
/// Plain strings and exception types only, so this project needs no SqlClient reference.
/// </summary>
public static class SqlClientNativeLoadFailure
{
    private const string TdsParserTypeName = "Microsoft.Data.SqlClient.TdsParser";
    private const string SniLibraryName = "Microsoft.Data.SqlClient.SNI";

    /// <summary>A base directory this long or longer is named as the likely cause.</summary>
    public const int LongPathThreshold = 200;

    /// <summary>
    /// True when <paramref name="exception"/>, or an exception inside it, is a <see cref="TypeInitializationException"/>
    /// for SqlClient's <c>TdsParser</c>, or a <see cref="DllNotFoundException"/> or <see cref="BadImageFormatException"/>
    /// that names the SNI library.
    /// </summary>
    public static bool IsMatch(Exception? exception)
    {
        for (var current = exception; current is not null; current = current.InnerException)
        {
            if (current is TypeInitializationException tie
                && string.Equals(tie.TypeName, TdsParserTypeName, StringComparison.Ordinal))
            {
                return true;
            }

            if (current is DllNotFoundException or BadImageFormatException
                && current.Message.Contains(SniLibraryName, StringComparison.OrdinalIgnoreCase))
            {
                return true;
            }
        }

        return false;
    }

    /// <summary>
    /// The explanation for a native-library load failure, or null when <paramref name="exception"/> is some other
    /// failure and the caller keeps its own message.
    /// </summary>
    /// <param name="exception">The exception a connect attempt threw.</param>
    /// <param name="baseDirectory">The app's base directory, normally <see cref="AppContext.BaseDirectory"/>.</param>
    public static string? Describe(Exception? exception, string? baseDirectory)
    {
        if (!IsMatch(exception))
        {
            return null;
        }

        var message = "SqlClient's native network library did not load, so no SQL Server connection can work from this install.";

        var reason = Reason(exception!);
        if (reason.Length > 0)
        {
            message += " Reason: " + reason;
            if (!reason.EndsWith('.'))
            {
                message += ".";
            }
        }

        var path = baseDirectory?.TrimEnd('\\', '/') ?? string.Empty;
        if (path.Length >= LongPathThreshold)
        {
            message += $" The install folder path is {path.Length} characters long, and Windows cannot load a library from a path that long."
                + " This is the likely cause. Move the install to a shorter folder path and start it again.";
        }

        return message;
    }

    /// <summary>
    /// One line: the message of the innermost exception, which is the actual load error. Empty when that is only the
    /// "type initializer threw" wrapper, which says nothing more than the sentence before it.
    /// </summary>
    private static string Reason(Exception exception)
    {
        var innermost = exception;
        while (innermost.InnerException is not null)
        {
            innermost = innermost.InnerException;
        }

        if (innermost is TypeInitializationException)
        {
            return string.Empty;
        }

        var text = innermost.Message?.Trim() ?? string.Empty;
        var newline = text.IndexOfAny(['\r', '\n']);
        return newline >= 0 ? text[..newline].TrimEnd() : text;
    }
}
