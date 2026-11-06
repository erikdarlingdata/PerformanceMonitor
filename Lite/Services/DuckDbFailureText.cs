/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor Lite.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using System.Collections.Generic;
using System.Text;
using System.Text.RegularExpressions;

namespace PerformanceMonitorLite.Services;

/// <summary>
/// The text Lite searches to tell one kind of DuckDB open failure from another (#7824). Both the startup storage check
/// and Import Data's classifier read it, so they cannot drift apart.
/// <para>It is built from the exception messages only: the outer exception, each inner exception down the chain, and
/// every inner exception of an <see cref="AggregateException"/>. <c>ToString()</c> is not used, because it adds the stack
/// text. A double-quoted span is blanked out, because DuckDB quotes the database file path in its messages
/// (<c>Cannot open file "D:\storage version backups\monitor.duckdb"</c>) and a folder named after a phrase would
/// otherwise decide the class. The phrases Lite looks for are DuckDB's own wording outside the quotes. Lower-cased.</para>
/// </summary>
internal static class DuckDbFailureText
{
    private static readonly Regex QuotedSpan = new("\"[^\"]*\"", RegexOptions.Compiled | RegexOptions.CultureInvariant);

    public static string Of(Exception ex)
    {
        var text = new StringBuilder();
        var pending = new Stack<Exception>();
        pending.Push(ex);
        while (pending.Count > 0)
        {
            var current = pending.Pop();
            text.Append(current.Message).Append('\n');
            if (current is AggregateException aggregate)
            {
                foreach (var inner in aggregate.InnerExceptions)
                {
                    pending.Push(inner);
                }
            }
            else if (current.InnerException != null)
            {
                pending.Push(current.InnerException);
            }
        }

        return QuotedSpan.Replace(text.ToString(), "\"\"").ToLowerInvariant();
    }
}
