/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using System.Data.Common;
using System.Globalization;
using System.Threading;
using System.Threading.Tasks;
using PerformanceMonitor.Collectors;

namespace PerformanceMonitor.Darling.Service;

/// <summary>
/// Gives the statement-statistics reads on a PostgreSQL target enough <c>work_mem</c> to keep their result in memory (#9062).
///
/// <para><c>pg_stat_statements(...)</c> and <c>aurora_stat_statements(...)</c> are functions in FROM. The executor stores
/// the whole function result in a tuplestore that <c>work_mem</c> caps, so at the default 4 MB a target holding about 9,600
/// entries spilled a temp file on every call (4.7 MB, about 5 GB a day on one instance). The hourly text fetch spilled the
/// same way, and its sort and windows spill too once the store fits.</para>
///
/// <para>The read is sent as two statements in ONE command: a <c>set_config('work_mem', ..., true)</c> first, the read after
/// it. Npgsql sends a multi-statement command as one extended-protocol batch with one Sync, which PostgreSQL runs as one
/// implicit transaction, so the local setting covers the read and ends with it. Nothing reaches the pooled connection, and no
/// BEGIN or COMMIT shows up in the target's statement statistics on the minute read. The hourly text fetch carries a positional parameter, which Npgsql does not split on, so it sets the value in a transaction of its own. The caller moves past the first result with
/// <c>NextResultAsync</c>.</para>
///
/// <para>The value is sized inside the same statement from the target's own <c>pg_stat_statements.max</c> (the entry count
/// the function can return), so it costs no extra round trip and needs no per-server state. Measured on PostgreSQL 18 with
/// about 4,900 entries: the plain read stopped spilling between 2.1 and 2.2 MB, so about 450 bytes an entry. The text read
/// (text on every row, a sort, two windows) needed about 3 KB an entry with 2 KB statements. The per-entry sizes below leave
/// headroom over both. The floor keeps a small <c>max</c> at the size the issue proved on a real target. The ceiling keeps a
/// huge <c>max</c> under work_mem's own limit (2047 MB, above which the setting is refused) and under what one backend
/// should hold. <c>work_mem</c> is a ceiling, not an allocation: a read that needs less takes less.</para>
/// </summary>
internal static class PgReadWorkMem
{
    /// <summary>Bytes an entry for the text fetch (query text, sort, windows). Measured about 3,000 with 2 KB statements.</summary>
    internal const int StatementTextBytesPerEntry = 4096;

    /// <summary>The least work_mem the read sets, in MB.</summary>
    internal const int FloorMegabytes = 16;

    /// <summary>The most work_mem the read sets, in MB.</summary>
    internal const int CeilingMegabytes = 512;

    /// <summary><c>pg_stat_statements.max</c>'s own default, used when the setting is not visible (an Aurora target that reads
    /// its own function without the module loaded).</summary>
    internal const int DefaultMaxEntries = 5000;

    /// <summary>The statement that sets the local work_mem for <paramref name="bytesPerEntry"/>, without its marker.</summary>
    internal static string SetSql(int bytesPerEntry) =>
        "SELECT pg_catalog.set_config('work_mem', (least("
        + CeilingMegabytes.ToString(CultureInfo.InvariantCulture)
        + ", greatest("
        + FloorMegabytes.ToString(CultureInfo.InvariantCulture)
        + ", pg_catalog.ceil(coalesce(nullif(pg_catalog.current_setting('pg_stat_statements.max', true), '')::numeric, "
        + DefaultMaxEntries.ToString(CultureInfo.InvariantCulture)
        + ") * "
        + bytesPerEntry.ToString(CultureInfo.InvariantCulture)
        + " / 1048576.0)))::integer)::text || 'MB', true)";

    /// <summary>
    /// <paramref name="markedReadSql"/> (already marked) behind the work_mem statement. The result has two result sets; the
    /// first is the setting and the caller moves past it with <c>NextResultAsync</c>.
    /// </summary>
    internal static string Wrap(string markedReadSql, int bytesPerEntry) =>
        PgOwnStatement.Mark(SetSql(bytesPerEntry)) + ";\n" + markedReadSql;

    /// <summary>
    /// Puts the work_mem statement in front of <paramref name="command"/>'s read when the definition asks for it
    /// (<paramref name="bytesPerEntry"/> is its <c>ReadWorkMemBytesPerEntry</c>) and the command is a PostgreSQL one. Returns
    /// whether it did, which is whether the caller moves past the first result with <see cref="SkipSettingResultAsync"/>.
    /// </summary>
    internal static bool ApplyTo(DbCommand command, int? bytesPerEntry)
    {
        if (bytesPerEntry is not { } bytes || command is not Npgsql.NpgsqlCommand)
        {
            return false;
        }

        command.CommandText = Wrap(command.CommandText, bytes);
        return true;
    }

    /// <summary>
    /// Moves a reader opened on a command <see cref="ApplyTo"/> changed past the setting's result to the read's. The read's own
    /// error (a missing extension, a timeout) arrives here, not at the open, so a failure closes the reader before it
    /// propagates.
    /// </summary>
    internal static async Task SkipSettingResultAsync(DbDataReader reader, CancellationToken cancellationToken)
    {
        try
        {
            await reader.NextResultAsync(cancellationToken);
        }
        catch
        {
            await reader.DisposeAsync();
            throw;
        }
    }
}
