/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor Lite.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using System.Collections.Generic;
using System.Data;
using System.Globalization;
using System.IO;
using System.Linq;
using System.Numerics;
using System.Threading.Tasks;
using DuckDB.NET.Data;
using PerformanceMonitorLite.Database;

namespace PerformanceMonitorLite.Services;

/// <summary>
/// Service for reading collected data from DuckDB.
/// Partial class - individual data type readers are in separate files.
/// </summary>
public partial class LocalDataService
{
    private readonly DuckDbInitializer _duckDb;

    public LocalDataService(DuckDbInitializer duckDb)
    {
        _duckDb = duckDb;
    }

    /// <summary>
    /// Creates and opens a DuckDB connection wrapped in a read lock.
    /// The lock prevents CHECKPOINT and compaction from reorganizing the database file
    /// while this connection is reading from it.
    /// </summary>
    internal async Task<LockedConnection> OpenConnectionAsync()
    {
        var readLock = _duckDb.AcquireReadLock();
        try
        {
            var connection = _duckDb.CreateConnection();
            await connection.OpenAsync();
            return new LockedConnection(connection, readLock);
        }
        catch
        {
            readLock.Dispose();
            throw;
        }
    }

    /// <summary>
    /// The write-lock wait <see cref="OpenWriteConnectionAsync"/> gets in a host that declares none: what a
    /// WPF dispatcher can afford to spend before <see cref="GetDatabaseStateDeviationsAsync"/> skips a
    /// maintenance cycle rather than stall the window. The shipped app declares none, so this is the app's.
    /// </summary>
    internal static readonly TimeSpan DefaultWriteLockBudget = TimeSpan.FromSeconds(5);

    /// <summary>
    /// The runtime configuration property a host sets, in seconds, to state its own
    /// <see cref="WriteLockBudget"/>. Declared as a <c>RuntimeHostConfigurationOption</c> in the host's
    /// project file, which lands it in that host's <c>runtimeconfig.json</c>.
    /// </summary>
    internal const string WriteLockBudgetConfigKey = "PerformanceMonitorLite.WriteLockBudgetSeconds";

    /// <summary>
    /// The largest budget a host can state. A wait that outlives the process or the job containing it
    /// cannot produce <see cref="OpenWriteConnectionAsync"/>'s timeout at all — something else kills the
    /// run first — so beyond this a declared budget is indistinguishable from no timeout, and being short
    /// of forever is the whole reason there is a number here. It is also what keeps the resolver total:
    /// <c>NumberStyles.Float</c> admits exponents, and .NET parses the invariant <c>Infinity</c> symbol
    /// whatever the style, so <c>1e300</c> and <c>Infinity</c> both reach
    /// <see cref="TimeSpan.FromSeconds"/> as positive doubles that overflow it. Out of a static
    /// initializer that throw is a <c>TypeInitializationException</c> on the first store call in
    /// the process — worse than the timeout it would be replacing, and a third outcome besides "the wait
    /// succeeded" and "fail loudly".
    /// </summary>
    internal static readonly TimeSpan MaxWriteLockBudget = TimeSpan.FromHours(1);

    /// <summary>
    /// How long <see cref="OpenWriteConnectionAsync"/> waits for the write lock in THIS host.
    ///
    /// <para><b>The wait belongs to the host, because the thing it protects does.</b>
    /// <c>DuckDbInitializer.s_dbLock</c> is process-wide, so an acquisition here queues behind every other
    /// holder in the process however few rows it means to touch, and a dispatcher awaiting this call cannot
    /// afford an unbounded queue — which is the entire reason this one caller passes a timeout while the
    /// app's other write-lock callers pass none. A host with no dispatcher to protect is in the position of
    /// those other callers: waiting is the correct answer, and expiring is the wrong one.</para>
    ///
    /// <para>So the host states it in its own project file, where the value is fixed before <c>Main</c> and
    /// out of reach of anything that runs afterwards. That matters more than it looks: this number governs
    /// every caller in the process at once, so a settable static would let one component — or one test —
    /// put every concurrent neighbour on whatever fuse it picked, which is a smaller copy of the problem
    /// rather than a fix for it.</para>
    ///
    /// <para><c>Lite.Tests</c> is the host that declares one. Its classes hold their own DuckDB files but
    /// share this lock, several of its test methods take the exclusive lock deliberately and hold it for
    /// seconds while they assert on timing, and its acquisitions have no dispatcher behind them. Its budget
    /// is two minutes — orders of magnitude above the holds its own suite takes, and still short enough to
    /// fail loudly, with this method's own message, if a holder wedges rather than merely being slow.</para>
    /// </summary>
    internal static readonly TimeSpan WriteLockBudget =
        ResolveWriteLockBudget(AppContext.GetData(WriteLockBudgetConfigKey));

    /// <summary>
    /// The seconds a host declared, as a budget. Absent, unparseable, non-positive, above
    /// <see cref="MaxWriteLockBudget"/>, and small enough to round away all resolve
    /// <see cref="DefaultWriteLockBudget"/>: the failure worth guarding against is a host quietly losing
    /// its dispatcher protection, not a host quietly gaining a longer wait. TOTAL — every input returns a
    /// budget in <c>(TimeSpan.Zero, MaxWriteLockBudget]</c> and none throws, which matters because this
    /// runs in a static initializer, where an exception takes the whole type down rather than one call.
    ///
    /// <para>Takes the raw value rather than reading the key itself, so the parse can be exercised without
    /// a second host. MSBuild types it as a JSON number in <c>runtimeconfig.json</c> and the host hands it
    /// back as a string, which is why it is parsed rather than cast — and parsed against the invariant
    /// culture, because a project file is not written in the machine's locale.</para>
    ///
    /// <para>The upper bound is not <see cref="TimeSpan"/>'s representable range, which reaches roughly
    /// 29,000 years: a budget that large parses, converts and then makes a wedged lock hang for the life
    /// of the process, turning a loud failure into a silent one. The bound has to mean something for the
    /// refusal to be worth having.</para>
    /// </summary>
    internal static TimeSpan ResolveWriteLockBudget(object? configured)
    {
        if (configured is string declared &&
            double.TryParse(declared, NumberStyles.Float, CultureInfo.InvariantCulture, out var seconds) &&
            seconds > 0 &&
            seconds <= MaxWriteLockBudget.TotalSeconds)
        {
            /* The BUDGET is what gets checked, not the number that produced it. A positive double under
               the ceiling can still round to TimeSpan.Zero - double.Epsilon does - and a zero timeout is
               TryEnterWriteLock giving up without waiting, which is a budget in name only. */
            var budget = TimeSpan.FromSeconds(seconds);
            if (budget > TimeSpan.Zero)
            {
                return budget;
            }
        }

        return DefaultWriteLockBudget;
    }

    /// <summary>
    /// Creates and opens a DuckDB connection wrapped in an exclusive write lock, bounded by
    /// <see cref="WriteLockBudget"/> so the UI thread cannot freeze behind an in-flight archival.
    ///
    /// <para><b>This doc comment used to say "use for UPDATE/DELETE/INSERT operations that must not race
    /// with archival or compaction", and was read as the house rule for the whole app (#2463).</b> It is
    /// not, and the INSERT in that sentence is the part that was wrong: excluding archival is what the
    /// READ lock already does, since a held read lock blocks <c>EnterWriteLock</c>. What this method
    /// additionally buys is exclusion of OTHER WRITERS, which an UPDATE or a DELETE needs — DuckDB's
    /// optimistic concurrency fails the loser of a write-write collision rather than queueing it — and
    /// which an append of new rows does not. The rule, with the measurements behind it, is on
    /// <c>DuckDbInitializer.s_dbLock</c>; the fourteen callers here are UPDATE, DELETE and #2208's
    /// multi-statement maintenance block, and all of them sit on the right side of it.</para>
    ///
    /// <para>The timeout is this method's own contribution and is not part of the lock rule: every other
    /// write-lock caller in the app waits indefinitely, which they can afford and the UI thread cannot.
    /// See <see cref="LocalDataService.GetDatabaseStateDeviationsAsync"/> for what a caller does when it
    /// expires.</para>
    /// </summary>
    internal async Task<LockedConnection> OpenWriteConnectionAsync()
    {
        var writeLock = _duckDb.AcquireWriteLock(timeout: WriteLockBudget);
        try
        {
            var connection = _duckDb.CreateConnection();
            await connection.OpenAsync();
            return new LockedConnection(connection, writeLock);
        }
        catch
        {
            writeLock.Dispose();
            throw;
        }
    }

    /// <summary>
    /// Safely converts a DuckDB value to double, handling BigInteger from SUM aggregations.
    /// </summary>
    private static double ToDouble(object value)
    {
        if (value is BigInteger bi)
            return (double)bi;
        return Convert.ToDouble(value);
    }

    /// <summary>
    /// Safely converts a DuckDB value to long, handling BigInteger from SUM/COUNT aggregations.
    /// </summary>
    private static long ToInt64(object value)
    {
        if (value is BigInteger bi)
            return (long)bi;
        return Convert.ToInt64(value);
    }

    /// <summary>
    /// The UTC offset of the desktop's currently selected server tab, for a read whose server-local
    /// <c>fromDate</c>/<c>toDate</c> can only have come from that tab's own toolbar pickers.
    ///
    /// <para>Named rather than spelled <c>ServerTimeHelper.UtcOffsetMinutes</c> inline because it is an
    /// answer, not a value: it says "this window belongs to whichever server the desktop has selected".
    /// That is true only for a read the selected tab drives. It is the wrong answer for a read that can
    /// run for a server other than the selected one, and such a read has to take the offset of the server
    /// it names — see <see cref="GetAlertCountsAsync"/>, which does.</para>
    ///
    /// <para>The offset is applied twice per window and the two applications have to name the same
    /// server or they stop cancelling: <c>ServerTab.GetCurrentWindow</c> converts the pickers from the
    /// display mode into server time, and the custom-range branch below converts back out to UTC. In
    /// <c>TimeDisplayMode.UTC</c> and <c>LocalTime</c> the pair cancels; in <c>ServerTime</c>, the
    /// default, only the branch below applies anything. A caller that changes one side's offset source
    /// without the other breaks the two modes that cancel, so the two are paired per path.</para>
    /// </summary>
    private static int SelectedServerTabUtcOffsetMinutes => ServerTimeHelper.UtcOffsetMinutes;

    /// <summary>
    /// Gets the time range for queries based on hoursBack or explicit date range.
    /// Returns UTC time for collection_time queries (most tables store collection_time in UTC).
    /// </summary>
    /// <param name="utcOffsetMinutes">
    /// The UTC offset of the server whose rows this window will select — the same server as the
    /// <c>server_id</c> in the predicate beside it. REQUIRED rather than defaulted: an offset and a
    /// server_id are two halves of one question, and taking the offset from ambient state is how they came
    /// to name two different servers. A caller with no server-specific offset to give has to say so at the
    /// call site instead of inheriting one silently. Ignored unless <paramref name="fromDate"/> and
    /// <paramref name="toDate"/> are both supplied, since only that branch converts.
    /// </param>
    private static (DateTime startTime, DateTime endTime) GetTimeRange(int hoursBack, DateTime? fromDate, DateTime? toDate, DateTime? asOfUtc, int utcOffsetMinutes)
    {
        if (fromDate.HasValue && toDate.HasValue)
        {
            /* Custom date range - convert from server time back to UTC for storage lookup */
            var startUtc = fromDate.Value.AddMinutes(-utcOffsetMinutes);
            var endUtc = toDate.Value.AddMinutes(-utcOffsetMinutes);
            return (startUtc, endUtc);
        }

        /*
            #2495: asOfUtc moves the END of the hoursBack window off "now" so a caller can ask about a
            past incident. It is deliberately NOT expressed as fromDate/toDate -- those are SERVER-LOCAL
            (converted back to UTC just above), while the MCP anchor is UTC, and routing a UTC instant
            through that branch would silently shift the window by the monitored server's offset.
        */
        var anchor = asOfUtc ?? DateTime.UtcNow;

        /* Use UTC directly since collection_time is stored in UTC */
        return (anchor.AddHours(-hoursBack), anchor);
    }

    /// <summary>
    /// Gets the time range in server local time (for tables like cpu_utilization_stats.sample_time).
    /// </summary>
    /// <param name="utcOffsetMinutes">
    /// The UTC offset of the server whose rows this window will select — the same server as the
    /// <c>server_id</c> in the predicate beside it. REQUIRED rather than defaulted: an offset and a
    /// server_id are two halves of one question, and taking the offset from ambient state is how they came
    /// to name two different servers. A caller with no server-specific offset to give has to say so at the
    /// call site instead of inheriting one silently.
    /// </param>
    private static (DateTime startTime, DateTime endTime) GetTimeRangeServerLocal(int hoursBack, DateTime? fromDate, DateTime? toDate, DateTime? asOfUtc, int utcOffsetMinutes)
    {
        /* The anchor arrives in UTC (see GetTimeRange) and is carried into server-local here, so both
           families answer the same instant even though they window on differently-based columns. */
        var serverNow = (asOfUtc ?? DateTime.UtcNow).AddMinutes(utcOffsetMinutes);

        if (fromDate.HasValue && toDate.HasValue)
        {
            /* fromDate/toDate are already in server time from the caller */
            return (fromDate.Value, toDate.Value);
        }

        return (serverNow.AddHours(-hoursBack), serverNow);
    }

    /// <summary>
    /// Starts query timing for performance logging. Use with 'using' statement.
    /// Only logs queries that exceed the slow query threshold (default 500ms).
    /// </summary>
    private static Helpers.QueryExecutionContext TimeQuery(string context, string sql)
    {
        return Helpers.QueryLogger.StartQuery(context, sql, source: "DuckDB");
    }

}
