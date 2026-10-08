using System;
using System.Collections.Generic;
using System.Globalization;
using System.Linq;
using System.Text.RegularExpressions;
using System.Threading;
using System.Threading.Tasks;
using Npgsql;

namespace Darling.Tests;

/// <summary>
/// The naming scheme of <see cref="ScratchPostgres"/> databases, and the start-of-run sweep that drops the ones a
/// killed run left behind (#4981).
///
/// <para><b>Why a sweep exists at all.</b> <c>ScratchPostgres.DisposeAsync</c> drops its own database, but a run
/// that is killed (a stopped debugger, a closed terminal, a machine restart) never reaches it. On a long-lived local
/// cluster those databases pile up, and each one that has the TimescaleDB extension holds one of the cluster's few
/// background-worker slots for its scheduler. Four hundred of them used every slot, and later tests failed with
/// "limit of 8 exceeded" and "out of background workers" with nothing wrong in the code under test.</para>
///
/// <para><b>Why the sweep is allowed to drop anything.</b> It is aimed at whatever <c>DARLING_TEST_PG</c> names, so
/// the rules that keep it from touching anything but its own debris are the whole design, and every one is pinned by
/// <see cref="ScratchDatabaseSweepTests"/>:
/// <list type="bullet">
/// <item>The name must match the factory's exact pattern, anchored at both ends: the prefix, a 14-digit UTC creation
/// stamp, and 12 lowercase hex characters. A near miss (another prefix, a different suffix length, uppercase hex, a
/// trailing extra) is never dropped. Databases minted before the stamp existed carry no age, so they match nothing
/// here and are left alone.</item>
/// <item>The creation stamp in the name must be older than <see cref="AbandonedAfter"/>. A run in flight, this
/// process's or a neighbour's, owns only young databases.</item>
/// <item>No client session may be connected to the database. Background workers (the TimescaleDB scheduler) do not
/// count: every database with the extension has one, and an abandoned database would otherwise never look idle.</item>
/// <item>The drop is a plain <c>DROP DATABASE</c>, never <c>WITH (FORCE)</c>, so a database that gained a session
/// between the check and the drop refuses to go instead of being pulled out from under a test.</item>
/// <item>Each drop is preceded by <see cref="ScratchPostgres.QuiesceTimescaleJobsAsync"/> (#5549): a database is never
/// dropped while one of its TimescaleDB job workers is still attached, the same rule every in-test drop follows.
/// The quiesce is best-effort and bounded, it never throws, and it opens one short client session of its own in the
/// database, so the drop retries briefly while that session finishes closing (<see cref="DropAttempts"/>).</item>
/// <item>The database the connection string itself names is never a candidate.</item>
/// </list></para>
/// </summary>
/* #1776 own-store: this class never touches a store's tables. It lists databases by name and drops the factory's
   own abandoned ones, so it cannot race the shared store. */
internal static class ScratchDatabaseSweep
{
    internal const string NamePrefix = "darling_scratch_";

    /// <summary>The creation stamp's layout: UTC, to the second.</summary>
    internal const string StampFormat = "yyyyMMddHHmmss";

    /// <summary>
    /// How old a scratch database must be before the sweep may drop it. No test keeps a database for anything near
    /// this long, so a database past it belongs to a run that is gone; the margin is what keeps a slow neighbour's
    /// databases safe.
    /// </summary>
    internal static readonly TimeSpan AbandonedAfter = TimeSpan.FromMinutes(30);

    /// <summary>
    /// The one pattern a droppable name matches. <c>\z</c>, not <c>$</c>: <c>$</c> also matches before a trailing
    /// newline, and <c>[0-9]</c>, not <c>\d</c>: <c>\d</c> accepts other scripts' digits.
    /// </summary>
    private static readonly Regex NamePattern = new(
        "^darling_scratch_(?<stamp>[0-9]{14})_(?<id>[0-9a-f]{12})\\z",
        RegexOptions.CultureInvariant | RegexOptions.ExplicitCapture);

    /// <summary>
    /// Databases with the factory's prefix that have no client session, cheapest filter first. This only narrows the
    /// list; <see cref="SelectAbandoned"/> is the exact match that decides.
    /// </summary>
    private const string IdleCandidatesSql = @"
SELECT d.datname
FROM pg_database AS d
WHERE d.datname LIKE 'darling\_scratch\_%'
  AND NOT d.datistemplate
  AND NOT EXISTS (
      SELECT 1 FROM pg_stat_activity AS a
      WHERE a.datid = d.oid AND a.backend_type = 'client backend')";

    /// <summary>
    /// How many times, and how far apart, the plain drop is tried while the database still reports "being accessed by
    /// other users" (SQLSTATE 55006). The quiesce just before it connected to the database and disconnected again, and the
    /// server's backend for that session can outlive the client's close by a few milliseconds. A session that is really
    /// there stays, so the last attempt's refusal is logged and the database is left, as before.
    /// </summary>
    internal const int DropAttempts = 30;

    /// <summary>The wait between two of the <see cref="DropAttempts"/>.</summary>
    internal static readonly TimeSpan DropRetryDelay = TimeSpan.FromMilliseconds(100);

    /// <summary>A fresh name: the prefix, the creation time to the second, then 12 hex characters.</summary>
    internal static string NewName(DateTime utcNow) =>
        NamePrefix
        + utcNow.ToUniversalTime().ToString(StampFormat, CultureInfo.InvariantCulture)
        + "_"
        + Guid.NewGuid().ToString("N")[..12];

    /// <summary>True when <paramref name="name"/> is exactly a name <see cref="NewName"/> can produce.</summary>
    internal static bool IsFactoryName(string? name) => TryParseCreatedUtc(name, out _);

    /// <summary>
    /// The creation time read out of a factory name; false for any other name, and for a stamp that is not a real
    /// date (a month of 13 is a near miss, not a database to drop).
    /// </summary>
    internal static bool TryParseCreatedUtc(string? name, out DateTime createdUtc)
    {
        createdUtc = default;
        if (name is null)
        {
            return false;
        }

        var match = NamePattern.Match(name);
        return match.Success
            && DateTime.TryParseExact(
                match.Groups["stamp"].Value,
                StampFormat,
                CultureInfo.InvariantCulture,
                DateTimeStyles.AssumeUniversal | DateTimeStyles.AdjustToUniversal,
                out createdUtc);
    }

    /// <summary>
    /// The pure decision: of <paramref name="names"/>, the factory-pattern ones whose creation stamp is at least
    /// <paramref name="olderThan"/> before <paramref name="utcNow"/>. A stamp in the future (a clock that ran ahead)
    /// is not old, so it stays.
    /// </summary>
    internal static IReadOnlyList<string> SelectAbandoned(IEnumerable<string> names, DateTime utcNow, TimeSpan olderThan) =>
        names
            .Where(name => TryParseCreatedUtc(name, out var createdUtc) && utcNow - createdUtc >= olderThan)
            .Distinct(StringComparer.Ordinal)
            .ToList();

    /// <summary>
    /// Drops the abandoned scratch databases on the cluster <paramref name="baseConnectionString"/> reaches and
    /// returns their names, logging each drop and each refusal.
    /// </summary>
    internal static async Task<IReadOnlyList<string>> SweepAsync(
        string baseConnectionString,
        DateTime utcNow,
        TimeSpan olderThan,
        Action<string> log,
        CancellationToken cancellationToken)
    {
        var ownDatabase = new NpgsqlConnectionStringBuilder(baseConnectionString).Database;
        var dropped = new List<string>();

        /* #5549: unpooled, like ScratchPostgres's own create and drop, so the backend that ran a DROP DATABASE ends
           with this connection instead of going back into the pool every live test draws from. */
        await using var admin = new NpgsqlConnection(ScratchPostgres.UnpooledAdminConnectionString(baseConnectionString));
        await admin.OpenAsync(cancellationToken);

        var idle = new List<string>();
        await using (var list = new NpgsqlCommand(IdleCandidatesSql, admin))
        await using (var reader = await list.ExecuteReaderAsync(cancellationToken))
        {
            while (await reader.ReadAsync(cancellationToken))
            {
                idle.Add(reader.GetString(0));
            }
        }

        foreach (var name in SelectAbandoned(idle, utcNow, olderThan))
        {
            if (string.Equals(name, ownDatabase, StringComparison.Ordinal))
            {
                continue;
            }

            /* #5549: no TimescaleDB job worker is left in the database the drop below removes. Best-effort: it never
               throws, so a database that cannot be opened, or one without the extension, goes straight to the drop; and
               it ends at once when nothing runs there, so a sweep over idle databases stays quick. */
            await ScratchPostgres.QuiesceTimescaleJobsAsync(baseConnectionString, name, cancellationToken);

            try
            {
                for (var attempt = 1; ; attempt++)
                {
                    try
                    {
                        /* The name matched the anchored pattern above, so it is hex and digits only: safe as a quoted
                           identifier. */
                        await using var drop = new NpgsqlCommand($"DROP DATABASE IF EXISTS \"{name}\"", admin);
                        await drop.ExecuteNonQueryAsync(cancellationToken);
                        break;
                    }
                    catch (PostgresException ex) when (ex.SqlState == PostgresErrorCodes.ObjectInUse && attempt < DropAttempts)
                    {
                        /* Most likely the quiesce's own session, still closing on the server. */
                        await Task.Delay(DropRetryDelay, cancellationToken);
                    }
                }

                dropped.Add(name);
                log($"Dropped abandoned scratch database {name}.");
            }
            catch (PostgresException ex)
            {
                /* Typically a session that connected after the check: not abandoned after all. Leave it. */
                log($"Left scratch database {name} in place: {ex.MessageText}");
            }
        }

        return dropped;
    }
}
