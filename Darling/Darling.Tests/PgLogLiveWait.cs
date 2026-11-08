using System;
using System.Threading;
using System.Threading.Tasks;
using Npgsql;
using NpgsqlTypes;
using Xunit;

namespace Darling.Tests;

/// <summary>
/// Waits, for the csvlog and jsonlog live tests, until a target's newest log file holds the text the test just
/// made the server write (#7142). The syslogger writes a record to the file a moment after the backend sent it,
/// so a test that reads the log right after its statement can run before its own record is on disk. The old
/// wait compared the file size taken AFTER the statement had run, so it could never see the test's own write
/// when that had already landed, returned early on any unrelated growth (the csvlog cluster is shared by four
/// live classes), and on its deadline returned without a word. This one looks for the text itself and fails
/// with a message when it never shows up.
/// </summary>
internal static class PgLogLiveWait
{
    /// <summary>The window read per poll, the same 4 MB tail the collectors read (<c>PgServerLogTail.TailBytes</c>).</summary>
    private const int TailBytes = 4 * 1024 * 1024;

    private static readonly TimeSpan Budget = TimeSpan.FromSeconds(30);

    /// <summary>
    /// Polls until the tail of the newest log file ending in <paramref name="extension"/> (<c>csv</c> or
    /// <c>json</c>) holds every one of <paramref name="markers"/>, up to 30 seconds, then fails the test naming
    /// the markers. Each marker is plain text with no quote or newline, so the format's own escaping leaves it intact.
    /// </summary>
    /// <summary>
    /// The newest log file ending in <paramref name="extension"/>, by the rule the wait and the collectors' tail share
    /// (<c>modification DESC, name DESC</c>), or null when the log directory holds none.
    /// </summary>
    public static async Task<string?> NewestFileAsync(NpgsqlConnection connection, string extension, CancellationToken ct) =>
        (await NewestFileAndSizeAsync(connection, extension, ct)).Name;

    /// <summary>The newest file by the same rule, with its size, so a wait can look only at what the file gains after a write.</summary>
    private static async Task<(string? Name, long Size)> NewestFileAndSizeAsync(NpgsqlConnection connection, string extension, CancellationToken ct)
    {
        await using var command = new NpgsqlCommand(
            "SELECT name, size FROM pg_catalog.pg_ls_logdir() WHERE name ~* @ext ORDER BY modification DESC, name DESC LIMIT 1", connection);
        command.Parameters.AddWithValue("ext", "\\." + extension + "$");
        await using var reader = await command.ExecuteReaderAsync(ct);
        return await reader.ReadAsync(ct) ? (reader.GetString(0), reader.GetInt64(1)) : (null, 0L);
    }

    /// <summary>
    /// Rotates the server log through <c>pg_rotate_logfile()</c> and returns once the newest file ending in
    /// <paramref name="extension"/> is a new one (#8205). The syslogger rotates on its next pass, not inside the call, so a
    /// read right after the call could still see the outgoing file. Fails after 10 seconds when no new file appears.
    /// </summary>
    public static async Task RotateAsync(NpgsqlConnection connection, string extension, CancellationToken ct)
    {
        var before = await NewestFileAsync(connection, extension, ct);
        await using (var rotate = new NpgsqlCommand("SELECT pg_catalog.pg_rotate_logfile()", connection))
        {
            await rotate.ExecuteNonQueryAsync(ct);
        }

        var deadline = DateTime.UtcNow + TimeSpan.FromSeconds(10);
        while (await NewestFileAsync(connection, extension, ct) == before)
        {
            if (DateTime.UtcNow >= deadline)
            {
                Assert.Fail("pg_rotate_logfile() did not produce a new ." + extension + " log file within 10 seconds (newest is still " + before + ").");
            }

            await Task.Delay(100, ct);
        }
    }

    public static async Task WaitForTextAsync(NpgsqlConnection connection, string extension, CancellationToken ct, params string[] markers) =>
        await WaitForTextCoreAsync(connection, extension, null, 0, ct, markers);

    /* The most times a record is written again because the log rotated after the write. */
    private const int RotationRetries = 2;

    /// <summary>
    /// Runs <paramref name="write"/>, waits for <paramref name="marker"/> to land, then runs <paramref name="read"/>, which
    /// returns true when the collector's read held the record (#8205). The collectors tail the newest log file, and the
    /// server rotates its log at midnight and at <c>log_rotation_size</c>, so a record written a moment before a rotation
    /// sits in the file before the one the read tails. When the newest file is not the one the record went into and the
    /// read did not hold it, the record is written again, up to <see cref="RotationRetries"/> more times. A read that misses
    /// the record with no rotation after the write fails here, naming the file.
    /// </summary>
    public static Task ReadOwnRecordAcrossRotationAsync(
        NpgsqlConnection connection, string extension, string marker, Func<Task> write, Func<Task<bool>> read, CancellationToken ct) =>
        ReadOwnRecordAcrossRotationAsync(connection, extension, async () => { await write(); return new[] { marker }; }, read, ct);

    /// <summary>
    /// The same, for a write whose markers are known only once it has run (a deadlock's victim pid, say): <paramref name="write"/>
    /// returns the markers to wait for. The wait looks only at what the newest file gained after the write, so an identical
    /// record an earlier run left on a reused target does not stand in for this run's own.
    /// </summary>
    public static async Task ReadOwnRecordAcrossRotationAsync(
        NpgsqlConnection connection, string extension, Func<Task<string[]>> write, Func<Task<bool>> read, CancellationToken ct)
    {
        for (var attempt = 0; ; attempt++)
        {
            var (fileAtWrite, sizeAtWrite) = await NewestFileAndSizeAsync(connection, extension, ct);
            var markers = await write();
            var landed = await WaitForTextCoreAsync(connection, extension, fileAtWrite, sizeAtWrite, ct, markers);
            if (landed && await read())
            {
                return;
            }

            var newest = await NewestFileAsync(connection, extension, ct);
            Assert.True(newest != fileAtWrite,
                "the collector's read of " + newest + " did not hold " + string.Join(" and ", markers) + ", and the log did not rotate after the write.");
            Assert.True(attempt < RotationRetries,
                "the log rotated after each of " + (RotationRetries + 1) + " writes of " + string.Join(" and ", markers) + ".");
        }
    }

    /// <summary>
    /// The wait itself. With <paramref name="fileAtWrite"/> given, it returns false instead of failing once the newest file
    /// is no longer that one: the log rotated after the write, and the record is in the file before. It reads the file from
    /// <paramref name="sizeAtWrite"/> on (within the same 4 MB tail), so only what landed after the write counts.
    /// </summary>
    private static async Task<bool> WaitForTextCoreAsync(
        NpgsqlConnection connection, string extension, string? fileAtWrite, long sizeAtWrite, CancellationToken ct, params string[] markers)
    {
        var deadline = DateTime.UtcNow + Budget;
        while (true)
        {
            await using (var command = new NpgsqlCommand(
                "WITH f AS (SELECT name, size FROM pg_catalog.pg_ls_logdir() WHERE name ~* @ext "
                + "ORDER BY modification DESC, name DESC LIMIT 1), "
                + "b AS (SELECT pg_catalog.pg_read_binary_file(pg_catalog.current_setting('log_directory') || '/' || f.name, "
                + "least(greatest(f.size - " + TailBytes + ", @from), f.size), " + TailBytes + ") AS body FROM f) "
                + "SELECT (SELECT name FROM f), coalesce(pg_catalog.bool_and(position(pg_catalog.convert_to(m, 'UTF8') IN b.body) > 0), false) "
                + "FROM b CROSS JOIN pg_catalog.unnest(@markers) AS m",
                connection))
            {
                command.Parameters.AddWithValue("ext", "\\." + extension + "$");
                command.Parameters.Add(new NpgsqlParameter("from", NpgsqlDbType.Bigint) { Value = sizeAtWrite });
                command.Parameters.Add(new NpgsqlParameter("markers", NpgsqlDbType.Array | NpgsqlDbType.Text) { Value = markers });
                await using var reader = await command.ExecuteReaderAsync(ct);
                Assert.True(await reader.ReadAsync(ct));
                if (reader.GetBoolean(1))
                {
                    return true;
                }

                if (fileAtWrite is not null && !reader.IsDBNull(0) && reader.GetString(0) != fileAtWrite)
                {
                    return false;
                }
            }

            if (DateTime.UtcNow >= deadline)
            {
                Assert.Fail("the newest ." + extension + " log never held " + string.Join(" and ", markers)
                    + " within " + Budget.TotalSeconds + " seconds, so the test's own record was not readable.");
            }

            await Task.Delay(200, ct);
        }
    }
}
