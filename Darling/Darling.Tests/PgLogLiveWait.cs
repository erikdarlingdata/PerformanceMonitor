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
    public static async Task<string?> NewestFileAsync(NpgsqlConnection connection, string extension, CancellationToken ct)
    {
        await using var command = new NpgsqlCommand(
            "SELECT name FROM pg_catalog.pg_ls_logdir() WHERE name ~* @ext ORDER BY modification DESC, name DESC LIMIT 1", connection);
        command.Parameters.AddWithValue("ext", "\\." + extension + "$");
        return await command.ExecuteScalarAsync(ct) as string;
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

    public static async Task WaitForTextAsync(NpgsqlConnection connection, string extension, CancellationToken ct, params string[] markers)
    {
        var deadline = DateTime.UtcNow + Budget;
        while (true)
        {
            await using (var command = new NpgsqlCommand(
                "WITH f AS (SELECT name, size FROM pg_catalog.pg_ls_logdir() WHERE name ~* @ext "
                + "ORDER BY modification DESC, name DESC LIMIT 1), "
                + "b AS (SELECT pg_catalog.pg_read_binary_file(pg_catalog.current_setting('log_directory') || '/' || f.name, "
                + "greatest(f.size - " + TailBytes + ", 0), " + TailBytes + ") AS body FROM f) "
                + "SELECT coalesce(pg_catalog.bool_and(position(pg_catalog.convert_to(m, 'UTF8') IN b.body) > 0), false) "
                + "FROM b CROSS JOIN pg_catalog.unnest(@markers) AS m",
                connection))
            {
                command.Parameters.AddWithValue("ext", "\\." + extension + "$");
                command.Parameters.Add(new NpgsqlParameter("markers", NpgsqlDbType.Array | NpgsqlDbType.Text) { Value = markers });
                if ((bool)(await command.ExecuteScalarAsync(ct))!)
                {
                    return;
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
