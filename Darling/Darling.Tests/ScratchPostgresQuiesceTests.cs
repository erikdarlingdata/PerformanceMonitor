using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.IO;
using System.Linq;
using System.Text.RegularExpressions;
using System.Threading.Tasks;
using Npgsql;
using PerformanceMonitor.Darling.Storage;
using Xunit;

namespace Darling.Tests;

/* #1776 own-store: this class mints its own scratch databases through ScratchPostgres and never touches another test's
   rows, so it is deliberately NOT [Collection("live-postgres")]. */

/// <summary>
/// PR #5480: <see cref="ScratchPostgres.QuiesceTimescaleJobsAsync"/> runs before a scratch database is dropped
/// WITH (FORCE), so no TimescaleDB policy worker is mid-run in the database being dropped (CI saw a worker die with
/// 0xC0000005 twice, both with such a drop in flight). It is best-effort: it never throws and never blocks the drop.
/// </summary>
[Collection("timing")]
public sealed class ScratchPostgresQuiesceLiveTests
{
    [Fact]
    public async Task AfterTheHelper_EveryJobIsUnscheduled_AndNoJobWorkerIsLeftInTheDatabase()
    {
        var baseConnectionString = Environment.GetEnvironmentVariable("DARLING_TEST_PG");
        Assert.SkipWhen(string.IsNullOrEmpty(baseConnectionString),
            "Set DARLING_TEST_PG to a Postgres connection string to run the live scratch-quiesce test (it mints its own scratch database).");

        var ct = TestContext.Current.CancellationToken;
        await using var scratch = await ScratchPostgres.CreateAsync(baseConnectionString!, ct);
        using var connection = new NpgsqlConnection(scratch.ConnectionString);
        await connection.OpenAsync(ct);
        await PgMigrations.MigrateAsync(connection, ct);
        Assert.SkipUnless(await LiveTimescaleProbe.TryEnableAsync(scratch.ConnectionString, ct),
            "TimescaleDB is not available on this PostgreSQL, so there are no policy jobs to quiesce.");

        /* The product's policies (retention, compression, continuous-aggregate refresh), installed the way the
           product installs them: they are scheduled on creation, which is what makes workers run when a test ends. */
        await TimescaleSupport.ConvertToHypertablesAsync(connection, null, ct);

        var scheduledBefore = await ScalarAsync(connection, "SELECT count(*) FROM timescaledb_information.jobs WHERE scheduled", ct);
        Assert.True(scheduledBefore > 0, "the product policies installed no scheduled job, so this test would prove nothing.");

        /* Start some workers right now, the state a test's dispose meets. */
        await using (var kick = new NpgsqlCommand(
            "SELECT alter_job(job_id, next_start => now()) FROM timescaledb_information.jobs WHERE scheduled", connection))
        {
            await kick.ExecuteNonQueryAsync(ct);
        }

        await ScratchPostgres.QuiesceTimescaleJobsAsync(scratch.UnpinnedConnectionString, scratch.DatabaseName);

        Assert.Equal(0L, await ScalarAsync(connection, "SELECT count(*) FROM timescaledb_information.jobs WHERE scheduled", ct));

        await using var admin = new NpgsqlConnection(new NpgsqlConnectionStringBuilder(scratch.UnpinnedConnectionString) { Database = "postgres" }.ConnectionString);
        await admin.OpenAsync(ct);
        Assert.Equal(0L, await ScratchPostgres.JobWorkerCountAsync(admin, scratch.DatabaseName, ct));
    }

    [Fact]
    public async Task ADatabaseWithoutTheExtension_ReturnsAtOnce_AndDoesNotThrow()
    {
        var baseConnectionString = Environment.GetEnvironmentVariable("DARLING_TEST_PG");
        Assert.SkipWhen(string.IsNullOrEmpty(baseConnectionString),
            "Set DARLING_TEST_PG to a Postgres connection string to run the live scratch-quiesce test (it mints its own scratch database).");

        await using var scratch = await ScratchPostgres.CreateAsync(baseConnectionString!, TestContext.Current.CancellationToken);

        var clock = Stopwatch.StartNew();
        await ScratchPostgres.QuiesceTimescaleJobsAsync(scratch.UnpinnedConnectionString, scratch.DatabaseName);

        Assert.True(clock.Elapsed < ScratchPostgres.QuiesceCap, $"took {clock.Elapsed}, past the cap {ScratchPostgres.QuiesceCap}.");
    }

    [Fact]
    public async Task ADatabaseThatIsAlreadyGone_ReturnsWithinTheCap_AndDoesNotThrow()
    {
        var baseConnectionString = Environment.GetEnvironmentVariable("DARLING_TEST_PG");
        Assert.SkipWhen(string.IsNullOrEmpty(baseConnectionString),
            "Set DARLING_TEST_PG to a Postgres connection string to run the live scratch-quiesce test (it mints its own scratch database).");

        var scratch = await ScratchPostgres.CreateAsync(baseConnectionString!, TestContext.Current.CancellationToken);
        var unpinned = scratch.UnpinnedConnectionString;
        var name = scratch.DatabaseName;
        await scratch.DisposeAsync();

        var clock = Stopwatch.StartNew();
        await ScratchPostgres.QuiesceTimescaleJobsAsync(unpinned, name);

        Assert.True(clock.Elapsed < ScratchPostgres.QuiesceCap, $"took {clock.Elapsed}, past the cap {ScratchPostgres.QuiesceCap}.");
    }

    /// <summary>
    /// #5549: the backend that ran a scratch database's CREATE DATABASE and DROP DATABASE must end with its
    /// connection. The admin string is the caller's <c>DARLING_TEST_PG</c>, so a pooled admin connection went back
    /// into the pool every live test draws from, and on CI the next test's <c>CALL run_job</c> ran on the backend
    /// that had just run the drop. The base string here carries its own application name, so its pool and its
    /// sessions belong to this test alone: once the scratch database is gone, none of them may still be open.
    /// </summary>
    [Fact]
    public async Task TheCreateAndTheDrop_LeaveNoBackendOpenInTheCallersPool()
    {
        var baseConnectionString = Environment.GetEnvironmentVariable("DARLING_TEST_PG");
        Assert.SkipWhen(string.IsNullOrEmpty(baseConnectionString),
            "Set DARLING_TEST_PG to a Postgres connection string to run the live scratch-pool test (it mints its own scratch database).");

        var ct = TestContext.Current.CancellationToken;
        var tag = "w5549-" + Guid.NewGuid().ToString("N")[..12];
        var tagged = new NpgsqlConnectionStringBuilder(baseConnectionString) { ApplicationName = tag }.ConnectionString;

        var scratch = await ScratchPostgres.CreateAsync(tagged, ct);
        await scratch.DisposeAsync();

        await using var probe = new NpgsqlConnection(baseConnectionString);
        await probe.OpenAsync(ct);
        var left = -1L;
        var lastQuery = "";
        /* An unpooled connection's backend leaves pg_stat_activity a moment after the close, not at it. A pooled one
           stays for the pool's idle lifetime (300 seconds by default), far past this wait. */
        for (var poll = 0; poll < 25; poll++)
        {
            await using var command = new NpgsqlCommand(
                "SELECT count(*), coalesce(max(query), '') FROM pg_stat_activity WHERE application_name = $1", probe);
            command.Parameters.AddWithValue(tag);
            await using var reader = await command.ExecuteReaderAsync(ct);
            await reader.ReadAsync(ct);
            left = reader.GetInt64(0);
            lastQuery = reader.GetString(1);
            if (left == 0)
            {
                break;
            }

            await reader.DisposeAsync();
            await Task.Delay(TimeSpan.FromMilliseconds(200), ct);
        }

        Assert.True(left == 0,
            $"{left} backend(s) from the caller's pool are still open after the scratch database's create and drop; the last one ran: {lastQuery}");
    }

    private static async Task<long> ScalarAsync(NpgsqlConnection connection, string sql, System.Threading.CancellationToken ct)
    {
        await using var command = new NpgsqlCommand(sql, connection);
        return (long)(await command.ExecuteScalarAsync(ct))!;
    }
}

/// <summary>
/// PR #5480: every <c>DROP DATABASE ... WITH (FORCE)</c> statement in Darling.Tests is preceded, in the same method, by
/// <c>QuiesceTimescaleJobsAsync</c>, so a new drop site cannot reintroduce the kill of a running TimescaleDB job worker.
/// A statement is a string literal holding both <c>DROP DATABASE</c> and <c>WITH (FORCE)</c>; comments are blanked first by <see cref="CSharpSourceWalker"/>.
/// </summary>
[Trait("Stage", "Guard")]
public sealed class ScratchPostgresQuiesceCensusTests
{
    private const string Helper = "QuiesceTimescaleJobsAsync";

    /// <summary>How many lines above a drop the helper call may sit. The farthest real site has it 3 lines above.</summary>
    private const int Window = 12;

    private static readonly Regex DropStatement = new("\".*DROP DATABASE.*WITH \\(FORCE\\)", RegexOptions.Compiled);

    /// <summary>
    /// Blanks comments (to spaces, keeping every newline so line numbers hold) and keeps string literals, quotes
    /// included, because the drop statement is itself a literal. Built on <see cref="CSharpSourceWalker"/>, so a
    /// line that merely starts with <c>*</c> inside a literal is still seen, and a comment is never.
    /// </summary>
    internal static string BlankComments(string text)
    {
        var keep = CSharpSourceWalker.CodeMask(text);
        foreach (var (start, body) in CSharpSourceWalker.StringLiteralBodies(text))
        {
            for (var i = start; i < start + body.Length; i++)
            {
                keep[i] = true;
            }

            if (start > 0 && text[start - 1] == '"')
            {
                keep[start - 1] = true;
            }

            if (start + body.Length < text.Length && text[start + body.Length] == '"')
            {
                keep[start + body.Length] = true;
            }
        }

        var sb = new System.Text.StringBuilder(text.Length);
        for (var i = 0; i < text.Length; i++)
        {
            sb.Append(keep[i] ? text[i] : text[i] == '\n' ? '\n' : ' ');
        }

        return sb.ToString();
    }

    private static (int Sites, List<string> Unguarded) Scan(IEnumerable<(string Name, string Source)> files)
    {
        var sites = 0;
        var unguarded = new List<string>();
        foreach (var (name, source) in files)
        {
            var lines = BlankComments(source).Split('\n');
            for (var i = 0; i < lines.Length; i++)
            {
                if (!DropStatement.IsMatch(lines[i]))
                {
                    continue;
                }

                sites++;
                var from = Math.Max(0, i - Window);
                if (!lines.Skip(from).Take(i - from).Any(line => line.Contains(Helper, StringComparison.Ordinal)))
                {
                    unguarded.Add($"{name}:{i + 1}");
                }
            }
        }

        return (sites, unguarded);
    }

    [Fact]
    public void EveryForceDropInTheTestProject_IsPrecededByTheQuiesceHelper()
    {
        var directory = FindTestProjectDirectory();
        Assert.True(directory is not null, "could not find Darling/Darling.Tests from the test binary.");

        var files = Directory.EnumerateFiles(directory!, "*.cs", SearchOption.AllDirectories)
            .Select(path => (Name: Path.GetRelativePath(directory!, path).Replace('\\', '/'), Path: path))
            .Where(found => !found.Name.Split('/').Any(segment => segment is "bin" or "obj"))
            .Where(found => found.Name != "ScratchPostgresQuiesceTests.cs")
            .Select(found => (found.Name, File.ReadAllText(found.Path)))
            .ToList();

        var (sites, unguarded) = Scan(files);

        Assert.True(sites >= 7, $"the scan found {sites} WITH (FORCE) drop site(s); the project has at least 7, so the match found nothing.");
        Assert.True(unguarded.Count == 0,
            "These WITH (FORCE) drops are not preceded by ScratchPostgres.QuiesceTimescaleJobsAsync (PR #5480): "
            + string.Join(", ", unguarded));
    }

    [Fact]
    public void ADropWithoutTheHelper_IsReported_AndOneWithItIsNot()
    {
        const string guarded =
            "await ScratchPostgres.QuiesceTimescaleJobsAsync(cs, name);\n"
            + "await using var drop = new NpgsqlCommand($\"DROP DATABASE IF EXISTS {name} WITH (FORCE)\", admin);\n";
        const string bare = "await using var drop = new NpgsqlCommand($\"DROP DATABASE IF EXISTS {name} WITH (FORCE)\", admin);\n";
        const string commentOnly = "/// the drop is DROP DATABASE ... WITH (FORCE)\n// \"DROP DATABASE x WITH (FORCE)\"\n";

        var clean = Scan([("G.cs", guarded)]);
        Assert.Equal(1, clean.Sites);
        Assert.Empty(clean.Unguarded);
        var offending = Scan([("B.cs", bare)]);
        Assert.Equal(1, offending.Sites);
        Assert.Equal(["B.cs:1"], offending.Unguarded);
        Assert.Equal(0, Scan([("C.cs", commentOnly)]).Sites);
    }

    /// <summary>
    /// The CI side of PR #5480: both workflows warn when the PostgreSQL log holds "terminated by exception", on every
    /// outcome (<c>if: always()</c>) and without ever failing the job, ahead of the step that stops the server.
    /// </summary>
    [Theory]
    [InlineData("build.yml")]
    [InlineData("nightly.yml")]
    public void TheWorkflow_WarnsOnAWorkerCrashInThePostgresLog_AlwaysAndOnlyAsAWarning(string workflow)
    {
        var path = Path.Combine(AppContext.BaseDirectory, "Fixtures", workflow);
        Assert.True(File.Exists(path), $"{workflow} was not copied beside the test binary (Darling.Tests.csproj links it into Fixtures\\).");
        var text = File.ReadAllText(path);

        var start = text.IndexOf("- name: Warn if a PostgreSQL worker crashed", StringComparison.Ordinal);
        Assert.True(start >= 0, $"{workflow} has no 'Warn if a PostgreSQL worker crashed' step.");
        var stop = text.IndexOf("- name: Stop PostgreSQL", start, StringComparison.Ordinal);
        Assert.True(stop > start, $"{workflow}: the warning step must come before 'Stop PostgreSQL'.");

        var step = text[start..stop];
        Assert.Contains("if: always()", step, StringComparison.Ordinal);
        Assert.Contains("darling-pg.log", step, StringComparison.Ordinal);
        Assert.Contains("terminated by exception", step, StringComparison.Ordinal);
        Assert.Contains("::warning", step, StringComparison.Ordinal);
        Assert.DoesNotContain("::error", step, StringComparison.Ordinal);
        Assert.Contains("exit 0", step, StringComparison.Ordinal);
    }

    private static string? FindTestProjectDirectory()
    {
        var directory = new DirectoryInfo(AppContext.BaseDirectory);
        for (var i = 0; i < 10 && directory is not null; i++)
        {
            if (File.Exists(Path.Combine(directory.FullName, "PerformanceMonitor.sln")))
            {
                var source = Path.Combine(directory.FullName, "Darling", "Darling.Tests");
                return Directory.Exists(source) ? source : null;
            }

            directory = directory.Parent;
        }

        return null;
    }
}
