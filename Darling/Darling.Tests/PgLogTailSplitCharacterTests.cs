// #4735 item 1: a text read of the log tail that starts inside a multi-byte character is refused by
// PostgreSQL (22021) before this process sees a byte. The read is retried from a later start, and the
// error the operator finally reads no longer blames a planted byte alone.
using System;
using System.Linq;
using Npgsql;
using PerformanceMonitor.Collectors;
using PerformanceMonitor.Darling.Service;
using Xunit;

namespace Darling.Tests;

public sealed class PgLogTailSplitCharacterTests
{
    /* Its storage name is never cached by any test, so the sentence takes its UTF8 branch whatever the statics
       collection is doing in parallel. */
    private static readonly ServerRuntime FaultRuntime =
        PgReadBinaryFileCapabilityTests.Runtime("split-character-tests", connectedDatabase: "appdb");

    private static CollectorContext Context(int shift = 0) => new()
    {
        ServerId = 1,
        ServerName = "target-a",
        CollectionTime = new DateTime(2026, 9, 29, 1, 0, 0, DateTimeKind.Unspecified),
        Deltas = new CollectorDeltaCalculator(),
        LogHashKey = TestLogHashKeys.Fixed,
        Target = new CollectorTargetInfo { Engine = CollectorTargetEngine.PostgreSql },
        PgLogReadShiftBytes = shift,
    };

    /// <summary>The three text routes read with pg_read_file, so each takes the shift; the binary twins read bytea,
    /// which carries no encoding check, and stay exactly as they were.</summary>
    [Theory]
    [InlineData(nameof(PgServerLogTail.TailCteSql), true)]
    [InlineData(nameof(PgServerLogTail.TailCsvCteSql), true)]
    [InlineData(nameof(PgServerLogTail.TailJsonCteSql), true)]
    [InlineData(nameof(PgServerLogTail.TailCteBinarySql), false)]
    [InlineData(nameof(PgServerLogTail.TailCsvCteBinarySql), false)]
    [InlineData(nameof(PgServerLogTail.TailJsonCteBinarySql), false)]
    public void OnlyTheTextRoutesShiftTheirReadStart(string constant, bool shifts)
    {
        var sql = constant switch
        {
            nameof(PgServerLogTail.TailCteSql) => PgServerLogTail.TailCteSql,
            nameof(PgServerLogTail.TailCsvCteSql) => PgServerLogTail.TailCsvCteSql,
            nameof(PgServerLogTail.TailJsonCteSql) => PgServerLogTail.TailJsonCteSql,
            nameof(PgServerLogTail.TailCteBinarySql) => PgServerLogTail.TailCteBinarySql,
            nameof(PgServerLogTail.TailCsvCteBinarySql) => PgServerLogTail.TailCsvCteBinarySql,
            _ => PgServerLogTail.TailJsonCteBinarySql,
        };

        Assert.Equal(shifts, sql.Contains("@log_read_shift", StringComparison.Ordinal));

        if (shifts)
        {
            /* The shifted start feeds BOTH the read and the resume row, so the next offset counts from where the
               retry actually started: the read call takes the shifted column, and the tail exposes it as read_from. */
            var readCall = sql[sql.IndexOf("pg_catalog.pg_read_file(", StringComparison.Ordinal)..];
            readCall = readCall[..readCall.IndexOf(") AS body", StringComparison.Ordinal)];
            Assert.Contains("n.read_from + sh.shift", readCall, StringComparison.Ordinal);
            Assert.Contains("n.read_from + sh.shift AS read_from", sql, StringComparison.Ordinal);
            Assert.Contains("CAST(@log_read_shift AS bigint) AS shift", sql, StringComparison.Ordinal);
        }
    }

    [Fact]
    public void WithResume_BindsTheShiftOnlyForTextThatUsesIt()
    {
        var text = PgServerLogTail.WithResume(PgServerLogTail.TailCteSql, Context(2));
        var shift = Assert.Single(text.Parameters, p => p.Name == "@log_read_shift");
        Assert.Equal(2L, shift.Value);

        var binary = PgServerLogTail.WithResume(PgServerLogTail.TailCteBinarySql, Context(2));
        Assert.DoesNotContain(binary.Parameters, p => p.Name == "@log_read_shift");

        /* Zero is the ordinary read: bound, so the statement is well formed, and a no-op. */
        var plain = PgServerLogTail.WithResume(PgServerLogTail.TailCteSql, Context());
        Assert.Equal(0L, Assert.Single(plain.Parameters, p => p.Name == "@log_read_shift").Value);
    }

    /// <summary>1, then 2, then 3 bytes later: four attempts in all, on the encoding refusal alone, on the text
    /// route alone. Recognised by SQLSTATE, never by message text.</summary>
    [Theory]
    [InlineData("22021", 0, false, true)]
    [InlineData("22021", 1, false, true)]
    [InlineData("22021", 2, false, true)]
    [InlineData("22021", 3, false, false)]
    [InlineData("22021", 0, true, false)]
    [InlineData("22P05", 0, false, false)]
    [InlineData("42501", 0, false, false)]
    [InlineData(null, 0, false, false)]
    public void ARefusalIsRetriedFromALaterStartUpToThreeTimes(string? sqlState, int shift, bool binaryRoute, bool retried)
        => Assert.Equal(retried, PgServerLogTail.ShouldRetryFromLaterStart(sqlState, shift, binaryRoute));

    [Fact]
    public void TheRetryIgnoresTheMessageText()
    {
        var byState = new PostgresException("some other wording entirely", "ERROR", "ERROR", "22021");
        Assert.True(PgServerLogTail.ShouldRetryFromLaterStart(byState.SqlState, 0, binaryRoute: false));

        var byWording = new PostgresException("invalid byte sequence for encoding \"UTF8\": 0xe3 0x81", "ERROR", "ERROR", "XX000");
        Assert.False(PgServerLogTail.ShouldRetryFromLaterStart(byWording.SqlState, 0, binaryRoute: false));
    }

    /// <summary>Four refusals in a row still record the error. The sentence now says the slice can start inside a
    /// multi-byte character, that the read was tried from a later start, and that later reads resume from a full
    /// line; the planted-byte guidance stays.</summary>
    [Fact]
    public void TheRecordedErrorNamesTheSplitCharacterAndTheFullLineResume()
    {
        var explanation = DarlingWorker.LogTailUndecodableByteExplanation(
            new PostgresException("invalid byte sequence for encoding \"UTF8\": 0xe3 0x81", "ERROR", "ERROR", "22021"),
            "pg_log_events",
            FaultRuntime);

        Assert.NotNull(explanation);
        Assert.Contains("multi-byte character", explanation, StringComparison.Ordinal);
        Assert.Contains("1, 2 and 3 bytes later", explanation, StringComparison.Ordinal);
        Assert.Contains("start of a full line", explanation, StringComparison.Ordinal);
        Assert.Contains("not valid UTF-8", explanation, StringComparison.Ordinal);
        Assert.Contains("EXECUTE ON FUNCTION pg_read_binary_file(text, bigint, bigint)", explanation, StringComparison.Ordinal);
    }

    /// <summary>The split-character sentence belongs to the encoding refusal only: a 22P05 is a conversion fault, not
    /// a start offset.</summary>
    [Fact]
    public void ATranslationFault22P05DoesNotClaimASplitCharacter()
    {
        var explanation = DarlingWorker.LogTailUndecodableByteExplanation(
            new PostgresException("character with byte sequence 0x81 in encoding WIN1252 has no equivalent in encoding UTF8", "ERROR", "ERROR", "22P05"),
            "pg_log_events",
            FaultRuntime);

        Assert.NotNull(explanation);
        Assert.DoesNotContain("multi-byte character", explanation, StringComparison.Ordinal);
    }
}
