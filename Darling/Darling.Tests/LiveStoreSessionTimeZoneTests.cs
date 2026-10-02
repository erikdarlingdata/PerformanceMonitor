using System;
using System.Threading.Tasks;
using Npgsql;
using Xunit;

namespace Darling.Tests;

/// <summary>
/// Every live test reaches the store through DARLING_TEST_PG, and many plant rows relative to UTC now and read them
/// back through SQL that compares naive UTC timestamps with the session's clock. The product pins every store session
/// to UTC (<c>DarlingStoreConnection.PinSessionTimeZoneUtc</c>), so a store whose server runs in another time zone
/// still reads correctly. The test process pins the variable the same way when it loads
/// (<see cref="LiveStoreSessionTimeZone"/>). Without that, a test store in another zone failed the Azure master-scope
/// fleet and daily-summary tests with no product bug.
/// </summary>
[Collection("live-postgres")]
public sealed class LiveStoreSessionTimeZoneTests
{
    [Fact]
    public async Task ALiveTestSession_RunsInUtc_AgainstDevPostgres()
    {
        var connectionString = Environment.GetEnvironmentVariable("DARLING_TEST_PG");
        Assert.SkipWhen(string.IsNullOrEmpty(connectionString),
            "Set DARLING_TEST_PG to a Postgres connection string to run the live session time zone test.");

        var ct = TestContext.Current.CancellationToken;
        await using var connection = new NpgsqlConnection(connectionString);
        await connection.OpenAsync(ct);
        await using var command = new NpgsqlCommand("SHOW timezone", connection);

        Assert.Equal("UTC", (string?)await command.ExecuteScalarAsync(ct));
    }
}
