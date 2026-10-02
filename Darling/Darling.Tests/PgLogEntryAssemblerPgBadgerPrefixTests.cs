// #4735 item 2: the common pgBadger prefix ('%t [%p]: user=%u,db=%d,app=%a,client=%h ') carries the user and the
// database as user= and db= fields. The stderr prefix parser used to read them only from user@db, so every error
// row under this prefix had a null user and database.
using PerformanceMonitor.Collectors;
using Xunit;

namespace Darling.Tests;

public sealed class PgLogEntryAssemblerPgBadgerPrefixTests
{
    private static PgLogEntry Only(string line)
    {
        var entries = PgLogEntryAssembler.Assemble(line + "\n");
        return Assert.Single(entries);
    }

    [Fact]
    public void APgBadgerPrefixYieldsTheUserAndTheDatabase()
    {
        var entry = Only("2026-09-29 10:15:00 UTC [4506]: user=app_rw,db=orders,app=psql,client=10.0.0.5 ERROR:  relation \"nope\" does not exist");

        Assert.Equal("app_rw", entry.UserName);
        Assert.Equal("orders", entry.DatabaseName);
    }

    [Fact]
    public void TheFieldsAreFoundAnywhereInThePrefixAndStopAtACommaOrWhitespace()
    {
        var entry = Only("2026-09-29 10:15:00 UTC [4506]: [3-1] app=psql db=orders user=app_rw client=10.0.0.5 ERROR:  boom");

        Assert.Equal("app_rw", entry.UserName);
        Assert.Equal("orders", entry.DatabaseName);
    }

    [Fact]
    public void AnEmptyValueStaysNull()
    {
        /* A background process renders %u and %d as nothing under this prefix. */
        var background = Only("2026-09-29 10:15:00 UTC [88]: user=,db=,app=,client= ERROR:  boom");
        Assert.Null(background.UserName);
        Assert.Null(background.DatabaseName);

        /* Each field stands alone: an empty database does not blank a user that is there. */
        var half = Only("2026-09-29 10:15:00 UTC [88]: user=app_rw,db=,app=psql ERROR:  boom");
        Assert.Equal("app_rw", half.UserName);
        Assert.Null(half.DatabaseName);
    }

    [Fact]
    public void TheUserAtDatabaseFormIsUnchanged()
    {
        var entry = Only("2026-09-29 10:15:00 UTC app_rw@orders [4506] ERROR:  boom");

        Assert.Equal("app_rw", entry.UserName);
        Assert.Equal("orders", entry.DatabaseName);
    }

    [Fact]
    public void AWordThatMerelyEndsInUserIsNotTheUserField()
    {
        var entry = Only("2026-09-29 10:15:00 UTC [4506]: superuser=yes,db=orders ERROR:  boom");

        Assert.Null(entry.UserName);
        Assert.Equal("orders", entry.DatabaseName);
    }
}
