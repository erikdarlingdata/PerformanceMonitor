// Copyright (c) Erik Darling Data. All rights reserved.
// Licensed under the terms in the LICENSE file in the repository root.

using PerformanceMonitor.Collectors;
using Xunit;

namespace Darling.Tests;

/// <summary>
/// #4348 — the monitoring role's <c>pg_monitor</c> grant includes <c>pg_read_all_settings</c>, so
/// <c>PgServerConfigCollector</c> sees <c>GUC_SUPERUSER_ONLY</c> settings such as a standby's
/// <c>primary_conninfo</c>, which can carry a replication password in plain text. These pin
/// <see cref="PgSettingRedactor"/>'s rules directly, without a database, so the corpus runs on every build.
/// </summary>
public sealed class PgSettingRedactorTests
{
    [Fact]
    public void RulesVersionIsPinned()
    {
        Assert.Equal(1, PgSettingRedactor.RulesVersion);
    }

    [Fact]
    public void NullValue_StaysNull()
    {
        Assert.Null(PgSettingRedactor.Redact("primary_conninfo", null));
    }

    /// <summary>
    /// Every case both matches its expected redaction and is idempotent — redacting an already-redacted
    /// value must be a no-op, because the collector (new rows) and the S1b scrub (old rows, #4348) call
    /// the same redactor, and the scrub can run over rows a newer collector already redacted.
    /// </summary>
    [Theory]
    // libpq password keyword: unquoted, first / middle / last, spaces around '='.
    [InlineData("primary_conninfo",
        "password=hunter2 host=127.0.0.1 port=5432 user=replicator application_name=s1a",
        "password=******** host=127.0.0.1 port=5432 user=replicator application_name=s1a")]
    [InlineData("primary_conninfo",
        "host=127.0.0.1 password=hunter2 port=5432",
        "host=127.0.0.1 password=******** port=5432")]
    [InlineData("primary_conninfo",
        "host=127.0.0.1 port=5432 password=hunter2",
        "host=127.0.0.1 port=5432 password=********")]
    [InlineData("primary_conninfo",
        "host=a password = hunter2 user=b",
        "host=a password=******** user=b")]
    // libpq password keyword: quoted, with '\'' and '\\' escapes inside the quotes.
    [InlineData("primary_conninfo",
        "password='hunter2' host=foo",
        "password=******** host=foo")]
    [InlineData("primary_conninfo",
        @"password='it\'s\\a secret' host=foo",
        "password=******** host=foo")]
    // sslpassword — a distinct libpq keyword, not a substring match on "password".
    [InlineData("primary_conninfo",
        "sslpassword=hunter2 sslmode=require",
        "sslpassword=******** sslmode=require")]
    [InlineData("primary_conninfo",
        "sslpassword='hunter2' sslmode=require",
        "sslpassword=******** sslmode=require")]
    // URI form: user info with and without a password, several hosts, percent-encoding, a query parameter.
    [InlineData("primary_conninfo",
        "postgresql://alice@host1:5432,host2:5432/db",
        "postgresql://alice@host1:5432,host2:5432/db")]
    [InlineData("primary_conninfo",
        "postgresql://alice:hunter2@host1:5432,host2:5432/db",
        "postgresql://alice:********@host1:5432,host2:5432/db")]
    [InlineData("primary_conninfo",
        "postgresql://alice:hun%40ter2@host/db",
        "postgresql://alice:********@host/db")]
    [InlineData("primary_conninfo",
        "postgresql://alice@host/db?sslmode=require&password=hunter2",
        "postgresql://alice@host/db?sslmode=require&password=********")]
    // assignment / option whose NAME contains PASSWORD, PASSWD, SECRET or TOKEN.
    [InlineData("archive_command",
        "PGPASSWORD=hunter2 psql -c 'select 1'",
        "PGPASSWORD=******** psql -c 'select 1'")]
    [InlineData("archive_command",
        "AWS_SECRET_ACCESS_KEY=hunter2secret wal-g wal-push %p",
        "AWS_SECRET_ACCESS_KEY=******** wal-g wal-push %p")]
    [InlineData("restore_command",
        "pg_dump --password=hunter2 --host=foo",
        "pg_dump --password=******** --host=foo")]
    [InlineData("restore_command",
        @"pg_dump --password=""hunter 2"" --host=foo",
        "pg_dump --password=******** --host=foo")]
    // ssl_passphrase_command: whole value masked; empty stays empty.
    [InlineData("ssl_passphrase_command",
        "/usr/bin/cat /etc/ssl/passphrase-hunter2.txt",
        "********")]
    [InlineData("ssl_passphrase_command",
        "",
        "")]
    // extension setting (dotted name): whole value masked when the LAST segment names a secret.
    [InlineData("anon.salt", "s0mesalt", "********")]
    [InlineData("myext.api_key", "AKIAABCDEFG", "********")]
    // the substring decision, pinned: "key" matches wherever it appears in the last segment, not only as
    // its own word — myext.turkey_interval is not about a key at all, and is still masked in full.
    [InlineData("myext.turkey_interval", "anything", "********")]
    // negative: myext.keep_alive does not contain any marker (password/passwd/passphrase/secret/salt/token/key)
    // as a substring of "keep_alive", so its value is left alone.
    [InlineData("myext.keep_alive", "30s", "30s")]
    // must not change at all.
    [InlineData("password_encryption", "scram-sha-256", "scram-sha-256")]
    [InlineData("unix_socket_directories", "passfile=/x/.pgpass", "passfile=/x/.pgpass")]
    [InlineData("primary_conninfo", "host=a user=b", "host=a user=b")]
    [InlineData("primary_conninfo", "", "")]
    public void RedactsPerTheRuling_AndIsIdempotent(string name, string value, string expected)
    {
        var actual = PgSettingRedactor.Redact(name, value);

        Assert.Equal(expected, actual);

        // f(f(x)) == f(x): re-running the redactor over its own output changes nothing further.
        Assert.Equal(actual, PgSettingRedactor.Redact(name, actual));
    }

    /// <summary>
    /// Every rule applies wherever the value's own text says so — not only to <c>primary_conninfo</c>.
    /// A password can hide behind an <c>archive_command</c> or <c>restore_command</c> just as easily.
    /// </summary>
    [Fact]
    public void EmbeddedSecretRulesAreNotGatedOnTheSettingName()
    {
        Assert.Equal(
            "PGPASSWORD=******** psql",
            PgSettingRedactor.Redact("restore_command", "PGPASSWORD=hunter2 psql"));
    }

    [Fact]
    public void NeverThrows_OnEmptyName()
    {
        var result = PgSettingRedactor.Redact(string.Empty, "password=hunter2");

        Assert.Equal("password=********", result);
    }

    [Fact]
    public void NeverThrows_OnNullName()
    {
        var result = PgSettingRedactor.Redact(null, "password=hunter2");

        Assert.Equal("password=********", result);
    }
}
