// Copyright (c) Erik Darling Data. All rights reserved.
// Licensed under the terms in the LICENSE file in the repository root.

using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Linq;
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
        Assert.Equal(2, PgSettingRedactor.RulesVersion);
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
    public static IEnumerable<object[]> RedactionCases()
    {
        // libpq password keyword: unquoted, first / middle / last, spaces around '='.
        yield return new object[] { "primary_conninfo",
        "password=hunter2 host=127.0.0.1 port=5432 user=replicator application_name=s1a",
        "password=******** host=127.0.0.1 port=5432 user=replicator application_name=s1a" };
        yield return new object[] { "primary_conninfo",
        "host=127.0.0.1 password=hunter2 port=5432",
        "host=127.0.0.1 password=******** port=5432" };
        yield return new object[] { "primary_conninfo",
        "host=127.0.0.1 port=5432 password=hunter2",
        "host=127.0.0.1 port=5432 password=********" };
        yield return new object[] { "primary_conninfo",
        "host=a password = hunter2 user=b",
        "host=a password=******** user=b" };
        // libpq password keyword: quoted, with '\'' and '\\' escapes inside the quotes.
        yield return new object[] { "primary_conninfo",
        "password='hunter2' host=foo",
        "password=******** host=foo" };
        yield return new object[] { "primary_conninfo",
        @"password='it\'s\\a secret' host=foo",
        "password=******** host=foo" };
        // sslpassword — a distinct libpq keyword, not a substring match on "password".
        yield return new object[] { "primary_conninfo",
        "sslpassword=hunter2 sslmode=require",
        "sslpassword=******** sslmode=require" };
        yield return new object[] { "primary_conninfo",
        "sslpassword='hunter2' sslmode=require",
        "sslpassword=******** sslmode=require" };
        // URI form: user info with and without a password, several hosts, percent-encoding, a query parameter.
        yield return new object[] { "primary_conninfo",
        "postgresql://alice@host1:5432,host2:5432/db",
        "postgresql://alice@host1:5432,host2:5432/db" };
        yield return new object[] { "primary_conninfo",
        "postgresql://alice:hunter2@host1:5432,host2:5432/db",
        "postgresql://alice:********@host1:5432,host2:5432/db" };
        yield return new object[] { "primary_conninfo",
        "postgresql://alice:hun%40ter2@host/db",
        "postgresql://alice:********@host/db" };
        yield return new object[] { "primary_conninfo",
        "postgresql://alice@host/db?sslmode=require&password=hunter2",
        "postgresql://alice@host/db?sslmode=require&password=********" };
        // An empty user name in the URI's user info still carries a password to libpq.
        yield return new object[] { "primary_conninfo",
        "postgresql://:hunter2@primary:5432/db",
        "postgresql://:********@primary:5432/db" };
        // assignment / option whose NAME contains PASSWORD, PASSWD, SECRET or TOKEN.
        yield return new object[] { "archive_command",
        "PGPASSWORD=hunter2 psql -c 'select 1'",
        "PGPASSWORD=******** psql -c 'select 1'" };
        yield return new object[] { "archive_command",
        "AWS_SECRET_ACCESS_KEY=hunter2secret wal-g wal-push %p",
        "AWS_SECRET_ACCESS_KEY=******** wal-g wal-push %p" };
        yield return new object[] { "restore_command",
        "pg_dump --password=hunter2 --host=foo",
        "pg_dump --password=******** --host=foo" };
        yield return new object[] { "restore_command",
        @"pg_dump --password=""hunter 2"" --host=foo",
        "pg_dump --password=******** --host=foo" };
        // A space-separated option with no '='.
        yield return new object[] { "archive_command",
        "mycmd --password hunter2 %p",
        "mycmd --password ******** %p" };
        yield return new object[] { "archive_command",
        "aws s3 cp s3://b/%f %p --secret-access-key hunter2secret",
        "aws s3 cp s3://b/%f %p --secret-access-key ********" };
        // The spaced-option path uses the same name list as the assignment path.
        yield return new object[] { "archive_command",
        "gpg --passphrase S1 --decrypt %p",
        "gpg --passphrase ******** --decrypt %p" };
        yield return new object[] { "archive_command",
        "openssl enc -pass pass:S3 -d",
        "openssl enc -pass ******** -d" };
        yield return new object[] { "archive_command",
        "mycmd --encryption-key S4 %p",
        "mycmd --encryption-key ******** %p" };
        yield return new object[] { "archive_command",
        "mycmd --credentials S5 %p",
        "mycmd --credentials ******** %p" };
        yield return new object[] { "archive_command",
        "mycmd --no-password -h x %p",
        "mycmd --no-password -h x %p" };
        // A quoted value glued to a trailing ';' rather than whitespace.
        yield return new object[] { "archive_command",
        "export PGPASSWORD='hunter 2'; psql",
        "export PGPASSWORD=******** psql" };
        yield return new object[] { "primary_conninfo",
        "password='x'host=y",
        "password=********" };
        // Backup-tool secret variable names covered by the assignment list.
        yield return new object[] { "archive_command",
        "WALG_LIBSODIUM_KEY=hunter2 wal-g wal-push %p",
        "WALG_LIBSODIUM_KEY=******** wal-g wal-push %p" };
        yield return new object[] { "archive_command",
        "AZURE_STORAGE_ACCESS_KEY=hunter2 wal-g wal-push %p",
        "AZURE_STORAGE_ACCESS_KEY=******** wal-g wal-push %p" };
        yield return new object[] { "archive_command",
        "WALG_PGP_KEY_PASSPHRASE=hunter2 wal-g wal-push %p",
        "WALG_PGP_KEY_PASSPHRASE=******** wal-g wal-push %p" };
        yield return new object[] { "restore_command",
        "PGBACKREST_REPO1_CIPHER_PASS=hunter2 pgbackrest restore",
        "PGBACKREST_REPO1_CIPHER_PASS=******** pgbackrest restore" };
        // KEY is bounded to its own segment, so libpq's sslkey keyword is never touched.
        yield return new object[] { "primary_conninfo",
        "sslkey=/path/to/client.key sslmode=require",
        "sslkey=/path/to/client.key sslmode=require" };
        // ssl_passphrase_command: whole value masked; empty stays empty.
        yield return new object[] { "ssl_passphrase_command",
        "/usr/bin/cat /etc/ssl/passphrase-hunter2.txt",
        "********" };
        yield return new object[] { "ssl_passphrase_command",
        "",
        "" };
        // extension setting (dotted name): whole value masked when a segment names a secret.
        yield return new object[] { "anon.salt", "s0mesalt", "********" };
        yield return new object[] { "myext.api_key", "AKIAABCDEFG", "********" };
        // the substring decision, pinned: "key" matches wherever it appears in the last segment, not only as
        // its own word — myext.turkey_interval is not about a key at all, and is still masked in full.
        yield return new object[] { "myext.turkey_interval", "anything", "********" };
        // negative: myext.keep_alive does not contain any marker (password/passwd/passphrase/secret/salt/token/key/
        // credential/pwd) as a substring of "keep_alive", so its value is left alone.
        yield return new object[] { "myext.keep_alive", "30s", "30s" };
        // "credential" and "pwd" are whole-value markers, and a marker in ANY
        // dot-separated segment counts, not only the last one.
        yield return new object[] { "myext.api_credentials", "AKIAABCDEFG", "********" };
        yield return new object[] { "app.db_pwd", "hunter2", "********" };
        yield return new object[] { "vault.secret.value", "hunter2", "********" };
        // Edge forms, probe-confirmed.
        yield return new object[] { "primary_conninfo",
        "postgresql://u:pa S8@h/db",
        "postgresql://u:********@h/db" };
        yield return new object[] { "restore_command",
        "password = S16 host=foo",
        "password=******** host=foo" };
        // curl user:password in -u / --user: the password part is masked, the user name kept.
        yield return new object[] { "restore_command",
        "curl -u admin:S22",
        "curl -u admin:********" };
        // A quoted value with an embedded space is masked WHOLE, not just up to the space.
        yield return new object[] { "restore_command",
        "curl -u u:\"a b\"",
        "curl -u u:********" };
        // A backslash-escaped double quote inside the quoted value does not end it early.
        yield return new object[] { "restore_command",
        "curl --user \"u:it\\\"s\"",
        "curl --user u:********" };
        yield return new object[] { "primary_conninfo",
        "password='S5 unterminated",
        "password=********" };
        yield return new object[] { "primary_conninfo",
        "password='it\\\nhas a newline' host=foo",
        "password=******** host=foo" };
        // must not change at all.
        yield return new object[] { "password_encryption", "scram-sha-256", "scram-sha-256" };
        // PASS is a deliberately unbounded substring (unlike the bounded KEY segment test), so a libpq
        // passfile keyword is masked too (over-masking is the safe direction, #4348).
        yield return new object[] { "unix_socket_directories", "passfile=/x/.pgpass", "passfile=********" };
        yield return new object[] { "primary_conninfo", "host=a user=b", "host=a user=b" };
        yield return new object[] { "primary_conninfo", "", "" };
        // A percent-encoded key name in a URI query — %77 is 'w', so
        // pass%77ord decodes to "password". The key text itself stays encoded in the output; only the value
        // is masked. Untouched neighbour: an encoded key that does NOT decode to a password marker is left alone.
        yield return new object[] { "primary_conninfo",
        "postgresql://alice@host/db?pass%77ord=hunter2",
        "postgresql://alice@host/db?pass%77ord=********" };
        // Every key character percent-encoded — %70ass%77ord decodes to "password" the same way a
        // partially-encoded key does; the whole key stays encoded in the output.
        yield return new object[] { "primary_conninfo",
        "postgresql://alice@host/db?%70ass%77ord=hunter2",
        "postgresql://alice@host/db?%70ass%77ord=********" };
        yield return new object[] { "primary_conninfo",
        "postgresql://alice@host/db?us%65r=x",
        "postgresql://alice@host/db?us%65r=x" };
        // A double-quoted "password = x" assignment (quote before the name, spaces around '=').
        // Untouched neighbour: an adjacent unrelated quoted field stays as it is.
        yield return new object[] { "custom.json_blob",
        "{\"password\" = \"hunter2\", \"host\" = \"foo\"}",
        "{\"password\" = \"********\", \"host\" = \"foo\"}" };
        // An unquoted name with a double-quoted value and spaces around '=' is LibpqPasswordKeyword's
        // shape (no quote sits before the bare name "password", so QuotedSpacedAssignment never engages
        // here), and that rule tightens the spacing around '=' the same way the unquoted-value case at the
        // top of this list does ("host=a password = hunter2 user=b" -> "...password=******** ...") --
        // consistent tightening, not a special case for a quoted value.
        yield return new object[] { "custom.json_blob",
        "password = \"hunter2\"",
        "password=********" };
        // A whole assignment sharing ONE quote pair -- the name has no separate closing quote of its own, so
        // the value's only closing quote is the outer one coming back around.
        yield return new object[] { "custom.json_blob",
        "\"password = hunter2\"",
        "\"password = ********\"" };
        yield return new object[] { "custom.json_blob",
        "'pwd = abc'",
        "'pwd = ********'" };
        // Untouched neighbour: a quoted assignment whose name carries no secret marker is left alone.
        yield return new object[] { "custom.json_blob",
        "\"host = foo\"",
        "\"host = foo\"" };
        // curl -u user:x and --user user:x mask the part after the colon, not the user name.
        yield return new object[] { "archive_command",
        "curl -u admin:hunter2 https://x",
        "curl -u admin:******** https://x" };
        yield return new object[] { "archive_command",
        "curl --user admin:hunter2 https://x",
        "curl --user admin:******** https://x" };
        // Untouched neighbour: -u with no colon (no password at all) is left alone.
        yield return new object[] { "archive_command",
        "curl -u admin https://x",
        "curl -u admin https://x" };
        // A quoted value with an embedded space is masked whole, not just up to the first space.
        yield return new object[] { "archive_command",
        "curl -u \"admin:hunter 2\" https://x",
        "curl -u admin:******** https://x" };
        // Tightly-bound forms: -uuser:x, --user=user:x, --proxy-user, -U (case-sensitive), and untouched --USER.
        yield return new object[] { "archive_command",
        "curl -uadmin:hunter2 https://x",
        "curl -u admin:******** https://x" };
        yield return new object[] { "archive_command",
        "curl --user=admin:hunter2 https://x",
        "curl --user admin:******** https://x" };
        yield return new object[] { "archive_command",
        "curl --proxy-user admin:hunter2 https://x",
        "curl --proxy-user admin:******** https://x" };
        yield return new object[] { "archive_command",
        "curl -U admin:hunter2 https://x",
        "curl -U admin:******** https://x" };
        yield return new object[] { "archive_command",
        "curl --USER admin:hunter2 https://x",
        "curl --USER admin:hunter2 https://x" };
        // sshpass -p x, including the tightly-bound -px and the path-prefixed /usr/bin/sshpass.
        yield return new object[] { "archive_command",
        "sshpass -p hunter2 ssh user@host",
        "sshpass -p ******** ssh user@host" };
        yield return new object[] { "archive_command",
        "sshpass -phunter2 ssh user@host",
        "sshpass -p ******** ssh user@host" };
        yield return new object[] { "archive_command",
        "/usr/bin/sshpass -p hunter2 ssh user@host",
        "/usr/bin/sshpass -p ******** ssh user@host" };
        // A quoted sshpass value with an embedded space is masked whole.
        yield return new object[] { "archive_command",
        "sshpass -p \"hunter 2\" ssh user@host",
        "sshpass -p ******** ssh user@host" };
        // An earlier option before -p is allowed.
        yield return new object[] { "archive_command",
        "sshpass -v -p hunter2 ssh user@host",
        "sshpass -v -p ******** ssh user@host" };
        yield return new object[] { "archive_command",
        "sshpass -e -p hunter2 ssh user@host",
        "sshpass -e -p ******** ssh user@host" };
        // -p is case-sensitive: sshpass's own -P takes the SSH prompt text, not a password, so it stays
        // unmasked while the real -p value after it is masked.
        yield return new object[] { "archive_command",
        "sshpass -P prompt -p s3 ssh user@host",
        "sshpass -P prompt -p ******** ssh user@host" };
        // Untouched neighbour: sshpass with no -p at all (a file-based password) never matches.
        yield return new object[] { "archive_command",
        "sshpass -f /etc/sshpass.txt ssh user@host",
        "sshpass -f /etc/sshpass.txt ssh user@host" };
        // A quoted URI query password value with an embedded space is masked whole — the pre-existing
        // UriQueryPassword rule (fixed alongside the new ones, same quote-aware value alternation).
        yield return new object[] { "primary_conninfo",
        "https://x/db?sslmode=require&password=\"hunter 2\"",
        "https://x/db?sslmode=require&password=********" };
        // A quoted, percent-encoded-key query password with an embedded space is masked whole.
        yield return new object[] { "primary_conninfo",
        "https://x/db?pass%77ord=\"hunter 2\"",
        "https://x/db?pass%77ord=********" };
        // A SAS or pre-signed URL signature query parameter.
        yield return new object[] { "primary_conninfo",
        "https://acct.blob.core.windows.net/c/f?sig=abc123%2Fdef",
        "https://acct.blob.core.windows.net/c/f?sig=********" };
        yield return new object[] { "primary_conninfo",
        "https://b.s3.amazonaws.com/f?X-Amz-Signature=abc123",
        "https://b.s3.amazonaws.com/f?X-Amz-Signature=********" };
        yield return new object[] { "primary_conninfo",
        "https://storage.googleapis.com/b/f?X-Goog-Signature=abc123",
        "https://storage.googleapis.com/b/f?X-Goog-Signature=********" };
        // Untouched neighbour: an adjacent, unrelated query parameter next to a signature stays as it is.
        yield return new object[] { "primary_conninfo",
        "https://acct.blob.core.windows.net/c/f?sv=2024&sig=abc123",
        "https://acct.blob.core.windows.net/c/f?sv=2024&sig=********" };
        // Allowlist check against the new rules: an allowlisted policy name stays untouched by every new regex.
        yield return new object[] { "rds.accepted_password_auth_method", "md5+password", "md5+password" };

    }

    [Theory]
    [MemberData(nameof(RedactionCases))]
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

    // These three names are password POLICY settings, not secrets — the allowlist keeps them
    // unmasked even though "password" sits in the dotted name.
    [Theory]
    [InlineData("rds.accepted_password_auth_method", "md5+password")]
    [InlineData("RDS.ACCEPTED_PASSWORD_AUTH_METHOD", "md5+password")]
    [InlineData("rds.restrict_password_commands", "on")]
    [InlineData("passwordcheck.min_password_length", "8")]
    public void AllowlistedPolicyNames_AreNotMasked(string name, string value)
    {
        Assert.Equal(value, PgSettingRedactor.Redact(name, value));
    }

    // Negative case: a REAL secret next to an allowlisted name in the same batch is still masked — the
    // allowlist is name-exact, not a blanket "don't touch anything dotted with .password. in it" escape.
    [Fact]
    public void AllowlistedPolicyName_DoesNotShieldARealSecretElsewhere()
    {
        Assert.Equal("md5+password", PgSettingRedactor.Redact("rds.accepted_password_auth_method", "md5+password"));
        Assert.Equal("********", PgSettingRedactor.Redact("app.db_password", "fake-secret-value"));
    }

    /// <summary>
    /// #4348: a frozen list of inputs whose expected output is hard-coded from dev's redactor (pre-lookahead),
    /// captured independently of <see cref="RedactionCases"/> above. Its job is narrower than that member's:
    /// it pins that the linear-pre-check change added to <c>AssignmentSecretName</c>, <c>OptionSecretSpaced</c>
    /// and <c>QuotedSpacedAssignment</c> (#4348) changed no match, only how the engine gets there. A
    /// regression there would show up here even if a future edit also touched the corpus above.
    /// </summary>
    [Theory]
    [InlineData("archive_command", "xpassword=s", "xpassword=********")]
    [InlineData("archive_command", "a.password=s", "a.password=********")]
    [InlineData("archive_command", "my-token=s", "my-token=********")]
    [InlineData("archive_command", "env PGPASSWORD=s cmd", "env PGPASSWORD=******** cmd")]
    [InlineData("archive_command", "--db.password=s", "--db.password=********")]
    [InlineData("archive_command", "-Dpassword=s", "-Dpassword=********")]
    [InlineData("archive_command", "foo.bar.PGPASSWORD=s", "foo.bar.PGPASSWORD=********")]
    [InlineData("archive_command", "a;PGPASSWORD=s", "a;PGPASSWORD=********")]
    [InlineData("archive_command", "sslkey=s", "sslkey=s")]
    [InlineData("archive_command", "ab-KEY-cd=s", "ab-KEY-cd=********")]
    [InlineData("archive_command", "abc--password=s", "abc--password=********")]
    [InlineData("archive_command", "password = \"hunter2\"", "password=********")]
    [InlineData("archive_command", "postgresql://u:p@h/db", "postgresql://u:********@h/db")]
    [InlineData("archive_command", "host=h password=p", "host=h password=********")]
    [InlineData("archive_command", "https://h/?password=p&x=1", "https://h/?password=********")]
    [InlineData("archive_command", "sshpass -p p ssh h", "sshpass -p ******** ssh h")]
    [InlineData("archive_command", "curl -u a:b", "curl -u a:********")]
    [InlineData("password_encryption", "password_encryption", "password_encryption")]
    [InlineData("archive_command", "host=localhost port=5432 dbname=mydb", "host=localhost port=5432 dbname=mydb")]
    [InlineData("archive_command", "application_name=myapp", "application_name=myapp")]
    [InlineData("archive_command", "sslmode=verify-full", "sslmode=verify-full")]
    [InlineData("archive_command", "passfile=/x/.pgpass", "passfile=********")]
    [InlineData("archive_command", "keep_alive=on", "keep_alive=on")]
    [InlineData("archive_command", "monkey=1", "monkey=1")]
    [InlineData("archive_command", "user=alice dbname=x", "user=alice dbname=x")]
    [InlineData("archive_command", "connect_timeout=10", "connect_timeout=10")]
    [InlineData("archive_command", "statement_timeout=5000", "statement_timeout=5000")]
    [InlineData("archive_command", "search_path=public", "search_path=public")]
    [InlineData("archive_command", "log_min_duration_statement=250", "log_min_duration_statement=250")]
    [InlineData("archive_command", "archive_command=/bin/true", "archive_command=/bin/true")]
    [InlineData("archive_command", "-Dpassword hunter2", "-Dpassword ********")]
    [InlineData("archive_command", "--passphrase secret123", "--passphrase ********")]
    [InlineData("archive_command", "openssl enc -pass pass:x", "openssl enc -pass ********")]
    [InlineData("archive_command", "rds.accepted_password_auth_method", "rds.accepted_password_auth_method")]
    [InlineData("vault.secret", "anything", "********")]
    [InlineData("anon.salt", "seedvalue", "********")]
    [InlineData("myext.api_key", "kv", "********")]
    [InlineData("myext.keep_alive", "on", "on")]
    [InlineData("ssl_passphrase_command", "echo x", "********")]
    [InlineData("archive_command", "pg_password=abc", "pg_password=********")]
    public void FrozenParityWithPreviousRedactor(string name, string value, string expectedFromDev)
    {
        Assert.Equal(expectedFromDev, PgSettingRedactor.Redact(name, value));
    }

    /// <summary>
    /// #4348: a long input with no separator must finish well inside a human-perceptible delay, not spend
    /// seconds backtracking. Warms up once (first call pays JIT/regex-compile cost, not what this pins),
    /// then asserts on the second run.
    /// </summary>
    [Theory]
    [InlineData(3200, true)]
    [InlineData(3200, false)]
    [InlineData(32000, true)]
    [InlineData(32000, false)]
    public void LongInputWithNoSeparator_MatchesWellUnderBudget(int repeatLength, bool trailingAssignment)
    {
        var body = string.Concat(Enumerable.Repeat("pass", repeatLength / 4));
        var value = trailingAssignment ? body + "=x" : body;

        // Warm-up run: pays JIT/regex-compile cost, not measured.
        _ = PgSettingRedactor.Redact("archive_command", value);

        var stopwatch = Stopwatch.StartNew();
        _ = PgSettingRedactor.Redact("archive_command", value);
        stopwatch.Stop();

        Assert.True(
            stopwatch.ElapsedMilliseconds < 50,
            $"Expected under 50ms, took {stopwatch.ElapsedMilliseconds}ms for length {value.Length}.");
    }

    /// <summary>
    /// #4348: forcing a timeout via the <see cref="PgSettingRedactor.MatchTimeoutForTest"/> seam must mask
    /// the whole value, invoke the timeout callback with the setting's NAME only, and never throw.
    /// </summary>
    [Fact]
    public void ForcedTimeout_MasksWholeValue_AndNamesOnlyTheSetting()
    {
        var longValue = string.Concat(Enumerable.Repeat("ZQXV", 2000)) + "=x";
        string? loggedName = null;
        var callbackCount = 0;

        PgSettingRedactor.MatchTimeoutForTest = new TimeSpan(1);
        try
        {
            var result = PgSettingRedactor.Redact("archive_command", longValue, name =>
            {
                loggedName = name;
                callbackCount++;
            });

            Assert.Equal("********", result);
            Assert.Equal(1, callbackCount);
            Assert.Equal("archive_command", loggedName);
            Assert.DoesNotContain("ZQXV", loggedName ?? string.Empty);
        }
        finally
        {
            PgSettingRedactor.MatchTimeoutForTest = null;
        }
    }

    /// <summary>
    /// #4348: <see cref="PgSettingRedactor.Redact"/> is documented as never throwing. A callback that throws
    /// must not escape past a forced timeout — the value is already masked by that point.
    /// </summary>
    [Fact]
    public void ForcedTimeout_ThrowingCallback_StillReturnsMaskAndDoesNotThrow()
    {
        var longValue = string.Concat(Enumerable.Repeat("ZQXV", 2000)) + "=x";

        PgSettingRedactor.MatchTimeoutForTest = new TimeSpan(1);
        try
        {
            string? result = null;
            var ex = Record.Exception(() =>
                result = PgSettingRedactor.Redact("archive_command", longValue, _ => throw new InvalidOperationException("boom")));

            Assert.Null(ex);
            Assert.Equal("********", result);
        }
        finally
        {
            PgSettingRedactor.MatchTimeoutForTest = null;
        }
    }

    /// <summary>
    /// #4348: every <c>Compiled</c> pattern is warmed at type initialisation, before any real
    /// <see cref="PgSettingRedactor.Redact"/> call can return, so the first real call never pays a
    /// first-use JIT cost against the 100ms match timeout. This pins that the warmup ran (rather than
    /// re-deriving that fact from timing, which would be flaky by construction on a busy runner) and that a
    /// short, well-formed value is masked in PART, not masked whole, the way a spurious timeout would
    /// produce.
    /// </summary>
    [Fact]
    public void Warmup_RanBeforeFirstRealCall_AndShortValueIsNotMaskedWhole()
    {
        Assert.True(PgSettingRedactor.WarmedUp);

        var result = PgSettingRedactor.Redact("archive_command", "PGPASSWORD=hunter2 psql -c 'select 1'");

        Assert.Equal("PGPASSWORD=******** psql -c 'select 1'", result);
    }

    /// <summary>
    /// #4348: warmup must reach each pattern's MATCH step, not only its scan (<c>TryFindNextPossibleStartingPosition</c>
    /// finding a candidate but <c>TryMatchAtCurrentPosition</c> never running). This pins that the warmup
    /// sample every <see cref="PgSettingRedactor.TimeBoundPattern"/> is warmed with actually produces at
    /// least one match for EVERY pattern in the list, so the match step is exercised, not skipped.
    /// </summary>
    [Fact]
    public void WarmupSample_MatchesEveryWarmedPattern()
    {
        Assert.NotEmpty(PgSettingRedactor.WarmedPatterns);

        foreach (var pattern in PgSettingRedactor.WarmedPatterns)
        {
            Assert.True(pattern.WarmupSampleMatches());
        }
    }
}
