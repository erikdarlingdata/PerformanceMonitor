/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using System.IO;
using System.Runtime.CompilerServices;
using System.Threading.Tasks;
using Npgsql;
using PerformanceMonitor.Collectors;
using PerformanceMonitor.Common;
using PerformanceMonitor.Darling.Service;
using PerformanceMonitor.Darling.Storage;
using PerformanceMonitor.Darling.Viewer;
using Xunit;

namespace Darling.Tests;

/// <summary>
/// The Viewer seals the SMTP password (#5366): it is sealed for the host, port, SSL flag and user name it is stored with,
/// a blank password box keeps the saved value only while those four stay the same, and the service opens what the Viewer
/// stored. The sentences never carry a password.
/// </summary>
public sealed class ViewerSmtpSealTests : IDisposable
{
    private const string Plain = "p@ss-not-real";

    private readonly PasswordPrivateKey _key = PasswordPrivateKey.Generate();

    public void Dispose() => _key.Dispose();

    private static NotificationRow Row(
        string host = "mail.example.test", int port = 587, bool ssl = true, string? username = "mailer", string? blob = "sealed:v1:stored") => new()
    {
        SmtpHost = host,
        SmtpPort = port,
        SmtpUseSsl = ssl,
        SmtpUsername = username,
        SmtpEncryptedPassword = blob,
    };

    /* ═══════════ the re-entry rule ═══════════ */

    [Fact]
    public void ABlankPassword_WithAChangedHost_IsRefused_WithTheReenterSentence()
    {
        var action = ViewerSmtpSeal.Decide(Row(), null, Row(host: "other-mail.example.test"), "");

        Assert.Equal(ViewerSmtpPasswordAction.Refuse, action);
        Assert.Equal("Changing how mail is sent needs the SMTP password again.", ViewerSmtpSeal.ReenterText);
    }

    [Theory]
    [InlineData("port")]
    [InlineData("ssl")]
    [InlineData("username")]
    [InlineData("username-cleared")]
    public void ABlankPassword_WithAnyOtherChangedBoundValue_IsRefused(string changed)
    {
        var edited = changed switch
        {
            "port" => Row(port: 465),
            "ssl" => Row(ssl: false),
            "username" => Row(username: "someone-else"),
            _ => Row(username: null),
        };

        Assert.Equal(ViewerSmtpPasswordAction.Refuse, ViewerSmtpSeal.Decide(Row(), null, edited, ""));
        Assert.Equal(ViewerSmtpPasswordAction.Refuse, ViewerSmtpSeal.Decide(Row(), null, edited, null));
    }

    [Fact]
    public void ABlankPassword_WithNothingChanged_KeepsTheSavedValue_AndAnEmptyUsernameIsTheSameAsNone()
    {
        Assert.Equal(ViewerSmtpPasswordAction.Keep, ViewerSmtpSeal.Decide(Row(), null, Row(), ""));
        Assert.Equal(ViewerSmtpPasswordAction.Keep, ViewerSmtpSeal.Decide(Row(username: null), null, Row(username: ""), ""));
    }

    [Fact]
    public void ABlankPassword_WithAChangedHost_IsFine_WhenNoPasswordIsSaved()
    {
        Assert.Equal(ViewerSmtpPasswordAction.Keep, ViewerSmtpSeal.Decide(Row(blob: null), null, Row(blob: null, host: "other-mail.example.test"), ""));
        Assert.Equal(ViewerSmtpPasswordAction.Keep, ViewerSmtpSeal.Decide(Row(blob: ""), null, Row(blob: "", port: 25), ""));
    }

    [Fact]
    public void ATypedPassword_IsSealed_WhateverChanged()
    {
        Assert.Equal(ViewerSmtpPasswordAction.Seal, ViewerSmtpSeal.Decide(Row(), null, Row(), Plain));
        Assert.Equal(ViewerSmtpPasswordAction.Seal, ViewerSmtpSeal.Decide(Row(), null, Row(host: "other-mail.example.test"), Plain));
        Assert.Equal(ViewerSmtpPasswordAction.Seal, ViewerSmtpSeal.Decide(Row(blob: null), null, Row(blob: null), Plain));
    }

    [Fact]
    public void AValueThatThisMachineCouldRead_AndIsLeftAlone_IsNotSealedAgain_UnlessTheSettingsChanged()
    {
        Assert.Equal(ViewerSmtpPasswordAction.Keep, ViewerSmtpSeal.Decide(Row(blob: "old-format"), Plain, Row(blob: "old-format"), Plain));
        Assert.Equal(ViewerSmtpPasswordAction.Seal, ViewerSmtpSeal.Decide(Row(blob: "old-format"), Plain, Row(blob: "old-format", port: 25), Plain));
        Assert.Equal(ViewerSmtpPasswordAction.Seal, ViewerSmtpSeal.Decide(Row(blob: "old-format"), Plain, Row(blob: "old-format"), Plain + "x"));
    }

    /* ═══════════ the seal ═══════════ */

    [Fact]
    public void ASealedSmtpPassword_OpensForItsOwnFourValues_AndNotAfterAnyOfThemChanges()
    {
        var sealer = new ViewerPasswordSealer(_key.PublicKey);

        var sealedText = sealer.SealSmtp(Plain, "mail.example.test", 587, true, "mailer");

        Assert.StartsWith("sealed:v1:", sealedText, StringComparison.Ordinal);
        Assert.DoesNotContain(Plain, sealedText, StringComparison.Ordinal);
        Assert.Equal(Plain, PasswordSeal.Open(sealedText, _key, PasswordBinding.ForSmtp("mail.example.test", 587, true, "mailer")));
        foreach (var changed in new[]
                 {
                     PasswordBinding.ForSmtp("other-mail.example.test", 587, true, "mailer"),
                     PasswordBinding.ForSmtp("mail.example.test", 25, true, "mailer"),
                     PasswordBinding.ForSmtp("mail.example.test", 587, false, "mailer"),
                     PasswordBinding.ForSmtp("mail.example.test", 587, true, "someone-else"),
                 })
        {
            Assert.Throws<PasswordSealException>(() => PasswordSeal.Open(sealedText, _key, changed));
        }
    }

    [Fact]
    public void ASealedSmtpPassword_OpensWithTheBindingTheServiceBuildsFromTheStoredRow()
    {
        var sealer = new ViewerPasswordSealer(_key.PublicKey);
        var row = Row(username: null, blob: null);
        row.SmtpEncryptedPassword = sealer.SealSmtp(Plain, row.SmtpHost, row.SmtpPort, row.SmtpUseSsl, row.SmtpUsername);

        /* The service builds its SmtpConfig from the stored columns; an unset user name reads back as empty text. */
        var smtp = new SmtpConfig
        {
            Host = row.SmtpHost, Port = row.SmtpPort, UseSsl = row.SmtpUseSsl, Username = "", EncryptedPassword = row.SmtpEncryptedPassword,
        };

        Assert.Equal(Plain, DarlingSecrets.ResolveSmtpPassword(smtp, DarlingPasswordKey.FromPrivateKey(_key), LegacyDpapi.Current));
    }

    [Fact]
    public void ALoneSurrogate_InThePassword_OrABoundField_IsRefused_WithASentenceThatNamesNoValue()
    {
        var sealer = new ViewerPasswordSealer(_key.PublicKey);
        const string bad = "abcd\ud800";

        var inPassword = Assert.Throws<ViewerPasswordRefusedException>(() => sealer.SealSmtp(bad, "mail.example.test", 587, true, "mailer"));
        var inHost = Assert.Throws<ViewerPasswordRefusedException>(() => sealer.SealSmtp(Plain, bad, 587, true, "mailer"));
        var inUser = Assert.Throws<ViewerPasswordRefusedException>(() => sealer.SealSmtp(Plain, "mail.example.test", 587, true, bad));

        Assert.Equal(ViewerPasswordSealer.PasswordCharactersText, inPassword.Message);
        Assert.Equal("The mail server contains characters that cannot be stored.", inHost.Message);
        Assert.Equal("The SMTP user name contains characters that cannot be stored.", inUser.Message);
        foreach (var message in new[] { inPassword.Message, inHost.Message, inUser.Message })
        {
            Assert.DoesNotContain("abcd", message, StringComparison.Ordinal);
        }

        Assert.False(ViewerSmtpSeal.IsValidText(bad));
        Assert.True(ViewerSmtpSeal.IsValidText("a\U0001F600b"));
        Assert.True(ViewerSmtpSeal.IsValidText(null));
    }

    [Fact]
    public void APasswordLongerThanASealedValueCanHold_IsRefused_WithTheTooLongSentence()
    {
        var sealer = new ViewerPasswordSealer(_key.PublicKey);

        var ex = Assert.Throws<ViewerPasswordRefusedException>(
            () => sealer.SealSmtp(new string('x', PasswordSeal.MaxPlaintextBytes + 1), "mail.example.test", 587, true, "mailer"));

        Assert.Equal(ViewerPasswordSealer.PasswordTooLongText, ex.Message);
    }

    /* ═══════════ the migration ═══════════ */

    [Fact]
    public async Task TheMigration_WithNoPasswordToMove_NeedsNoKey_AndLeavesTheRowWithoutOne()
    {
        var row = Row();

        var outcome = await ViewerControlPlaneMigration.SealSmtpPasswordAsync(null!, row, "", TestContext.Current.CancellationToken);

        Assert.Equal(ViewerControlPlaneMigration.SmtpSealOutcome.Done, outcome);
        Assert.Null(row.SmtpEncryptedPassword);
    }

    /* ═══════════ the source: no old-format seal is left for SMTP ═══════════ */

    [Fact]
    public void NoViewerSmtpPath_StillCallsTheOldFormatSeal()
    {
        var viewer = Path.Combine(RepoRoot(), "Darling", "PerformanceMonitor.Darling.Viewer");
        foreach (var file in new[] { "SettingsWindow.xaml.cs", "ViewerControlPlaneMigration.cs" })
        {
            var text = File.ReadAllText(Path.Combine(viewer, file));
            Assert.DoesNotContain("ViewerServerSecret.Protect(", text, StringComparison.Ordinal);
        }
    }

    [Fact]
    public void TheSettingsWindow_SealsThroughTheKeyRead_AndThePathsWithNoWindowPassNoOwner()
    {
        var viewer = Path.Combine(RepoRoot(), "Darling", "PerformanceMonitor.Darling.Viewer");
        var settings = File.ReadAllText(Path.Combine(viewer, "SettingsWindow.xaml.cs"));
        var migration = File.ReadAllText(Path.Combine(viewer, "ViewerControlPlaneMigration.cs"));

        Assert.Contains("ViewerPasswordKey.GetSealKeyAsync(_dataService!, this)", settings, StringComparison.Ordinal);
        Assert.Contains("SealSmtp(", settings, StringComparison.Ordinal);
        Assert.Contains("ViewerPasswordKey.GetSealKeyAsync(dataService, null, cancellationToken)", migration, StringComparison.Ordinal);
        Assert.Contains("SealSmtp(", migration, StringComparison.Ordinal);
    }

    private static string RepoRoot([CallerFilePath] string thisFile = "")
        => Path.GetFullPath(Path.Combine(Path.GetDirectoryName(thisFile)!, "..", ".."));
}

/* #1776 own-store: each fact mints its own scratch database through ScratchPostgres and never touches another
   test's rows, so it is deliberately NOT [Collection("live-postgres")]. */

/// <summary>
/// #5366 against a real store: a sealed SMTP password the Viewer saves reads back exactly as written and is opened by the
/// service for the stored host, port, SSL flag and user name; a value sealed for one host does not open once the stored host
/// is changed underneath it.
/// </summary>
public sealed class ViewerSmtpSealLiveTests
{
    private const string Plain = "p@ss-not-real";

    private static string? BaseConnectionString => Environment.GetEnvironmentVariable("DARLING_TEST_PG");

    private static SmtpConfig ServiceReadOf(NotificationRow stored) => new()
    {
        Host = stored.SmtpHost,
        Port = stored.SmtpPort,
        UseSsl = stored.SmtpUseSsl,
        Username = stored.SmtpUsername ?? "",
        EncryptedPassword = stored.SmtpEncryptedPassword!,
    };

    [Fact]
    public async Task ASavedSealedSmtpPassword_IsOpenedByTheServiceRead_AndNotAfterTheStoredHostChanges()
    {
        var baseConnectionString = BaseConnectionString;
        Assert.SkipWhen(string.IsNullOrEmpty(baseConnectionString), "Set DARLING_TEST_PG to run the live SMTP seal round trip.");

        var ct = TestContext.Current.CancellationToken;
        var scratch = await ScratchPostgres.CreateAsync(baseConnectionString!, ct);
        var bodySucceeded = false;
        try
        {
            await using (var connection = new NpgsqlConnection(scratch.ConnectionString))
            {
                await connection.OpenAsync(ct);
                await PgMigrations.MigrateAsync(connection, ct);
            }

            await using var viewer = new ViewerDataService(scratch.ConnectionString);
            using var key = PasswordPrivateKey.Generate();
            var ring = DarlingPasswordKey.FromPrivateKey(key);

            /* What the Settings window writes: the row's own four values, sealed. */
            var row = new NotificationRow
            {
                SmtpHost = "mail.example.test", SmtpPort = 465, SmtpUseSsl = false, SmtpUsername = "mailer",
                SmtpFromAddress = "alerts@example.test", SmtpRecipients = "dba@example.test",
            };
            row.SmtpEncryptedPassword = new ViewerPasswordSealer(key.PublicKey)
                .SealSmtp(Plain, row.SmtpHost, row.SmtpPort, row.SmtpUseSsl, row.SmtpUsername);
            await viewer.UpsertNotificationAsync(row, ct);

            var stored = await viewer.GetNotificationAsync(ct);
            Assert.NotNull(stored);
            Assert.DoesNotContain(Plain, stored!.SmtpEncryptedPassword, StringComparison.Ordinal);
            Assert.Equal(Plain, DarlingSecrets.ResolveSmtpPassword(ServiceReadOf(stored), ring, LegacyDpapi.Current));

            /* A direct store UPDATE of the host: the value was sealed for the old one. */
            await using (var connection = new NpgsqlConnection(scratch.ConnectionString))
            {
                await connection.OpenAsync(ct);
                await using var update = new NpgsqlCommand("UPDATE config_notification SET smtp_host = 'other-mail.example.test' WHERE id = 1", connection);
                Assert.Equal(1, await update.ExecuteNonQueryAsync(ct));
            }

            var changed = await viewer.GetNotificationAsync(ct);
            Assert.Equal("other-mail.example.test", changed!.SmtpHost);
            var ex = Assert.Throws<InvalidOperationException>(
                () => DarlingSecrets.ResolveSmtpPassword(ServiceReadOf(changed), ring, LegacyDpapi.Current));
            Assert.DoesNotContain(Plain, ex.Message, StringComparison.Ordinal);

            bodySucceeded = true;
        }
        finally
        {
            await LiveStoreCleanup.RunAsync(scratch.ConnectionString, bodySucceeded, async (_, _) => await Task.CompletedTask);
            await scratch.DisposeAsync();
        }
    }
}
