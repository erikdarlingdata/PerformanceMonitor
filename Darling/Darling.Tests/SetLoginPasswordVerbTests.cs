/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Threading.Tasks;
using Npgsql;
using PerformanceMonitor.Darling.Service;
using PerformanceMonitor.Darling.Service.Mcp;
using PerformanceMonitor.Darling.Storage;
using Xunit;

namespace Darling.Tests;

/* #1776 own-store: the live fact mints its own scratch database through ScratchPostgres and never touches another
   test's rows, so the class is deliberately NOT [Collection("live-postgres")]. */

/// <summary>
/// <c>--set-login-password</c> (#9689): one password, read from stdin, saved for every server that uses a login, disabled servers included.
/// The pure facts cover the arguments, the password line and the rule for each server. The live fact runs the store half
/// on a store whose saved passwords were sealed with a key this host does not hold, as a store moved from another machine is.
/// </summary>
public sealed class SetLoginPasswordVerbTests
{
    private const string Login = "monitor";
    private const string NewPassword = "n3w-Pass word ";

    private static string? ConnectionString => Environment.GetEnvironmentVariable("DARLING_TEST_PG");

    private static MonitoredServer Server(
        string name, string? stored, string username = Login, string auth = "sql", string? filePassword = null,
        string? remediationUser = null, string? remediationStored = null, int? id = null) =>
        new()
        {
            Name = name, Host = name + ".example", Auth = auth, Username = username, EncryptedPassword = stored, Password = filePassword,
            RemediationUsername = remediationUser, RemediationEncryptedPassword = remediationStored, StoredServerId = id,
        };

    private static string SealFor(MonitoredServer server, IPasswordKeyRing ring, string password) =>
        ring.Seal(password, DarlingMcpServerAdminTools.BindingOf(server));

    [Fact]
    public void TheVerbIsKnown_SoItNeverStartsTheService()
    {
        Assert.True(DarlingCliCommands.IsSetLoginPasswordVerb("--set-login-password"));
        Assert.True(DarlingCliCommands.IsSetLoginPasswordVerb("--SET-LOGIN-PASSWORD"));
        Assert.False(DarlingCliCommands.IsSetLoginPasswordVerb("--set-login"));
        Assert.True(DarlingCliCommands.IsKnownVerb("--set-login-password"));
    }

    [Fact]
    public void Arguments_TakeTheLoginTheDryRunAndTheConfigPath_InAnyOrder()
    {
        Assert.True(DarlingCliCommands.TryParseSetLoginPasswordArgs(
            ["--dry-run", "--config", "/etc/darling/darling.json", "--username", Login], out var parsed, out var error));
        Assert.Null(error);
        Assert.Equal(Login, parsed!.Username);
        Assert.True(parsed.DryRun);
        Assert.Equal("/etc/darling/darling.json", parsed.ConfigPath);

        Assert.True(DarlingCliCommands.TryParseSetLoginPasswordArgs(["--username", Login], out parsed, out _));
        Assert.False(parsed!.DryRun);
        Assert.Null(parsed.ConfigPath);
    }

    [Fact]
    public void OptionNames_MatchWithoutRegardToCapitals_AndTheLoginValueIsKeptAsTyped()
    {
        Assert.True(DarlingCliCommands.TryParseSetLoginPasswordArgs(
            ["--USERNAME", "Monitor", "--Dry-Run", "--Config", "/etc/darling/darling.json"], out var parsed, out var error));
        Assert.Null(error);
        Assert.Equal("Monitor", parsed!.Username);
        Assert.True(parsed.DryRun);
        Assert.Equal("/etc/darling/darling.json", parsed.ConfigPath);
    }

    [Theory]
    [InlineData()]
    [InlineData("--dry-run")]
    [InlineData("--username")]
    [InlineData("--username", "--dry-run")]
    [InlineData("--username", "a", "--username", "b")]
    [InlineData("--username", "a", "--config")]
    [InlineData("--username", "a", "--password", "x")]
    [InlineData("--username", "a", "--name", "x")]
    public void Arguments_ThatAreIncompleteRepeatedOrUnknown_AreRefused(params string[] rest)
    {
        Assert.False(DarlingCliCommands.TryParseSetLoginPasswordArgs(rest, out var parsed, out var error));
        Assert.Null(parsed);
        Assert.False(string.IsNullOrWhiteSpace(error));
    }

    /// <summary>Each shape a password typed as an argument can take, put at each place an argument can be wrong.</summary>
    public static IEnumerable<TheoryDataRow<string[], string>> StrayArguments()
    {
        foreach (var typed in new[] { "hunter2-on-the-command-line", "--hunter2", "-hunter2", "--password=hunter2", "--USERNAME=hunter2", "hunter2 with spaces" })
        {
            yield return new(["--username", Login, typed], typed);
            yield return new([typed, "--username", Login], typed);
            yield return new(["--username", Login, "--dry-run", typed], typed);
            yield return new(["--username", Login, "--config", "/etc/darling/darling.json", typed], typed);
        }
    }

    [Theory]
    [MemberData(nameof(StrayArguments))]
    public void AnArgumentTheParserDoesNotKnow_IsRefused_NamedByPositionOnly_NeverByItsText(string[] rest, string typed)
    {
        Assert.False(DarlingCliCommands.TryParseSetLoginPasswordArgs(rest, out var parsed, out var error));
        Assert.Null(parsed);
        Assert.DoesNotContain(typed, error, StringComparison.Ordinal);
        var position = Array.IndexOf(rest, typed) + 1;
        Assert.Contains($"position {position} ", error, StringComparison.Ordinal);
        Assert.StartsWith(typed.StartsWith('-') ? "Unknown option" : "Unexpected argument", error, StringComparison.Ordinal);
    }

    [Theory]
    [InlineData("--username", "--hunter2")]
    [InlineData("--config", "-hunter2")]
    public void AKnownOptionWhoseValueIsMissing_IsRefused_WithoutRepeatingWhatFollowsIt(string option, string next)
    {
        Assert.False(DarlingCliCommands.TryParseSetLoginPasswordArgs(
            option == "--username" ? [option, next] : ["--username", Login, option, next], out _, out var error));
        Assert.DoesNotContain(next, error, StringComparison.Ordinal);
    }

    [Fact]
    public async Task AStrayArgument_StopsTheVerbBeforeItReadsStdinOrOpensAStore_AndIsInNoOutput()
    {
        const string Typed = "hunter2-on-the-command-line";
        var output = new StringWriter();
        var error = new StringWriter();
        var asked = false;
        var exit = await DarlingCliCommands.SetLoginPasswordAsync(
            ["--username", Login, Typed], Stream.Null, output, error, TestContext.Current.CancellationToken,
            readPassword: _ => { asked = true; return "x"; });
        Assert.Equal(DarlingCliCommands.CollectorToggleExitCode.UsageOrConfig, exit);
        Assert.False(asked);
        Assert.DoesNotContain(Typed, output.ToString() + error.ToString(), StringComparison.Ordinal);
    }

    private static Stream Bytes(string text) => new MemoryStream(System.Text.Encoding.UTF8.GetBytes(text));

    private static string? Read(Stream stdin, out string? refusal) =>
        DarlingCliCommands.ReadLoginPassword("p: ", stdin, TextWriter.Null, inputRedirected: true, out refusal);

    [Theory]
    [InlineData("n3w-Pass word \n", "n3w-Pass word ")]
    [InlineData("n3w-Pass word ", "n3w-Pass word ")]
    [InlineData("pw\r\n", "pw")]
    [InlineData("\n\npw\n\n\n", "pw")]
    [InlineData("  pw  \n", "  pw  ")]
    [InlineData("pässwörd☃\U0001F600\n", "pässwörd☃\U0001F600")]
    public void StdinWithOneNonEmptyLine_IsThePassword_AsReadFromItsBytes_AndATrailingNewlineIsFine(string stdin, string expected)
    {
        var password = Read(Bytes(stdin), out var refusal);
        Assert.Null(refusal);
        Assert.Equal(expected, password);
    }

    [Fact]
    public void StdinAtTheLongestPasswordPlusItsLineEnding_IsAccepted_AndOneByteMoreIsRefusedAsTooLong()
    {
        var longest = new string('p', PasswordSeal.MaxPlaintextBytes);
        Assert.Equal(longest, Read(Bytes(longest + "\r\n"), out var refusal));
        Assert.Null(refusal);

        Assert.Null(Read(Bytes(longest + "\r\n\n"), out refusal));
        Assert.Equal(DarlingMcpServerAdminTools.PasswordTooLongText + " Nothing was changed.", refusal);

        /* A stream far larger than a password is cut off at the cap, not read to its end. */
        var endless = new CountingStream(PasswordSeal.MaxPlaintextBytes * 100);
        Assert.Null(Read(endless, out refusal));
        Assert.NotNull(refusal);
        Assert.True(endless.BytesRead <= PasswordSeal.MaxPlaintextBytes + 3);
    }

    [Theory]
    [InlineData("first-secret\nsecond-secret\n")]
    [InlineData("first-secret\n\nsecond-secret")]
    [InlineData("first-secret\r\nsecond-secret\r\n")]
    [InlineData("\nfirst-secret\n \n")]
    public void StdinWithMoreThanOneNonEmptyLine_IsRefused_WithAReason_AndRepeatsNoLine(string stdin)
    {
        Assert.Null(Read(Bytes(stdin), out var refusal));
        Assert.Equal(DarlingCliCommands.LoginPasswordHasManyLinesText, refusal);
        Assert.DoesNotContain("secret", refusal, StringComparison.Ordinal);
    }

    [Theory]
    [InlineData("secret\rmore")]
    [InlineData("secret\r")]
    public void ALoneCarriageReturn_IsRefused_BecauseItIsNotALineEndingEveryReaderAgreesOn(string stdin)
    {
        Assert.Null(Read(Bytes(stdin), out var refusal));
        Assert.Equal(DarlingCliCommands.LoginPasswordHasStrayReturnText, refusal);
    }

    [Fact]
    public void StdinThatStartsWithAByteOrderMark_IsRefused_NotSavedWithItAsTheFirstCharacter()
    {
        Assert.Null(Read(new MemoryStream([0xEF, 0xBB, 0xBF, (byte)'p', (byte)'w', (byte)'\n']), out var refusal));
        Assert.Equal(DarlingCliCommands.LoginPasswordHasByteOrderMarkText, refusal);
    }

    [Fact]
    public void StdinThatIsNotUtf8_IsRefused_WhetherItIsUtf16WithNulBytesOrBytesNoUtf8SequenceMakes()
    {
        Assert.Null(Read(new MemoryStream(System.Text.Encoding.Unicode.GetBytes("pw\r\n")), out var refusal));
        Assert.Equal(DarlingCliCommands.LoginPasswordHasNulText, refusal);

        Assert.Null(Read(new MemoryStream([(byte)'p', 0xFF, 0xFE, (byte)'w']), out refusal));
        Assert.Equal(DarlingMcpServerAdminTools.PasswordCharactersText + " Nothing was changed.", refusal);
    }

    [Fact]
    public void EmptyStdin_GivesNoPassword_AndNoManyLinesRefusal_SoTheNoPasswordRefusalAnswers()
    {
        foreach (var empty in new[] { "", "\n\n", "\r\n" })
        {
            Assert.Null(Read(Bytes(empty), out var refusal));
            Assert.Null(refusal);
        }

        Assert.Equal(DarlingCliCommands.NoLoginPasswordText, DarlingCliCommands.LoginPasswordRefusal(null));
    }

    /// <summary>A stream of <c>length</c> bytes that counts what was read from it.</summary>
    private sealed class CountingStream(int length) : Stream
    {
        public int BytesRead { get; private set; }

        public override int Read(byte[] buffer, int offset, int count)
        {
            var give = Math.Min(count, length - BytesRead);
            Array.Fill(buffer, (byte)'p', offset, give);
            BytesRead += give;
            return give;
        }

        public override bool CanRead => true;
        public override bool CanSeek => false;
        public override bool CanWrite => false;
        public override long Length => length;
        public override long Position { get => BytesRead; set => throw new NotSupportedException(); }
        public override void Flush() { }
        public override long Seek(long offset, SeekOrigin origin) => throw new NotSupportedException();
        public override void SetLength(long value) => throw new NotSupportedException();
        public override void Write(byte[] buffer, int offset, int count) => throw new NotSupportedException();
    }

    [Fact]
    public void TheLineBeforeThePasswordIsRead_NamesTheStoreAndTheKey_AndNeverThePasswordOfTheConnection()
    {
        using var key = PasswordPrivateKey.Generate();
        var ring = DarlingPasswordKey.FromPrivateKey(key);
        const string Connection = "Host=db.lab.example;Port=6543;Database=darling_lab;Username=owner;Password=connection-secret";

        var line = DarlingCliCommands.StoreAndKeyLine(Connection, ring);
        Assert.Contains("db.lab.example", line, StringComparison.Ordinal);
        Assert.Contains("6543", line, StringComparison.Ordinal);
        Assert.Contains("darling_lab", line, StringComparison.Ordinal);
        Assert.Contains(PasswordSeal.DisplayKeyId(key.PublicKey.KeyId), line, StringComparison.Ordinal);
        Assert.DoesNotContain("connection-secret", line, StringComparison.Ordinal);
        Assert.DoesNotContain("owner", line, StringComparison.Ordinal);

        var notReady = DarlingCliCommands.StoreAndKeyLine(Connection, DarlingPasswordKey.Refusing("The key is not ready."));
        Assert.Contains("db.lab.example", notReady, StringComparison.Ordinal);
        Assert.Contains("The key is not ready.", notReady, StringComparison.Ordinal);
    }

    [Fact]
    public void ARunStops_WhenTheStoresCurrentKeyIsNoLongerTheOneTheRingWasChosenFor()
    {
        using var key = PasswordPrivateKey.Generate();
        using var other = PasswordPrivateKey.Generate();
        var ring = DarlingPasswordKey.FromPrivateKey(key);

        Assert.Null(DarlingCliCommands.PasswordKeyChangedReason(key.PublicKey.KeyId, ring));
        Assert.NotNull(DarlingCliCommands.PasswordKeyChangedReason(other.PublicKey.KeyId, ring));
        Assert.NotNull(DarlingCliCommands.PasswordKeyChangedReason(null, ring));
        /* A ring that cannot seal is answered by its own reason, not this one. */
        Assert.Null(DarlingCliCommands.PasswordKeyChangedReason(other.PublicKey.KeyId, DarlingPasswordKey.Refusing("The key is not ready.")));
    }

    [Fact]
    public void ThePlan_NeverReplacesAServicePrincipalsClientSecret_WhoseClientIdEqualsTheLogin()
    {
        using var key = PasswordPrivateKey.Generate();
        var ring = DarlingPasswordKey.FromPrivateKey(key);
        var principal = Server("principal", null, auth: "serviceprincipal");
        var sql = Server("sql", null);

        var plan = DarlingCliCommands.PlanLoginPassword([principal, sql], Login, NewPassword, ring);

        Assert.Equal(DarlingCliCommands.LoginPasswordAction.Skip, plan.Single(l => l.Server.Name == "principal").Action);
        Assert.Contains("service principal", plan.Single(l => l.Server.Name == "principal").Reason, StringComparison.Ordinal);
        Assert.Equal(DarlingCliCommands.LoginPasswordAction.Set, plan.Single(l => l.Server.Name == "sql").Action);
    }

    [Fact]
    public void ThePlan_NeverWritesOverAPasswordDarlingJsonSupplies_WhetherItIsDeclaredOrWasCopiedInBecauseTheStoreColumnWasEmpty()
    {
        using var key = PasswordPrivateKey.Generate();
        using var foreign = PasswordPrivateKey.Generate();
        var ring = DarlingPasswordKey.FromPrivateKey(key);
        var foreignRing = DarlingPasswordKey.FromPrivateKey(foreign);

        /* A sealed value darling.json declares for the server. */
        var declared = Server("declared", null, id: 9689201);
        declared.EncryptedPassword = SealFor(declared, foreignRing, "old");
        declared.EncryptedPasswordDeclaredByFile = true;
        /* A value the registry read copied from darling.json because the store's own column held nothing. */
        var copied = Server("copied", null, id: 9689202);
        copied.EncryptedPassword = SealFor(copied, foreignRing, "old");
        /* The same value held by the store itself is the verb's to replace. */
        var held = Server("held", null, id: 9689203);
        held.EncryptedPassword = SealFor(held, foreignRing, "old");
        /* A remediation login the file declares, and one the file names with its password. */
        var remediationDeclared = Server("remediation-declared", null, username: "someone-else", remediationUser: Login, id: 9689204);
        remediationDeclared.RemediationEncryptedPassword = foreignRing.Seal("old", remediationDeclared.RemediationBinding);
        remediationDeclared.RemediationEncryptedPasswordDeclaredByFile = true;
        var remediationFromFile = Server("remediation-from-file", null, username: "someone-else", remediationUser: Login, id: 9689205);
        remediationFromFile.RemediationEncryptedPassword = foreignRing.Seal("old", remediationFromFile.RemediationBinding);
        var remediationHeld = Server("remediation-held", null, username: "someone-else", remediationUser: Login, id: 9689206);
        remediationHeld.RemediationEncryptedPassword = foreignRing.Seal("old", remediationHeld.RemediationBinding);

        var context = new DarlingCliCommands.LoginPasswordContext(
            new HashSet<int>(), new HashSet<int> { 9689202 }, new HashSet<int> { 9689205 });
        var plan = DarlingCliCommands.PlanLoginPassword(
            [declared, copied, held, remediationDeclared, remediationFromFile, remediationHeld], Login, NewPassword, ring, context);

        DarlingCliCommands.LoginPasswordAction ActionOf(string name) => plan.Single(l => l.Server.Name == name).Action;
        Assert.Equal(DarlingCliCommands.LoginPasswordAction.Skip, ActionOf("declared"));
        Assert.Equal(DarlingCliCommands.LoginPasswordAction.Skip, ActionOf("copied"));
        Assert.Equal(DarlingCliCommands.LoginPasswordAction.Set, ActionOf("held"));
        Assert.Equal(DarlingCliCommands.LoginPasswordAction.Skip, ActionOf("remediation-declared"));
        Assert.Equal(DarlingCliCommands.LoginPasswordAction.Skip, ActionOf("remediation-from-file"));
        Assert.Equal(DarlingCliCommands.LoginPasswordAction.Set, ActionOf("remediation-held"));
    }

    [Fact]
    public void ThePasswordLine_IsRefusedWhenMissing_AReference_TooLong_OrNotText_AndTheRefusalNeverRepeatsIt()
    {
        Assert.NotNull(DarlingCliCommands.LoginPasswordRefusal(null));
        Assert.NotNull(DarlingCliCommands.LoginPasswordRefusal(""));
        Assert.Null(DarlingCliCommands.LoginPasswordRefusal(NewPassword));
        Assert.Null(DarlingCliCommands.LoginPasswordRefusal(new string('p', PasswordSeal.MaxPlaintextBytes)));

        foreach (var refused in new[] { "env:DARLING_SQL_PASSWORD", "file:/run/secrets/sql", new string('p', PasswordSeal.MaxPlaintextBytes + 1), "bad\uD800text" })
        {
            var refusal = DarlingCliCommands.LoginPasswordRefusal(refused);
            Assert.NotNull(refusal);
            Assert.DoesNotContain(refused, refusal, StringComparison.Ordinal);
        }
    }

    [Fact]
    public void ThePlan_SetsEverySavedValueThatDoesNotOpenOrDiffers_LeavesTheSameOneAlone_AndSkipsWhatIsNotASavedValue()
    {
        using var key = PasswordPrivateKey.Generate();
        using var foreign = PasswordPrivateKey.Generate();
        var ring = DarlingPasswordKey.FromPrivateKey(key);
        var foreignRing = DarlingPasswordKey.FromPrivateKey(foreign);

        var none = Server("none", null);
        var foreignSeal = Server("foreign", null);
        foreignSeal.EncryptedPassword = SealFor(foreignSeal, foreignRing, "old");
        var same = Server("same", null);
        same.EncryptedPassword = SealFor(same, ring, NewPassword);
        var differs = Server("differs", null);
        differs.EncryptedPassword = SealFor(differs, ring, "old");
        var moved = Server("moved", null);
        moved.EncryptedPassword = SealFor(Server("somewhere-else", null), ring, NewPassword);
        var oldFormat = Server("old-format", "AQAAANCMnd8BFdERjHoAwE/Cl+sBAAAA");
        var env = Server("env", "env:DARLING_SQL_PASSWORD");
        var file = Server("file", "file:/run/secrets/sql");
        var fromJson = Server("from-json", null, filePassword: "file:/run/secrets/sql");
        var integrated = Server("integrated", null, auth: "integrated");
        var otherLogin = Server("other-login", null, username: "someone-else");
        var otherCase = Server("other-case", null, username: Login.ToUpperInvariant());

        var plan = DarlingCliCommands.PlanLoginPassword(
            [none, foreignSeal, same, differs, moved, oldFormat, env, file, fromJson, integrated, otherLogin, otherCase], Login, NewPassword, ring);

        var actions = plan.ToDictionary(l => l.Server.Name, l => l.Action, StringComparer.Ordinal);
        Assert.Equal(DarlingCliCommands.LoginPasswordAction.Set, actions["none"]);
        Assert.Equal(DarlingCliCommands.LoginPasswordAction.Set, actions["foreign"]);
        Assert.Equal(DarlingCliCommands.LoginPasswordAction.Unchanged, actions["same"]);
        Assert.Equal(DarlingCliCommands.LoginPasswordAction.Set, actions["differs"]);
        Assert.Equal(DarlingCliCommands.LoginPasswordAction.Set, actions["moved"]);
        Assert.Equal(DarlingCliCommands.LoginPasswordAction.Set, actions["old-format"]);
        Assert.Equal(DarlingCliCommands.LoginPasswordAction.Skip, actions["env"]);
        Assert.Equal(DarlingCliCommands.LoginPasswordAction.Skip, actions["file"]);
        Assert.Equal(DarlingCliCommands.LoginPasswordAction.Skip, actions["from-json"]);
        Assert.Equal(DarlingCliCommands.LoginPasswordAction.Skip, actions["integrated"]);
        Assert.False(actions.ContainsKey("other-login"));
        Assert.False(actions.ContainsKey("other-case"));

        /* No line of the list carries the password or a stored value. */
        var printed = string.Join("\n", plan.Select(l => DarlingCliCommands.FormatLoginPasswordLine(l, dryRun: true)));
        Assert.DoesNotContain(NewPassword, printed, StringComparison.Ordinal);
        Assert.DoesNotContain("sealed:", printed, StringComparison.Ordinal);
        Assert.DoesNotContain("/run/secrets/sql", printed, StringComparison.Ordinal);
        Assert.DoesNotContain("DARLING_SQL_PASSWORD", printed, StringComparison.Ordinal);
    }

    [Fact]
    public void ThePlan_WithAKeyThatOpensNothing_SetsEverySavedValue_AndNeverCallsOneUnchanged()
    {
        using var key = PasswordPrivateKey.Generate();
        var ring = DarlingPasswordKey.FromPrivateKey(key);
        var same = Server("same", null);
        same.EncryptedPassword = SealFor(same, ring, NewPassword);
        var reference = Server("file", "file:/run/secrets/sql");

        foreach (var cannotOpen in new IPasswordKeyRing[] { new DarlingCliSealKey.PublishedKeyRing(key.PublicKey), DarlingPasswordKey.Refusing("The key is not ready.") })
        {
            var plan = DarlingCliCommands.PlanLoginPassword([same, reference], Login, NewPassword, cannotOpen);
            Assert.Equal(DarlingCliCommands.LoginPasswordAction.Set, plan.Single(l => l.Server.Name == "same").Action);
            Assert.Equal(DarlingCliCommands.LoginPasswordAction.Skip, plan.Single(l => l.Server.Name == "file").Action);
        }
    }

    [Fact]
    public void ThePlan_IncludesADisabledServer_AndItsLineSaysTheServerIsDisabled()
    {
        using var key = PasswordPrivateKey.Generate();
        var ring = DarlingPasswordKey.FromPrivateKey(key);
        var on = Server("on", null, id: 9689101);
        var off = Server("off", null, id: 9689102);
        var offByReference = Server("off-by-reference", "file:/run/secrets/sql", id: 9689103);
        var context = new DarlingCliCommands.LoginPasswordContext(new HashSet<int> { 9689102, 9689103 }, new HashSet<int>(), new HashSet<int>());

        var plan = DarlingCliCommands.PlanLoginPassword([on, off, offByReference], Login, NewPassword, ring, context);

        Assert.Equal(DarlingCliCommands.LoginPasswordAction.Set, plan.Single(l => l.Server.Name == "on").Action);
        Assert.Equal(DarlingCliCommands.LoginPasswordAction.Set, plan.Single(l => l.Server.Name == "off").Action);
        Assert.Equal(DarlingCliCommands.LoginPasswordAction.Skip, plan.Single(l => l.Server.Name == "off-by-reference").Action);
        var lines = plan.ToDictionary(l => l.Server.Name, l => DarlingCliCommands.FormatLoginPasswordLine(l, dryRun: false), StringComparer.Ordinal);
        Assert.DoesNotContain("disabled", lines["on"], StringComparison.Ordinal);
        Assert.Contains("the server is disabled", lines["off"], StringComparison.Ordinal);
        Assert.Contains("the server is disabled", lines["off-by-reference"], StringComparison.Ordinal);
    }

    [Fact]
    public void ThePlan_CoversARemediationLoginWhoseNameMatches_OnItsOwnLine_UnderTheSameRule()
    {
        using var key = PasswordPrivateKey.Generate();
        using var foreign = PasswordPrivateKey.Generate();
        var ring = DarlingPasswordKey.FromPrivateKey(key);
        var foreignRing = DarlingPasswordKey.FromPrivateKey(foreign);

        var both = Server("both", null, remediationUser: Login);
        both.RemediationEncryptedPassword = foreignRing.Seal("old", both.RemediationBinding);
        var onlyRemediation = Server("only-remediation", null, username: "someone-else", remediationUser: Login);
        onlyRemediation.RemediationEncryptedPassword = foreignRing.Seal("old", onlyRemediation.RemediationBinding);
        var same = Server("same", null, remediationUser: Login);
        same.RemediationEncryptedPassword = ring.Seal(NewPassword, same.RemediationBinding);
        var differs = Server("differs", null, remediationUser: Login);
        differs.RemediationEncryptedPassword = ring.Seal("old", differs.RemediationBinding);
        var reference = Server("reference", null, remediationUser: Login, remediationStored: "env:DARLING_REMEDIATION_PASSWORD");
        var unarmed = Server("unarmed", null, remediationUser: Login);
        var otherName = Server("other-name", null, remediationUser: "someone-else");
        otherName.RemediationEncryptedPassword = foreignRing.Seal("old", otherName.RemediationBinding);
        var otherCase = Server("other-case", null, username: "someone-else", remediationUser: Login.ToUpperInvariant());
        otherCase.RemediationEncryptedPassword = foreignRing.Seal("old", otherCase.RemediationBinding);

        var plan = DarlingCliCommands.PlanLoginPassword([both, onlyRemediation, same, differs, reference, unarmed, otherName, otherCase], Login, NewPassword, ring);

        (string Name, DarlingCliCommands.LoginPasswordSlot Slot)[] keyed = plan.Select(l => (l.Server.Name, l.Slot)).ToArray();
        DarlingCliCommands.LoginPasswordAction ActionOf(string name, DarlingCliCommands.LoginPasswordSlot slot) =>
            plan.Single(l => l.Server.Name == name && l.Slot == slot).Action;
        const DarlingCliCommands.LoginPasswordSlot Remediation = DarlingCliCommands.LoginPasswordSlot.Remediation;
        const DarlingCliCommands.LoginPasswordSlot Main = DarlingCliCommands.LoginPasswordSlot.Server;

        /* A server whose own login and remediation login both match has two lines, each set. */
        Assert.Equal(DarlingCliCommands.LoginPasswordAction.Set, ActionOf("both", Main));
        Assert.Equal(DarlingCliCommands.LoginPasswordAction.Set, ActionOf("both", Remediation));
        /* A server whose own login is another name is in the answer for its remediation login alone. */
        Assert.DoesNotContain(("only-remediation", Main), keyed);
        Assert.Equal(DarlingCliCommands.LoginPasswordAction.Set, ActionOf("only-remediation", Remediation));
        Assert.Equal(DarlingCliCommands.LoginPasswordAction.Unchanged, ActionOf("same", Remediation));
        Assert.Equal(DarlingCliCommands.LoginPasswordAction.Set, ActionOf("differs", Remediation));
        Assert.Equal(DarlingCliCommands.LoginPasswordAction.Skip, ActionOf("reference", Remediation));
        /* A remediation login with a name and no saved password is not armed by this verb. */
        Assert.Equal(DarlingCliCommands.LoginPasswordAction.Skip, ActionOf("unarmed", Remediation));
        /* Another name, or the same name in other capitals, is not covered. */
        Assert.DoesNotContain(keyed, k => k.Name is "other-name" or "other-case" && k.Slot == Remediation);

        var printed = string.Join("\n", plan.Select(l => DarlingCliCommands.FormatLoginPasswordLine(l, dryRun: true)));
        Assert.Contains("remediation login " + Login, printed, StringComparison.Ordinal);
        Assert.DoesNotContain(NewPassword, printed, StringComparison.Ordinal);
        Assert.DoesNotContain("DARLING_REMEDIATION_PASSWORD", printed, StringComparison.Ordinal);
    }

    [Fact]
    public async Task OnAStoreWhoseSavedPasswordsWereSealedWithAnotherKey_TheDryRunListsAndChangesNothing_AndTheRunSealsThemWithThisKey()
    {
        Assert.SkipWhen(string.IsNullOrEmpty(ConnectionString), "Set DARLING_TEST_PG to run --set-login-password against a scratch store.");
        var ct = TestContext.Current.CancellationToken;
        await using var scratch = await ScratchPostgres.CreateAsync(ConnectionString!, ct);
        await using var source = NpgsqlDataSource.Create(scratch.ConnectionString);
        var bodySucceeded = false;
        try
        {
            /* The store publishes this host's key, as a start after --reset-password-key leaves it. The saved passwords were
               sealed with the key of the machine the store came from, which this host does not hold. */
            using var key = PasswordPrivateKey.Generate();
            using var foreign = PasswordPrivateKey.Generate();
            var ring = DarlingPasswordKey.FromPrivateKey(key);
            var foreignRing = DarlingPasswordKey.FromPrivateKey(foreign);
            await using (var publish = await source.OpenConnectionAsync(ct))
            {
                await DarlingPasswordKeyStore.PublishAsync(publish, key.PublicKey.KeyId, key.PublicKey.Spki, ct);
            }

            var foreignText = SealFor(Server("any", null), foreignRing, "the old machine's password");
            /* The remediation login of "remediated" is the shared login, sealed with the foreign key for no binding this host can open. */
            var rows = new (int Id, string Name, string Username, string? Stored, bool Enabled, string? RemediationUser, string? RemediationStored)[]
            {
                (9689001, "moved-a", Login, foreignText, true, null, null),
                (9689002, "moved-b", Login, foreignText, true, null, null),
                (9689003, "never-saved", Login, null, true, null, null),
                (9689004, "by-reference", Login, "file:/run/secrets/sql", true, null, null),
                (9689005, "other-login", "someone-else", foreignText, true, null, null),
                (9689006, "switched-off", Login, foreignText, false, null, null),
                (9689007, "remediated", "someone-else", foreignText, true, Login, foreignText),
                (9689008, "other-remediation", "someone-else", foreignText, true, "someone-else", foreignText),
            };
            await using (var seed = await source.OpenConnectionAsync(ct))
            {
                foreach (var row in rows)
                {
                    await using var insert = new NpgsqlCommand(@"
INSERT INTO config.config_monitored_servers (server_id, name, host, auth, username, encrypted_password, is_enabled, remediation_username, remediation_encrypted_password)
VALUES (@id, @name, @host, 'sql', @username, @stored, @enabled, @remediation_user, @remediation_stored);", seed);
                    insert.Parameters.AddWithValue("id", row.Id);
                    insert.Parameters.AddWithValue("name", row.Name);
                    insert.Parameters.AddWithValue("host", row.Name + ".example");
                    insert.Parameters.AddWithValue("username", row.Username);
                    insert.Parameters.AddWithValue("stored", (object?)row.Stored ?? DBNull.Value);
                    insert.Parameters.AddWithValue("enabled", row.Enabled);
                    insert.Parameters.AddWithValue("remediation_user", (object?)row.RemediationUser ?? DBNull.Value);
                    insert.Parameters.AddWithValue("remediation_stored", (object?)row.RemediationStored ?? DBNull.Value);
                    await insert.ExecuteNonQueryAsync(ct);
                }
            }

            var config = new DarlingConfig();
            var before = await ReadStoredAsync(source, ct);

            /* The dry run lists the passwords it would set (the disabled server's and the remediation login's among them) and the one
               it skips, and writes nothing. */
            var dryOutput = new StringWriter();
            var dryError = new StringWriter();
            var dryExit = await DarlingCliCommands.SetLoginPasswordInStoreAsync(
                source, config, ring, null, new DarlingCliCommands.SetLoginPasswordArgs(Login, DryRun: true, null), NewPassword, dryOutput, dryError, ct);
            Assert.Equal(DarlingCliCommands.CollectorToggleExitCode.Success, dryExit);
            Assert.Equal(before, await ReadStoredAsync(source, ct));
            foreach (var listed in new[] { "moved-a", "moved-b", "never-saved", "by-reference", "switched-off", "remediated" })
            {
                Assert.Contains(listed, dryOutput.ToString(), StringComparison.Ordinal);
            }

            Assert.Contains("the server is disabled", dryOutput.ToString(), StringComparison.Ordinal);
            Assert.Contains("remediation login " + Login, dryOutput.ToString(), StringComparison.Ordinal);
            Assert.DoesNotContain("other-login", dryOutput.ToString(), StringComparison.Ordinal);
            Assert.DoesNotContain("other-remediation", dryOutput.ToString(), StringComparison.Ordinal);

            /* A key that is not ready: the dry run still lists, the run refuses, and neither writes. */
            var notReady = DarlingPasswordKey.Refusing("The key is not ready.");
            var args = new DarlingCliCommands.SetLoginPasswordArgs(Login, DryRun: false, null);
            Assert.Equal(DarlingCliCommands.CollectorToggleExitCode.Success, await DarlingCliCommands.SetLoginPasswordInStoreAsync(
                source, config, notReady, null, args with { DryRun = true }, NewPassword, new StringWriter(), new StringWriter(), ct));
            Assert.Equal(DarlingCliCommands.CollectorToggleExitCode.UsageOrConfig, await DarlingCliCommands.SetLoginPasswordInStoreAsync(
                source, config, notReady, null, args, NewPassword, new StringWriter(), new StringWriter(), ct));
            Assert.Equal(before, await ReadStoredAsync(source, ct));

            /* A login no enabled server uses changes nothing and is a usage failure. */
            Assert.Equal(DarlingCliCommands.CollectorToggleExitCode.UsageOrConfig, await DarlingCliCommands.SetLoginPasswordInStoreAsync(
                source, config, ring, null, args with { Username = "nobody" }, NewPassword, new StringWriter(), new StringWriter(), ct));

            /* The run seals the servers' passwords, the disabled server's and the remediation login's with this host's key. */
            var output = new StringWriter();
            var error = new StringWriter();
            var exit = await DarlingCliCommands.SetLoginPasswordInStoreAsync(source, config, ring, null, args, NewPassword, output, error, ct);
            Assert.Equal(DarlingCliCommands.CollectorToggleExitCode.Success, exit);
            var after = await ReadStoredAsync(source, ct);
            foreach (var untouched in new[] { 9689004, 9689005, 9689008 })
            {
                Assert.Equal(before[untouched], after[untouched]);
            }

            await using (var read = await source.OpenConnectionAsync(ct))
            {
                var servers = await StoreConfigProvider.ReadMonitoredServersAsync(read, config, ct, null, new HashSet<int>());
                foreach (var name in new[] { "moved-a", "moved-b", "never-saved", "switched-off" })
                {
                    var server = servers.Single(s => s.Name == name);
                    Assert.Equal(NewPassword, DarlingSecrets.ResolvePassword(server, out _, ring));
                }

                /* The remediation login's password opens with this host's key, and the server's own login keeps its value. */
                var remediated = servers.Single(s => s.Name == "remediated");
                Assert.Equal(NewPassword, DarlingSecrets.ResolveRemediationPassword(remediated, ring));
                Assert.Equal(foreignText, remediated.EncryptedPassword);
            }

            /* The password is in no line the verb wrote. */
            Assert.DoesNotContain(NewPassword, dryOutput.ToString() + dryError.ToString() + output.ToString() + error.ToString(), StringComparison.Ordinal);

            /* A second run finds them all saved and writes no row. */
            var again = await DarlingCliCommands.SetLoginPasswordInStoreAsync(source, config, ring, null, args, NewPassword, new StringWriter(), new StringWriter(), ct);
            Assert.Equal(DarlingCliCommands.CollectorToggleExitCode.Success, again);
            Assert.Equal(after, await ReadStoredAsync(source, ct));
            bodySucceeded = true;
        }
        finally
        {
            await LiveStoreCleanup.RunAsync(scratch.ConnectionString, bodySucceeded, (_, _) => Task.CompletedTask);
        }
    }

    /// <summary>#9693: the stop inside the store run. A password one character longer than SQL Server takes stops the run before
    /// a row is written, with exit code 1 and the limit named, and the dry run says a run would stop. A password of exactly the
    /// limit is saved.</summary>
    [Fact]
    public async Task OnAStore_APasswordLongerThanSqlServerTakes_StopsBeforeARowIsWritten_AndOneAtTheLimitIsSaved()
    {
        Assert.SkipWhen(string.IsNullOrEmpty(ConnectionString), "Set DARLING_TEST_PG to run --set-login-password against a scratch store.");
        var ct = TestContext.Current.CancellationToken;
        await using var scratch = await ScratchPostgres.CreateAsync(ConnectionString!, ct);
        await using var source = NpgsqlDataSource.Create(scratch.ConnectionString);
        var bodySucceeded = false;
        try
        {
            using var key = PasswordPrivateKey.Generate();
            var ring = DarlingPasswordKey.FromPrivateKey(key);
            await using (var setup = await source.OpenConnectionAsync(ct))
            {
                await DarlingPasswordKeyStore.PublishAsync(setup, key.PublicKey.KeyId, key.PublicKey.Spki, ct);
                await using var insert = new NpgsqlCommand(@"
INSERT INTO config.config_monitored_servers (server_id, name, host, auth, username, is_enabled)
VALUES (9693301, 'long-password', 'long-password.example', 'sql', @username, TRUE);", setup);
                insert.Parameters.AddWithValue("username", Login);
                await insert.ExecuteNonQueryAsync(ct);
            }

            var before = await ReadStoredAsync(source, ct);
            var tooLong = new string('p', DarlingCliCommands.SqlServerMaxPasswordCharacters + 1);
            var args = new DarlingCliCommands.SetLoginPasswordArgs(Login, DryRun: false, null);

            var output = new StringWriter();
            var error = new StringWriter();
            var exit = await DarlingCliCommands.SetLoginPasswordInStoreAsync(source, new DarlingConfig(), ring, null, args, tooLong, output, error, ct);

            Assert.Equal(DarlingCliCommands.CollectorToggleExitCode.UsageOrConfig, exit);
            Assert.Contains(DarlingCliCommands.SqlServerPasswordTooLongText, error.ToString(), StringComparison.Ordinal);
            Assert.DoesNotContain(tooLong, output.ToString() + error.ToString(), StringComparison.Ordinal);
            Assert.Equal(before, await ReadStoredAsync(source, ct));

            var dryOutput = new StringWriter();
            var dryExit = await DarlingCliCommands.SetLoginPasswordInStoreAsync(
                source, new DarlingConfig(), ring, null, new DarlingCliCommands.SetLoginPasswordArgs(Login, DryRun: true, null), tooLong, dryOutput, new StringWriter(), ct);
            Assert.Equal(DarlingCliCommands.CollectorToggleExitCode.Success, dryExit);
            Assert.Contains(DarlingCliCommands.SqlServerPasswordTooLongText, dryOutput.ToString(), StringComparison.Ordinal);
            Assert.Equal(before, await ReadStoredAsync(source, ct));

            var atTheLimit = new string('p', DarlingCliCommands.SqlServerMaxPasswordCharacters);
            var saved = await DarlingCliCommands.SetLoginPasswordInStoreAsync(source, new DarlingConfig(), ring, null, args, atTheLimit, new StringWriter(), new StringWriter(), ct);
            Assert.Equal(DarlingCliCommands.CollectorToggleExitCode.Success, saved);
            await using (var read = await source.OpenConnectionAsync(ct))
            {
                var server = (await StoreConfigProvider.ReadMonitoredServersAsync(read, new DarlingConfig(), ct)).Single(s => s.Name == "long-password");
                Assert.Equal(atTheLimit, DarlingSecrets.ResolvePassword(server, out _, ring));
            }

            bodySucceeded = true;
        }
        finally
        {
            await LiveStoreCleanup.RunAsync(scratch.ConnectionString, bodySucceeded, (_, _) => Task.CompletedTask);
        }
    }

    [Fact]
    public async Task OnAStoreWithoutTheLegacyPinTable_TheRunStillSets_BecauseTheSwallowedPinReadDoesNotAbortItsTransaction()
    {
        Assert.SkipWhen(string.IsNullOrEmpty(ConnectionString), "Set DARLING_TEST_PG to run --set-login-password against a scratch store.");
        var ct = TestContext.Current.CancellationToken;
        await using var scratch = await ScratchPostgres.CreateAsync(ConnectionString!, ct);
        await using var source = NpgsqlDataSource.Create(scratch.ConnectionString);
        var bodySucceeded = false;
        try
        {
            using var key = PasswordPrivateKey.Generate();
            var ring = DarlingPasswordKey.FromPrivateKey(key);
            await using (var setup = await source.OpenConnectionAsync(ct))
            {
                await DarlingPasswordKeyStore.PublishAsync(setup, key.PublicKey.KeyId, key.PublicKey.Spki, ct);
                await using var drop = new NpgsqlCommand("DROP TABLE IF EXISTS config.legacy_secret_pin CASCADE;", setup);
                await drop.ExecuteNonQueryAsync(ct);
                await using var insert = new NpgsqlCommand(@"
INSERT INTO config.config_monitored_servers (server_id, name, host, auth, username, is_enabled)
VALUES (9689301, 'no-pin-table', 'no-pin-table.example', 'sql', @username, TRUE);", setup);
                insert.Parameters.AddWithValue("username", Login);
                await insert.ExecuteNonQueryAsync(ct);
            }

            var output = new StringWriter();
            var error = new StringWriter();
            var exit = await DarlingCliCommands.SetLoginPasswordInStoreAsync(
                source, new DarlingConfig(), ring, null, new DarlingCliCommands.SetLoginPasswordArgs(Login, DryRun: false, null), NewPassword, output, error, ct);
            Assert.Equal(DarlingCliCommands.CollectorToggleExitCode.Success, exit);
            Assert.Equal("", error.ToString());

            await using (var read = await source.OpenConnectionAsync(ct))
            {
                var server = (await StoreConfigProvider.ReadMonitoredServersAsync(read, new DarlingConfig(), ct)).Single(s => s.Name == "no-pin-table");
                Assert.Equal(NewPassword, DarlingSecrets.ResolvePassword(server, out _, ring));
            }

            bodySucceeded = true;
        }
        finally
        {
            await LiveStoreCleanup.RunAsync(scratch.ConnectionString, bodySucceeded, (_, _) => Task.CompletedTask);
        }
    }

    /// <summary>Each server's stored value and change stamp, as text, so two reads compare row for row.</summary>
    private static async Task<Dictionary<int, string>> ReadStoredAsync(NpgsqlDataSource source, System.Threading.CancellationToken ct)
    {
        var stored = new Dictionary<int, string>();
        await using var connection = await source.OpenConnectionAsync(ct);
        await using var command = new NpgsqlCommand(
            "SELECT server_id, coalesce(encrypted_password, '<null>') || ' / ' || coalesce(remediation_encrypted_password, '<null>') || ' @ ' || coalesce(modified_at::text, '<null>') FROM config.config_monitored_servers;", connection);
        await using var reader = await command.ExecuteReaderAsync(ct);
        while (await reader.ReadAsync(ct))
        {
            stored[reader.GetInt32(0)] = reader.GetString(1);
        }

        return stored;
    }

    /* ---- a password longer than SQL Server takes (#9693) ---- */

    [Theory]
    [InlineData("sqlserver", 128, false)]
    [InlineData("sqlserver", 129, true)]
    [InlineData("postgresql", 129, false)]
    [InlineData("postgresql", 4000, false)]
    public void APasswordOver128Characters_StopsARunThatWouldSetItForASqlServer_AndNotForPostgres(string engine, int length, bool stops)
    {
        using var key = PasswordPrivateKey.Generate();
        var ring = DarlingPasswordKey.FromPrivateKey(key);
        var password = new string('k', length);
        var server = Server("target", null);
        server.Engine = engine;

        var plan = DarlingCliCommands.PlanLoginPassword([server], Login, password, ring);
        var reason = DarlingCliCommands.LoginPasswordTooLongReason(plan, password);

        Assert.Equal(DarlingCliCommands.LoginPasswordAction.Set, Assert.Single(plan).Action);
        Assert.Equal(stops, reason is not null);
        Assert.DoesNotContain(password, reason ?? "", StringComparison.Ordinal);
    }

    [Fact]
    public void ALongPassword_DoesNotStopARunWhoseOnlySqlServerIsSkipped()
    {
        using var key = PasswordPrivateKey.Generate();
        var ring = DarlingPasswordKey.FromPrivateKey(key);
        var password = new string('k', 200);
        var postgres = Server("pg", null);
        postgres.Engine = "postgresql";
        var byReference = Server("sql-by-reference", "env:SOME_VARIABLE");

        var plan = DarlingCliCommands.PlanLoginPassword([postgres, byReference], Login, password, ring);

        Assert.Equal(DarlingCliCommands.LoginPasswordAction.Skip, plan.Single(l => l.Server.Name == "sql-by-reference").Action);
        Assert.Null(DarlingCliCommands.LoginPasswordTooLongReason(plan, password));
    }
}
