/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using System.Collections.Generic;
using System.Data;
using System.Globalization;
using System.IO;
using System.Linq;
using System.Text;
using System.Threading;
using System.Threading.Tasks;
using Npgsql;
using PerformanceMonitor.Common;
using PerformanceMonitor.Darling.Service.Mcp;
using PerformanceMonitor.Darling.Storage;

namespace PerformanceMonitor.Darling.Service;

public static partial class DarlingCliCommands
{
    private const string SetLoginPasswordVerb = "--set-login-password";

    /// <summary>The verb <see cref="SetLoginPasswordAsync"/> handles: save one password, read from stdin, for every monitored
    /// server that connects with one login, disabled servers included (#9689). A store moved to another machine, or a reset
    /// password key, leaves every saved password to be entered again, and a fleet that shares one login otherwise needs one
    /// entry per server.</summary>
    public static bool IsSetLoginPasswordVerb(string arg) =>
        string.Equals(arg, SetLoginPasswordVerb, StringComparison.OrdinalIgnoreCase);

    /// <summary>What <c>--set-login-password</c> was asked to do. The password is not here: it is never an argument.</summary>
    internal sealed record SetLoginPasswordArgs(string Username, bool DryRun, string? ConfigPath);

    /// <summary>What the verb does with one server that uses the login.</summary>
    internal enum LoginPasswordAction
    {
        /// <summary>The password is sealed and saved for this server.</summary>
        Set,

        /// <summary>The saved password already opens and is the one given, so the row is left as it is.</summary>
        Unchanged,

        /// <summary>The server's password does not come from a saved value, so no saved value is written over it.</summary>
        Skip,
    }

    /// <summary>Which saved password of a server a plan line is about. A server keeps its own login's password, and may keep a
    /// second saved login beside it, the remediation login, sealed with the same key.</summary>
    internal enum LoginPasswordSlot
    {
        /// <summary>The password the monitoring connection uses.</summary>
        Server,

        /// <summary>The password of the server's remediation login (its own username, its own sealed value).</summary>
        Remediation,
    }

    /// <summary>What the registry read knows beyond the servers themselves: which are switched off, which have no value in the
    /// store's own <c>encrypted_password</c> column (so any value on the server came from darling.json), and which servers
    /// darling.json gives a remediation login with its password (the service writes that into the store again at each start).</summary>
    internal sealed record LoginPasswordContext(
        IReadOnlySet<int> DisabledServerIds, IReadOnlySet<int> MainEmptyInStoreServerIds, IReadOnlySet<int> FileRemediationServerIds)
    {
        /// <summary>No server is switched off and the store holds every value.</summary>
        public static readonly LoginPasswordContext None = new(new HashSet<int>(), new HashSet<int>(), new HashSet<int>());
    }

    /// <summary>One saved password of one server that uses the login, what the verb does with it, and why.
    /// <paramref name="ServerDisabled"/> is true for a server that is switched off: it is set like any other.</summary>
    internal sealed record LoginPasswordPlanLine(
        MonitoredServer Server, LoginPasswordAction Action, string Reason,
        LoginPasswordSlot Slot = LoginPasswordSlot.Server, bool ServerDisabled = false);

    /// <summary>The one usage line, printed with every argument refusal.</summary>
    public static string SetLoginPasswordUsageText() =>
        $"Usage: {SetLoginPasswordVerb} --username <login> [--dry-run] [--config <path to darling.json>]   (the password is read from stdin, never from an argument)";

    /// <summary>
    /// Parses the arguments STRICTLY: <c>--username</c> is required, and anything that is not a known option is refused.
    /// Option names match without regard to capitals, as every other verb's do. A refusal never repeats an argument it does
    /// not know, whether or not it starts with a dash: it names the argument's position and nothing of its text, because the
    /// likeliest stray argument is a password put on the command line by mistake, and the refusal goes to the terminal and to
    /// whatever captures it (#9689).
    /// </summary>
    internal static bool TryParseSetLoginPasswordArgs(string[] rest, out SetLoginPasswordArgs? parsed, out string? errorMessage)
    {
        parsed = null;
        string? username = null;
        string? configPath = null;
        var dryRun = false;
        for (var i = 0; i < rest.Length; i++)
        {
            var arg = rest[i];
            var position = i + 1;
            if (string.Equals(arg, "--dry-run", StringComparison.OrdinalIgnoreCase))
            {
                dryRun = true;
                continue;
            }

            var isUsername = string.Equals(arg, "--username", StringComparison.OrdinalIgnoreCase);
            if (isUsername || string.Equals(arg, "--config", StringComparison.OrdinalIgnoreCase))
            {
                /* The two known options are named by their own constant text, never by the argument as typed. */
                var name = isUsername ? "--username" : "--config";
                if ((isUsername ? username : configPath) is not null)
                {
                    errorMessage = $"{name} is given more than once (argument {position} after {SetLoginPasswordVerb}). Give it one time.";
                    return false;
                }

                if (i + 1 >= rest.Length || rest[i + 1].StartsWith('-'))
                {
                    errorMessage = $"{name} (argument {position} after {SetLoginPasswordVerb}) needs a value after it.";
                    return false;
                }

                if (isUsername)
                {
                    username = rest[++i];
                }
                else
                {
                    configPath = rest[++i];
                }

                continue;
            }

            errorMessage = (arg.StartsWith('-') ? "Unknown option" : "Unexpected argument")
                + $" at position {position} after {SetLoginPasswordVerb}. Its text is not shown, in case it is a password. The password is never an argument: give it on stdin.";
            return false;
        }

        if (string.IsNullOrWhiteSpace(username))
        {
            errorMessage = $"{SetLoginPasswordVerb} needs --username <login>: the login whose servers get the password.";
            return false;
        }

        parsed = new SetLoginPasswordArgs(username, dryRun, configPath);
        errorMessage = null;
        return true;
    }

    /// <summary>The refusal for a run that was given no password.</summary>
    internal const string NoLoginPasswordText =
        "No password was given on stdin. Pipe it in (one line), or type it at the prompt. It is never an argument.";

    /// <summary>The refusal for stdin that holds more than one non-empty line: the password is one line, and a file or a
    /// command that prints more than that is not the password alone. Nothing is changed.</summary>
    internal const string LoginPasswordHasManyLinesText = StrictStdinPassword.HasManyLinesText;

    /// <summary>The refusal for an <c>env:</c> or <c>file:</c> reference where the password was expected.</summary>
    internal const string LoginPasswordIsReferenceText =
        "The text on stdin is an env: or file: reference, not a password. This verb saves a password. To use a reference, set it on the server itself.";

    private static readonly UTF8Encoding s_strictPasswordUtf8 = new(false, true);

    /// <summary>The refusal for stdin that starts with a UTF-8 byte order mark: it would be saved as the first character of the password.</summary>
    internal const string LoginPasswordHasByteOrderMarkText = StrictStdinPassword.HasByteOrderMarkText;

    /// <summary>The refusal for stdin that holds a NUL byte, which UTF-16 text (a Windows pipe that encodes as UTF-16) does.</summary>
    internal const string LoginPasswordHasNulText = StrictStdinPassword.HasNulText;

    /// <summary>The refusal for a carriage return that is not part of a line ending.</summary>
    internal const string LoginPasswordHasStrayReturnText = StrictStdinPassword.HasStrayReturnText;

    /// <summary>The longest password SQL Server takes for a login, in characters (UTF-16 code units). The SQL Server driver refuses
    /// a longer one when the connection is built, for SQL authentication and for a service-principal secret alike.</summary>
    internal const int SqlServerMaxPasswordCharacters = 128;

    /// <summary>The refusal for a password longer than SQL Server takes. It names the limit and never the password.</summary>
    internal const string SqlServerPasswordTooLongText =
        "SQL Server takes a password of at most 128 characters, and this one is longer.";

    /// <summary>
    /// PURE: the refusal for a password meant for a SQL Server target that is longer than SQL Server takes (#9693), or null. A
    /// PostgreSQL target has no such limit: its driver sends any password the reader takes, and a token used as a password there
    /// (an RDS IAM token) is several hundred characters, so the limit is never applied to one.
    /// </summary>
    internal static string? SqlServerPasswordRefusal(string? password, bool targetIsPostgres) =>
        !targetIsPostgres && password is { Length: > SqlServerMaxPasswordCharacters } ? SqlServerPasswordTooLongText : null;

    /// <summary>
    /// Reads the one password line (#9689). At a terminal (<paramref name="inputRedirected"/> false) it is the hidden prompt.
    /// From a pipe or a file it reads <paramref name="stdin"/> as BYTES, never through the console's own text decoding, so the
    /// bytes the operator sent are the characters saved on every platform: at most <see cref="PasswordSeal.MaxPlaintextBytes"/>
    /// plus a line ending are read (more is refused as too long, so a pipe of any size is never held), a byte order mark, a NUL
    /// byte, a lone carriage return and a byte sequence that is not UTF-8 are each refused with a sentence that does not repeat
    /// the text, and the text must hold one non-empty line. Blank lines and the line ending (<c>\n</c> or <c>\r\n</c>) are
    /// nothing. A second non-empty line sets <paramref name="refusal"/> and returns null. The line is used as read, never
    /// trimmed. A pipe is read to its end, so a command that keeps stdin open and prints nothing more makes the verb wait for it
    /// to close.
    /// </summary>
    internal static string? ReadLoginPassword(string prompt, Stream stdin, TextWriter output, bool inputRedirected, out string? refusal)
    {
        refusal = null;
        if (!inputRedirected)
        {
            return ReadPasswordFromConsole(prompt, output);
        }

        return StrictStdinPassword.Read(
            stdin, PasswordSeal.MaxPlaintextBytes,
            DarlingMcpServerAdminTools.PasswordTooLongText + " Nothing was changed.",
            DarlingMcpServerAdminTools.PasswordCharactersText + " Nothing was changed.",
            out refusal);
    }

    /// <summary>
    /// PURE: null when the line read from stdin can be saved as a password, otherwise the sentence to refuse it with. The
    /// sentence never repeats the value. The line is used exactly as read: nothing is trimmed, so a password that ends in
    /// a space keeps it.
    /// </summary>
    internal static string? LoginPasswordRefusal(string? line)
    {
        if (string.IsNullOrEmpty(line))
        {
            return NoLoginPasswordText;
        }

        if (DarlingSecretSource.IsReference(line))
        {
            return LoginPasswordIsReferenceText;
        }

        try
        {
            return s_strictPasswordUtf8.GetByteCount(line) > PasswordSeal.MaxPlaintextBytes
                ? DarlingMcpServerAdminTools.PasswordTooLongText
                : null;
        }
        catch (EncoderFallbackException)
        {
            return DarlingMcpServerAdminTools.PasswordCharactersText;
        }
    }

    /// <summary>
    /// PURE: the reason a run must stop when <paramref name="password"/> is longer than SQL Server takes and a line of
    /// <paramref name="plan"/> would set it for a SQL Server (#9693), or null. Lines that skip or leave a password unchanged, and
    /// lines for a PostgreSQL server, never stop a run. The reason names the limit and how many servers, never the password.
    /// </summary>
    internal static string? LoginPasswordTooLongReason(IEnumerable<LoginPasswordPlanLine> plan, string password)
    {
        var servers = plan
            .Where(l => l.Action == LoginPasswordAction.Set && SqlServerPasswordRefusal(password, l.Server.IsPostgres) is not null)
            .Select(l => l.Server.ServerId)
            .Distinct()
            .Count();
        return servers == 0
            ? null
            : $"{SqlServerPasswordTooLongText} {CountOf(servers, "server that would get it is", "servers that would get it are")} SQL Server.";
    }

    /// <summary>
    /// PURE: what the verb does with each saved password of each server that uses <paramref name="username"/> (compared
    /// exactly, as the service compares a login), in the order given. A server is in the answer for its own login and, on a
    /// line of its own, for its remediation login when that login's name matches too. Servers with another login are not in the
    /// answer at all. A server in <paramref name="disabledServerIds"/> is planned like any other and its lines say it is
    /// switched off.
    ///
    /// <para>A password that comes from an <c>env:</c> or <c>file:</c> reference, or from darling.json, is skipped:
    /// a saved value written beside it would take the place of the source the operator chose. A saved value that opens
    /// with <paramref name="ring"/> and is already <paramref name="password"/> is unchanged. Every other password is set:
    /// one with no saved password, one whose saved value was sealed with another key (a store moved from another machine,
    /// or a key that was reset), one saved in the old Windows format, and one whose saved password is a different one. A
    /// ring that cannot open anything (the key file is not readable by this account, or the key is not ready) makes no
    /// password unchanged, so each is set. A remediation login that has a name and no saved password is skipped: the verb
    /// never arms a remediation login, it only replaces a password that one already has.</para>
    /// </summary>
    internal static IReadOnlyList<LoginPasswordPlanLine> PlanLoginPassword(
        IEnumerable<MonitoredServer> servers, string username, string password, IPasswordKeyRing ring,
        LoginPasswordContext? context = null)
    {
        context ??= LoginPasswordContext.None;
        var plan = new List<LoginPasswordPlanLine>();
        foreach (var server in servers)
        {
            var disabled = context.DisabledServerIds.Contains(server.ServerId);
            if (string.Equals(server.Username, username, StringComparison.Ordinal))
            {
                plan.Add(PlanServerPassword(server, password, ring, context) with { ServerDisabled = disabled });
            }

            if (string.Equals(server.RemediationUsername, username, StringComparison.Ordinal))
            {
                plan.Add(PlanRemediationPassword(server, password, ring, context) with { ServerDisabled = disabled });
            }
        }

        return plan;
    }

    private static LoginPasswordPlanLine PlanServerPassword(
        MonitoredServer server, string password, IPasswordKeyRing ring, LoginPasswordContext context)
    {
        /* Only a SQL login's password is replaced. A service principal carries a client secret in the same slot, and its
           "username" is a client id that can equal a SQL login's name: the secret is not this verb's to replace. */
        if (!server.UsesSqlAuth)
        {
            return new(server, LoginPasswordAction.Skip, server.UsesServicePrincipal
                ? "its authentication is a service principal, and its client secret is not a login password"
                : "its authentication uses no saved password");
        }

        var stored = server.EncryptedPassword;
        if (DarlingSecretSource.IsReference(stored))
        {
            return new(server, LoginPasswordAction.Skip, $"its password comes from {ReferenceKind(stored)}");
        }

        /* A value on the server that the store's own column does not hold was copied in from darling.json at read time, and
           darling.json declares it: a saved value written over it would take its place for good. */
        if (!string.IsNullOrWhiteSpace(stored)
            && (server.EncryptedPasswordDeclaredByFile || context.MainEmptyInStoreServerIds.Contains(server.ServerId)))
        {
            return new(server, LoginPasswordAction.Skip, "darling.json supplies its password");
        }

        if (string.IsNullOrWhiteSpace(stored))
        {
            return string.IsNullOrWhiteSpace(server.Password)
                ? new(server, LoginPasswordAction.Set, "no saved password")
                : new(server, LoginPasswordAction.Skip, "darling.json supplies its password");
        }

        string opened;
        try
        {
            opened = DarlingSecrets.ResolvePassword(server, out _, ring);
        }
        catch (InvalidOperationException)
        {
            return new(server, LoginPasswordAction.Set, WhySavedPasswordDoesNotOpen("the saved password", stored, ring));
        }

        return string.Equals(opened, password, StringComparison.Ordinal)
            ? new(server, LoginPasswordAction.Unchanged, "already has this password")
            : new(server, LoginPasswordAction.Set, "the saved password is a different one");
    }

    /// <summary>The same rule for the server's remediation login: its saved password is sealed with the same key, for its own
    /// connection binding, and is opened and written the same way. The login is never armed here: with no saved password
    /// there is nothing for a lost key to have taken.</summary>
    private static LoginPasswordPlanLine PlanRemediationPassword(
        MonitoredServer server, string password, IPasswordKeyRing ring, LoginPasswordContext context)
    {
        var stored = server.RemediationEncryptedPassword;
        if (string.IsNullOrWhiteSpace(stored))
        {
            return new(server, LoginPasswordAction.Skip, "it has no saved password, and this verb does not arm a remediation login", LoginPasswordSlot.Remediation);
        }

        if (DarlingSecretSource.IsReference(stored))
        {
            return new(server, LoginPasswordAction.Skip, $"its password comes from {ReferenceKind(stored)}", LoginPasswordSlot.Remediation);
        }

        /* A remediation login that darling.json names with its password is written into the store again at each start. */
        if (server.RemediationEncryptedPasswordDeclaredByFile || context.FileRemediationServerIds.Contains(server.ServerId))
        {
            return new(server, LoginPasswordAction.Skip, "darling.json supplies its remediation login", LoginPasswordSlot.Remediation);
        }

        string? opened;
        try
        {
            opened = DarlingSecrets.ResolveRemediationPassword(server, ring);
        }
        catch (InvalidOperationException)
        {
            return new(server, LoginPasswordAction.Set, WhySavedPasswordDoesNotOpen("the saved remediation password", stored, ring), LoginPasswordSlot.Remediation);
        }

        return string.Equals(opened, password, StringComparison.Ordinal)
            ? new(server, LoginPasswordAction.Unchanged, "already has this password", LoginPasswordSlot.Remediation)
            : new(server, LoginPasswordAction.Set, "the saved remediation password is a different one", LoginPasswordSlot.Remediation);
    }

    private static string ReferenceKind(string? stored) =>
        stored!.StartsWith("env:", StringComparison.Ordinal) ? "an env: reference" : "a file: reference";

    private static string WhySavedPasswordDoesNotOpen(string what, string stored, IPasswordKeyRing ring)
    {
        if (!PasswordSeal.IsSealed(stored))
        {
            return $"{what} is in the old Windows format, which does not open here";
        }

        return ring.Status.CanSeal && !string.Equals(PasswordSeal.KeyIdOf(stored), ring.Status.KeyId, StringComparison.Ordinal)
            ? $"{what} was sealed with another password key"
            : $"{what} could not be opened here";
    }

    /// <summary>One line of the list: the server, the login, and what is done with it. A remediation login is named as one,
    /// and a switched-off server says so. Never a password or a stored value.</summary>
    internal static string FormatLoginPasswordLine(LoginPasswordPlanLine line, bool dryRun)
    {
        var action = line.Action switch
        {
            LoginPasswordAction.Set => dryRun ? "would set" : "set",
            LoginPasswordAction.Unchanged => "unchanged",
            _ => "skip",
        };
        var login = line.Slot == LoginPasswordSlot.Remediation
            ? $"remediation login {line.Server.RemediationUsername}"
            : $"login {line.Server.Username}";
        var disabled = line.ServerDisabled ? ", the server is disabled" : "";
        return $"  {line.Server.DisplayName}  {login}  {action} ({line.Reason}{disabled})";
    }

    private static string CountOf(int count, string one, string many) =>
        count == 1 ? $"1 {one}" : string.Create(CultureInfo.InvariantCulture, $"{count} {many}");

    /// <summary>
    /// <c>--set-login-password --username &lt;login&gt; [--dry-run] [--config &lt;path&gt;]</c> (#9689): reads one password
    /// from stdin and saves it, sealed with the service's password key, for every server that uses the login, disabled
    /// servers included, and for the remediation login of a server when that login's name matches. The password is read from
    /// stdin only (a pipe of one non-empty line, or a hidden prompt at a terminal): never an argument, never an
    /// environment variable, and it is written to no output, error or log line. <c>--dry-run</c> reads the password the
    /// same way, lists what a run would do with each server, and changes nothing.
    /// Exit codes are <see cref="CollectorToggleExitCode"/>'s: 0 done, 1 usage or configuration (which includes no password,
    /// more than one line on stdin, no server with the login, and a password key that cannot seal), 2 a store that cannot be reached or refuses the write.
    /// </summary>
    public static async Task<int> SetLoginPasswordAsync(
        string[] rest, Stream input, TextWriter output, TextWriter error, CancellationToken cancellationToken,
        Func<string, string?>? readPassword = null)
    {
        if (!TryParseSetLoginPasswordArgs(rest, out var args, out var argError))
        {
            error.WriteLine(argError);
            output.WriteLine(SetLoginPasswordUsageText());
            return CollectorToggleExitCode.UsageOrConfig;
        }

        var created = TryCreateCollectorStoreDataSource(SetLoginPasswordVerb, args!.ConfigPath, error);
        if (created is null)
        {
            return CollectorToggleExitCode.UsageOrConfig;
        }

        await using var dataSource = created;
        DarlingConfig config;
        try
        {
            config = DarlingConfig.Load(args.ConfigPath);
        }
        catch (Exception ex) when (ex is not OperationCanceledException)
        {
            error.WriteLine($"Could not load configuration: {ex.Message}");
            return CollectorToggleExitCode.UsageOrConfig;
        }

        try
        {
            /* The key --add-server seals with: the key file read only, else the key the store publishes (#5366). */
            var sealKey = await DarlingCliSealKey.ResolveAsync(
                config, DarlingConfig.ResolveConfigPath(args.ConfigPath), dataSource, null, cancellationToken);

            /* Before the password is read: the store this configuration reaches and the key the password will be sealed to. A
               configuration that names a store or a key the operator did not expect is seen here, before a password is spent on
               it, and a dry run shows the same two lines. */
            output.WriteLine(StoreAndKeyLine(dataSource.ConnectionString, sealKey.Ring));

            /* The password is asked for after the configuration is known usable, the way --add-server asks. It goes into the
               seal and nowhere else. */
            var prompt = $"Password for login {args.Username} (not shown): ";
            string? inputRefusal = null;
            var password = readPassword is not null
                ? readPassword(prompt)
                : ReadLoginPassword(prompt, input, output, Console.IsInputRedirected, out inputRefusal);
            if ((inputRefusal ?? LoginPasswordRefusal(password)) is { } refusal)
            {
                error.WriteLine(refusal);
                return CollectorToggleExitCode.UsageOrConfig;
            }

            return await SetLoginPasswordInStoreAsync(
                dataSource, config, sealKey.Ring, sealKey.Notice, args, password!, output, error, cancellationToken);
        }
        catch (Exception ex) when (ex is not OperationCanceledException)
        {
            error.WriteLine($"Could not set the password in the store: {ex.Message} Nothing was changed.");
            return CollectorToggleExitCode.StoreUnavailable;
        }
    }

    /// <summary>
    /// PURE: the line printed before the password is read. It names the store's host, port and database as the configuration
    /// reaches them (read from the connection string without its password) and the password key the password will be sealed
    /// to, or why none can be, so a configuration that points somewhere unexpected is seen before a password is spent on it.
    /// </summary>
    internal static string StoreAndKeyLine(string connectionString, IPasswordKeyRing ring)
    {
        var store = new NpgsqlConnectionStringBuilder(connectionString);
        var where = $"Store {store.Host}:{store.Port}, database {store.Database}.";
        return ring.Status.CanSeal && ring.Status.KeyId is { } keyId
            ? $"{where} The password will be sealed to password key {PasswordSeal.DisplayKeyId(keyId)}."
            : $"{where} No password key is ready to seal with: {ring.Status.Reason ?? DarlingPasswordKey.NotReadyReason}";
    }

    /// <summary>
    /// PURE: the reason a run must stop when the key the password would be sealed to is not the store's current key, or null.
    /// The key is chosen on its own connection before the registry is read, so a key reset between the two leaves a ring for a
    /// retired key, and every value sealed to it would open for nobody. A ring that cannot seal is answered elsewhere.
    /// </summary>
    internal static string? PasswordKeyChangedReason(string? currentKeyId, IPasswordKeyRing ring)
    {
        if (!ring.Status.CanSeal)
        {
            return null;
        }

        return string.Equals(currentKeyId, ring.Status.KeyId, StringComparison.Ordinal)
            ? null
            : "The store's password key is no longer the one this run chose (a password key reset or a new key came in between). Nothing was saved. Run the verb again.";
    }

    /// <summary>
    /// The store half of <see cref="SetLoginPasswordAsync"/>, on a connection with the store owner's rights. The registry
    /// (disabled servers included) is read and written in one REPEATABLE READ transaction, so the rows written are the rows
    /// that were listed: a server edited or removed by someone else meanwhile fails the transaction, and nothing is changed.
    /// Each password is sealed for the connection settings its row holds, as every saved password is, so it opens for that
    /// connection only (a remediation password for its own binding, which names the remediation login). The write is the
    /// slot's <c>encrypted_password</c> or <c>remediation_encrypted_password</c> and the row's <c>modified_at</c>. Throws what
    /// the store throws.
    /// </summary>
    internal static async Task<int> SetLoginPasswordInStoreAsync(
        NpgsqlDataSource dataSource, DarlingConfig config, IPasswordKeyRing ring, string? sealNotice, SetLoginPasswordArgs args,
        string password, TextWriter output, TextWriter error, CancellationToken cancellationToken)
    {
        await using var connection = await dataSource.OpenConnectionAsync(cancellationToken);

        /* The pins are read before the transaction: a store without the pin table, or a role that may not read it, answers with
           an error this read swallows, and that error would leave a transaction it ran inside aborted. */
        var pins = await StoreConfigProvider.ReadLegacyPinsAsync(connection, cancellationToken);
        await using var transaction = await connection.BeginTransactionAsync(IsolationLevel.RepeatableRead, cancellationToken);

        /* The store's current key, read and held (FOR SHARE) for the length of the transaction: a key reset that comes in after
           the key was chosen is seen here, and one that comes in later waits for this run to finish. */
        string? currentKeyId;
        await using (var currentKey = new NpgsqlCommand("SELECT key_id FROM config.password_key WHERE state = 'current' FOR SHARE;", connection, transaction))
        {
            currentKey.CommandTimeout = ServiceCommandDeadlines.BootstrapSeconds;
            currentKeyId = (string?)await currentKey.ExecuteScalarAsync(cancellationToken);
        }

        var disabledServerIds = new HashSet<int>();
        var servers = await StoreConfigProvider.ReadMonitoredServersAsync(connection, config, cancellationToken, pins, disabledServerIds);
        var plan = PlanLoginPassword(
            servers, args.Username, password, ring,
            new LoginPasswordContext(disabledServerIds, await ReadServersWithNoStoredPasswordAsync(connection, transaction, cancellationToken), FileRemediationServerIds(config)));
        if (plan.Count == 0)
        {
            error.WriteLine($"No server uses the login '{args.Username}'. Nothing was changed.");
            return CollectorToggleExitCode.UsageOrConfig;
        }

        output.WriteLine();
        foreach (var line in plan)
        {
            output.WriteLine(FormatLoginPasswordLine(line, args.DryRun));
        }

        output.WriteLine();
        var toSet = plan.Where(l => l.Action == LoginPasswordAction.Set).ToList();

        /* #9693: a password longer than SQL Server takes can never open a SQL Server connection, so it is not saved for one. The
           run stops before anything is written, PostgreSQL servers included, because one run saves one password for the login. */
        var tooLongReason = LoginPasswordTooLongReason(plan, password);
        var tail = $"{CountOf(plan.Count(l => l.Action == LoginPasswordAction.Unchanged), "password", "passwords")} unchanged, "
            + $"{CountOf(plan.Count(l => l.Action == LoginPasswordAction.Skip), "password", "passwords")} skipped.";
        if (args.DryRun)
        {
            output.WriteLine($"Dry run: a run would set {CountOf(toSet.Count, "saved password", "saved passwords")}. {tail} Nothing was changed.");
            if (tooLongReason is not null)
            {
                output.WriteLine($"A run would stop before it saved anything: {tooLongReason}");
            }
            else if (toSet.Count > 0 && !ring.Status.CanSeal)
            {
                output.WriteLine($"A run would stop before it saved anything: {ring.Status.Reason ?? DarlingPasswordKey.NotReadyReason}");
            }
            else if (toSet.Count > 0 && PasswordKeyChangedReason(currentKeyId, ring) is { } keyChanged)
            {
                output.WriteLine($"A run would stop before it saved anything: {keyChanged}");
            }

            return CollectorToggleExitCode.Success;
        }

        if (tooLongReason is not null)
        {
            error.WriteLine($"No password was saved. {tooLongReason}");
            return CollectorToggleExitCode.UsageOrConfig;
        }

        if (toSet.Count > 0 && !ring.Status.CanSeal)
        {
            error.WriteLine($"No password was saved. {ring.Status.Reason ?? DarlingPasswordKey.NotReadyReason}");
            return CollectorToggleExitCode.UsageOrConfig;
        }

        if (toSet.Count > 0 && PasswordKeyChangedReason(currentKeyId, ring) is { } changedKey)
        {
            error.WriteLine(changedKey);
            return CollectorToggleExitCode.UsageOrConfig;
        }

        var changed = 0;
        foreach (var line in toSet)
        {
            var remediation = line.Slot == LoginPasswordSlot.Remediation;
            string sealedText;
            try
            {
                sealedText = ring.Seal(password, remediation ? line.Server.RemediationBinding : DarlingMcpServerAdminTools.BindingOf(line.Server));
            }
            catch (Exception ex) when (ex is PasswordSealException or ArgumentException)
            {
                error.WriteLine($"The password could not be sealed for server '{line.Server.DisplayName}'. Nothing was changed.");
                return CollectorToggleExitCode.UsageOrConfig;
            }

            /* The column is one of two fixed names, never text from the store or the command line. */
            var column = remediation ? "remediation_encrypted_password" : "encrypted_password";
            await using var update = new NpgsqlCommand($@"
UPDATE config.config_monitored_servers
SET {column} = @sealed, modified_at = (now() AT TIME ZONE 'UTC')
WHERE server_id = @server_id;", connection, transaction);
            update.CommandTimeout = ServiceCommandDeadlines.BootstrapSeconds;
            update.Parameters.AddWithValue("sealed", sealedText);
            update.Parameters.AddWithValue("server_id", line.Server.ServerId);
            changed += await update.ExecuteNonQueryAsync(cancellationToken);
        }

        /* Once every row is written the commit does not take the caller's token: a caller that goes away then must not
           leave the count unknown. */
        await transaction.CommitAsync(CancellationToken.None);
        output.WriteLine($"Set {CountOf(changed, "saved password", "saved passwords")} for the login '{args.Username}'. {tail}");
        if (changed > 0 && ring is DarlingCliSealKey.PublishedKeyRing && sealNotice is not null)
        {
            output.WriteLine(sealNotice);
        }

        return CollectorToggleExitCode.Success;
    }

    /// <summary>The ids of the servers whose own <c>encrypted_password</c> column holds nothing. A password the registry read shows
    /// for one of them was copied in from darling.json.</summary>
    private static async Task<IReadOnlySet<int>> ReadServersWithNoStoredPasswordAsync(
        NpgsqlConnection connection, NpgsqlTransaction transaction, CancellationToken cancellationToken)
    {
        var ids = new HashSet<int>();
        await using var command = new NpgsqlCommand(
            "SELECT server_id FROM config.config_monitored_servers WHERE btrim(coalesce(encrypted_password, '')) = '';", connection, transaction);
        command.CommandTimeout = ServiceCommandDeadlines.BootstrapSeconds;
        await using var reader = await command.ExecuteReaderAsync(cancellationToken);
        while (await reader.ReadAsync(cancellationToken))
        {
            ids.Add(reader.GetInt32(0));
        }

        return ids;
    }

    /// <summary>The servers for which darling.json names a remediation login and its password: the service writes that pair into the
    /// store again at each start, so a saved value written here would be replaced.</summary>
    private static HashSet<int> FileRemediationServerIds(DarlingConfig config) =>
        config.Servers
            .Where(f => f is not null && !string.IsNullOrWhiteSpace(f.RemediationUsername) && !string.IsNullOrWhiteSpace(f.RemediationEncryptedPassword))
            .Select(f => f.ServerId)
            .ToHashSet();
}
