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
using PerformanceMonitor.Darling.Service.Mcp;
using PerformanceMonitor.Darling.Storage;

namespace PerformanceMonitor.Darling.Service;

public static partial class DarlingCliCommands
{
    private const string SetLoginPasswordVerb = "--set-login-password";

    /// <summary>The verb <see cref="SetLoginPasswordAsync"/> handles: save one password, read from stdin, for every enabled
    /// monitored server that connects with one login (#9689). A store moved to another machine, or a reset password key, leaves
    /// every saved password to be entered again, and a fleet that shares one login otherwise needs one entry per server.</summary>
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

    /// <summary>One server that uses the login, what the verb does with it, and why.</summary>
    internal sealed record LoginPasswordPlanLine(MonitoredServer Server, LoginPasswordAction Action, string Reason);

    /// <summary>The one usage line, printed with every argument refusal.</summary>
    public static string SetLoginPasswordUsageText() =>
        $"Usage: {SetLoginPasswordVerb} --username <login> [--dry-run] [--config <path to darling.json>]   (the password is read from stdin, never from an argument)";

    /// <summary>
    /// Parses the arguments STRICTLY: <c>--username</c> is required, and anything that is not a known flag is refused. A
    /// bare value is refused without being repeated, because the likeliest bare value is a password put on the command
    /// line by mistake, and the refusal goes to the terminal and to whatever captures it (#9689).
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
            var hasValue = i + 1 < rest.Length && !rest[i + 1].StartsWith('-');
            if (string.Equals(arg, "--dry-run", StringComparison.OrdinalIgnoreCase))
            {
                dryRun = true;
                continue;
            }

            if (string.Equals(arg, "--username", StringComparison.OrdinalIgnoreCase) && hasValue && username is null)
            {
                username = rest[++i];
                continue;
            }

            if (string.Equals(arg, "--config", StringComparison.OrdinalIgnoreCase) && hasValue && configPath is null)
            {
                configPath = rest[++i];
                continue;
            }

            errorMessage = arg.StartsWith("--", StringComparison.Ordinal)
                ? $"Unknown or incomplete option for {SetLoginPasswordVerb}: {arg}"
                : $"{SetLoginPasswordVerb} takes no bare value (it is not shown here). The password is never an argument: give it on stdin.";
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

    /// <summary>The refusal for an <c>env:</c> or <c>file:</c> reference where the password was expected.</summary>
    internal const string LoginPasswordIsReferenceText =
        "The text on stdin is an env: or file: reference, not a password. This verb saves a password. To use a reference, set it on the server itself.";

    private static readonly UTF8Encoding s_strictPasswordUtf8 = new(false, true);

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
    /// PURE: what the verb does with each server that uses <paramref name="username"/> (compared exactly, as the service
    /// compares a login), in the order given. Servers with another login are not in the answer at all.
    ///
    /// <para>A server whose password comes from an <c>env:</c> or <c>file:</c> reference, or from darling.json, is skipped:
    /// a saved value written beside it would take the place of the source the operator chose. A saved value that opens
    /// with <paramref name="ring"/> and is already <paramref name="password"/> is unchanged. Every other server is set:
    /// one with no saved password, one whose saved value was sealed with another key (a store moved from another machine,
    /// or a key that was reset), one saved in the old Windows format, and one whose saved password is a different one. A
    /// ring that cannot open anything (the key file is not readable by this account, or the key is not ready) makes no
    /// server unchanged, so each is set.</para>
    /// </summary>
    internal static IReadOnlyList<LoginPasswordPlanLine> PlanLoginPassword(
        IEnumerable<MonitoredServer> servers, string username, string password, IPasswordKeyRing ring)
    {
        var plan = new List<LoginPasswordPlanLine>();
        foreach (var server in servers)
        {
            if (!string.Equals(server.Username, username, StringComparison.Ordinal))
            {
                continue;
            }

            if (!server.RequiresResolvedSecret)
            {
                plan.Add(new(server, LoginPasswordAction.Skip, "its authentication uses no saved password"));
                continue;
            }

            var stored = server.EncryptedPassword;
            if (DarlingSecretSource.IsReference(stored))
            {
                var kind = stored!.StartsWith("env:", StringComparison.Ordinal) ? "env:" : "file:";
                plan.Add(new(server, LoginPasswordAction.Skip, $"its password comes from {(kind == "env:" ? "an" : "a")} {kind} reference"));
                continue;
            }

            if (string.IsNullOrWhiteSpace(stored))
            {
                plan.Add(string.IsNullOrWhiteSpace(server.Password)
                    ? new(server, LoginPasswordAction.Set, "no saved password")
                    : new(server, LoginPasswordAction.Skip, "darling.json supplies its password"));
                continue;
            }

            string opened;
            try
            {
                opened = DarlingSecrets.ResolvePassword(server, out _, ring);
            }
            catch (InvalidOperationException)
            {
                plan.Add(new(server, LoginPasswordAction.Set, WhySavedPasswordDoesNotOpen(stored, ring)));
                continue;
            }

            plan.Add(string.Equals(opened, password, StringComparison.Ordinal)
                ? new(server, LoginPasswordAction.Unchanged, "already has this password")
                : new(server, LoginPasswordAction.Set, "the saved password is a different one"));
        }

        return plan;
    }

    private static string WhySavedPasswordDoesNotOpen(string stored, IPasswordKeyRing ring)
    {
        if (!PasswordSeal.IsSealed(stored))
        {
            return "the saved password is in the old Windows format, which does not open here";
        }

        return ring.Status.CanSeal && !string.Equals(PasswordSeal.KeyIdOf(stored), ring.Status.KeyId, StringComparison.Ordinal)
            ? "the saved password was sealed with another password key"
            : "the saved password could not be opened here";
    }

    /// <summary>One line of the list: the server, the login, and what is done with it. Never a password or a stored value.</summary>
    internal static string FormatLoginPasswordLine(LoginPasswordPlanLine line, bool dryRun)
    {
        var action = line.Action switch
        {
            LoginPasswordAction.Set => dryRun ? "would set" : "set",
            LoginPasswordAction.Unchanged => "unchanged",
            _ => "skip",
        };
        return $"  {line.Server.DisplayName}  login {line.Server.Username}  {action} ({line.Reason})";
    }

    private static string CountOf(int count, string one, string many) =>
        count == 1 ? $"1 {one}" : string.Create(CultureInfo.InvariantCulture, $"{count} {many}");

    /// <summary>
    /// <c>--set-login-password --username &lt;login&gt; [--dry-run] [--config &lt;path&gt;]</c> (#9689): reads one password
    /// from stdin and saves it, sealed with the service's password key, for every enabled server that uses the login.
    /// The password is read from stdin only (a pipe, or a hidden prompt at a terminal): never an argument, never an
    /// environment variable, and it is written to no output, error or log line. <c>--dry-run</c> reads the password the
    /// same way, lists what a run would do with each server, and changes nothing.
    /// Exit codes are <see cref="CollectorToggleExitCode"/>'s: 0 done, 1 usage or configuration (which includes no password,
    /// no server with the login, and a password key that cannot seal), 2 a store that cannot be reached or refuses the write.
    /// </summary>
    public static async Task<int> SetLoginPasswordAsync(
        string[] rest, TextReader input, TextWriter output, TextWriter error, CancellationToken cancellationToken,
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

        /* The password is asked for after the configuration is known usable, the way --add-server asks. It goes into the
           seal and nowhere else. */
        var password = (readPassword ?? (label => ReadPasswordFromConsole(label, input, output)))(
            $"Password for login {args.Username} (not shown): ");
        if (LoginPasswordRefusal(password) is { } refusal)
        {
            error.WriteLine(refusal);
            return CollectorToggleExitCode.UsageOrConfig;
        }

        try
        {
            /* The key --add-server seals with: the key file read only, else the key the store publishes (#5366). */
            var sealKey = await DarlingCliSealKey.ResolveAsync(
                config, DarlingConfig.ResolveConfigPath(args.ConfigPath), dataSource, null, cancellationToken);
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
    /// The store half of <see cref="SetLoginPasswordAsync"/>, on a connection with the store owner's rights. The registry
    /// is read and written in one REPEATABLE READ transaction, so the rows written are the rows that were listed: a server
    /// edited or removed by someone else meanwhile fails the transaction, and nothing is changed. Each password is sealed
    /// for the connection settings its row holds, as every saved password is, so it opens for that connection only. The
    /// write is the row's <c>encrypted_password</c> and <c>modified_at</c>. Throws what the store throws.
    /// </summary>
    internal static async Task<int> SetLoginPasswordInStoreAsync(
        NpgsqlDataSource dataSource, DarlingConfig config, IPasswordKeyRing ring, string? sealNotice, SetLoginPasswordArgs args,
        string password, TextWriter output, TextWriter error, CancellationToken cancellationToken)
    {
        await using var connection = await dataSource.OpenConnectionAsync(cancellationToken);
        await using var transaction = await connection.BeginTransactionAsync(IsolationLevel.RepeatableRead, cancellationToken);
        var servers = await StoreConfigProvider.ReadMonitoredServersAsync(connection, config, cancellationToken);
        var plan = PlanLoginPassword(servers, args.Username, password, ring);
        if (plan.Count == 0)
        {
            error.WriteLine($"No enabled server uses the login '{args.Username}'. Nothing was changed.");
            return CollectorToggleExitCode.UsageOrConfig;
        }

        output.WriteLine();
        foreach (var line in plan)
        {
            output.WriteLine(FormatLoginPasswordLine(line, args.DryRun));
        }

        output.WriteLine();
        var toSet = plan.Where(l => l.Action == LoginPasswordAction.Set).ToList();
        var tail = $"{CountOf(plan.Count(l => l.Action == LoginPasswordAction.Unchanged), "server", "servers")} unchanged, "
            + $"{CountOf(plan.Count(l => l.Action == LoginPasswordAction.Skip), "server", "servers")} skipped.";
        if (args.DryRun)
        {
            output.WriteLine($"Dry run: a run would set the saved password for {CountOf(toSet.Count, "server", "servers")}. {tail} Nothing was changed.");
            if (toSet.Count > 0 && !ring.Status.CanSeal)
            {
                output.WriteLine($"A run would stop before it saved anything: {ring.Status.Reason ?? DarlingPasswordKey.NotReadyReason}");
            }

            return CollectorToggleExitCode.Success;
        }

        if (toSet.Count > 0 && !ring.Status.CanSeal)
        {
            error.WriteLine($"No password was saved. {ring.Status.Reason ?? DarlingPasswordKey.NotReadyReason}");
            return CollectorToggleExitCode.UsageOrConfig;
        }

        var changed = 0;
        foreach (var line in toSet)
        {
            string sealedText;
            try
            {
                sealedText = ring.Seal(password, DarlingMcpServerAdminTools.BindingOf(line.Server));
            }
            catch (Exception ex) when (ex is PasswordSealException or ArgumentException)
            {
                error.WriteLine($"The password could not be sealed for server '{line.Server.DisplayName}'. Nothing was changed.");
                return CollectorToggleExitCode.UsageOrConfig;
            }

            await using var update = new NpgsqlCommand(@"
UPDATE config.config_monitored_servers
SET encrypted_password = @sealed, modified_at = (now() AT TIME ZONE 'UTC')
WHERE server_id = @server_id;", connection, transaction);
            update.CommandTimeout = ServiceCommandDeadlines.BootstrapSeconds;
            update.Parameters.AddWithValue("sealed", sealedText);
            update.Parameters.AddWithValue("server_id", line.Server.ServerId);
            changed += await update.ExecuteNonQueryAsync(cancellationToken);
        }

        /* Once every row is written the commit does not take the caller's token: a caller that goes away then must not
           leave the count unknown. */
        await transaction.CommitAsync(CancellationToken.None);
        output.WriteLine($"Set the saved password for {CountOf(changed, "server", "servers")} with the login '{args.Username}'. {tail}");
        if (changed > 0 && ring is DarlingCliSealKey.PublishedKeyRing && sealNotice is not null)
        {
            output.WriteLine(sealNotice);
        }

        return CollectorToggleExitCode.Success;
    }
}
