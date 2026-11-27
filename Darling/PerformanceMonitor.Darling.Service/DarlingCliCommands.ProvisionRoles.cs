/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using System.Collections.Generic;
using System.Globalization;
using System.IO;
using System.Text;
using System.Threading;
using System.Threading.Tasks;
using Npgsql;
using PerformanceMonitor.Darling.Storage;

namespace PerformanceMonitor.Darling.Service;

/// <summary>The exit codes of <c>--prepare-provision-roles</c> (#8779). Code 3 is psql's guard stop in the script this verb prepares.</summary>
internal static class ProvisionRolesExitCodes
{
    internal const int Prepared = 0;

    /// <summary>Unexpected: the store refuses the login, the shipped script no longer matches the renderer, a file cannot be written.</summary>
    internal const int Failed = 1;

    /// <summary>Bad arguments or input files.</summary>
    internal const int Usage = 2;

    internal const int SchemaWaitTimedOut = 4;
    internal const int OwnershipRefused = 5;
}

public static partial class DarlingCliCommands
{
    /// <summary>The CA file the chart mounts for the store; the only Root Certificate a connection-string login may name.</summary>
    private const string ChartStoreCaPath = "/etc/darling/store-ca/ca.crt";

    private static readonly string[] ProvisionRolesRoleNames = ["admin", "viewer", "web", "mcp"];

    /// <summary>The verb <see cref="PrepareProvisionRolesAsync"/> handles (#8779).</summary>
    public static bool IsPrepareProvisionRolesVerb(string arg) =>
        string.Equals(arg, "--prepare-provision-roles", StringComparison.OrdinalIgnoreCase);

    internal enum ProvisionRolesLoginKind
    {
        Password,
        ConnectionString,
    }

    /// <summary>What <c>--prepare-provision-roles</c> was asked for. The host options are set exactly when <see cref="LoginKind"/> is
    /// <see cref="ProvisionRolesLoginKind.Password"/> (<see cref="RootCertificate"/> is optional there).</summary>
    internal sealed record PrepareProvisionRolesArgs(
        string LoginFile, ProvisionRolesLoginKind LoginKind, string PasswordsDirectory, string OutDirectory, int WaitSeconds,
        string? Host, int? Port, string? Database, string? Username, SslMode? SslMode, string? RootCertificate);

    /// <summary>The login the verb connects with and writes into pgpass and the libpq env file.</summary>
    internal sealed record ProvisionRolesLogin(
        string Host, int Port, string Database, string Username, string Password, SslMode SslMode, string? RootCertificate, string? SearchPath = null);

    /// <summary>The grammar the verb prints when its arguments are wrong. Pure ASCII.</summary>
    public static string PrepareProvisionRolesUsageText() =>
        "Usage:" + Environment.NewLine +
        "  --prepare-provision-roles --login <file> --login-kind password|connection-string --passwords <dir> --out <dir> --wait-seconds <n>" + Environment.NewLine +
        "      [--host <host> --port <port> --database <name> --username <login> --ssl-mode <mode> [--root-certificate <path>]]" + Environment.NewLine +
        "      Waits for the store schema, checks the login owns the collect and config objects, then writes provision-roles.sql, pgpass and" + Environment.NewLine +
        "      libpq.env (mode 0600) into --out for psql. --login is a file holding the owner's password (--login-kind password) or a connection" + Environment.NewLine +
        "      string (--login-kind connection-string). The host options are required with password and refused with connection-string." + Environment.NewLine +
        "      --passwords is a directory with a file each named admin, viewer, web and mcp. --ssl-mode is an Npgsql name: Disable, Allow," + Environment.NewLine +
        "      Prefer, Require, VerifyCA or VerifyFull. Prints no password.";

    /// <summary>Parses the verb's arguments strictly: every option takes a value and may appear once; anything unknown is refused.</summary>
    internal static bool TryParsePrepareProvisionRolesArgs(string[] rest, out PrepareProvisionRolesArgs? parsed, out string? error)
    {
        parsed = null;
        error = null;
        var values = new Dictionary<string, string>(StringComparer.Ordinal);
        for (var i = 0; i < rest.Length; i++)
        {
            var option = rest[i].ToLowerInvariant();
            if (option is not ("--login" or "--login-kind" or "--passwords" or "--out" or "--wait-seconds" or "--host" or "--port"
                or "--database" or "--username" or "--ssl-mode" or "--root-certificate"))
            {
                error = rest[i].StartsWith('-') ? $"Unknown option {rest[i]}." : $"Unexpected argument '{rest[i]}'.";
                return false;
            }

            if (i + 1 >= rest.Length || rest[i + 1].StartsWith("--", StringComparison.Ordinal))
            {
                error = $"{rest[i]} needs a value.";
                return false;
            }

            if (!values.TryAdd(option, rest[++i]))
            {
                error = $"{rest[i - 1]} was given twice.";
                return false;
            }
        }

        foreach (var required in new[] { "--login", "--login-kind", "--passwords", "--out", "--wait-seconds" })
        {
            if (!values.ContainsKey(required))
            {
                error = $"{required} is required.";
                return false;
            }
        }

        if (!int.TryParse(values["--wait-seconds"], NumberStyles.None, CultureInfo.InvariantCulture, out var waitSeconds))
        {
            error = "--wait-seconds must be a whole number of seconds (0 or more).";
            return false;
        }

        ProvisionRolesLoginKind kind;
        switch (values["--login-kind"].ToLowerInvariant())
        {
            case "password":
                kind = ProvisionRolesLoginKind.Password;
                break;
            case "connection-string":
                kind = ProvisionRolesLoginKind.ConnectionString;
                break;
            default:
                error = "--login-kind must be password or connection-string.";
                return false;
        }

        var hostOptions = new[] { "--host", "--port", "--database", "--username", "--ssl-mode", "--root-certificate" };
        if (kind == ProvisionRolesLoginKind.ConnectionString)
        {
            foreach (var option in hostOptions)
            {
                if (values.ContainsKey(option))
                {
                    error = $"{option} is refused with --login-kind connection-string: the connection string names the store.";
                    return false;
                }
            }

            parsed = new PrepareProvisionRolesArgs(
                values["--login"], kind, values["--passwords"], values["--out"], waitSeconds, null, null, null, null, null, null);
            return true;
        }

        foreach (var option in new[] { "--host", "--port", "--database", "--username", "--ssl-mode" })
        {
            if (!values.ContainsKey(option))
            {
                error = $"{option} is required with --login-kind password.";
                return false;
            }
        }

        if (values["--host"].Contains(',', StringComparison.Ordinal))
        {
            error = "--host must be one host: a comma-separated list is refused, because the pgpass line matches one host.";
            return false;
        }

        if (!int.TryParse(values["--port"], NumberStyles.None, CultureInfo.InvariantCulture, out var port) || port is < 1 or > 65535)
        {
            error = "--port must be a number from 1 to 65535.";
            return false;
        }

        if (!Enum.TryParse<SslMode>(values["--ssl-mode"], ignoreCase: true, out var sslMode) || !Enum.IsDefined(sslMode))
        {
            error = "--ssl-mode must be one of Disable, Allow, Prefer, Require, VerifyCA, VerifyFull.";
            return false;
        }

        parsed = new PrepareProvisionRolesArgs(
            values["--login"], kind, values["--passwords"], values["--out"], waitSeconds,
            values["--host"], port, values["--database"], values["--username"], sslMode, values.GetValueOrDefault("--root-certificate"));
        return true;
    }

    /// <summary>The login from the file's text (one trailing newline dropped) and the host options. Null with <paramref name="error"/> when the
    /// text is unusable; the error never holds the text.</summary>
    internal static ProvisionRolesLogin? TryReadProvisionRolesLogin(PrepareProvisionRolesArgs args, string fileText, out string? error)
    {
        error = null;
        var text = fileText.EndsWith('\n') ? fileText[..^1] : fileText;
        if (args.LoginKind == ProvisionRolesLoginKind.Password)
        {
            if (!PasswordIsUsable(text, out error))
            {
                return null;
            }

            return new ProvisionRolesLogin(args.Host!, args.Port!.Value, args.Database!, args.Username!, text, args.SslMode!.Value, args.RootCertificate);
        }

        NpgsqlConnectionStringBuilder builder;
        try
        {
            builder = new NpgsqlConnectionStringBuilder(text);
        }
        catch (Exception ex) when (ex is ArgumentException or FormatException)
        {
            error = "The connection string in the login file could not be parsed (check its syntax).";
            return null;
        }

        if ((!string.IsNullOrEmpty(builder.RootCertificate) && builder.RootCertificate != ChartStoreCaPath)
            || !string.IsNullOrEmpty(builder.SslCertificate) || !string.IsNullOrEmpty(builder.SslKey) || !string.IsNullOrEmpty(builder.SslPassword))
        {
            error = $"The connection string may carry no Root Certificate other than {ChartStoreCaPath}, and no SSL Certificate, SSL Key or SSL Password.";
            return null;
        }

        if ((builder.SslMode is SslMode.VerifyCA or SslMode.VerifyFull) && string.IsNullOrEmpty(builder.RootCertificate))
        {
            error = $"The connection string has SSL Mode {builder.SslMode} and no Root Certificate. psql (libpq) needs a root certificate file to verify the server: add Root Certificate={ChartStoreCaPath} and set store.external.caSecret, or use another SSL Mode.";
            return null;
        }

        if (string.IsNullOrEmpty(builder.Host) || builder.Host.Contains(',', StringComparison.Ordinal))
        {
            error = "The connection string needs exactly one Host.";
            return null;
        }

        if (string.IsNullOrEmpty(builder.Database) || string.IsNullOrEmpty(builder.Username))
        {
            error = "The connection string needs Database and Username.";
            return null;
        }

        if (!PasswordIsUsable(builder.Password ?? string.Empty, out error))
        {
            return null;
        }

        return new ProvisionRolesLogin(builder.Host, builder.Port, builder.Database, builder.Username, builder.Password!, builder.SslMode,
            string.IsNullOrEmpty(builder.RootCertificate) ? null : builder.RootCertificate,
            string.IsNullOrWhiteSpace(builder.SearchPath) ? null : builder.SearchPath);
    }

    private static bool PasswordIsUsable(string password, out string? error)
    {
        error = null;
        if (password.Length == 0)
        {
            error = "The login has no password.";
        }
        else if (password.Contains('\n', StringComparison.Ordinal) || password.Contains('\r', StringComparison.Ordinal))
        {
            error = "The login's password contains a line break, which a pgpass line cannot hold.";
        }

        return error is null;
    }

    /// <summary>
    /// <c>--prepare-provision-roles</c> (#8779): the init step of the chart's store-roles Job. Reads the owner's login and the four role
    /// passwords from files, waits for the store to reach this build's schema, checks the login owns every object in the <c>collect</c> and
    /// <c>config</c> schemas, then writes <c>provision-roles.sql</c> (SCRAM verifiers in place of the passwords), <c>pgpass</c> and
    /// <c>libpq.env</c> into <c>--out</c>. It needs no darling.json and prints no password or verifier.
    /// </summary>
    public static Task<int> PrepareProvisionRolesAsync(string[] rest, TextWriter stdout, TextWriter stderr, CancellationToken ct) =>
        PrepareProvisionRolesCoreAsync(rest, stdout, stderr, TimeSpan.FromSeconds(5), TimeProvider.System, null, ct);

    internal static async Task<int> PrepareProvisionRolesCoreAsync(
        string[] rest, TextWriter stdout, TextWriter stderr, TimeSpan schemaPoll, TimeProvider time, string? shippedScriptPath, CancellationToken ct)
    {
        if (!TryParsePrepareProvisionRolesArgs(rest, out var args, out var argError))
        {
            stderr.WriteLine(argError);
            stderr.WriteLine(PrepareProvisionRolesUsageText());
            return ProvisionRolesExitCodes.Usage;
        }

        string loginText;
        try
        {
            loginText = File.ReadAllText(args!.LoginFile);
        }
        catch (Exception ex) when (ex is IOException or UnauthorizedAccessException)
        {
            stderr.WriteLine($"Could not read the login file {args!.LoginFile}: {ex.Message}");
            return ProvisionRolesExitCodes.Usage;
        }

        var login = TryReadProvisionRolesLogin(args, loginText, out var loginError);
        if (login is null)
        {
            stderr.WriteLine(loginError);
            return ProvisionRolesExitCodes.Usage;
        }

        var passwords = new Dictionary<string, string>(StringComparer.Ordinal);
        var refusals = new List<string>();
        foreach (var role in ProvisionRolesRoleNames)
        {
            string raw;
            try
            {
                raw = File.ReadAllText(Path.Combine(args.PasswordsDirectory, role));
            }
            catch (Exception ex) when (ex is IOException or UnauthorizedAccessException)
            {
                refusals.Add($"The password file for role {role} could not be read ({ex.GetType().Name}); it must be {Path.Combine(args.PasswordsDirectory, role)}.");
                continue;
            }

            var refusal = ProvisionRolesRenderer.CheckRolePassword(role, raw, out var password);
            if (refusal is not null)
            {
                refusals.Add(refusal);
                continue;
            }

            passwords[role] = password;
        }

        if (refusals.Count > 0)
        {
            foreach (var refusal in refusals)
            {
                stderr.WriteLine(refusal);
            }

            return ProvisionRolesExitCodes.Usage;
        }

        var connectionString = BuildProvisionRolesConnectionString(login);

        try
        {
            await using var dataSource = NpgsqlDataSource.Create(DarlingStoreConnection.PinSessionTimeZoneUtc(connectionString));
            var required = StorageVersion.SchemaVersion;

            async Task<int?> ReadVersionAsync(CancellationToken token)
            {
                try
                {
                    await using var probe = await dataSource.OpenConnectionAsync(token);
                    return await ProvisionRolesStoreChecks.ReadStoredSchemaVersionAsync(probe, token);
                }
                catch (PostgresException ex) when (ex.SqlState.StartsWith("28", StringComparison.Ordinal))
                {
                    throw new ProvisionRolesLoginRefusedException($"The store refused the login ({ex.SqlState}): {ex.MessageText}");
                }
            }

            var (reached, lastSeen, lastError) = await ProvisionRolesStoreChecks.WaitForSchemaAsync(
                ReadVersionAsync, required, TimeSpan.FromSeconds(args.WaitSeconds), schemaPoll, stdout, time, ct);
            if (!reached)
            {
                stderr.WriteLine(ProvisionRolesStoreChecks.DescribeSchemaTimeout(lastSeen, required, args.WaitSeconds, lastError));
                return ProvisionRolesExitCodes.SchemaWaitTimedOut;
            }

            string currentUser, currentDatabase;
            IReadOnlyList<ProvisionRolesStoreChecks.ForeignOwnedObject> foreign;
            await using (var connection = await dataSource.OpenConnectionAsync(ct))
            {
                foreign = await ProvisionRolesStoreChecks.ReadForeignOwnedObjectsAsync(connection, ct);
                await using var identity = new NpgsqlCommand("SELECT current_user::text, current_database()::text", connection);
                await using var reader = await identity.ExecuteReaderAsync(ct);
                await reader.ReadAsync(ct);
                currentUser = reader.GetString(0);
                currentDatabase = reader.GetString(1);
            }

            var nameRefusal = ProvisionRolesRenderer.CheckName("owner", currentUser) ?? ProvisionRolesRenderer.CheckName("database", currentDatabase);
            if (nameRefusal is not null)
            {
                stderr.WriteLine(nameRefusal);
                return ProvisionRolesExitCodes.Usage;
            }

            var ownership = ProvisionRolesStoreChecks.DescribeOwnership(currentUser, foreign);
            if (ownership is not null)
            {
                stderr.WriteLine(ownership);
                return ProvisionRolesExitCodes.OwnershipRefused;
            }

            var scriptPath = shippedScriptPath ?? Path.Combine(AppContext.BaseDirectory, "provision-roles.sql");
            string shipped;
            try
            {
                shipped = File.ReadAllText(scriptPath);
            }
            catch (Exception ex) when (ex is IOException or UnauthorizedAccessException)
            {
                stderr.WriteLine($"Could not read the shipped script {scriptPath}: {ex.Message}");
                return ProvisionRolesExitCodes.Failed;
            }

            var rendered = ProvisionRolesRenderer.Render(shipped, currentUser, currentDatabase, passwords);
            Directory.CreateDirectory(args.OutDirectory);
            WriteOwnerOnlyFile(Path.Combine(args.OutDirectory, "provision-roles.sql"), rendered);
            WriteOwnerOnlyFile(
                Path.Combine(args.OutDirectory, "pgpass"),
                ProvisionRolesRenderer.PgPassLine(login.Host, login.Port, currentDatabase, login.Username, login.Password) + "\n");
            WriteOwnerOnlyFile(
                Path.Combine(args.OutDirectory, "libpq.env"),
                ProvisionRolesRenderer.LibpqEnvFile(
                    login.Host, login.Port, currentDatabase, login.Username, ProvisionRolesRenderer.ToLibpqSslMode(login.SslMode), login.RootCertificate));

            stdout.WriteLine(string.Create(CultureInfo.InvariantCulture,
                $"Prepared provision-roles.sql for owner {currentUser} in database {currentDatabase} at schema version {lastSeen}; passwords sent as SCRAM-SHA-256 verifiers."));
            return ProvisionRolesExitCodes.Prepared;
        }
        catch (ProvisionRolesLoginRefusedException ex)
        {
            stderr.WriteLine(ex.Message);
            return ProvisionRolesExitCodes.Failed;
        }
        catch (ProvisionRolesRenderException ex)
        {
            stderr.WriteLine(ex.Message);
            return ProvisionRolesExitCodes.Failed;
        }
        catch (OperationCanceledException) when (ct.IsCancellationRequested)
        {
            stderr.WriteLine("Cancelled.");
            return ProvisionRolesExitCodes.Failed;
        }
        catch (Exception ex) when (ex is NpgsqlException or IOException or UnauthorizedAccessException or TimeoutException)
        {
            stderr.WriteLine($"{ex.GetType().Name}: {ex.Message}");
            return ProvisionRolesExitCodes.Failed;
        }
    }

    /// <summary>The verb's store connection string. It carries the store search path (the version table lives in <c>collect</c>, or
    /// <c>public</c> on older stores) unless the login set its own, and the caller pins the UTC session time zone outside that, as
    /// the service builds its own.</summary>
    internal static string BuildProvisionRolesConnectionString(ProvisionRolesLogin login)
    {
        var builder = new NpgsqlConnectionStringBuilder
        {
            Host = login.Host,
            Port = login.Port,
            Database = login.Database,
            Username = login.Username,
            Password = login.Password,
            SslMode = login.SslMode,
            RootCertificate = login.RootCertificate,
            Timeout = 10,
            CommandTimeout = 30,
            ApplicationName = "darling-provision-roles",
        };
        if (!string.IsNullOrWhiteSpace(login.SearchPath))
        {
            builder.SearchPath = login.SearchPath;
        }

        return DarlingStoreConnection.EnsureStoreSearchPath(builder.ConnectionString);
    }

    private static void WriteOwnerOnlyFile(string path, string content)
    {
        var options = new FileStreamOptions { Mode = FileMode.Create, Access = FileAccess.Write, Share = FileShare.None };
        if (!OperatingSystem.IsWindows())
        {
            options.UnixCreateMode = UnixFileMode.UserRead | UnixFileMode.UserWrite;
        }

        using (var stream = new FileStream(path, options))
        {
            var bytes = new UTF8Encoding(encoderShouldEmitUTF8Identifier: false).GetBytes(content);
            stream.Write(bytes, 0, bytes.Length);
        }

        if (OperatingSystem.IsLinux())
        {
            File.SetUnixFileMode(path, UnixFileMode.UserRead | UnixFileMode.UserWrite);
        }
    }

    /// <summary>The store said no to the login (an SQLSTATE 28 class error): waiting would not change that.</summary>
    private sealed class ProvisionRolesLoginRefusedException : InvalidOperationException
    {
        public ProvisionRolesLoginRefusedException(string message) : base(message)
        {
        }
    }
}
