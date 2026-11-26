namespace PerformanceMonitor.Exporter;

/// <summary>
/// The `export` and `check-config` verbs, with the console streams passed in so a test runs them in process. Exit codes:
/// 0 = ok, 1 = a failure, 2 = a configuration error (a bad config, a missing credential, a store the exporter cannot read).
/// </summary>
public static class CliVerbs
{
    public static async Task<int> ExportAsync(string[] args, TextWriter output, TextWriter error, CancellationToken ct)
    {
        try
        {
            var invocation = ExportCommand.Parse(args, DateTime.UtcNow);
            DuckDbSession.Configure(invocation.DuckDbMemoryLimit, invocation.DuckDbThreads);
            return await ExportRunner.RunAsync(invocation.Options, output, ct).ConfigureAwait(false);
        }
        catch (UsageException ex)
        {
            error.WriteLine(ex.Message);
            return 1;
        }
        catch (Exception ex) when (ex is ConfigException or CredentialException)
        {
            error.WriteLine($"Config error: {ex.Message}");
            return 2;
        }
        catch (Exception ex)
        {
            // The whole exception (its type, every inner exception and the stack) goes to stdout, which the scheduler keeps in the
            // run's log, so a failure names the layer that raised it: a bare "The operation has timed out" can come from a
            // connection open, a read or an upload. stderr stays one line, because the scheduler records it as the family's last
            // error and the stale alert quotes it, so the line only gains the exception types.
            output.WriteLine(ex.ToString());
            var types = ex.GetType().Name + (ex.InnerException is { } inner ? " <- " + inner.GetType().Name : "");
            error.WriteLine($"Export failed: {ex.Message} ({types})");
            return 1;
        }
    }

    /// <summary>
    /// check-config --config F: loads and validates the config, resolves the credential, checks the store's collation and
    /// prints the config with secrets redacted. A store that cannot be reached is a warning on stderr and not a failure.
    /// </summary>
    public static async Task<int> CheckConfigAsync(string[] args, TextWriter output, TextWriter error, CancellationToken ct)
    {
        string? path = null;
        for (var i = 1; i < args.Length; i++)
        {
            if (args[i] == "--config" && i + 1 < args.Length)
            {
                path = args[++i];
            }
            else
            {
                error.WriteLine($"check-config: unknown argument {args[i]}");
                return 2;
            }
        }

        if (path is null)
        {
            error.WriteLine("Usage: darling-export check-config --config <file>");
            return 2;
        }

        try
        {
            var config = ConfigLoader.Load(path);
            var secret = CredentialResolver.Resolve(config.Store.Credential);
            var builder = StoreCheck.BuildConnection(config, secret);
            var notChecked = await StoreCheck.CheckCollationAsync(builder, ct).ConfigureAwait(false);

            output.WriteLine(ConfigPrinter.Render(config, credentialResolved: true));
            if (notChecked is null)
            {
                output.WriteLine("store: collation C");
            }
            else
            {
                error.WriteLine($"warning: store collation not checked: {notChecked}");
            }
            return 0;
        }
        catch (Exception ex) when (ex is ConfigException or CredentialException)
        {
            error.WriteLine($"Config error: {ex.Message}");
            return 2;
        }
    }
}
