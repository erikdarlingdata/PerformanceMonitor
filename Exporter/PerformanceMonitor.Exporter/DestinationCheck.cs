namespace PerformanceMonitor.Exporter;

/// <summary>
/// Checks, before anything is exported, what every window's upload needs. <see cref="Assert(ExporterConfig)"/> checks the
/// local folder `run` uploads to: the upload never creates that folder, so a missing or unwritable one fails every window the
/// same way, hour after hour. It is a <see cref="ConfigException"/> (exit 2) that names the folder, and a destination that is
/// not a local folder has nothing to check. <see cref="AssertCredentials"/> checks the environment variables the destination
/// names for its keys.
/// </summary>
public static class DestinationCheck
{
    /// <summary>Checks the local folder of <paramref name="config"/>'s destination, or the default landing folder when it has none.</summary>
    public static void Assert(ExporterConfig config) => Assert(config, WriteProbe);

    internal static void Assert(ExporterConfig config, Action<string> writeProbe)
    {
        if (Run.RunConfigMapping.LocalUploadRoot(config) is not var (root, key))
        {
            return;
        }

        if (!Directory.Exists(root))
        {
            throw new ConfigException($"{key}: the local destination folder '{root}' does not exist. The exporter does not create it, so create it before the run");
        }

        try
        {
            writeProbe(root);
        }
        catch (Exception ex) when (ex is UnauthorizedAccessException or IOException)
        {
            throw new ConfigException($"{key}: the local destination folder '{root}' is not writable ({ex.Message.ReplaceLineEndings(" ").Trim()})");
        }
    }

    /// <summary>
    /// Checks that every environment variable the destination names for its keys is set: the S3 access key and secret key, the
    /// Azure connection string or the Azure SAS. An unset one fails every window at its upload, so it is a
    /// <see cref="ConfigException"/> (exit 2) that names the config key only. Neither the value nor the variable name reaches a message,
    /// because a pasted key can look like a name.
    /// </summary>
    public static void AssertCredentials(ExporterConfig config, Func<string, string?>? getEnvironmentVariable = null)
    {
        if (config.Destination is not { } d)
        {
            return;
        }

        Upload.UploadSettings parsed;
        try { parsed = Upload.UploadSettings.FromJson(d); }
        catch (ArgumentException ex) { throw new ConfigException(ex.Message); }

        var read = getEnvironmentVariable ?? Environment.GetEnvironmentVariable;
        void Require(string key, string name)
        {
            if (string.IsNullOrWhiteSpace(read(name)))
            {
                throw new ConfigException($"{key}: the environment variable it names is not set");
            }
        }

        switch (parsed)
        {
            case Upload.UploadSettings.S3 { Credentials: Upload.S3Credentials.EnvKeys keys }:
                Require("destination.credentials.access_key_id_env", keys.AccessKeyIdEnv);
                Require("destination.credentials.secret_access_key_env", keys.SecretAccessKeyEnv);
                break;
            case Upload.UploadSettings.AzureBlob { Auth: Upload.AzureAuth.ConnectionStringEnv cs }:
                Require("destination.auth.name", cs.Name);
                break;
            case Upload.UploadSettings.AzureBlob { Auth: Upload.AzureAuth.SasUrlEnv sas }:
                Require("destination.auth.name", sas.Name);
                break;
        }
    }

    /// <summary>Creates and removes a hidden partial file, the kind the local upload writes, so the check meets the permissions the upload will.</summary>
    private static void WriteProbe(string root)
    {
        var path = Path.Combine(root, $".write-check.{Guid.NewGuid():N}.partial");
        using (new FileStream(path, FileMode.CreateNew, FileAccess.Write, FileShare.None))
        {
        }

        File.Delete(path);
    }
}
