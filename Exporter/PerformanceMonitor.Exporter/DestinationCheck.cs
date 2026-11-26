namespace PerformanceMonitor.Exporter;

/// <summary>
/// Checks the local folder `run` uploads to before anything is exported. The upload never creates that folder, so a missing
/// or unwritable one fails every window the same way, hour after hour. A missing or unwritable folder is a
/// <see cref="ConfigException"/> (exit 2) that names the folder. A destination that is not a local folder has nothing to check.
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
