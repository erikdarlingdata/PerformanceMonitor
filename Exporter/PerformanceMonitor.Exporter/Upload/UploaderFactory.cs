namespace PerformanceMonitor.Exporter.Upload;

/// <summary>Builds the uploader for a parsed destination.</summary>
public static class UploaderFactory
{
    public static IUploader Create(UploadSettings settings, Func<string, string?>? getEnvironmentVariable = null) => settings switch
    {
        UploadSettings.Local local => new LocalUploader(local.Path),
        UploadSettings.S3 s3 => new S3Uploader(s3, getEnvironmentVariable),
        UploadSettings.AzureBlob azure => new AzureBlobUploader(azure, getEnvironmentVariable),
        _ => throw new ArgumentException($"Unsupported upload destination {settings.GetType().Name}."),
    };
}
