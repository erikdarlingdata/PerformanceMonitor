namespace PerformanceMonitor.Exporter;

/// <summary>
/// The version of the file contract: the Parquet layout, file names and manifest shape that a consumer of the
/// export reads. <see cref="ManifestWriter"/> writes it as the first key (<c>contract_version</c>) of every core and
/// optional manifest. A manifest with no <c>contract_version</c> counts as version 1.
///
/// What does NOT bump <see cref="Version"/> (a consumer written for this version keeps working):
///   * adding a column, a table or a manifest field.
///
/// What DOES bump it (a consumer written for the old version would misread or lose data):
///   * removing, renaming or retyping a column;
///   * changing a column's units or meaning;
///   * changing the key layout or the file names;
///   * changing what an existing manifest field means.
///
/// Version 1 is the shape the exporter wrote before the field existed.
/// </summary>
public static class FileContract
{
    /// <summary>The current file-contract version. Bump it by the rules on <see cref="FileContract"/>.</summary>
    public const int Version = 1;
}
