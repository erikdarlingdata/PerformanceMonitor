namespace PerformanceMonitor.Darling.Service;

/// <summary>
/// The one sentence builder for a web write the store refuses (#6445). The web host signs in to the store as the read-only <c>viewer</c> role
/// unless <c>postgres.webConnectionString</c> names another login, and the service already knows which kind of store it runs on. On the managed
/// store that setting is a validation error, so naming it (or "the admin login") sends the user to an option the store refuses. The sentence
/// therefore depends on <see cref="PostgresConfig.Managed"/>: a managed store says the dashboard lists only and the Viewer app makes the change,
/// a store the operator brought keeps the advice to give the web an admin store login.
/// </summary>
internal static class DarlingWebStoreWrites
{
    /// <summary>Whether this config describes the managed store. A host with no config (a test that maps the routes directly) is not managed.</summary>
    internal static bool IsManaged(PostgresConfig? postgres) => postgres?.Managed == true;

    /// <summary>The 403 sentence for a write the web's store login cannot make. <paramref name="cannot"/> finishes "this dashboard cannot ..."
    /// (for example <c>record feedback</c>). <paramref name="instead"/> is the imperative that says where to do it, with no final period.
    /// <paramref name="bringYourOwnExtra"/> is appended on a store you bring yourself only.</summary>
    internal static string Refused(string cannot, bool managedStore, string instead = "Use the Viewer app to make the change",
        string? bringYourOwnExtra = null) =>
        managedStore
            ? $"This dashboard lists only on this store, so it cannot {cannot}. {instead}."
            : $"This dashboard's store login cannot {cannot}. {instead}, or give the web an admin store login "
              + "(set postgres.webConnectionString to an admin login)." + bringYourOwnExtra;
}
