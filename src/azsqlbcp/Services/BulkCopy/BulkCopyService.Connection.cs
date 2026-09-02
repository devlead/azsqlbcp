using Microsoft.Data.SqlClient;

namespace AzSqlBcp.Services.BulkCopy;

public sealed partial class BulkCopyService
{
    internal static SqlConnection CreateConnection(
        string server,
        string database,
        string accessToken,
        int port = 1433,
        bool readOnlyIntent = false,
        bool trustServerCertificate = false) =>
        new(BuildConnectionString(server, database, port, readOnlyIntent, trustServerCertificate))
        {
            AccessToken = accessToken
        };

    private static string BuildConnectionString(
        string server,
        string database,
        int port = 1433,
        bool readOnlyIntent = false,
        bool trustServerCertificate = false)
    {
        var builder = new SqlConnectionStringBuilder
        {
            DataSource = $"tcp:{server},{port}",
            InitialCatalog = database,
            Encrypt = true,
            TrustServerCertificate = trustServerCertificate
        };

        if (readOnlyIntent)
            builder.ApplicationIntent = ApplicationIntent.ReadOnly;

        return builder.ConnectionString;
    }

    internal static string QuoteTable(string table)
    {
        var parts = table.Split('.', 2, StringSplitOptions.TrimEntries | StringSplitOptions.RemoveEmptyEntries);
        return parts.Length == 2
            ? $"{QuoteIdent(parts[0])}.{QuoteIdent(parts[1])}"
            : $"{QuoteIdent("dbo")}.{QuoteIdent(parts[0])}";
    }

    internal static string QuoteIdent(string name) =>
        "[" + name.Replace("]", "]]", StringComparison.Ordinal) + "]";
}
