using System.Data;
using Microsoft.Data.SqlClient;

namespace AzSqlBcp.Services;

public sealed partial class BulkCopyService
{
    private static async Task<(long? Min, long? Max, long Count)> GetBoundsAsync(
        string server,
        string database,
        string qualifiedTable,
        string partitionQuoted,
        string accessToken,
        int port,
        bool readOnlyIntent,
        bool trustServerCertificate,
        CancellationToken cancellationToken)
    {
        await using var conn = CreateConnection(server, database, accessToken, port, readOnlyIntent, trustServerCertificate);
        await conn.OpenAsync(cancellationToken).ConfigureAwait(false);

        await using var cmd = new SqlCommand
        {
            Connection = conn,
            CommandType = CommandType.Text,
            CommandText =
                $"SELECT MIN({partitionQuoted}), MAX({partitionQuoted}), COUNT_BIG(*) FROM {qualifiedTable}",
            CommandTimeout = 500000
        };

        await using var reader = await cmd.ExecuteReaderAsync(cancellationToken).ConfigureAwait(false);
        if (!await reader.ReadAsync(cancellationToken).ConfigureAwait(false) || reader.IsDBNull(0))
            return (null, null, 0);

        return (reader.GetInt64(0), reader.GetInt64(1), reader.GetInt64(2));
    }

    private static async Task<long> GetCountAsync(
        string server,
        string database,
        string qualifiedTable,
        string accessToken,
        int port,
        bool readOnlyIntent,
        bool trustServerCertificate,
        CancellationToken cancellationToken)
    {
        await using var conn = CreateConnection(server, database, accessToken, port, readOnlyIntent, trustServerCertificate);
        await conn.OpenAsync(cancellationToken).ConfigureAwait(false);

        await using var cmd = new SqlCommand
        {
            Connection = conn,
            CommandType = CommandType.Text,
            CommandText = $"SELECT COUNT_BIG(*) FROM {qualifiedTable}",
            CommandTimeout = 500000
        };

        var result = await cmd.ExecuteScalarAsync(cancellationToken).ConfigureAwait(false);
        return result is long count ? count : Convert.ToInt64(result);
    }
}
