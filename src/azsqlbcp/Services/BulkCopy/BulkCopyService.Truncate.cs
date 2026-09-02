using System.Data;
using Microsoft.Data.SqlClient;

namespace AzSqlBcp.Services.BulkCopy;

public sealed partial class BulkCopyService
{
    private static async Task TruncateAsync(
        string server,
        string database,
        string qualifiedTable,
        string accessToken,
        int port,
        bool trustServerCertificate,
        CancellationToken cancellationToken)
    {
        await using var conn = CreateConnection(server, database, accessToken, port, trustServerCertificate: trustServerCertificate);
        await conn.OpenAsync(cancellationToken).ConfigureAwait(false);

        await using var cmd = new SqlCommand
        {
            Connection = conn,
            CommandType = CommandType.Text,
            CommandText = $"TRUNCATE TABLE {qualifiedTable}",
            CommandTimeout = 500000
        };

        await cmd.ExecuteNonQueryAsync(cancellationToken).ConfigureAwait(false);
    }
}
