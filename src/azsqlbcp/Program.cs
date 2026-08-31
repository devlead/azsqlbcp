using AzSqlBcp.Commands;
using AzSqlBcp.Services;

public partial class Program
{
    static partial void AddServices(IServiceCollection services)
    {
        services.AddSingleton<TokenCache>();
        services.AddSingleton<BulkCopyService>();
    }

    static partial void ConfigureApp(AppServiceConfig appServiceConfig)
    {
        appServiceConfig.SetApplicationName("azsqlbcp");
        appServiceConfig.SetDefaultCommand<CopyCommand>();
    }
}
