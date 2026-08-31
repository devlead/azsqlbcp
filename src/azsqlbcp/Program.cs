using AzSqlBcp.Commands;
using Spectre.Console.Cli;

var app = new CommandApp<CopyCommand>();
app.Configure(config =>
{
    config.SetApplicationName("azsqlbcp");
    config.ValidateExamples();
});

return await app.RunAsync(args);
