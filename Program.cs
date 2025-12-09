using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Hosting;
using Serilog;

var host = new HostBuilder()
    .ConfigureAppConfiguration((context, config) =>
    {
        config.AddJsonFile("local.settings.json", optional: true, reloadOnChange: true)
              .AddEnvironmentVariables();
    })
    .ConfigureFunctionsWorkerDefaults()
    .UseSerilog((ctx, services, loggerConfiguration) =>
    {
        loggerConfiguration
            .ReadFrom.Configuration(ctx.Configuration)
            .ReadFrom.Services(services)
            .Enrich.FromLogContext();
    })
    .ConfigureServices((context, services) =>
    {
        services.AddSingleton<IGacaSyncService, GacaSyncService>();
        services.AddSingleton<GacaRepository>();
    })
    .Build();

await host.RunAsync();
