using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Hosting.WindowsServices;
using Serilog;
using GACASYNC;

namespace GacaSync;

public class Program
{
    public static void Main(string[] args)
    {
        // Ensure all relative paths (logs/App_Data/Files) resolve next to the EXE, not system32.
        Directory.SetCurrentDirectory(AppContext.BaseDirectory);

        // Make sure logs directory exists early
        var logsDir = Path.Combine(AppContext.BaseDirectory, "logs");
        Directory.CreateDirectory(logsDir);

        // Bootstrap logger: visible before Serilog full config is loaded
        var bootstrapPath = Path.Combine(logsDir, "bootstrap-.txt");
        Log.Logger = new LoggerConfiguration()
            .MinimumLevel.Debug()
            .WriteTo.Console()
            .WriteTo.File(
                bootstrapPath,
                rollingInterval: RollingInterval.Day,
                retainedFileCountLimit: 7,
                shared: true,
                outputTemplate: "[{Timestamp:yyyy-MM-dd HH:mm:ss.fff} {Level:u3}] {Message:lj}{NewLine}{Exception}"
            )
            .CreateBootstrapLogger();

        // Catch any unhandled crashes and background task exceptions
        AppDomain.CurrentDomain.UnhandledException += (_, e) =>
            Log.Fatal(e.ExceptionObject as Exception, "Unhandled exception (AppDomain).");

        TaskScheduler.UnobservedTaskException += (_, e) =>
        {
            Log.Fatal(e.Exception, "Unobserved task exception.");
            e.SetObserved();
        };

        try
        {
            Log.Information("Bootstrapping host... ContentRoot={ContentRoot}", AppContext.BaseDirectory);

            var host = Host.CreateDefaultBuilder(args)
                .UseContentRoot(AppContext.BaseDirectory)
                .UseWindowsService() // Run as Windows Service
                .UseSerilog((ctx, services, cfg) =>
                {
                    // Bind Serilog from appsettings.json and DI
                    cfg.ReadFrom.Configuration(ctx.Configuration)
                       .ReadFrom.Services(services)
                       .Enrich.FromLogContext();
                })
                .ConfigureServices((ctx, services) =>
                {
                    // Options & DI graph
                    services.AddSingleton<IGacaSyncService, GacaSyncService>();
                    services.AddSingleton<GacaRepository>(); // IConfiguration injected automatically
                    services.AddHostedService<Worker>();
                })
                .Build();

            Log.Information("Host built. Starting run loop...");
            host.Run(); // Blocking call
            Log.Information("Host.Run() exited normally.");
        }
        catch (Exception ex)
        {
            Log.Fatal(ex, "Host terminated unexpectedly.");
        }
        finally
        {
            Log.Information("Host exiting. Flushing logs...");
            Log.CloseAndFlush();
        }
    }
}
