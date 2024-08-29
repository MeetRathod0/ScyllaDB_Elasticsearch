using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Hosting;
using ScyllaElastic.Service;
public class Program
{

    static void Main(string[] args)
    {
        
        CreateHostBuilder(args).Build().Run();
        Console.ReadKey(); // NEW 
    }
    public static IHostBuilder CreateHostBuilder(string[] args) =>Host.CreateDefaultBuilder(args)
        .UseWindowsService()
        .ConfigureServices((hostContext, services) =>
            services.AddHostedService<ElasticToScyllaService>()
        );
    

}