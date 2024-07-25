using System;
using System.Data.SqlClient;
using System.Threading;
using System.Threading.Channels;
using System.Threading.Tasks;
using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging;

namespace SqlDependencyHosted
{
    public class Program
    {
        public static void Main(string[] args)
        {
            CreateHostBuilder(args).Build().Run();
        }

        public static IHostBuilder CreateHostBuilder(string[] args) =>
            Host.CreateDefaultBuilder(args)
            .ConfigureAppConfiguration((hostingContext, config) =>
                {
                    config.AddJsonFile("appsettings.json", optional: true, reloadOnChange: true)
                          .AddEnvironmentVariables();
                })
                .ConfigureLogging((hostingContext, logging) =>
                {
                    logging.AddConfiguration(hostingContext.Configuration.GetSection("Logging"));
                    logging.AddConsole();
                })
                .ConfigureServices((hostContext, services) =>
                {
                    services.AddHostedService<SqlDependencyService>();
                });
    }

    public class SqlDependencyService : BackgroundService
    {
        private readonly ILogger<SqlDependencyService> _logger;
        private readonly string _connectionString = "Server=localhost;Database=CHTDB;User id=modula;password=+Sql1234;TrustServerCertificate=True;";
        private int bucle;

        public SqlDependencyService(ILogger<SqlDependencyService> logger)
        {
            _logger = logger;
            bucle = 0;
            try
            {
                SqlDependency.Start(_connectionString);
            }
            catch (System.Exception ex)
            {
                _logger.LogError(ex, $"Se produjo un problema en SqlDependency.Start({_connectionString})");
            }
        }

        protected override async Task ExecuteAsync(CancellationToken stoppingToken)
        {

            _logger.LogInformation("Background App ExecuteAsync.");


            // Registrar la dependencia al inicio
            RegisterSqlDependency();
            try
            {
                _logger.LogInformation("Microservicio SQL Dependency está corriendo.");
                // Mantener el servicio en ejecución hasta que se solicite la cancelación
                while (!stoppingToken.IsCancellationRequested)
                {
                    try
                    {
                        await Task.Delay(5000, stoppingToken);
                        bucle++;
                        _logger.LogInformation("BackgroundService is running : {0}", bucle);
                    }
                    catch (TaskCanceledException ex)
                    {
                        _logger.LogWarning("La tarea se canceló: {0}", ex.Message);
                        // Opcional: Puedes manejar o registrar el detalle del `TaskCanceledException` aquí
                    }
                    catch (Exception ex)
                    {
                        _logger.LogError(ex, "Se produjo un error inesperado en el bucle principal.");
                    }
                }
            }
            finally
            {
                // Asegúrate de detener la dependencia SQL cuando el servicio se detiene
                SqlDependency.Stop(_connectionString);
                _logger.LogInformation("SqlDependency detenido.");
            }
        }

        private void RegisterSqlDependency()
        {
            try
            {
                using (SqlConnection connection = new SqlConnection(_connectionString))
                {
                    connection.Open();
                    using (SqlCommand command = new SqlCommand("SELECT [EntryNo],[WMSModulaStatus] FROM [dbo].[CHT$ChTWmsModulaEntry$f126fcbc-bdf1-430a-827c-0aefdb708eb9]", connection))
                    {
                        SqlDependency dependency = new SqlDependency(command);
                        dependency.OnChange += OnDependencyChange;
                        using (SqlDataReader reader = command.ExecuteReader())
                        {
                            // Procesa los datos iniciales aquí si es necesario
                        }
                        _logger.LogInformation("RegisterSqlDependency registered : {0}", bucle);
                    }
                }
            }
            catch (Exception ex)
            {
                _logger.LogError(ex, "Se produjo un problema al registrar la dependencia SQL. El servicio se detendrá.");      
                // Asegúrate de detener la dependencia SQL cuando el servicio se detiene
                SqlDependency.Stop(_connectionString);
                _logger.LogInformation("SqlDependency detenido.");
                Environment.Exit(1); // Salir de la aplicación con un código de error          
            }
        }

        private void OnDependencyChange(object sender, SqlNotificationEventArgs e)
        {
            _logger.LogWarning("Cambio detectado en la base de datos: {0}", e.Info);
            try
            {
                //TODO  Aquí puedes agregar la lógica para manejar los cambios detectados

                if (e.Info == SqlNotificationInfo.Update) {
                    throw new Exception("VAYA PESSS!!!");
                }


            }
            catch (Exception ex)
            {
                _logger.LogError(ex, "Error al procesar el cambio en la base de datos. Se volverá a registrar la dependencia.");
               // _logger.LogTrace(ex,_connectionString);
            }
            finally
            {
                // Volver a registrar la dependencia
                RegisterSqlDependency();
            }
        }
    }
}

