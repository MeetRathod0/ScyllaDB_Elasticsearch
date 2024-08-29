using Cassandra;
using Cassandra.Mapping;
using Elasticsearch.Net;
using Microsoft.Extensions.Configuration;
using Nest;
using Nest.JsonNetSerializer;
using Newtonsoft.Json;
using ScyllaElastic.Models;


namespace ScyllaElastic.Config;

public class Configuration
{
    private readonly IConfiguration _configuration;
    public static ConnectionSettings settings { get; set; }
    public static ElasticClient clientES { get; set; } 
    public Cluster cluster; // cluster object
    public static Cassandra.ISession session; // session 
    public static IMapper mapper; // mapper for model
    public static string LocalConnection { get; set; }
    public static string ELKbaseurl { get; set; }
    public static string ELKConnectionUserName { get; set; }
    

    public Configuration(IConfiguration configuration) {
        _configuration = configuration;
        
        LocalConnection  = @"Data Source=localhost\sqlexpress; Initial Catalog=db; User Id=sa;Password=Admin@123;Encrypt=False;Integrated Security=False";
        
        ELKbaseurl = "http://localhost:9200";
        
        var pool = new SingleNodeConnectionPool(new Uri(ELKbaseurl));

        settings = new ConnectionSettings(pool,sourceSerializer: (builtin, settings) => new JsonNetSerializer(
                                            builtin, settings,
                                            () => new JsonSerializerSettings { NullValueHandling = NullValueHandling.Ignore })
                                        ).DefaultFieldNameInferrer(s=>s)
                                        .RequestTimeout(TimeSpan.FromMinutes(10));
        clientES = new ElasticClient(settings);

        // connect to scylla
        cluster = Cluster.Builder()
            // hosts
            .AddContactPoint("192.168.10.129")
            // username & password
            .WithCredentials("cassandra", "cassandra")
            .Build();
        // connect to stud keyspace
        session = cluster.Connect("stud");

        // register UDT model
        //MappingConfiguration.Global.Define<ScyllaMapper>();
        // mapper object create
        mapper = new Mapper(session);

    }
    
}