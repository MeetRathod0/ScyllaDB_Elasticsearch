using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.Hosting;
using Nest;
using System.Data;

using ScyllaElastic.Config;
using ScyllaElastic.Models;
using Newtonsoft.Json;


namespace ScyllaElastic.Service;

public class ElasticToScyllaService: BackgroundService
{
    private Configuration config;
    private readonly IConfiguration _elasticClient;
    public ElasticToScyllaService(IConfiguration elasticClient)
    {
        config = new Configuration(elasticClient);
        _elasticClient = elasticClient;
        
    }

    public async Task  AddInScylla() 
    {
        var scroll = new Time(15,TimeUnit.Second);
        var res = await Configuration.clientES.SearchAsync<ElasticModel>(
            i => i.Index(Config.Indices.get_data)
            .Query(q=>q.MatchAll())
            .Size(10000).Scroll(scroll)
            );
        var result = new List<ElasticModel>();
        var list_ids = new List<string>();
        try
            {
                do
                { 
                    if (!res.IsValid || string.IsNullOrEmpty(res.ScrollId))
                        throw new Exception("Search Error");
                    if (!res.Documents.Any())
                        break;
                    // get _source list
                    result.AddRange(res.Documents);
                    // get _id list
                    list_ids.AddRange(res.Hits.Select(s=>s.Id));
                    res = await Configuration.clientES.ScrollAsync<ElasticModel>(scroll, res.ScrollId);
                    //Console.WriteLine(result.Count + " get from " + res.Total);
                
                } while (res.Total >= 0 && res.Total >= result.Count && res.Documents.Any() && res.IsValid);

                // for loop
                foreach (ElasticModel model in result) 
                {
                    // model name same as table name
                    StudentModel smodel=new StudentModel();
                    smodel.id = model.id;
                    smodel.name = model.name;
                    smodel.dateofbirth = Convert.ToDateTime(model.account_date);
            
                    // insert into scylla
                   Configuration.mapper.Insert<StudentModel>(smodel);
                }

            }
            catch (Exception ex)
            {

                throw ex;
            }
            finally {
                var clearScrollResponse = Configuration.clientES.ClearScrollAsync(c=>c.ScrollId(res.ScrollId));
            }


    }

    public override Task StartAsync(CancellationToken cancellationToken)
    {
        return base.StartAsync(cancellationToken);
    }

    protected override async Task ExecuteAsync(CancellationToken stoppingToken)
    {
        await AddInScylla();
    }

}
