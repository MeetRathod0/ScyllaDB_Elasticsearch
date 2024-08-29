// using Cassandra;
// using Cassandra.Mapping;
// using ScyllaElastic.Models;
// namespace ScyllaElastic;



// public class ScyllaDBService
// {
//     private Cluster cluster; // cluster object
//     private Cassandra.ISession session; // session 
//     private IMapper mapper; // mapper for model

//     public ScyllaDBService() {
//         // connect to scylla
//         cluster = Cluster.Builder()
//             // hosts
//             .AddContactPoint("192.168.10.129")
//             // username & password
//             .WithCredentials("cassandra", "cassandra")
//             .Build();
//         // connect to stud keyspace
//         session = cluster.Connect("stud");

//         // register UDT model
//         session.UserDefinedTypes.Define(
//             //  udt models
//             UdtMap.For<kam_udt>(),
//             UdtMap.For<vendor_para_udt>(),
//             UdtMap.For<party_udt>(),
//             UdtMap.For<stone_revise_udt>(),
//             UdtMap.For<stone_hit_udt>(),
//             UdtMap.For<shipping_udt>(),
//             UdtMap.For<rate_amt_percentage_diff_udt>(),
//             UdtMap.For<business_logic_udt>(),
//             UdtMap.For<remark_udt>(),
//             UdtMap.For<para_udt>(),
//             UdtMap.For<lot_udt>(),
//             UdtMap.For<certificate_number_udt>(),
//             UdtMap.For<extra_para_udt>(),
//             UdtMap.For<lab_udt>(),
//             UdtMap.For<comment_udt>(),
//             UdtMap.For<twin_udt>(),
//             UdtMap.For<size_udt>(),
//             UdtMap.For<layout_udt>(),
//             UdtMap.For<order_udt>(),
//             UdtMap.For<svs_udt>(),
//             UdtMap.For<presentation_udt>(),
//             UdtMap.For<inclusion_udt>()
//         );
//         //MappingConfiguration.Global.Define<ScyllaMapper>();
//         // mapper object create
//         mapper = new Mapper(session);
        
//     }   
    

//     public List<stone_data> Get()
//     {
        
//         // for normal fetch without model use below line
//         //var res = session.Execute("select * from student");

//         // fetch all rows from the table and mapped into student model
//         IEnumerable<stone_data> result = mapper.Fetch<stone_data>("select * from stone_data"); 
//         return result.ToList();
//     }

//     // insert new record
//     public void Add(stone_data model){
//         // pass into insert
//         mapper.Insert<stone_data>(model);
//     }

// }