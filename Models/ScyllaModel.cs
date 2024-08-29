namespace ScyllaElastic.Models;

public class StudentModel
{
    public long id { get; set; }
    public string name { get; set; }
    public DateTime dateofbirth { get; set; }
    public int is_active {get; set;}
    
}