using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace HadleyCapitalManagement
{
    public class Commodity
    {
        public int Id { get; set; }
        public string Name { get; set; }
        public string Code { get; set; }
        public string MonthCodes { get; set; }
        public string FirstContract { get; set; }

        public List<Contract> Contracts { get; set; }
    }
}
