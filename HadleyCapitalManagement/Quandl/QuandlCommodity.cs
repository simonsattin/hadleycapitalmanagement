using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace HadleyCapitalManagement.Quandl
{
    public class QuandlCommodity
    {
        public int Id { get; set; }
        public string Name { get; set; }
        public string Code { get; set; }
        public string MonthCodes { get; set; }
        public string FirstContract { get; set; }
        public string ContractListUrl { get; set; }
        public string ContractListRegex { get; set; }

        public List<QuandlContract> Contracts { get; set; }
    }
}
