using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace HadleyCapitalManagement.Quandl
{
    public class QuandlContract
    {
        public int Id { get; set; }
        public int CommodityId { get; set; }        
        public string MonthCode { get; set; }
        public string Name { get; set; }
        public string Year { get; set; }

        public QuandlCommodity Commodity { get; set; }
    }
}
