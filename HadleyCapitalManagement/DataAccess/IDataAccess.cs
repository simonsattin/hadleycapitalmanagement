using HadleyCapitalManagement.Quandl;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace HadleyCapitalManagement.DataAccess
{
    public interface IDataAccess
    {
        List<QuandlCommodity> GetCommodityList();
        List<QuandlContract> GetContractList(int commodityId);
        List<QuandlContractPrice> GetContractPriceList(int contractId);
        void AddContract(QuandlContract contract);
    }
}
