using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace HadleyCapitalManagement.DataAccess
{
    public interface IDataAccess
    {
        List<Commodity> GetCommodityList();
        List<Contract> GetContractList(int commodityId);
        List<ContractPrice> GetContractPriceList(int contractId);
        void AddContract(Contract contract);
    }
}
