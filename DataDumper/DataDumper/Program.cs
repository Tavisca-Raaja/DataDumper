using Aerospike.Client;
using System.IO;
using Microsoft.VisualBasic.FileIO;
using System;

namespace DataDumper
{
    class Program
    {
        static void Main(string[] args)
        {
            var client = new AerospikeClient("18.235.70.103",3000);
            string nameSpace = "AirEngine";
            string setName = "Raaja";
            int recordsCount = 0;
            using (TextFieldParser dataSpliter = new TextFieldParser(@"c:\DataSet.csv"))
            {
                dataSpliter.SetDelimiters(",");
                while(!dataSpliter.EndOfData)
                {
                    if (recordsCount < 20001)
                    {
                        string[] column = dataSpliter.ReadFields();
                        var key = new Key(nameSpace, setName, column[4]);
                        client.Put(new WritePolicy(), key, new Bin[] { new Bin("text", column[0]), new Bin("favorited", column[1]), new Bin("favoriteCount", column[2]), new Bin("created", column[3]), new Bin("id", column[4]), new Bin("statusSource", column[5]), new Bin("screenName", column[6]), new Bin("retweetCount", column[7]), new Bin("timestamp", column[8]) });
                        recordsCount++;
                    }
                    else
                        break;
                }
            }
        }
    }
}
