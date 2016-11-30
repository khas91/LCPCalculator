using CommandLine;
using CommandLine.Text;
using System;
using System.Collections.Generic;
using System.Data.SqlClient;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace LCPCalculator
{
    class Options
    {
        [Option('m', "Min Term", Required = true, HelpText = "Minimum Term to search for LCPs")]
        public String min_term { get; set; }
        [Option('x', "Max Term", Required = true, HelpText = "Maximum Term to search for LCPs")]
        public String max_term { get; set; }
        [ParserState]
        public IParserState LastParserState { get; set; }
        [HelpOption]
        public string GetUsage()
        {
            return HelpText.AutoBuild(this,
              (HelpText current) => HelpText.DefaultParsingErrorsHandler(this, current));
        }
    }

    class LCPCalculator
    {
        static void Main(string[] args)
        {
            Options options = new Options();
            SqlConnection conn = new SqlConnection("Server=vulcan;database=MIS;Trusted_Connection=yes");
            Dictionary<String, Tuple<String, String, String>> courseLCPDictionary = new Dictionary<string, Tuple<string, string, string>>();
            List<String> courseList = new List<string>();

            try
            {
                conn.Open();
            }
            catch (Exception)
            {
                
                throw;
            }
            
            SqlCommand comm = new SqlCommand("SELECT                                                              "
	                                         +"       *                                                           "
                                             +"   FROM                                                            "
	                                         +"       MIS.dbo.ST_OCP_LCP_A_55 ocp                                 "
                                             +"   WHERE                                                           "
	                                         +"       LEFT(ocp.CRS_ID, 3) IN ('AHS', 'ASE')                       "
	                                         +"       AND COMP_POINT_TY = 'LS'                                    "
	                                         +"       AND (END_TRM = '' OR END_TRM >= '" + options.min_term + "') "
	                                         +"       AND EFF_TRM <= '" + options.max_term + "'", conn);

            SqlDataReader reader = comm.ExecuteReader();

            while (reader.Read())
            {
                String courseID = reader["CRS_ID"].ToString().Replace("*","");
                String LCPValue = reader["COMP_POINT_ID"].ToString();
                String min_term = reader["EFF_TRM"].ToString();
                String max_term = reader["END_TRM"].ToString();

                courseLCPDictionary.Add(courseID, new Tuple<string, string, string>(min_term, max_term, LCPValue));
                courseList.Add(courseID);
            }
        }
    }
}
