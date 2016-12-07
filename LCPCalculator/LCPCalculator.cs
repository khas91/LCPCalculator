using CommandLine;
using CommandLine.Text;
using System;
using System.Collections.Generic;
using System.Data.SqlClient;
using System.Linq;
using System.Text;
using System.IO;
using System.Threading.Tasks;
using TermLogic;

namespace LCPCalculator
{
    class Options
    {
        [Option('m', "MinTerm", Required = true)]
        public String min_term { get; set; }
        [Option('x', "MaxTerm", Required = true)]
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
        static int Main(string[] args)
        {
            Options options = new Options();

            if (!CommandLine.Parser.Default.ParseArguments(args, options))
            {
                return -1;
            }

            List<Tuple<String, String, OrionTerm>> previouslyCalculatedLCPs = new List<Tuple<string, string, OrionTerm>>();
            OrionTerm i = new OrionTerm(options.min_term);
            OrionTerm max_term = new OrionTerm(options.max_term);

            while (i <= max_term)
            {
                previouslyCalculatedLCPs = previouslyCalculatedLCPs.Concat(calculateLCPsForTerm(i, previouslyCalculatedLCPs)).ToList();
                i++;
            }
                    
            StreamWriter file = new StreamWriter("LCPs.csv");

            foreach (Tuple<String, String, OrionTerm> LCP in previouslyCalculatedLCPs)
            {
                file.WriteLine(LCP.Item1 + "," + LCP.Item2 + "," + LCP.Item3);
            }

            file.Close();

            return 0;

        }

        public static List<Tuple<String, String, OrionTerm>> calculateLCPsForTerm(OrionTerm term, List<Tuple<String, String, OrionTerm>> previouslyCalculatedLCPs)
        {
            SqlConnection conn = new SqlConnection("server=vulcan;Trusted_Connection=true;database=StateSubmission");
            SqlCommand comm;
            SqlDataReader reader;

            Dictionary<String, String[]> LCPDictionary = new Dictionary<string, string[]>();
            Dictionary<String, List<String>> previouslyReportedLCPs = new Dictionary<string, List<String>>();
            List<Tuple<String, String, OrionTerm>> calculatedLCPs = new List<Tuple<string, string, OrionTerm>>();
            List<String> coursePrefixes = new List<string>();

            try
            {
                conn.Open();
            }
            catch (Exception)
            {
                
                throw;
            }

            comm = new SqlCommand("SELECT                                                       "
	                              +"     CRS_ID, COMP_POINT_ID, COMP_POINT_SEQ                  "
                                  +" FROM                                                       "
	                              +"     MIS.dbo.ST_OCP_LCP_A_55 lcp                            "
                                  +" WHERE                                                      "
	                              +"     lcp.COMP_POINT_TY = 'LS'                               "
	                              +"     AND lcp.EFF_TRM <= '" + term + "'                      "
	                              +"     AND (lcp.END_TRM = '' OR lcp.END_TRM >= '" + term + "')"
                                  +" ORDER BY                                                   "
                                  +"     lcp.CRS_ID, lcp.COMP_POINT_SEQ DESC", conn);

            reader = comm.ExecuteReader();

            while (reader.Read())
            {
                String coursePrefix = reader["CRS_ID"].ToString().Replace("*", "");
                String lcpValue = reader["COMP_POINT_ID"].ToString();
                int priority = int.Parse(reader["COMP_POINT_SEQ"].ToString());

                if (!LCPDictionary.ContainsKey(coursePrefix))
                {
                    LCPDictionary.Add(coursePrefix, new String[priority]);
                }

                if (!coursePrefixes.Contains(coursePrefix))
                {
                    coursePrefixes.Add(coursePrefix);
                }

                LCPDictionary[coursePrefix][priority - 1] = lcpValue;
            }

            reader.Close();

            comm = new SqlCommand("SELECT                                                                                "
	                              +"     DE1021,OrionTerm,DE2105                                                         "
                                  +" FROM                                                                                "
	                              +"     StateSubmission.SDB.recordType5 r5                                              "
	                              +"     INNER JOIN MIS.dbo.vwTermYearXwalk xwalk ON xwalk.StateReportingTerm = r5.DE1028"
                                  +" WHERE                                                                               "
	                              +"     xwalk.OrionTerm < '" + term + "'                                                "
	                              +"     AND xwalk.StateReportingYear IN ('" + term.getStateReportingYear() + "','       "
                                  + term.getStateReportingYear().prevReportingYear() + "')                               "
	                              +"     AND r5.SubmissionType = 'E'                                                     "
	                              +"     AND r5.DE2105 <> 'Z'                                                            "
                                  +" ORDER BY                                                                            "
	                              +"     DE1021", conn);

            reader = comm.ExecuteReader();

            while (reader.Read())
            {
                String studentID = reader["DE1021"].ToString();
                String LCP = reader["DE2105"].ToString();

                OrionTerm LCPterm = new OrionTerm(reader["OrionTerm"].ToString());

                if (!previouslyReportedLCPs.ContainsKey(studentID))
                {
                    previouslyReportedLCPs.Add(studentID, new List<string>());
                }

                previouslyReportedLCPs[studentID].Add(LCP);
            }

            reader.Close();

            comm = new SqlCommand("SELECT                                                          "
	                              +"     r6.DE1021, r6.DE3008                                      "
                                  +" FROM                                                          "
	                              +"     StateSubmission.SDB.RecordType6 r6                        "
                                  +" WHERE                                                         "
	                              +"     LEFT(r6.DE3008, 3) IN ('AHS','ASE')                       "
	                              +"     AND r6.DE1028 = '" + term.ToStateReportingTermShort() + "'"
	                              +"     AND r6.SubmissionType = 'E'                               "
	                              +"     AND r6.DE3007 IN ('A','B','C','D','P','S')                "
                                  +" ORDER BY                                                      "
	                              +"     r6.DE1021", conn);

            reader = comm.ExecuteReader();

            String termS = term.ToString();

            while (reader.Read())
            {
                String courseID = reader["DE3008"].ToString();
                String studentID = reader["DE1021"].ToString();

                String[] eligibleLCPs = null;

                foreach (String coursePrefix in coursePrefixes)
                {
                    if (String.Compare(courseID, 0, coursePrefix, 0, coursePrefix.Length) == 0)
                    {
                        eligibleLCPs = LCPDictionary[coursePrefix];
                    }
                }

                if (eligibleLCPs == null)
                {
                    continue;
                }

                List<String> studentLCPs = previouslyReportedLCPs.ContainsKey(studentID) ? previouslyReportedLCPs[studentID] : new List<String>();
                
                for (int i = 0; i < eligibleLCPs.Length; i++)
                {
                    if (eligibleLCPs[i] == null)
                    {
                        continue;
                    }

                    bool previouslyCalculated = false;

                    if (!studentLCPs.Contains(eligibleLCPs[i]))
                    {                     
                        foreach (Tuple<String, String, OrionTerm> previouslyCalculatedLCP in previouslyCalculatedLCPs)
                        {
                            if (previouslyCalculatedLCP.Item1 == studentID && previouslyCalculatedLCP.Item2 == eligibleLCPs[i])
                            {
                                previouslyCalculated = true;
                                break;
                            }
                        }

                        foreach (Tuple<String, String, OrionTerm> previouslyCalculatedLCP in calculatedLCPs)
                        {
                            if (previouslyCalculatedLCP.Item1 == studentID && previouslyCalculatedLCP.Item2 == eligibleLCPs[i])
                            {
                                previouslyCalculated = true;
                                break;
                            }
                        }

                        if (!previouslyCalculated)
                        {
                            Tuple<String, String, OrionTerm> newLCP = new Tuple<string, string, OrionTerm>(studentID, eligibleLCPs[i], term);
                            calculatedLCPs.Add(newLCP);
                            continue;
                        }
                    }
                }
            }

            reader.Close();
            conn.Close();

            return calculatedLCPs;
        }
    }

}
