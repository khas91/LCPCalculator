using CommandLine;
using CommandLine.Text;
using System;
using System.Collections.Generic;
using System.Data.SqlClient;
using System.Linq;
using System.Text;
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
                return -1;

            OrionTerm min_term = new OrionTerm(options.min_term);
            OrionTerm max_term = new OrionTerm(options.max_term);

            List<Tuple<String, String, OrionTerm>> LCPs = new List<Tuple<string, string, OrionTerm>>();
            Dictionary<Tuple<String, String>, OrionTerm> prevLCPs = new Dictionary<Tuple<string, string>, OrionTerm>();

            while (min_term <= max_term)
            {
                LCPs = LCPs.Concat(calcLCPs(min_term.ToString(), prevLCPs)).ToList();

                foreach (Tuple<String, String, OrionTerm> LCP in LCPs)
                {
                    Tuple<String, String> key = new Tuple<string, string>(LCP.Item1, LCP.Item2);
                    prevLCPs.Add(key, LCP.Item3);
                }

                min_term++;
            }

            Console.WriteLine("dub");

            return 0;
        }

        private static List<Tuple<String,String, OrionTerm>> calcLCPs(String term, Dictionary<Tuple<String, String>, OrionTerm> prevLCPs)
        {
            SqlConnection conn = new SqlConnection("Server=vulcan;database=MIS;Trusted_Connection=yes");
            Dictionary<String, String[]> courseLCPDictionary = new Dictionary<string, String[]>();
            Dictionary<String, List<String>> prevStudentLCPs = new Dictionary<string, List<string>>();
            Dictionary<String, List<String>> calculatedLCPs = new Dictionary<string, List<string>>();
            List<String> courseList = new List<String>();
            OrionTerm runTerm = new OrionTerm(term);
            List<Tuple<String, String, OrionTerm>> totalLCPs = new List<Tuple<string, string, OrionTerm>>();

            try
            {
                conn.Open();
            }
            catch (Exception)
            {

                throw;
            }

            SqlCommand comm = new SqlCommand("SELECT                                                    "
                                             + "       *                                                 "
                                             + "   FROM                                                  "
                                             + "       MIS.dbo.ST_OCP_LCP_A_55 ocp                       "
                                             + "   WHERE                                                 "
                                             + "       COMP_POINT_TY = 'LS'                          "
                                             + "       AND (END_TRM = '' OR END_TRM >= '" + runTerm + "')"
                                             + "       AND EFF_TRM <= '" + runTerm + "'", conn);

            SqlDataReader reader = comm.ExecuteReader();

            while (reader.Read())
            {
                String courseID = reader["CRS_ID"].ToString().Replace("*", "");
                String LCPValue = reader["COMP_POINT_ID"].ToString();
                String min_term_string = reader["EFF_TRM"].ToString();
                String max_term_string = reader["END_TRM"].ToString();
                OrionTerm min_term = new OrionTerm(reader["EFF_TRM"].ToString());
                OrionTerm max_term = max_term_string == "" ? null : new OrionTerm(reader["END_TRM"].ToString());
                int priority = int.Parse(reader["COMP_POINT_SEQ"].ToString());

                if (!courseLCPDictionary.ContainsKey(courseID))
                {
                    courseLCPDictionary.Add(courseID, new String[19]);
                }

                if (min_term < runTerm && (max_term == null || max_term > runTerm))
                {
                    courseLCPDictionary[courseID][priority - 1] = LCPValue;
                }

                if (!courseList.Contains(courseID))
                {
                    courseList.Add(courseID);
                }
            }

            reader.Close();

            comm = new SqlCommand("SELECT                                                                                   "
                                  + "      *                                                                                 "
                                  + "  FROM                                                                                  "
                                  + "      StateSubmission.SDB.RecordType5 r5                                                "
                                  + "      INNER JOIN MIS.dbo.vwTermYearXwalk xwalk ON r5.DE1028 = xwalk.StateReportingTerm  "
                                  + "  WHERE                                                                                 "
                                  + "      r5.DE2105 <> 'Z'                                                                  "
                                  + "      AND xwalk.OrionTerm <= '" + runTerm + "'                                          "
                                  + "      AND r5.SubmissionType = 'E'                                                       "
                                  + "      AND xwalk.StateReportingYear IN ('" + runTerm.getStateReportingYear() + "','"
                                  + runTerm.getStateReportingYear().prevReportingYear() + "')", conn);

            reader = comm.ExecuteReader();

            while (reader.Read())
            {
                String studentID = reader["DE1021"].ToString();
                String LCPvalue = reader["DE2105"].ToString();

                if (!prevStudentLCPs.ContainsKey(studentID))
                {
                    prevStudentLCPs.Add(studentID, new List<string>());
                }

                prevStudentLCPs[studentID].Add(LCPvalue);
            }

            reader.Close();

            comm = new SqlCommand("SELECT                                                               "
                                  + "      *                                                            "
                                  + "  FROM                                                             "
                                  + "      StateSubmission.SDB.RecordType6 r6                           "
                                  + "  WHERE                                                            "
                                  + "      r6.DE3007 IN ('A','B','C','D','P','S')                       "
                                  + "      AND LEFT(r6.DE3008, 3) IN ('ASE','AHS')                      "
                                  + "      AND r6.DE1028 = '" + runTerm.ToStateReportingTermShort() + "'"
                                  + "  ORDER BY                                                         "
                                  + "      r6.DE1021", conn);

            reader = comm.ExecuteReader();

            while (reader.Read())
            {
                String courseID = reader["DE3008"].ToString();
                String studentID = reader["DE1021"].ToString();
                OrionTerm dbTerm = new StateReportingTermShort(reader["DE1028"].ToString()).ToOrionTerm();
                String[] eligibleLCPs = null;

                foreach (String coursePrefix in courseList)
                {
                    if (String.Compare(coursePrefix, 0, courseID, 0, coursePrefix.Length) == 0)
                    {
                        eligibleLCPs = courseLCPDictionary[coursePrefix];
                    }
                }

                if (eligibleLCPs == null)
                {
                    continue;
                }

                for (int i = 0; i < eligibleLCPs.Length; i++)
                {
                    if (eligibleLCPs[i] != null
                        && (!prevStudentLCPs.ContainsKey(studentID) || !prevStudentLCPs[studentID].Contains(eligibleLCPs[i])))
                    {
                        Tuple<String, String> key = new Tuple<string, string>(studentID, eligibleLCPs[i]);

                        if (!calculatedLCPs.ContainsKey(studentID))
                        {
                            calculatedLCPs.Add(studentID, new List<String>());
                        }
                        if (!calculatedLCPs[studentID].Contains(eligibleLCPs[i]) &&
                            (!prevLCPs.ContainsKey(key) || (prevLCPs[key].getStateReportingYear() != runTerm.getStateReportingYear()
                            && prevLCPs[key].getStateReportingYear() != runTerm.getStateReportingYear().prevReportingYear())))
                        {
                            calculatedLCPs[studentID].Add(eligibleLCPs[i]);
                            totalLCPs.Add(new Tuple<string, string, OrionTerm>(studentID, eligibleLCPs[i], dbTerm));
                        }
                        break;
                    }
                }
            }

            reader.Close();

            conn.Close();

            return totalLCPs;
        }
    }

}
