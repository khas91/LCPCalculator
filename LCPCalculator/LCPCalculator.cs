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
using System.Globalization;

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

            comm = new SqlCommand("SELECT                                                                                                                       "
                                  +"       CASE                                                                                                                 "
		                          +"          WHEN prev.PREV_STDNT_SSN IS NULL THEN r5.DE1021                                                                   "
		                          +"          ELSE stdnt.STUDENT_SSN                                                                                            "
	                              +"      END AS [Student_SSN]                                                                                                  "
	                              +"       ,OrionTerm,DE2105                                                                                                    "
                                  +"   FROM                                                                                                                     "
                                  +"       StateSubmission.SDB.recordType5 r5                                                                                   "
	                              +"       INNER JOIN MIS.dbo.vwTermYearXwalk xwalk ON xwalk.StateReportingTerm = r5.DE1028                                     "
	                              +"       LEFT JOIN MIS.dbo.ST_STDNT_A_PREV_STDNT_SSN_USED_125 prev ON prev.PREV_STDNT_SSN_TY + prev.PREV_STDNT_SSN = r5.DE1021"
	                              +"       LEFT JOIN MIS.dbo.ST_STDNT_A_125 stdnt ON stdnt.[ISN_ST_STDNT_A] = prev.[ISN_ST_STDNT_A]                             "
                                  +"   WHERE                                                                                                                    "
                                  +"       xwalk.OrionTerm < '" + term + "'                                                                                     "
                                  +"       AND r5.SubmissionType = 'E'                                                                                          "
                                  +"       AND r5.DE2105 <> 'Z'                                                                                                 "
                                  +"   ORDER BY                                                                                                                 "
                                  +"       [Student_SSN]", conn);

            reader = comm.ExecuteReader();

            while (reader.Read())
            {
                String studentID = reader["Student_SSN"].ToString().Trim();
                String LCP = reader["DE2105"].ToString().Trim();

                OrionTerm LCPterm = new OrionTerm(reader["OrionTerm"].ToString());

                if (!previouslyReportedLCPs.ContainsKey(studentID))
                {
                    previouslyReportedLCPs.Add(studentID, new List<string>());
                }

                previouslyReportedLCPs[studentID].Add(LCP);
            }

            reader.Close();

            comm = new SqlCommand("SELECT                                                                                                                      "
                                  +"      CASE                                                                                                                 "
		                          +"          WHEN prev.PREV_STDNT_SSN IS NULL THEN r6.DE1021                                                                  "
		                          +"          ELSE stdnt.STUDENT_SSN                                                                                           "
	                              +"      END AS [Student_SSN], r6.DE3008                                                                                      "
                                  +"  FROM                                                                                                                     "
                                  +"      StateSubmission.SDB.RecordType6 r6                                                                                   "
	                              +"      LEFT JOIN MIS.dbo.ST_STDNT_A_PREV_STDNT_SSN_USED_125 prev ON prev.PREV_STDNT_SSN_TY + prev.PREV_STDNT_SSN = r6.DE1021"
	                              +"      LEFT JOIN MIS.dbo.ST_STDNT_A_125 stdnt ON stdnt.[ISN_ST_STDNT_A] = prev.[ISN_ST_STDNT_A]                             "                    
                                  +"  WHERE                                                                                                                    "
                                  +"      LEFT(r6.DE3008, 3) IN ('AHS','ASE')                                                                                  "
                                  +"      AND r6.DE1028 = '" + term.ToStateReportingTermShort() + "'                                                           "
                                  +"      AND r6.SubmissionType = 'E'                                                                                          "
                                  +"      AND r6.DE3007 IN ('A','B','C','D','P','S')                                                                           "
                                  +"  ORDER BY                                                                                                                 "
                                  +"      [Student_SSN]", conn);

            reader = comm.ExecuteReader();

            String termS = term.ToString();

            while (reader.Read())
            {
                String courseID = reader["DE3008"].ToString().Trim();
                String studentID = reader["Student_SSN"].ToString().Trim();

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
                            break;
                        }
                    }
                }
            }

            reader.Close();

            comm = new SqlCommand("SELECT                                                                                                                        "
                                  + "       CASE                                                                                                                 "
                                  + "          WHEN prev.PREV_STDNT_SSN IS NULL THEN r5.DE1021                                                                   "
                                  + "          ELSE stdnt.STUDENT_SSN                                                                                            "
                                  + "      END AS [Student_SSN]                                                                                                  "
                                  + "       ,OrionTerm,DE2105                                                                                                    "
                                  + "   FROM                                                                                                                     "
                                  + "       StateSubmission.SDB.recordType5 r5                                                                                   "
                                  + "       INNER JOIN MIS.dbo.vwTermYearXwalk xwalk ON xwalk.StateReportingTerm = r5.DE1028                                     "
                                  + "       LEFT JOIN MIS.dbo.ST_STDNT_A_PREV_STDNT_SSN_USED_125 prev ON prev.PREV_STDNT_SSN_TY + prev.PREV_STDNT_SSN = r5.DE1021"
                                  + "       LEFT JOIN MIS.dbo.ST_STDNT_A_125 stdnt ON stdnt.[ISN_ST_STDNT_A] = prev.[ISN_ST_STDNT_A]                             "
                                  + "   WHERE                                                                                                                    "
                                  + "       xwalk.OrionTerm < '" + term + "'                                                                                     "
                                  + "       AND xwalk.StateReportingYear NOT IN ('" + term.getStateReportingYear() + "','" 
                                  + term.getStateReportingYear().prevReportingYear() +"')" 
                                  + "       AND r5.SubmissionType = 'E'                                                                                          "
                                  + "       AND r5.DE2105 <> 'Z'                                                                                                 "
                                  + "   ORDER BY                                                                                                                 "
                                  + "       [Student_SSN]", conn);

            reader = comm.ExecuteReader();

            previouslyReportedLCPs.Clear();

            while (reader.Read())
            {
                String studentID = reader["Student_SSN"].ToString().Trim();
                String LCP = reader["DE2105"].ToString().Trim();

                OrionTerm LCPterm = new OrionTerm(reader["OrionTerm"].ToString());

                if (!previouslyReportedLCPs.ContainsKey(studentID))
                {
                    previouslyReportedLCPs.Add(studentID, new List<string>());
                }

                previouslyReportedLCPs[studentID].Add(LCP);
            }

            reader.Close();

            int[] mathRanges = new int[] { 0, 314, 442, 506, 565 };
            String[] mathLCPs = new String[] { "A", "B", "C", "D" };
            int[] readingRanges = new int[] { 0, 368, 461, 518, 566 };
            String[] readingLCPs = new String[] { "E", "F", "G", "H" };
            int[] languageRanges = new int[] { 0, 390, 491, 524, 559 };
            String[] languageLCPs = new String[] { "J", "K", "M", "N" };

            comm = new SqlCommand("SELECT                                                                                              "
                                  + "      class.STDNT_ID                                                                              "
                                  + "      ,class.CRS_ID                                                                               "
                                  + "      ,class.REF_NUM                                                                              "      
                                  + "      ,MAX(log.LOG_DATE) AS REG_DT                                                                "
                                  + "      ,test.TST_DT                                                                                "
                                  + "      ,test.SUBTEST                                                                               "
                                  + "      ,test.SCALE_SCORE                                                                           "
                                  + "  FROM                                                                                            "
                                  + "      MIS.dbo.ST_STDNT_CLS_A_235 class                                                            "
                                  + "      INNER JOIN MIS.dbo.ST_STDNT_CLS_LOG_230 log ON log.REF_NUM = class.REF_NUM                  "
                                  + "      INNER JOIN Adhoc.dbo.Course_Subject_Area_Xwalk xwalk ON xwalk.CRS_ID = LEFT(class.CRS_ID, 7)"
                                  + "      INNER JOIN MIS.dbo.ST_SUBTEST_A_155 test ON test.STUDENT_ID = class.STDNT_ID                "
                                  + "   		                                       AND test.SUBTEST = xwalk.SUBJECT                "
                                  + "  WHERE                                                                                           "
                                  + "      class.EFF_TRM = '" + term + "'                                                              "
                                  + "      AND log.LOG_ACTION = 'A'                                                                    "
                                  + "      AND test.TST_TY = 'TABE'                                                                    "
                                  + "  GROUP BY                                                                                        "
                                  + "      class.STDNT_ID                                                                              "
                                  + "      ,class.CRS_ID                                                                               "
	                              + "      ,class.REF_NUM                                                                              "
                                  + "      ,test.TST_DT                                                                                "
                                  + "      ,test.SUBTEST                                                                               "
                                  + "      ,test.SCALE_SCORE                                                                           "
                                  + "  ORDER BY                                                                                        "
                                  + "      class.STDNT_ID                                                                              "
                                  + "      ,class.REF_NUM", conn);

            reader = comm.ExecuteReader();

            String curStudent = null;
            String curCourse = null;
            String curRefNum = null;
            String curSubject = null;
            DateTime curPreTestDate = new DateTime();
            DateTime curPostTestDate = new DateTime();
            int preTestScore = 0;
            int postTestScore = 0;

            while (reader.Read())
            {
                String studentID = reader["STDNT_ID"].ToString();
                String subject = reader["SUBTEST"].ToString();
                String refNum = reader["REF_NUM"].ToString();
                String courseID = reader["CRS_ID"].ToString();
                int score = int.Parse(reader["SCALE_SCORE"].ToString());
                DateTime registrationDate = DateTime.ParseExact(reader["REG_DT"].ToString(), "yyyyMMdd", CultureInfo.InvariantCulture, DateTimeStyles.None);
                DateTime testDate = DateTime.ParseExact(reader["TST_DT"].ToString(), "yyyyMMdd", CultureInfo.InvariantCulture, DateTimeStyles.None);
                                                
                if (curStudent != null && curRefNum != refNum)
                {
                    if (preTestScore != 0 && postTestScore != 0)
                    {
                        int[] scoreRanges = null;
                        String[] LCPs = null;
                        int initialFunctioningLevel = 0;
                        int finalFunctioningLevel = 0;

                        if (curSubject == "RE")
                        {
                            scoreRanges = readingRanges;
                            LCPs = readingLCPs;
                        }
                        else if (curSubject == "MA")
                        {
                            scoreRanges = mathRanges;
                            LCPs = mathLCPs;
                        }
                        else
                        {
                            scoreRanges = languageRanges;
                            LCPs = languageLCPs;
                        }

                        for (int i = 0; i < scoreRanges.Length && preTestScore >= scoreRanges[i]; i++)
                        {
                            initialFunctioningLevel = i;
                        }

                        for (int i = initialFunctioningLevel; i < scoreRanges.Length && postTestScore >= scoreRanges[i]; )
                        {
                            finalFunctioningLevel = i++;
                        }

                        for (int i = initialFunctioningLevel; i < finalFunctioningLevel; i++)
                        {
                            List<String> studentLCPs = previouslyReportedLCPs.ContainsKey(studentID) ? previouslyReportedLCPs[studentID] : new List<String>();

                            bool previouslyCalculated = false;

                            if (!studentLCPs.Contains(LCPs[i]))
                            {
                                Tuple<String, String, OrionTerm> LCP = new Tuple<string, string, OrionTerm>(studentID, LCPs[i], term);

                                foreach (Tuple<String, String, OrionTerm> previouslyCalculatedLCP in previouslyCalculatedLCPs)
                                {
                                    if (previouslyCalculatedLCP.Item1 == studentID && previouslyCalculatedLCP.Item2 == LCPs[i] &&
                                        (previouslyCalculatedLCP.Item3.getStateReportingYear() == term.getStateReportingYear()
                                        || previouslyCalculatedLCP.Item3.getStateReportingYear() == term.getStateReportingYear().prevReportingYear()))
                                    {
                                        previouslyCalculated = true;
                                        break;
                                    }
                                }

                                foreach (Tuple<String, String, OrionTerm> previouslyCalculatedLCP in calculatedLCPs)
                                {
                                    if (previouslyCalculatedLCP.Item1 == studentID && previouslyCalculatedLCP.Item2 == LCPs[i] &&
                                        (previouslyCalculatedLCP.Item3.getStateReportingYear() == term.getStateReportingYear()
                                        || previouslyCalculatedLCP.Item3.getStateReportingYear() == term.getStateReportingYear().prevReportingYear()))
                                    {
                                        previouslyCalculated = true;
                                        break;
                                    }
                                }

                                if (!previouslyCalculated)
                                {
                                    Tuple<String, String, OrionTerm> newLCP = new Tuple<string, string, OrionTerm>(studentID, LCPs[i], term);
                                    calculatedLCPs.Add(newLCP);
                                }
                            }
                        }
                    }

                    curPreTestDate = new DateTime();
                    curPostTestDate = new DateTime();
                    preTestScore = 0;
                    postTestScore = 0;
                }

                if (score > postTestScore && testDate > registrationDate)
                {
                    curPostTestDate = testDate;
                    postTestScore = score;
                }
                if (testDate <= registrationDate && testDate > curPreTestDate && score != 0)
                {
                    curPreTestDate = testDate;
                    preTestScore = score;
                }

                curStudent = studentID;
                curRefNum = refNum;
                curCourse = courseID;
                curSubject = subject;
            }


            conn.Close();

            return calculatedLCPs;
        }
    }

}
