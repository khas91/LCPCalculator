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

            List<Tuple<String, String, String, OrionTerm>> previouslyCalculatedLCPs = new List<Tuple<string, string, string, OrionTerm>>();
            OrionTerm i = new OrionTerm(options.min_term);
            OrionTerm max_term = new OrionTerm(options.max_term);

            while (i <= max_term)
            {
                previouslyCalculatedLCPs = previouslyCalculatedLCPs.Concat(calculateLCPsForTerm(i, previouslyCalculatedLCPs)).ToList();
                i++;
            }

            StreamWriter file = new StreamWriter("..\\..\\..\\LCPs.csv");

            file.WriteLine("StudentID,Type,COMP_POINT_ID,Term");

            foreach (Tuple<String, String, String, OrionTerm> LCP in previouslyCalculatedLCPs)
            {
                file.WriteLine(LCP.Item1 + "," + LCP.Item2 + "," + LCP.Item3 + "," + LCP.Item4);
            }

            file.Close();

            return 0;

        }

        public static List<Tuple<String, String, String, OrionTerm>> calculateLCPsForTerm(OrionTerm term, List<Tuple<String, String, String, OrionTerm>> previouslyCalculatedLCPs)
        {
            SqlConnection conn = new SqlConnection("server=vulcan;Trusted_Connection=true;database=StateSubmission");
            SqlCommand comm;
            SqlDataReader reader;

            Dictionary<String, String[]> LCPDictionary = new Dictionary<string, string[]>();
            Dictionary<String, List<String>> previouslyReportedLCPs = new Dictionary<string, List<String>>();
            List<Tuple<String, String, String, OrionTerm>> calculatedLCPs = new List<Tuple<string, string, string, OrionTerm>>();
            List<String> coursePrefixes = new List<string>();
            List<String> listeningStudents = new List<string>();
            List<String> readingStudents = new List<string>();

            try
            {
                conn.Open();
            }
            catch (Exception)
            {
                throw;
            }

            comm = new SqlCommand("SELECT                                                       "
                                  + "     CRS_ID, COMP_POINT_ID, COMP_POINT_SEQ                  "
                                  + " FROM                                                       "
                                  + "     MIS.dbo.ST_OCP_LCP_A_55 lcp                            "
                                  + " WHERE                                                      "
                                  + "     lcp.COMP_POINT_TY = 'LS'                               "
                                  + "     AND lcp.EFF_TRM <= '" + term + "'                      "
                                  + "     AND (lcp.END_TRM = '' OR lcp.END_TRM >= '" + term + "')"
                                  + " ORDER BY                                                   "
                                  + "     lcp.CRS_ID, lcp.COMP_POINT_SEQ DESC", conn);

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
                                  + "      CASE                                                                                                                  "
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
                                  + "       AND r5.SubmissionType = 'E'                                                                                          "
                                  + "       AND r5.DE2105 <> 'Z'                                                                                                 "
                                  + "   ORDER BY                                                                                                                 "
                                  + "       [Student_SSN]", conn);

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
            
            comm = new SqlCommand("SELECT                                                                                                                       "
                                  + "      CASE                                                                                                                 "
                                  + "          WHEN prev.PREV_STDNT_SSN IS NULL THEN r6.DE1021                                                                  "
                                  + "          ELSE stdnt.STUDENT_SSN                                                                                           "
                                  + "      END AS [Student_SSN], r6.DE3008                                                                                      "
                                  + "  FROM                                                                                                                     "
                                  + "      StateSubmission.SDB.RecordType6 r6                                                                                   "
                                  + "      LEFT JOIN MIS.dbo.ST_STDNT_A_PREV_STDNT_SSN_USED_125 prev ON prev.PREV_STDNT_SSN_TY + prev.PREV_STDNT_SSN = r6.DE1021"
                                  + "      LEFT JOIN MIS.dbo.ST_STDNT_A_125 stdnt ON stdnt.[ISN_ST_STDNT_A] = prev.[ISN_ST_STDNT_A]                             "
                                  + "  WHERE                                                                                                                    "
                                  + "      LEFT(r6.DE3008, 3) IN ('AHS','ASE')                                                                                  "
                                  + "      AND r6.DE1028 = '" + term.ToStateReportingTermShort() + "'                                                           "
                                  + "      AND r6.SubmissionType = 'E'                                                                                          "
                                  + "      AND r6.DE3007 IN ('A','B','C','D','P','S')                                                                           "
                                  + "  ORDER BY                                                                                                                 "
                                  + "      [Student_SSN]", conn);

            reader = comm.ExecuteReader();

            if (!reader.HasRows)
            {
                reader.Close();
                comm = new SqlCommand("SELECT                                                                                                                       "
                                      + "      CASE                                                                                                                 "
                                      + "          WHEN prev.PREV_STDNT_SSN IS NULL THEN r6.DE1021                                                                  "
                                      + "          ELSE stdnt.STUDENT_SSN                                                                                           "
                                      + "      END AS [Student_SSN], r6.DE3008                                                                                      "
                                      + "  FROM                                                                                                                     "
                                      + "      State_Report_Data.dbo.sdb_rtype_6 r6                                                                                 "
                                      + "      LEFT JOIN MIS.dbo.ST_STDNT_A_PREV_STDNT_SSN_USED_125 prev ON prev.PREV_STDNT_SSN_TY + prev.PREV_STDNT_SSN = r6.DE1021"
                                      + "      LEFT JOIN MIS.dbo.ST_STDNT_A_125 stdnt ON stdnt.[ISN_ST_STDNT_A] = prev.[ISN_ST_STDNT_A]                             "
                                      + "  WHERE                                                                                                                    "
                                      + "      LEFT(r6.DE3008, 3) IN ('AHS','ASE')                                                                                  "
                                      + "      AND r6.DE3007 IN ('A','B','C','D','P','S')                                                                           "
                                      + "  ORDER BY                                                                                                                 "
                                      + "      [Student_SSN]", conn);

                reader = comm.ExecuteReader();
            }

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
                        foreach (Tuple<String, String, String, OrionTerm> previouslyCalculatedLCP in previouslyCalculatedLCPs)
                        {
                            if (previouslyCalculatedLCP.Item1 == studentID && previouslyCalculatedLCP.Item3 == eligibleLCPs[i])
                            {
                                previouslyCalculated = true;
                                break;
                            }
                        }

                        foreach (Tuple<String, String, String, OrionTerm> previouslyCalculatedLCP in calculatedLCPs)
                        {
                            if (previouslyCalculatedLCP.Item1 == studentID && previouslyCalculatedLCP.Item2 == eligibleLCPs[i])
                            {
                                previouslyCalculated = true;
                                break;
                            }
                        }

                        if (!previouslyCalculated)
                        {
                            Tuple<String, String, String, OrionTerm> newLCP = new Tuple<string, string, string, OrionTerm>(studentID, "AHS", eligibleLCPs[i], term);
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
                                  + term.getStateReportingYear().prevReportingYear() + "')"
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

            Dictionary<String, int[]> testFormRanges = new Dictionary<string, int[]>();
          
            comm = new SqlCommand("SELECT                                                                                                                            "
                                  +"      class.STDNT_ID, class.CRS_ID, class.REF_NUM, xwalk.SUBJECT                                                                 "
	                              +"      ,pretest.TST_DT AS [Pretest Date], pretest.SCALE_SCORE AS [Pretest Score],                                                 "
                                  +"      pretabe.LOWER_RANGE AS [Pretest Lower], pretabe.UPPER_RANGE AS [Pretest Upper], MAX(classlog.LOG_DATE) AS [RegDate]        "
	                              +"      ,posttest.TST_DT AS [Posttest Date], posttest.SCALE_SCORE AS [Posttest Score],                                             "
                                  +"      posttabe.LOWER_RANGE AS [Posttest Lower], posttabe.UPPER_RANGE AS [Posttest Upper]                                         "
                                  +"  FROM                                                                                                                           "
                                  +"      MIS.[dbo].[ST_STDNT_CLS_A_235]                                                                                             "
                                  +"          class                                                                                                                  "
                                  +"      INNER JOIN MIS.[dbo].[ST_STDNT_CLS_LOG_230] classlog ON classlog.REF_NUM = class.REF_NUM                                   "
                                  +"                                                           AND classlog.STDNT_ID = class.STDNT_ID                                "
                                  +"      INNER JOIN Adhoc.[dbo].[Course_Subject_Area_Xwalk] xwalk ON xwalk.CRS_ID = LEFT(class.CRS_ID, 7)                           "
	                              +"      INNER JOIN MIS.dbo.ST_SUBTEST_A_155 pretest ON pretest.SUBTEST = xwalk.SUBJECT                                             "
                                  +"                                               AND pretest.STUDENT_ID = class.STDNT_ID                                           "
                                  +"      INNER JOIN MIS.dbo.ST_SUBTEST_A_155 posttest ON posttest.SUBTEST = xwalk.SUBJECT                                           "
                                  +"                                               AND posttest.STUDENT_ID = class.STDNT_ID                                          "
                                  +"      LEFT JOIN Adhoc.dbo.TABEFormRanges pretabe ON pretabe.Form = LEFT(pretest.TST_FRM, 1)                                      "
                                  +"                                               AND pretabe.SUBTEST = pretest.SUBTEST                                             "
                                  +"      LEFT JOIN Adhoc.dbo.TABEFormRanges posttabe ON posttabe.Form = LEFT(posttest.TST_FRM, 1)                                   "
                                  +"                                               AND posttabe.SUBTEST = posttest.SUBTEST                                           "
                                  +"  WHERE                                                                                                                          "
                                  +"      class.EFF_TRM = '" + term + "'                                                                                             "
	                              +"      AND classlog.LOG_ACTION = 'A'                                                                                              "
	                              +"      AND pretest.TST_TY = 'TABE'                                                                                                "
                                  +"      AND pretest.SCALE_SCORE > 0                                                                                                "
                                  +"      AND posttest.TST_TY = 'TABE'                                                                                               "
                                  +"      AND pretest.SCALE_SCORE<posttest.SCALE_SCORE                                                                               "
                                  +"      AND pretest.TST_DT> (SELECT                                                                                                "
                                  +"                              SESS_BEG_DT                                                                                        "
                                  +"                          FROM                                                                                                   "
                                  +"                              MIS.dbo.vwTermYearXwalk xwalk                                                                      "
                                  +"                          WHERE                                                                                                  "
                                  +"                              OrionTerm = '" + term.getStateReportingYear().prevReportingYear().getNthTerm(1).ToOrionTerm() + "')"                                  +"  GROUP BY                                                                                                                 "
                                  +"      class.STDNT_ID, class.CRS_ID, class.REF_NUM, xwalk.SUBJECT                                                                 "
	                              +"      , pretest.TST_DT, pretest.SCALE_SCORE, posttest.TST_DT, posttest.SCALE_SCORE                                               "
	                              +"      , posttabe.LOWER_RANGE, posttabe.UPPER_RANGE, pretabe.LOWER_RANGE, pretabe.UPPER_RANGE                                     "
                                  +"  HAVING                                                                                                                         "
                                  +"      posttest.TST_DT > MAX(classlog.LOG_DATE)                                                                                   "
                                  +"      AND pretest.TST_DT <= MAX(classlog.LOG_DATE)                                                                               "
                                  +"  ORDER BY                                                                                                                       "
                                  +"      class.STDNT_ID, class.REF_NUM, pretest.TST_DT", conn);
            reader = comm.ExecuteReader();

            while (reader.Read())
            {
                String studentID = reader["STDNT_ID"].ToString();
                String subject = reader["SUBJECT"].ToString();
                String refNum = reader["REF_NUM"].ToString();
                String courseID = reader["CRS_ID"].ToString();

                int pretestScore = int.Parse(reader["Pretest Score"].ToString());
                int posttestScore = int.Parse(reader["Posttest Score"].ToString());
                
                int preLowerBound = int.Parse(reader["Pretest Lower"].ToString() == "" ? "0" : reader["Pretest Lower"].ToString());
                int preUpperBound = int.Parse(reader["Pretest Upper"].ToString() == "" ? "999" : reader["Pretest Upper"].ToString());

                int postLowerBound = int.Parse(reader["Posttest Lower"].ToString() == "" ? "0" : reader["Posttest Lower"].ToString());
                int postUpperBound = int.Parse(reader["Posttest Upper"].ToString() == "" ? "999" : reader["Posttest Upper"].ToString());
                
                DateTime registrationDate = DateTime.ParseExact(reader["RegDate"].ToString(), "yyyyMMdd", CultureInfo.InvariantCulture, DateTimeStyles.None);
                DateTime pretestDate = DateTime.ParseExact(reader["Pretest Date"].ToString(), "yyyyMMdd", CultureInfo.InvariantCulture, DateTimeStyles.None);
                DateTime posttestDate = DateTime.ParseExact(reader["Posttest Date"].ToString(), "yyyyMMdd", CultureInfo.InvariantCulture, DateTimeStyles.None);

                int[] scoreRanges = null;
                String[] LCPs = null;
                int initialFunctioningLevel = 0;
                int finalFunctioningLevel = 0;

                if (subject == "RE")
                {
                    scoreRanges = readingRanges;
                    LCPs = readingLCPs;
                }
                else if (subject == "MA")
                {
                    scoreRanges = mathRanges;
                    LCPs = mathLCPs;
                }
                else
                {
                    scoreRanges = languageRanges;
                    LCPs = languageLCPs;
                }

                for (int i = 0; i < scoreRanges.Length && pretestScore >= scoreRanges[i]; i++)
                {
                    initialFunctioningLevel = i;
                }

                for (int i = initialFunctioningLevel; i < scoreRanges.Length && posttestScore >= scoreRanges[i];)
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

                        foreach (Tuple<String, String, String, OrionTerm> previouslyCalculatedLCP in previouslyCalculatedLCPs)
                        {
                            if (previouslyCalculatedLCP.Item1 == studentID && previouslyCalculatedLCP.Item3 == LCPs[i] &&
                                (previouslyCalculatedLCP.Item4.getStateReportingYear() == term.getStateReportingYear()
                                || previouslyCalculatedLCP.Item4.getStateReportingYear() == term.getStateReportingYear().prevReportingYear()))
                            {
                                previouslyCalculated = true;
                                break;
                            }
                        }

                        foreach (Tuple<String, String, String, OrionTerm> previouslyCalculatedLCP in calculatedLCPs)
                        {
                            if (previouslyCalculatedLCP.Item1 == studentID && previouslyCalculatedLCP.Item3 == LCPs[i] &&
                                (previouslyCalculatedLCP.Item4.getStateReportingYear() == term.getStateReportingYear()
                                || previouslyCalculatedLCP.Item4.getStateReportingYear() == term.getStateReportingYear().prevReportingYear()))
                            {
                                previouslyCalculated = true;
                                break;
                            }
                        }

                        if (!previouslyCalculated)
                        {
                            String LCPType = pretestScore < preLowerBound || pretestScore > preUpperBound || posttestScore < postLowerBound || posttestScore > postUpperBound ? "ABEI" : "ABE";

                            Tuple<String, String, String, OrionTerm> newLCP = new Tuple<string, string, string, OrionTerm>(studentID, LCPType, LCPs[i], term);
                            calculatedLCPs.Add(newLCP);
                        }
                    }
                }
            }

            reader.Close();

            comm = new SqlCommand("SELECT                                                                                           "
                                  + "      STUDENT_ID                                                                               "
                                  + "      ,Form                                                                                    "
                                  + "      ,TST_SCR                                                                                 "
                                  + "  FROM                                                                                         "
                                  + "      (SELECT                                                                                  "
                                  + "          STUDENT_ID                                                                           "
                                  + "          ,RIGHT(TST_FRM, 1) AS[Form]                                                          "
                                  + "          ,TST_SCR                                                                             "
                                  + "          ,TST_DT                                                                              "
                                  + "          ,ROW_NUMBER() OVER(PARTITION BY STUDENT_ID, RIGHT(TST_FRM, 1) ORDER BY TST_DT) AS RN "
                                  + "      FROM                                                                                     "
                                  + "          MIS.dbo.ST_SUBTEST_A_155 test                                                        "
                                  + "      WHERE                                                                                    "
                                  + "          test.TST_TY = 'CASA'                                                                 "
                                  + "          AND test.TST_SCR > 0                                                                 "
                                  + "          AND RIGHT(test.TST_FRM, 1) IN('R', 'L')) SRC                                         "
                                  + "  WHERE                                                                                        "
                                  + "      RN = 1                                                                                   "
                                  + "  ORDER BY                                                                                     "
                                  + "      STUDENT_ID", conn);

            reader = comm.ExecuteReader();

            String curStudent = "";
            float curReadingScore = 0;
            float curListeningScore = 0;

            while (reader.Read())
            {
                String student = reader["STUDENT_ID"].ToString();
                String form = reader["Form"].ToString();
                float score = float.Parse(reader["TST_SCR"].ToString());

                if (student != curStudent && curStudent != "")
                {
                    if (curReadingScore > curListeningScore)
                    {
                        listeningStudents.Add(curStudent);
                    }
                    else
                    {
                        readingStudents.Add(curStudent);
                    }

                    curReadingScore = 0;
                    curListeningScore = 0;
                }

                if (form == "R")
                {
                    curReadingScore = score;
                }
                else
                {
                    curListeningScore = score;
                }


                curStudent = student;
            }

            reader.Close();

            int[] ESOLLevels = new int[] { 0, 179, 189, 199, 209, 219, 234};
            String[] ESOLLCPs = new string[] { "A", "B", "C", "D", "E", "F" };

            comm = new SqlCommand("SELECT                                                                                                                                          "
                                  +"      STUDENT_ID                                                                                                                               "
                                  +"      ,[Pretest Date]                                                                                                                          "
                                  +"      ,[Pretest Form]                                                                                                                          "
                                  +"      ,[Pretest Score]                                                                                                                         "
                                  +"      , REF_NUM                                                                                                                                "
                                  +"      , RegDate                                                                                                                                "
                                  +"      ,[Post-test Date]                                                                                                                        "
                                  +"      ,[Post-test Form]                                                                                                                        "
                                  +"      ,[Post-test Score]                                                                                                                       "
                                  +"  FROM                                                                                                                                         "
                                  +"      (SELECT                                                                                                                                  "
                                  +"          pretest.STUDENT_ID,                                                                                                                  "
                                  +"          pretest.TST_DT AS[Pretest Date],                                                                                                     "
                                  +"          RIGHT(pretest.TST_FRM, 1) AS[Pretest Form],                                                                                          "
                                  +"          pretest.TST_SCR AS[Pretest Score],                                                                                                   "
                                  +"          class.REF_NUM,                                                                                                                       "
                                  +"          MAX(classlog.LOG_DATE) AS[RegDate],                                                                                                  "
		                          +"          posttest.TST_DT AS[Post-test Date],                                                                                                  "
                                  +"          RIGHT(posttest.TST_FRM, 1) AS[Post-test Form],                                                                                       "
		                          +"          posttest.TST_SCR[Post-test Score],                                                                                                   "
                                  +"          ROW_NUMBER() OVER(PARTITION BY pretest.STUDENT_ID, RIGHT(posttest.TST_FRM, 1), class.REF_NUM ORDER BY pretest.TST_DT DESC) AS RN,    "
                                  +"         ROW_NUMBER() OVER(PARTITION BY pretest.STUDENT_ID, RIGHT(posttest.TST_FRM, 1), class.REF_NUM ORDER BY posttest.TST_SCR DESC) AS RN2   "
                                  +"      FROM                                                                                                                                     "
                                  +"          MIS.dbo.ST_STDNT_CLS_A_235 class                                                                                                     "
                                  +"          INNER JOIN MIS.dbo.ST_STDNT_CLS_LOG_230 classlog ON classlog.REF_NUM = class.REF_NUM                                                 "
                                  +"                                                           AND classlog.STDNT_ID = class.STDNT_ID                                              "
                                  +"          INNER JOIN MIS.dbo.ST_SUBTEST_A_155 pretest ON pretest.STUDENT_ID = class.STDNT_ID                                                   "
                                  +"          INNER JOIN MIS.dbo.ST_SUBTEST_A_155 posttest ON posttest.STUDENT_ID = class.STDNT_ID                                                 "
                                  +"                                                       AND RIGHT(pretest.TST_FRM, 1) = RIGHT(posttest.TST_FRM, 1)                              "
                                  +"      WHERE                                                                                                                                    "
                                  +"          LEFT(class.CRS_ID, 3) = 'ELL'                                                                                                        "
		                          +"          AND class.EFF_TRM = '" + term.ToString() + "'                                                                                        "
		                          +"          AND pretest.TST_TY = 'CASA'                                                                                                          "
		                          +"          AND posttest.TST_TY = 'CASA'                                                                                                         "
                                  +"          AND classlog.LOG_ACTION = 'A'                                                                                                        "
                                  +"          AND RIGHT(pretest.TST_FRM, 1) IN('L','R')                                                                                            "
                                  +"          AND RIGHT(posttest.TST_FRM, 1) IN('L','R')                                                                                           "
                                  +"          AND pretest.TST_SCR > 0                                                                                                              "
		                          +"          AND posttest.TST_SCR > 0                                                                                                             "
                                  +"          AND pretest.TST_SCR<posttest.TST_SCR                                                                                                 "
                                  +"      GROUP BY                                                                                                                                 "
                                  +"          pretest.STUDENT_ID, class.REF_NUM,RIGHT(pretest.TST_FRM, 1),pretest.TST_DT                                                           "
		                          +"          ,posttest.TST_DT, RIGHT(posttest.TST_FRM, 1), pretest.TST_SCR, posttest.TST_SCR                                                      "
                                  +"      HAVING                                                                                                                                   "
                                  +"          pretest.TST_DT <= MAX(classlog.LOG_DATE)                                                                                             "
                                  +"          AND posttest.TST_DT > MAX(classlog.LOG_DATE)) SRC                                                                                    "
                                  +"  WHERE                                                                                                                                        "
                                  +"      SRC.RN = 1                                                                                                                               "
	                              +"      AND SRC.RN2 = 1                                                                                                                          "
                                  +"  ORDER BY                                                                                                                                     "
                                  +"      SRC.STUDENT_ID, SRC.REF_NUM", conn);

            reader = comm.ExecuteReader();

            curStudent = "";
            float preTestListeningScore = 0;
            float postTestListeningScore = 0;
            float preTestReadingScore = 0;
            float postTestReadingScore = 0;
            String testForm;

            while (reader.Read())
            {
                curStudent = reader["STUDENT_ID"].ToString();
                testForm = reader["Pretest Form"].ToString();

                if (testForm == "L")
                {
                    preTestListeningScore = float.Parse(reader["Pretest Score"].ToString());
                    postTestListeningScore = float.Parse(reader["Post-test Score"].ToString());
                }
                else
                {
                    preTestReadingScore = float.Parse(reader["Pretest Score"].ToString());
                    postTestReadingScore = float.Parse(reader["Post-test Score"].ToString());
                }

                if ((testForm == "L" && listeningStudents.Contains(curStudent)) || (testForm == "R" && readingStudents.Contains(curStudent)))
                {
                    int pretestScore = (int)((testForm == "L") ? preTestListeningScore : preTestReadingScore);
                    int posttestScore = (int)((testForm == "L") ? postTestListeningScore : postTestReadingScore);

                    int initialFunctioningLevel = 0;
                    int finalFunctioningLevel = 0;

                    for (int i = 0; i < ESOLLevels.Length && pretestScore > ESOLLevels[i]; i++)
                    {
                        initialFunctioningLevel = i;
                    }

                    for (int i = initialFunctioningLevel; i < ESOLLevels.Length && posttestScore > ESOLLevels[i];)
                    {
                        finalFunctioningLevel = i++;
                    }

                    for (int i = initialFunctioningLevel; i < finalFunctioningLevel; i++)
                    {
                        List<String> studentLCPs = previouslyReportedLCPs.ContainsKey(curStudent) ? previouslyReportedLCPs[curStudent] : new List<String>();

                        bool previouslyCalculated = false;

                        if (!studentLCPs.Contains(ESOLLCPs[i]))
                        {
                            Tuple<String, String, OrionTerm> LCP = new Tuple<string, string, OrionTerm>(curStudent, ESOLLCPs[i], term);

                            foreach (Tuple<String, String, String, OrionTerm> previouslyCalculatedLCP in previouslyCalculatedLCPs)
                            {
                                if (previouslyCalculatedLCP.Item1 == curStudent && previouslyCalculatedLCP.Item3 == ESOLLCPs[i] &&
                                    (previouslyCalculatedLCP.Item4.getStateReportingYear() == term.getStateReportingYear()
                                    || previouslyCalculatedLCP.Item4.getStateReportingYear() == term.getStateReportingYear().prevReportingYear()))
                                {
                                    previouslyCalculated = true;
                                    break;
                                }
                            }

                            foreach (Tuple<String, String, String, OrionTerm> previouslyCalculatedLCP in calculatedLCPs)
                            {
                                if (previouslyCalculatedLCP.Item1 == curStudent && previouslyCalculatedLCP.Item2 == ESOLLCPs[i] &&
                                    (previouslyCalculatedLCP.Item4.getStateReportingYear() == term.getStateReportingYear()
                                    || previouslyCalculatedLCP.Item4.getStateReportingYear() == term.getStateReportingYear().prevReportingYear()))
                                {
                                    previouslyCalculated = true;
                                    break;
                                }
                            }

                            if (!previouslyCalculated)
                            {
                                Tuple<String, String, String, OrionTerm> newLCP = new Tuple<string, string, string, OrionTerm>(curStudent, "ESOL", ESOLLCPs[i], term);
                                calculatedLCPs.Add(newLCP);
                            }
                        }
                    }
                }   
            }

            reader.Close();



            comm = new SqlCommand("SELECT                                                                          "
                                  + "      class.STDNT_ID                                                          "
                                  + "      ,lcp.COMP_POINT_ID                                                      "
                                  + "  FROM                                                                        "
                                  + "      MIS.dbo.ST_OCP_LCP_A_55 lcp                                             "
                                  + "      INNER JOIN MIS.dbo.ST_STDNT_CLS_A_235 class ON class.CRS_ID = lcp.CRS_ID"
                                  + "  WHERE                                                                       "
                                  + "      lcp.CIP_CD = '1532010303'                                               "
                                  + "      AND class.EFF_TRM = '" + term.ToString() + "'", conn);

            reader = comm.ExecuteReader();

            while (reader.Read())
            {
                curStudent = reader["STDNT_ID"].ToString();
                String lcp = reader["COMP_POINT_ID"].ToString();

                bool previouslyCalculated = false;

                Tuple<String, String, OrionTerm> LCP = new Tuple<string, string, OrionTerm>(curStudent, lcp, term);

                foreach (Tuple<String, String, String, OrionTerm> previouslyCalculatedLCP in previouslyCalculatedLCPs)
                {
                    if (previouslyCalculatedLCP.Item1 == curStudent && previouslyCalculatedLCP.Item3 == lcp &&
                        (previouslyCalculatedLCP.Item4.getStateReportingYear() == term.getStateReportingYear()
                        || previouslyCalculatedLCP.Item4.getStateReportingYear() == term.getStateReportingYear().prevReportingYear()))
                    {
                        previouslyCalculated = true;
                        break;
                    }
                }

                foreach (Tuple<String, String, String, OrionTerm> previouslyCalculatedLCP in calculatedLCPs)
                {
                    if (previouslyCalculatedLCP.Item1 == curStudent && previouslyCalculatedLCP.Item2 == lcp &&
                        (previouslyCalculatedLCP.Item4.getStateReportingYear() == term.getStateReportingYear()
                        || previouslyCalculatedLCP.Item4.getStateReportingYear() == term.getStateReportingYear().prevReportingYear()))
                    {
                        previouslyCalculated = true;
                        break;
                    }
                }

                if (!previouslyCalculated)
                {
                    Tuple<String, String, String, OrionTerm> newLCP = new Tuple<string, string, string, OrionTerm>(curStudent, "ALS", lcp, term);
                    calculatedLCPs.Add(newLCP);
                }
            }

            reader.Close();
            conn.Close();

            return calculatedLCPs;
        }
    }
}
