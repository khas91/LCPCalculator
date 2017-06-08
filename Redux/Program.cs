using System;
using System.Collections.Generic;
using System.Data;
using System.Data.SqlClient;
using System.IO;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using TermLogic;

namespace Redux
{
    class Program
    {
        static void Main(string[] args)
        {
            
            Dictionary<String, Student> studentDictionary = new Dictionary<string, Student>();
            List<String> ESOLStudentIDs = new List<String>();
            List<String> ABEStudentIDs = new List<string>();
            Dictionary<String, String> ESOLstudentCurrentContinuousEnrollmentPeriodStartTerm = new Dictionary<string, string>();
            Dictionary<String, String> ABEstudentCurrentContinuousEnrollmentPeriodStartTerm = new Dictionary<string, string>();
            Dictionary<String, List<Tuple<String, String>>> studentLCPs = new Dictionary<string, List<Tuple<string, string>>>();
            Dictionary<String, List<Tuple<String, String, DateTime>>> newLCPs = new Dictionary<string, List<Tuple<string, string, DateTime>>>();
            Dictionary<String, String> ESOLstudentReadingorListeningDesignations = new Dictionary<string, string>();
            Dictionary<String, String[]> AHSLCPDictionary = new Dictionary<string, string[]>();
            List<String> AHSCoursePrefixes = new List<string>();

            EducationalFunctioningLevel[] abeEFLs = new EducationalFunctioningLevel[4];


            for (OrionTerm term = new OrionTerm("20172"); term <= new OrionTerm("20172"); term++)
            {
                

                for (int i = 0; i < 4; i++)
                {
                    abeEFLs[i] = new EducationalFunctioningLevel();
                }

                abeEFLs[0].lowerBound = 0;
                abeEFLs[0].upperBound = 1.9f;
                abeEFLs[1].lowerBound = 2.0f;
                abeEFLs[1].upperBound = 3.9f;
                abeEFLs[2].lowerBound = 4.0f;
                abeEFLs[2].upperBound = 5.9f;
                abeEFLs[3].lowerBound = 6.0f;
                abeEFLs[3].upperBound = 8.9f;

                EducationalFunctioningLevel[] esolEFLs = new EducationalFunctioningLevel[6];

                for (int i = 0; i < 6; i++)
                {
                    esolEFLs[i] = new EducationalFunctioningLevel();
                }

                esolEFLs[0].lowerBound = 0;
                esolEFLs[0].upperBound = 179;
                esolEFLs[1].lowerBound = 180;
                esolEFLs[1].upperBound = 190;
                esolEFLs[2].lowerBound = 191;
                esolEFLs[2].upperBound = 200;
                esolEFLs[3].lowerBound = 201;
                esolEFLs[3].upperBound = 210;
                esolEFLs[4].lowerBound = 211;
                esolEFLs[4].upperBound = 220;
                esolEFLs[5].lowerBound = 221;
                esolEFLs[5].upperBound = 235;

                String[] esolLCPs = { "A", "B", "C", "D", "E", "F" };
                String[] abeMathLCPs = { "A", "B", "C", "D" };
                String[] abeReadingLCPs = { "E", "F", "G", "H" };
                String[] abeLanguageLCPs = { "J", "K", "M", "N" };

                SqlConnection conn = new SqlConnection("server=vulcan;Trusted_Connection=true;database=StateSubmission");
                SqlCommand comm;
                SqlDataReader reader;

                try
                {
                    conn.Open();
                }
                catch (Exception)
                {
                    throw;
                }

                comm = new SqlCommand(@"SELECT TOP 2
	                                        CONVERT(DATE, SESS_BEG_DT) AS [TermBeginDate]
	                                        ,CONVERT(DATE, SESS_END_DT) AS [TermEndDate]
                                        FROM
	                                        MIS.dbo.vwTermYearXwalk xwalk
                                        WHERE
	                                        xwalk.OrionTerm >= '" + term + "'", conn);

                reader = comm.ExecuteReader();
                reader.Read();

                DateTime termBeginDate = DateTime.Parse(reader["TermBeginDate"].ToString());
                DateTime termEndDate = DateTime.Parse(reader["TermEndDate"].ToString());

                reader.Read();

                DateTime nextTermStartDate = DateTime.Parse(reader["TermBeginDate"].ToString());

                reader.Close();

                comm = new SqlCommand(@"SELECT
	                                    class.STDNT_ID
	                                    ,class.CRS_ID
	                                    ,class.EFF_TRM
	                                    ,class.REF_NUM
	                                    ,CONVERT(DATE, CAST(MAX(log.LOG_DATE) AS VARCHAR(MAX))) AS [Registration Date]
	                                    ,CASE
		                                    WHEN ISDATE(class.ATT_DATE) > 0 THEN CONVERT(DATE, class.ATT_DATE)
	                                    END AS [Last Attendance Date]
	                                    ,CASE
		                                    WHEN af.INITIAL_FUNCTIONING_LEVEL IS NOT NULL THEN af.INITIAL_FUNCTIONING_LEVEL
	                                    END AS [Initial Functioning Level]
                                    INTO
	                                    #ESOLEnrollments
                                    FROM
	                                    MIS.dbo.ST_STDNT_CLS_A_235 class
	                                    INNER JOIN MIS.dbo.ST_COURSE_A_150 course ON course.CRS_ID = class.CRS_ID
												                                    AND course.EFF_TRM <= class.EFF_TRM
												                                    AND (course.END_TRM = '' OR course.END_TRM >= class.EFF_TRM)
	                                    INNER JOIN MIS.dbo.ST_STDNT_CLS_LOG_230 log ON log.REF_NUM = class.REF_NUM
												                                    AND log.STDNT_ID = class.STDNT_ID
	                                    LEFT JOIN MIS.dbo.ST_OCP_LCP_A_55 af ON af.CRS_ID = class.CRS_ID
											                                    AND af.COMP_POINT_TY = 'AF'
											                                    AND (af.END_TRM = '' OR af.END_TRM >= class.EFF_TRM)
											                                    AND af.EFF_TRM <= class.EFF_TRM
                                    WHERE
	                                    course.ICS_NUM = '13204' 
                                        AND af.INITIAL_FUNCTIONING_LEVEL NOT IN ('H', 'K', 'L', 'M', 'X')
	                                    AND log.LOG_ACTION = 'A'
	                                    AND class.TRNSCTN_TY = 'A'
                                    GROUP BY
	                                    class.STDNT_ID
	                                    ,class.CRS_ID
	                                    ,class.REF_NUM
	                                    ,class.ATT_DATE
	                                    ,class.EFF_TRM
	                                    ,af.INITIAL_FUNCTIONING_LEVEL
	                                    ,class.GRD_DT

                                    SELECT
	                                    *
                                    FROM
	                                    #ESOLEnrollments", conn);

                reader = comm.ExecuteReader();

                while (reader.Read())
                {
                    Student student = null;

                    DateTime lastAttendanceDate;
                    OrionTerm effTerm = new OrionTerm(reader["EFF_TRM"].ToString());
                    String studentID = reader["STDNT_ID"].ToString();
                    String courseID = reader["CRS_ID"].ToString();
                    String refNum = reader["REF_NUM"].ToString();
                    DateTime.TryParse(reader["Last Attendance Date"].ToString(), out lastAttendanceDate);
                    DateTime registrationDate = DateTime.Parse(reader["Registration Date"].ToString());
                    String initialFunctioningLevel = reader["Initial Functioning Level"].ToString();

                    if (!studentDictionary.ContainsKey(studentID))
                    {
                        student = new Student();
                        studentDictionary.Add(studentID, student);
                        ESOLStudentIDs.Add(studentID);
                    }
                    else
                    {
                        student = studentDictionary[studentID];
                    }

                    Course course = new Course();
                    course.courseID = courseID;
                    course.refNum = refNum;
                    course.registrationDate = registrationDate;
                    course.lastAttDate = lastAttendanceDate;
                    course.term = effTerm;

                    course.type = "ESOL";

                    switch (initialFunctioningLevel)
                    {
                        case "B":
                            course.EFL = 0;
                            break;
                        case "C":
                            course.EFL = 1;
                            break;
                        case "D":
                            course.EFL = 2;
                            break;
                        case "E":
                            course.EFL = 3;
                            break;
                        case "F":
                            course.EFL = 4;
                            break;
                        case "G":
                            course.EFL = 5;
                            break;
                    }

                    student.courses.Add(course);
                }

                reader.Close();

                comm = new SqlCommand(@"SELECT
	                                        AdultEdStudents.STDNT_ID
	                                        ,OrionTerm
	                                        ,CAST(LEFT(StateReportingYear, 4) AS INT) AS [StateReportingYear]
                                        FROM
	                                        (SELECT
		                                        DISTINCT STDNT_ID
	                                        FROM
		                                        #ESOLEnrollments) AdultEdStudents
	                                        LEFT JOIN (SELECT
					                                        r6.DE1021 AS [STDNT_ID]
					                                        ,xwalk.OrionTerm
					                                        ,xwalk.StateReportingYear
				                                        FROM
					                                        StateSubmission.SDB.RecordType6 r6
					                                        INNER JOIN MIS.dbo.vwTermYearXwalk xwalk ON xwalk.StateReportingTerm = r6.DE1028
                                                        WHERE
					                                        r6.DE3001 = '13204'
					                                        AND r6.DE3022 NOT IN ('H', 'K', 'L', 'M', 'X')) prevClass ON prevClass.STDNT_ID = AdultEdStudents.STDNT_ID
																												      AND prevClass.OrionTerm < '" + term + @"'
                                        ORDER BY
	                                        AdultEdStudents.STDNT_ID
	                                        ,OrionTerm DESC", conn);

                reader = comm.ExecuteReader();

                Dictionary<String, int> mostRecentStateReportingYearSeen = new Dictionary<string, int>();
                Dictionary<String, String> mostRecentTermSeen = new Dictionary<string, string>();
                int currentStateReportingYear = int.Parse(term.getStateReportingYear().ToString().Substring(0, 4));

                ESOLstudentCurrentContinuousEnrollmentPeriodStartTerm.Clear();
                ESOLstudentReadingorListeningDesignations.Clear();

                String currentStudent = "";

                while (reader.Read())
                {
                    int stateReportingYear = 0;
                    String studentID = reader["STDNT_ID"].ToString();
                    String orionTerm = reader["OrionTerm"].ToString();

                    if (currentStudent != "" && currentStudent != studentID && !ESOLstudentCurrentContinuousEnrollmentPeriodStartTerm.ContainsKey(currentStudent))
                    {
                        ESOLstudentCurrentContinuousEnrollmentPeriodStartTerm.Add(currentStudent, mostRecentTermSeen[currentStudent]);
                    }

                    if (!int.TryParse(reader["StateReportingYear"].ToString(), out stateReportingYear))
                    {
                        ESOLstudentCurrentContinuousEnrollmentPeriodStartTerm.Add(studentID, term.ToString());
                        continue;
                    }

                    if (ESOLstudentCurrentContinuousEnrollmentPeriodStartTerm.ContainsKey(studentID))
                    {
                        continue;
                    }


                    if (!mostRecentStateReportingYearSeen.ContainsKey(studentID))
                    {
                        mostRecentStateReportingYearSeen.Add(studentID, stateReportingYear);
                        mostRecentTermSeen.Add(studentID, orionTerm);

                    }
                    
                    if (stateReportingYear <= currentStateReportingYear - 2)
                    {
                        ESOLstudentCurrentContinuousEnrollmentPeriodStartTerm.Add(studentID, term.ToString());
                        continue;
                    }

                    if (stateReportingYear <= mostRecentStateReportingYearSeen[studentID] - 2)
                    {
                        ESOLstudentCurrentContinuousEnrollmentPeriodStartTerm.Add(studentID, mostRecentTermSeen[studentID]);
                    }

                    mostRecentStateReportingYearSeen[studentID] = stateReportingYear;
                    mostRecentTermSeen[studentID] = orionTerm;

                    currentStudent = studentID;
                }

                reader.Close();

                comm = new SqlCommand("TRUNCATE TABLE Adhoc.dbo.StudentCurrentContinuousEnrollmentPeriodStartDate", conn);
                comm.ExecuteNonQuery();

                DataTable table = new DataTable("StudentCurrentContinuousEnrollmentPeriodStartDate");
                DataColumn column;
                DataRow row;

                column = new DataColumn();
                column.DataType = System.Type.GetType("System.String");
                column.ColumnName = "STDNT_ID";

                table.Columns.Add(column);

                column = new DataColumn();
                column.DataType = System.Type.GetType("System.String");
                column.ColumnName = "Term";
                table.Columns.Add(column);

                column = new DataColumn();
                column.DataType = System.Type.GetType("System.String");
                column.ColumnName = "Type";
                table.Columns.Add(column);

                DataColumn[] primaryKeyColumns = new DataColumn[2];
                primaryKeyColumns[0] = table.Columns["STDNT_ID"];
                primaryKeyColumns[1] = table.Columns["Type"];
                table.PrimaryKey = primaryKeyColumns;


                foreach (String student in ESOLstudentCurrentContinuousEnrollmentPeriodStartTerm.Keys.ToArray())
                {
                    row = table.NewRow();
                    row["STDNT_ID"] = student;
                    row["Term"] = ESOLstudentCurrentContinuousEnrollmentPeriodStartTerm[student];
                    row["Type"] = "ESOL";
                    table.Rows.Add(row);
                }

                SqlBulkCopy bulkCopy = new SqlBulkCopy(conn);

                bulkCopy.DestinationTableName = "Adhoc.dbo.StudentCurrentContinuousEnrollmentPeriodStartDate";

                bulkCopy.WriteToServer(table);

                bulkCopy.Close();

                comm = new SqlCommand(@"SELECT
	                                        STDNT_ID
	                                        ,MIN([Registration Date]) AS [Start Date]
                                        INTO
	                                        #searchDates
                                        FROM
	                                        (
	                                        SELECT
		                                        class.STDNT_ID
		                                        ,class.CRS_ID
		                                        ,class.REF_NUM
		                                        ,CONVERT(DATE, CAST(MAX(log.LOG_DATE) AS VARCHAR)) AS [Registration Date]
	                                        FROM
		                                        Adhoc.dbo.StudentCurrentContinuousEnrollmentPeriodStartDate stuterm 
		                                        INNER JOIN MIS.dbo.ST_STDNT_CLS_A_235 class ON class.STDNT_ID = stuterm.STDNT_ID
													                                        AND class.EFF_TRM = stuterm.Term
		                                        INNER JOIN MIS.dbo.ST_COURSE_A_150 course ON course.CRS_ID = class.CRS_ID
		                                        INNER JOIN MIS.dbo.ST_STDNT_CLS_LOG_230 log ON log.REF_NUM = class.REF_NUM
													                                        AND log.STDNT_ID = class.STDNT_ID
		                                        INNER JOIN MIS.dbo.ST_OCP_LCP_A_55 af ON af.CRS_ID = class.CRS_ID
	                                        WHERE
		                                        stuterm.Type = 'ESOL'
		                                        AND course.EFF_TRM <= class.EFF_TRM
		                                        AND (course.END_TRM = '' OR course.END_TRM >= class.EFF_TRM)
		                                        AND log.LOG_ACTION = 'A'
		                                        AND class.TRNSCTN_TY = 'A'
		                                        AND af.COMP_POINT_TY = 'AF'
		                                        AND af.EFF_TRM <= class.EFF_TRM
		                                        AND (af.END_TRM = '' OR af.END_TRM >= class.EFF_TRM)
		                                        AND course.ICS_NUM = '13204'
		                                        AND af.INITIAL_FUNCTIONING_LEVEL NOT IN ('H','K','L','M','X')
	                                        GROUP BY
		                                        class.STDNT_ID
		                                        ,class.CRS_ID
		                                        ,class.REF_NUM
	                                        ) SRC
                                        GROUP BY
	                                        STDNT_ID

                                        SELECT
	                                        SRC.STDNT_ID
	                                        ,SRC.TST_FRM
	                                        ,SRC.TST_SCR
                                        FROM
	                                        (
	                                        SELECT
		                                        search.STDNT_ID
		                                        ,test.TST_FRM
		                                        ,test.TST_SCR
		                                        ,ROW_NUMBER() OVER (PARTITION BY CASE WHEN test.TST_FRM LIKE '%R%' THEN 'R' ELSE 'L' END, test.STUDENT_ID ORDER BY test.TST_DT DESC) AS RN
	                                        FROM
		                                        #searchDates search
		                                        INNER JOIN MIS.dbo.ST_SUBTEST_A_155 test ON test.STUDENT_ID = search.STDNT_ID
	                                        WHERE
		                                        test.TST_TY = 'CASA'
		                                        AND test.TST_SCR > 0
		                                        AND CONVERT(DATE, test.TST_DT) <= search.[Start Date]
	                                        ) SRC
                                        WHERE
	                                        RN = 1
                                        ORDER BY
	                                        SRC.STDNT_ID", conn);

                reader = comm.ExecuteReader();

                currentStudent = "";
                float listeningScore = 0;
                float readingScore = 0;

                while (reader.Read())
                {
                    String studentID = reader["STDNT_ID"].ToString();
                    String form = reader["TST_FRM"].ToString();

                    if (currentStudent != "" & studentID != currentStudent)
                    {
                        String designation = listeningScore < readingScore ? "L" : "R";

                        ESOLstudentReadingorListeningDesignations.Add(currentStudent, designation);
                    }

                    if (form.Contains("L"))
                    {
                        listeningScore = float.Parse(reader["TST_SCR"].ToString());
                    }
                    else
                    {
                        readingScore = float.Parse(reader["TST_SCR"].ToString());
                    }

                    currentStudent = studentID;
                }

                reader.Close();

                comm = new SqlCommand("TRUNCATE TABLE Adhoc.dbo.CASASReadingOrListeningDesignations", conn);
                comm.ExecuteNonQuery();

                table = new DataTable("ReadingOrListeningDesignations");

                column = new DataColumn();
                column.DataType = System.Type.GetType("System.String");
                column.ColumnName = "STDNT_ID";

                table.Columns.Add(column);

                column = new DataColumn();
                column.DataType = System.Type.GetType("System.String");
                column.ColumnName = "DESIGNATION";
                table.Columns.Add(column);

                primaryKeyColumns = new DataColumn[1];
                primaryKeyColumns[0] = table.Columns["STDNT_ID"];
                table.PrimaryKey = primaryKeyColumns;

                foreach (String student in ESOLstudentReadingorListeningDesignations.Keys.ToArray())
                {
                    row = table.NewRow();
                    row["STDNT_ID"] = student;
                    row["DESIGNATION"] = ESOLstudentReadingorListeningDesignations[student];
                    table.Rows.Add(row);
                }

                bulkCopy = new SqlBulkCopy(conn);
                bulkCopy.DestinationTableName = "Adhoc.dbo.CASASReadingOrListeningDesignations";

                bulkCopy.WriteToServer(table);

                bulkCopy.Close();

                comm = new SqlCommand(@"SELECT
	                                        stuterm.STDNT_ID
	                                        ,xwalk.OrionTerm
	                                        ,r5.DE2101
	                                        ,r5.DE2105
                                        FROM
	                                        Adhoc.[dbo].[StudentCurrentContinuousEnrollmentPeriodStartDate] stuterm
	                                        INNER JOIN StateSubmission.SDB.RecordType5 r5 ON r5.DE1021 = stuterm.STDNT_ID
	                                        INNER JOIN MIS.dbo.vwTermYearXwalk xwalk ON xwalk.StateReportingTerm = r5.DE1028
                                        WHERE
	                                        r5.DE2105 <> 'Z'
	                                        AND xwalk.OrionTerm >= stuterm.Term
	                                        AND r5.DE2101 = '1532010300'
                                            AND xwalk.OrionTerm < '" + term + @"'
                                        ORDER BY
	                                        stuterm.STDNT_ID
	                                        ,xwalk.OrionTerm", conn);

                reader = comm.ExecuteReader();

                while (reader.Read())
                {
                    String studentID = reader["STDNT_ID"].ToString();
                    String CIP = reader["DE2101"].ToString();
                    String LCPval = reader["DE2105"].ToString();

                    Tuple<String, String> LCP = new Tuple<string, string>(CIP, LCPval.Trim());

                    if (!studentLCPs.ContainsKey(studentID))
                    {
                        studentLCPs.Add(studentID, new List<Tuple<string, string>>());
                    }

                    studentLCPs[studentID].Add(LCP);
                }

                reader.Close();

                comm = new SqlCommand(@"SELECT
	                                        casas.STDNT_ID
	                                        ,test.TST_FRM
                                            ,test.TST_SCR
	                                        ,CONVERT(DATE, test.TST_DT) AS [Test Date]
                                        FROM
	                                        Adhoc.dbo.CASASReadingOrListeningDesignations casas
	                                        INNER JOIN MIS.dbo.ST_SUBTEST_A_155 test ON test.STUDENT_ID = casas.STDNT_ID
											                                         AND test.TST_FRM LIKE '%' + casas.DESIGNATION + '%'
                                        WHERE
	                                        test.TST_TY = 'CASA'
	                                        AND test.TST_SCR > 0", conn);

                reader = comm.ExecuteReader();

                while (reader.Read())
                {
                    String studentID = reader["STDNT_ID"].ToString();

                    Test test = new Test();
                    test.type = "CASAS";
                    test.score = float.Parse(reader["TST_SCR"].ToString());
                    test.testDate = DateTime.Parse(reader["Test Date"].ToString());

                    studentDictionary[studentID].tests.Add(test);
                }

                reader.Close();

                foreach (String studentID in ESOLStudentIDs)
                {
                    Student student = studentDictionary[studentID];

                    if (!studentLCPs.ContainsKey(studentID))
                    {
                        studentLCPs.Add(studentID, new List<Tuple<string, string>>());
                    }

                    if (!newLCPs.ContainsKey(studentID))
                    {
                        newLCPs.Add(studentID, new List<Tuple<string, string, DateTime>>());
                    }

                    Course[] courses = student.getCoursesInOrder("ESOL");
                    Course[] previousCourses = courses.Where(course => course.term < term).ToArray();
                    Course[] currentCourses = courses.Where(course => course.term == term).ToArray();
                    Course[] laterCourses = courses.Where(course => course.term > term).ToArray();

                    if (currentCourses.Length == 0)
                    {
                        continue;
                    }

                    Test[] postTests = null;
                    int initialFunctioningLevel;
                    float postTestScore;
                    Test[] pretests;

                    for (int i = 0; i < currentCourses.Length; i++)
                    {
                        pretests = student.getTestsForDateRange(currentCourses[i].registrationDate.AddMonths(-11), currentCourses[i].registrationDate, "CASAS");

                        if (pretests.Where(test => esolEFLs[currentCourses[i].EFL].lowerBound < test.score && esolEFLs[currentCourses[i].EFL].upperBound > test.score).Count() == 0)
                        {
                            continue;
                        }

                        postTests = student.getTestsForDateRange(courses[i].registrationDate,
                            ((i < currentCourses.Length - 1) ? currentCourses[i + 1].registrationDate : nextTermStartDate), "CASAS");

                        initialFunctioningLevel = currentCourses[i].EFL;

                        if (postTests.Length == 0)
                        {
                            continue;
                        }

                        postTestScore = postTests.Last().score;

                        for (int j = initialFunctioningLevel; j < esolEFLs.Length && postTestScore > esolEFLs[j].upperBound; j++)
                        {
                            Tuple<String, String, DateTime> LCP = new Tuple<string, string, DateTime>("1532010300", esolLCPs[j], postTests.Last().testDate);

                            if (!studentLCPs[studentID].Any(prev => prev.Equals(LCP)))
                            {
                                studentLCPs[studentID].Add(new Tuple<String, String>(LCP.Item1, LCP.Item2));
                                newLCPs[studentID].Add(LCP);
                            }
                        }
                    }
                }

                comm = new SqlCommand(@"SELECT
	                                        class.STDNT_ID
	                                        ,class.CRS_ID
	                                        ,class.EFF_TRM
	                                        ,class.REF_NUM
	                                        ,CONVERT(DATE, CAST(MAX(log.LOG_DATE) AS VARCHAR(MAX))) AS [Registration Date]
	                                        ,CASE
		                                        WHEN ISDATE(class.ATT_DATE) > 0 THEN CONVERT(DATE, class.ATT_DATE)
	                                        END AS [Last Attendance Date]
	                                        ,xwalk.INIT_FUNC_LEVEL
	                                        ,xwalk.SUBJECT
                                        INTO
	                                        #ABEEnrollments
                                        FROM
	                                        MIS.dbo.ST_STDNT_CLS_A_235 class
	                                        INNER JOIN MIS.dbo.ST_COURSE_A_150 course ON course.CRS_ID = class.CRS_ID
												                                        AND course.EFF_TRM <= class.EFF_TRM
												                                        AND (course.END_TRM = '' OR course.END_TRM >= class.EFF_TRM)
	                                        INNER JOIN MIS.dbo.ST_STDNT_CLS_LOG_230 log ON log.REF_NUM = class.REF_NUM
												                                        AND log.STDNT_ID = class.STDNT_ID
	                                        INNER JOIN Adhoc.dbo.Course_Subject_Area_Xwalk xwalk ON xwalk.CRS_ID = course.CRS_ID
                                        WHERE
	                                        course.ICS_NUM IN ('13104', '13201', '13202', '13203')
	                                        AND log.LOG_ACTION = 'A'
	                                        AND class.TRNSCTN_TY = 'A'
                                        GROUP BY
	                                        class.STDNT_ID
	                                        ,class.CRS_ID
	                                        ,class.REF_NUM
	                                        ,class.ATT_DATE
	                                        ,class.EFF_TRM
	                                        ,class.GRD_DT
	                                        ,xwalk.INIT_FUNC_LEVEL
	                                        ,xwalk.SUBJECT

                                        SELECT
	                                        *
                                        FROM
	                                        #ABEEnrollments", conn);

                reader = comm.ExecuteReader();

                while (reader.Read())
                {
                    Student student = null;

                    DateTime lastAttendanceDate;
                    OrionTerm effTerm = new OrionTerm(reader["EFF_TRM"].ToString());
                    String studentID = reader["STDNT_ID"].ToString();
                    String courseID = reader["CRS_ID"].ToString();
                    String refNum = reader["REF_NUM"].ToString();
                    DateTime.TryParse(reader["Last Attendance Date"].ToString(), out lastAttendanceDate);
                    DateTime registrationDate = DateTime.Parse(reader["Registration Date"].ToString());
                    String initialFunctioningLevel = reader["INIT_FUNC_LEVEL"].ToString();
                    String subject = reader["SUBJECT"].ToString();

                    if (!studentDictionary.ContainsKey(studentID))
                    {
                        student = new Student();
                        studentDictionary.Add(studentID, student);
                        ABEStudentIDs.Add(studentID);
                    }
                    else
                    {
                        student = studentDictionary[studentID];
                        ABEStudentIDs.Add(studentID);
                    }

                    Course course = new Course();
                    course.courseID = courseID;
                    course.refNum = refNum;
                    course.registrationDate = registrationDate;
                    course.lastAttDate = lastAttendanceDate;
                    course.term = effTerm;
                    course.subject = subject;
                    course.type = "ABE";
                    course.EFL = int.Parse(reader["INIT_FUNC_LEVEL"].ToString()) - 1;

                    student.courses.Add(course);
                }

                reader.Close();

                comm = new SqlCommand(@"SELECT
	                                        AdultEdStudents.STDNT_ID
	                                        ,OrionTerm
	                                        ,CAST(LEFT(StateReportingYear, 4) AS INT) AS [StateReportingYear]
                                        FROM
	                                        (SELECT
		                                        DISTINCT STDNT_ID
	                                        FROM
		                                        #ABEEnrollments) AdultEdStudents
	                                        LEFT JOIN (SELECT
					                                        r6.DE1021 AS [STDNT_ID]
					                                        ,xwalk.OrionTerm
					                                        ,xwalk.StateReportingYear
				                                        FROM
					                                        StateSubmission.SDB.RecordType6 r6
					                                        INNER JOIN MIS.dbo.vwTermYearXwalk xwalk ON xwalk.StateReportingTerm = r6.DE1028
                                                        WHERE
					                                        r6.DE3001 IN ('13104', '13201', '13202', '13203')) prevClass ON prevClass.STDNT_ID = AdultEdStudents.STDNT_ID
																												         AND prevClass.OrionTerm < '" + term + @"'
                                        ORDER BY
	                                        AdultEdStudents.STDNT_ID
	                                        ,OrionTerm DESC", conn);

                reader = comm.ExecuteReader();

                mostRecentStateReportingYearSeen = new Dictionary<string, int>();
                mostRecentTermSeen = new Dictionary<string, string>();
                currentStateReportingYear = int.Parse(term.getStateReportingYear().ToString().Substring(0, 4));

                currentStudent = "";

                ABEstudentCurrentContinuousEnrollmentPeriodStartTerm.Clear();

                while (reader.Read())
                {
                    int stateReportingYear = 0;
                    String studentID = reader["STDNT_ID"].ToString();
                    String orionTerm = reader["OrionTerm"].ToString();

                    if (currentStudent != "" && currentStudent != studentID && !ABEstudentCurrentContinuousEnrollmentPeriodStartTerm.ContainsKey(currentStudent))
                    {
                        ABEstudentCurrentContinuousEnrollmentPeriodStartTerm.Add(currentStudent, mostRecentTermSeen[currentStudent]);
                    }

                    if (!int.TryParse(reader["StateReportingYear"].ToString(), out stateReportingYear))
                    {
                        ABEstudentCurrentContinuousEnrollmentPeriodStartTerm.Add(studentID, term.ToString());
                        continue;
                    }

                    if (ABEstudentCurrentContinuousEnrollmentPeriodStartTerm.ContainsKey(studentID))
                    {
                        continue;
                    }


                    if (!mostRecentStateReportingYearSeen.ContainsKey(studentID))
                    {
                        mostRecentStateReportingYearSeen.Add(studentID, stateReportingYear);
                        mostRecentTermSeen.Add(studentID, orionTerm);

                        if (stateReportingYear <= currentStateReportingYear - 2)
                        {
                            ABEstudentCurrentContinuousEnrollmentPeriodStartTerm.Add(studentID, term.ToString());
                            continue;
                        }
                    }

                    if (stateReportingYear <= mostRecentStateReportingYearSeen[studentID] - 2)
                    {
                        ABEstudentCurrentContinuousEnrollmentPeriodStartTerm.Add(studentID, mostRecentTermSeen[studentID]);
                    }

                    mostRecentStateReportingYearSeen[studentID] = stateReportingYear;
                    mostRecentTermSeen[studentID] = orionTerm;

                    currentStudent = studentID;
                }

                reader.Close();

                comm = new SqlCommand("TRUNCATE TABLE Adhoc.dbo.StudentCurrentContinuousEnrollmentPeriodStartDate", conn);
                comm.ExecuteNonQuery();

                table = new DataTable("StudentCurrentContinuousEnrollmentPeriodStartDate");

                column = new DataColumn();
                column.DataType = System.Type.GetType("System.String");
                column.ColumnName = "STDNT_ID";

                table.Columns.Add(column);

                column = new DataColumn();
                column.DataType = System.Type.GetType("System.String");
                column.ColumnName = "Term";
                table.Columns.Add(column);

                column = new DataColumn();
                column.DataType = System.Type.GetType("System.String");
                column.ColumnName = "Type";
                table.Columns.Add(column);

                primaryKeyColumns = new DataColumn[2];
                primaryKeyColumns[0] = table.Columns["STDNT_ID"];
                primaryKeyColumns[1] = table.Columns["Type"];
                table.PrimaryKey = primaryKeyColumns;


                foreach (String student in ABEstudentCurrentContinuousEnrollmentPeriodStartTerm.Keys.ToArray())
                {
                    row = table.NewRow();
                    row["STDNT_ID"] = student;
                    row["Term"] = ABEstudentCurrentContinuousEnrollmentPeriodStartTerm[student];
                    row["Type"] = "ABE";
                    table.Rows.Add(row);
                }

                bulkCopy = new SqlBulkCopy(conn);

                bulkCopy.DestinationTableName = "Adhoc.dbo.StudentCurrentContinuousEnrollmentPeriodStartDate";

                bulkCopy.WriteToServer(table);

                bulkCopy.Close();

                comm = new SqlCommand(@"SELECT
	                                        stuterm.STDNT_ID
	                                        ,xwalk.OrionTerm
	                                        ,r5.DE2101
	                                        ,r5.DE2105
                                        FROM
	                                        Adhoc.[dbo].[StudentCurrentContinuousEnrollmentPeriodStartDate] stuterm
	                                        INNER JOIN StateSubmission.SDB.RecordType5 r5 ON r5.DE1021 = stuterm.STDNT_ID
	                                        INNER JOIN MIS.dbo.vwTermYearXwalk xwalk ON xwalk.StateReportingTerm = r5.DE1028
                                        WHERE
	                                        r5.DE2105 <> 'Z'
	                                        AND xwalk.OrionTerm >= stuterm.Term
	                                        AND r5.DE2101 = '1532010200'
                                            AND xwalk.OrionTerm < '" + term + @"'
                                        ORDER BY
	                                        stuterm.STDNT_ID
	                                        ,xwalk.OrionTerm", conn);

                reader = comm.ExecuteReader();

                while (reader.Read())
                {
                    String studentID = reader["STDNT_ID"].ToString();
                    String CIP = reader["DE2101"].ToString();
                    String LCPval = reader["DE2105"].ToString();

                    Tuple<String, String> LCP = new Tuple<string, string>(CIP, LCPval.Trim());

                    if (!studentLCPs.ContainsKey(studentID))
                    {
                        studentLCPs.Add(studentID, new List<Tuple<string, string>>());
                    }

                    studentLCPs[studentID].Add(LCP);
                }

                reader.Close();

                comm = new SqlCommand(@"SELECT DISTINCT 
	                                        abe.STDNT_ID
	                                        ,test.TST_FRM
                                            ,test.TST_SCR
                                            ,test.SUBTEST
	                                        ,CONVERT(DATE, test.TST_DT) AS [Test Date]
                                        FROM
	                                        #ABEEnrollments abe
	                                        INNER JOIN MIS.dbo.ST_SUBTEST_A_155 test ON test.STUDENT_ID = abe.STDNT_ID
                                        WHERE
	                                        test.TST_TY = 'TABE'
	                                        AND test.TST_SCR > 0", conn);

                reader = comm.ExecuteReader();

                while (reader.Read())
                {
                    String studentID = reader["STDNT_ID"].ToString();

                    Test test = new Test();
                    test.type = "TABE";
                    test.score = float.Parse(reader["TST_SCR"].ToString());
                    test.subject = reader["SUBTEST"].ToString();
                    test.testDate = DateTime.Parse(reader["Test Date"].ToString());

                    studentDictionary[studentID].tests.Add(test);
                }

                reader.Close();

                foreach (String studentID in ABEStudentIDs)
                {
                    Student student = studentDictionary[studentID];

                    foreach (String subject in new String[] { "MA", "LA", "RE" })
                    {
                        String[] abeLCPs;

                        switch (subject)
                        {
                            case "MA":
                                abeLCPs = abeMathLCPs;
                                break;
                            case "LA":
                                abeLCPs = abeLanguageLCPs;
                                break;
                            default:
                                abeLCPs = abeReadingLCPs;
                                break;
                        }

                        if (!studentLCPs.ContainsKey(studentID))
                        {
                            studentLCPs.Add(studentID, new List<Tuple<string, string>>());
                        }

                        if (!newLCPs.ContainsKey(studentID))
                        {
                            newLCPs.Add(studentID, new List<Tuple<string, string, DateTime>>());
                        }

                        Course[] courses = student.getCoursesInOrder("ABE", subject);
                        Course[] previousCourses = courses.Where(course => course.term < term).ToArray();
                        Course[] currentCourses = courses.Where(course => course.term == term).ToArray();
                        Course[] laterCourses = courses.Where(course => course.term > term).ToArray();

                        if (currentCourses.Length == 0)
                        {
                            continue;
                        }

                        Test[] postTests = null;
                        int initialFunctioningLevel;
                        float postTestScore;
                        Test[] pretests;

                        for (int i = 0; i < currentCourses.Length; i++)
                        {
                            pretests = student.getTestsForDateRange(currentCourses[i].registrationDate.AddMonths(-12), currentCourses[i].registrationDate, "TABE", subject);

                            if (pretests.Where(test => abeEFLs[currentCourses[i].EFL].lowerBound < test.score && abeEFLs[currentCourses[i].EFL].upperBound > test.score).Count() == 0)
                            {
                                continue;
                            }

                            postTests = student.getTestsForDateRange(currentCourses[i].registrationDate,
                                ((i < currentCourses.Length - 1) ? currentCourses[i + 1].registrationDate : nextTermStartDate), "TABE", subject);

                            initialFunctioningLevel = currentCourses[i].EFL;

                            if (postTests.Length == 0)
                            {
                                continue;
                            }

                            postTestScore = postTests.Last().score;

                            for (int j = initialFunctioningLevel; j < abeEFLs.Length && postTestScore > abeEFLs[j].upperBound; j++)
                            {
                                Tuple<String, String, DateTime> LCP = new Tuple<string, string, DateTime>("1532010200", abeLCPs[j], postTests.Last().testDate);

                                if (!studentLCPs[studentID].Any(prev => prev.Equals(LCP)))
                                {
                                    studentLCPs[studentID].Add(new Tuple<String, String>(LCP.Item1, LCP.Item2));
                                    newLCPs[studentID].Add(LCP);
                                }
                            }
                        }
                    }
                }

                comm = new SqlCommand(@"SELECT                                                    
                                           CRS_ID, COMP_POINT_ID, COMP_POINT_SEQ                  
                                       FROM                                                       
                                           MIS.dbo.ST_OCP_LCP_A_55 lcp                            
                                       WHERE                                                      
                                           lcp.COMP_POINT_TY = 'LS'                               
                                           AND lcp.EFF_TRM <= '" + term + @"'                      
                                           AND (lcp.END_TRM = '' OR lcp.END_TRM >= '" + term + @"')
                                       ORDER BY                                                   
                                           lcp.CRS_ID, lcp.COMP_POINT_SEQ DESC", conn);

                reader = comm.ExecuteReader();

                while (reader.Read())
                {
                    String coursePrefix = reader["CRS_ID"].ToString().Replace("*", "");
                    String lcpValue = reader["COMP_POINT_ID"].ToString();
                    int priority = int.Parse(reader["COMP_POINT_SEQ"].ToString());

                    if (!AHSLCPDictionary.ContainsKey(coursePrefix))
                    {
                        AHSLCPDictionary.Add(coursePrefix, new String[priority]);
                    }

                    if (!AHSCoursePrefixes.Contains(coursePrefix))
                    {
                        AHSCoursePrefixes.Add(coursePrefix);
                    }

                    AHSLCPDictionary[coursePrefix][priority - 1] = lcpValue;
                }

                reader.Close();

                comm = new SqlCommand(@"SELECT
	                                        r5.DE1021, r5.DE2105
                                        FROM
	                                        StateSubmission.SDB.RecordType5 r5
	                                        INNER JOIN MIS.dbo.vwTermYearXwalk xwalk ON xwalk.StateReportingTerm = r5.DE1028
                                        WHERE
	                                        r5.DE2105 <> 'Z'
	                                        AND r5.DE2101 = '1532010202'
	                                        AND xwalk.OrionTerm < '" + term + "'", conn);

                reader = comm.ExecuteReader();

                while (reader.Read())
                {
                    String studentID = reader["DE1021"].ToString();
                    String lcp = reader["DE2105"].ToString();

                    if (!studentLCPs.ContainsKey(studentID))
                    {
                        studentLCPs.Add(studentID, new List<Tuple<string, string>>());
                    }

                    studentLCPs[studentID].Add(new Tuple<string, string>("1532010202", lcp));
                }

                reader.Close();

                comm = new SqlCommand(@"SELECT
	                                        class.STDNT_ID, class.CRS_ID
                                        FROM
	                                        MIS.dbo.ST_STDNT_CLS_A_235 class
	                                        INNER JOIN MIS.dbo.ST_COURSE_A_150 course ON course.CRS_ID = class.CRS_ID
	                                        INNER JOIN MIS.dbo.UTL_CODE_TABLE_120 code ON code.CODE = class.GRADE
	                                        INNER JOIN MIS.dbo.UTL_CODE_TABLE_GENERIC_120 gen ON gen.ISN_UTL_CODE_TABLE = code.ISN_UTL_CODE_TABLE
                                        WHERE
	                                        course.ICS_NUM = '13202'
	                                        AND class.EFF_TRM = '" + term + @"'
	                                        AND gen.cnxarraycolumn = '8'
	                                        AND code.TABLE_NAME = 'GRADE'
	                                        AND gen.FIELD_VALUE = 'Y'", conn);

                reader = comm.ExecuteReader();

                while (reader.Read())
                {
                    String studentID = reader["STDNT_ID"].ToString();
                    String courseID = reader["CRS_ID"].ToString();

                    if (!studentLCPs.ContainsKey(studentID))
                    {
                        studentLCPs.Add(studentID, new List<Tuple<string, string>>());
                    }

                    if (!newLCPs.ContainsKey(studentID))
                    {
                        newLCPs.Add(studentID, new List<Tuple<string, string, DateTime>>());
                    }

                    foreach (String prefix in AHSCoursePrefixes)
                    {
                        if (courseID.StartsWith(prefix))
                        {
                            for (int i = 0; i < AHSLCPDictionary[prefix].Length; i++)
                            {
                                if (AHSLCPDictionary[prefix][i] == null)
                                {
                                    continue;
                                }

                                Tuple<String, String, DateTime> LCP = new Tuple<string, string, DateTime>("1532010202", AHSLCPDictionary[prefix][i], new DateTime());

                                if (!studentLCPs[studentID].Any(prev => prev.Equals(LCP)))
                                {
                                    studentLCPs[studentID].Add(new Tuple<string, string>(LCP.Item1, LCP.Item2));
                                    newLCPs[studentID].Add(LCP);
                                    break;
                                }
                            }
                        }
                    }
                }

                reader.Close();

                comm = new SqlCommand(@"SELECT DISTINCT
	                                        class.STDNT_ID
	                                        ,lcp.COMP_POINT_ID
                                        FROM	
	                                        MIS.dbo.ST_OCP_LCP_A_55 lcp
	                                        INNER JOIN MIS.dbo.ST_STDNT_CLS_A_235 class ON class.CRS_ID = lcp.CRS_ID
	                                        INNER JOIN MIS.dbo.UTL_CODE_TABLE_120 code ON code.CODE = class.GRADE
	                                        INNER JOIN MIS.dbo.UTL_CODE_TABLE_GENERIC_120 gen ON gen.ISN_UTL_CODE_TABLE = code.ISN_UTL_CODE_TABLE
	                                        LEFT JOIN (SELECT	
					                                        *
				                                        FROM
					                                        StateSubmission.SDB.RecordType5 r5
					                                        INNER JOIN MIS.dbo.vwTermYearXwalk xwalk ON xwalk.StateReportingTerm = r5.DE1028) SRC ON SRC.DE1021 = class.STDNT_ID
																										                                          AND SRC.OrionTerm < class.EFF_TRM
																										                                          AND SRC.DE2105 = lcp.COMP_POINT_ID
                                        WHERE
	                                        lcp.CIP_CD = '1533010200'
	                                        AND class.EFF_TRM = '" + term + @"'
	                                        AND gen.cnxarraycolumn = '8'
	                                        AND code.TABLE_NAME = 'GRADE'
	                                        AND gen.FIELD_VALUE = 'Y'
	                                        AND lcp.COMP_POINT_TY = 'L9'
	                                        AND SRC.DE1021 IS NULL", conn);

                reader = comm.ExecuteReader();

                while (reader.Read())
                {
                    String studentID = reader["STDNT_ID"].ToString();
                    String lcpval = reader["COMP_POINT_ID"].ToString();

                    if (!studentLCPs.ContainsKey(studentID))
                    {
                        studentLCPs.Add(studentID, new List<Tuple<string, string>>());
                    }

                    if (!newLCPs.ContainsKey(studentID))
                    {
                        newLCPs.Add(studentID, new List<Tuple<string, string, DateTime>>());
                    }

                    Tuple<String, String, DateTime> LCP = new Tuple<string, string, DateTime>("1533010200", lcpval, new DateTime());

                    studentLCPs[studentID].Add(new Tuple<string, string>(LCP.Item1, LCP.Item2));
                    newLCPs[studentID].Add(LCP);
                }

                reader.Close();

            }

            ///////////////////////////////////////////////////////////////////////////////////output

            using (StreamWriter output = new StreamWriter("LCPs.csv"))
            {
                output.WriteLine("STDNT_ID,CIP_CD,LCP");

                foreach (String studentID in newLCPs.Keys.ToList())
                {
                    foreach (Tuple<String, String, DateTime> LCP in newLCPs[studentID])
                    {
                        output.WriteLine(String.Join(",", studentID, LCP.Item1, LCP.Item2, LCP.Item3));
                    }
                }
            }
        }
    }


    public class EducationalFunctioningLevel
    {
        public float lowerBound { get; set; }
        public float upperBound { get; set; }
        
    }

    public class Test
    {
        public String type { get; set; }
        public String subject { get; set; }
        public String form { get; set; }
        public DateTime testDate { get; set; }
        public float score { get; set; }

    }

    public class Course
    {
        public int EFL { get; set; }
        public String courseID { get; set; }
        public String refNum { get; set; }
        public String subject { get; set; }
        public String type { get; set; }
        public DateTime registrationDate { get; set; }
        public DateTime lastAttDate { get; set; }
        public OrionTerm term { get; set; }

    }

    public class Student
    {
        public String studentID { get; set; }
        public List<Test> tests { get; set; }
        public List<Course> courses { get; set; }
        public OrionTerm firstTermContinuousEnrollment { get; set; }

        public Student()
        {
            tests = new List<Test>();
            courses = new List<Course>();
        }

        public Test[] getTestsForDateRange(DateTime from, DateTime to, String type, String subject = null)
        {
            List<Test> testsInRange = tests.Where(test => test.testDate >= from && test.testDate <= to && test.type == type && test.subject == subject).ToList();

            Test[] testArray = testsInRange.ToArray();

            DateTime[] testDates = testsInRange.Select(test => test.testDate).ToArray();

            Array.Sort(testDates, testArray);

            return testArray;
        }

        public Course[] getCoursesInOrder(String type, String subject = null)
        {
            List<Course> requiredCourses = courses.Where(course => course.type == type && course.subject == subject).ToList();

            DateTime[] courseRegistrationDates = requiredCourses.Select(course => course.registrationDate).ToArray();

            Course[] courseArray = requiredCourses.ToArray();

            Array.Sort(courseRegistrationDates, courseArray);

            return courseArray;
        }
    }
}
