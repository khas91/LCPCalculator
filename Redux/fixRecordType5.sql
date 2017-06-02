SELECT DISTINCT
	'07' AS [DE1017] 
	,[STDNT_ID]
	,'20172' AS [DE1028]
	,'5' AS [DE1010A] 
	,lcp.[CIP_CD] AS [DE2101]
	,'Z' AS [DE2103]
	,'20170531' AS [DE2102]
	,'05052017' AS [DE2121]
	,'Z' AS [DE2104]
	,[LCP] AS [DE2105]
	,'00000' AS [DE2106]
	,'N' AS [DE2107]
	,'Z' AS [DE2108]
	,'999' AS [DE2110]
	,'A' AS [DE102A]
	,'Z' AS [DE2111]
	,CASE
		WHEN LEFT([STDNT_ID], 4) = '0000' THEN 'D' + [STDNT_ID]
		ELSE STDNT_ID
	END AS [DE1021]
	,prog.PGM_CD
	,'999' AS [DE2116]
	,'999' AS [DE2117]
	,'999' AS [DE2118]
	,'999' AS [DE2119]
	,'999' AS [DE2120]
	,CASE
		WHEN master.FLEID <> '' THEN master.FLEID
		ELSE xwalk.FL_EDU_ID
	END AS [FLEID]
FROM 
	[Adhoc].[dbo].[TotalLCPs] lcp
	INNER JOIN (SELECT
					*
					,ROW_NUMBER() OVER (PARTITION BY CIP_CD ORDER BY EFF_TRM_D DESC) RN
				FROM
					MIS.dbo.ST_PROGRAMS_A_136
				WHERE
					END_TRM = ''
				    AND EFF_TRM_D <> '') prog ON prog.CIP_CD = lcp.CIP_CD
											  AND RN = 1
	INNER JOIN MIS.dbo.ST_STDNT_SSN_SID_XWALK_606 xwalk ON xwalk.STUDENT_SSN = lcp.STDNT_ID
	LEFT JOIN Adhoc.dbo.FLEID_MASTER master ON master.SSN = lcp.STDNT_ID
WHERE
	lcp.[STDNT_ID] IN (SELECT DISTINCT stdnt_id FROM State_Report_Data.dbo.sdb_rtype_1 r1)




DELETE r5
FROM
	State_Report_Data.dbo.sdb_rtype_5 r5
WHERE
	r5.DE2105 <> 'Z'

INSERT INTO
	State_Report_Data.dbo.sdb_rtype_5 
	SELECT DISTINCT
		'07' AS [DE1017] 
		,[STDNT_ID]
		,'20172' AS [DE1028]
		,'5' AS [DE1010A] 
		,lcp.[CIP_CD] AS [DE2101]
		,'Z' AS [DE2103]
		,'20170531' AS [DE2102]
		,'31052017' AS [DE2121]
		,'Z' AS [DE2104]
		,[LCP] AS [DE2105]
		,'00000' AS [DE2106]
		,'N' AS [DE2107]
		,'Z' AS [DE2108]
		,'999' AS [DE2110]
		,'A' AS [DE102A]
		,'Z' AS [DE2111]
		,CASE
			WHEN LEFT([STDNT_ID], 4) = '0000' THEN 'D' + [STDNT_ID]
			ELSE STDNT_ID
		END AS [DE1021]
		,prog.PGM_CD
		,'999' AS [DE2116]
		,'999' AS [DE2117]
		,'999' AS [DE2118]
		,'999' AS [DE2119]
		,'999' AS [DE2120]
		,CASE
			WHEN master.FLEID <> '' THEN master.FLEID
			ELSE xwalk.FL_EDU_ID
		END AS [FLEID]
	FROM 
		[Adhoc].[dbo].[TotalLCPs] lcp
		INNER JOIN (SELECT
						*
						,ROW_NUMBER() OVER (PARTITION BY CIP_CD ORDER BY EFF_TRM_D DESC) RN
					FROM
						MIS.dbo.ST_PROGRAMS_A_136
					WHERE
						END_TRM = ''
						AND EFF_TRM_D <> '') prog ON prog.CIP_CD = lcp.CIP_CD
												  AND RN = 1
		INNER JOIN MIS.dbo.ST_STDNT_SSN_SID_XWALK_606 xwalk ON xwalk.STUDENT_SSN = lcp.STDNT_ID
		LEFT JOIN Adhoc.dbo.FLEID_MASTER master ON master.SSN = lcp.STDNT_ID
	WHERE
		lcp.[STDNT_ID] IN (SELECT DISTINCT stdnt_id FROM State_Report_Data.dbo.sdb_rtype_1 r1)
	