USE [MIS]
GO

/****** Object:  StoredProcedure [dbo].[sp_create_sdb_new_LCPs_part_1]    Script Date: 1/20/2017 10:37:36 AM ******/
SET ANSI_NULLS ON
GO

SET QUOTED_IDENTIFIER ON
GO

/** ===================================================
 Author:		Florida State College at Jacksonville
 Create date: 10/21/2008
 Description:	Kickoff procedure for complete SDB Build
 Modification History: 
				Date: 06-13-2011
				Description: Record type 8 build is updated.
				Modified By: M Nuruzzaman
				
Request to move to production by M Nuruzzaman on 29Jun2011
Moved to production by K LaValley on 30Jun2011

[dbo].[sp_create_sdb] '20161','B','35',215

-- example RT8 build
[dbo].[sp_create_sdb] '20153','E','8',115
select * from [State_Report_Data].[dbo].[sdb_rtype_1]
select * from [State_Report_Data].[dbo].[sdb_rtype_8]


-- =================================================== **/
CREATE PROCEDURE [dbo].[sp_create_sdb_new_LCPs_part_1] 
	@term varchar(6),  --Term being built.
	@run char(1), --Beginning (B) or End of term (E).
	@mode char(2),  --Mode 35 to build records for 3 and 5 or 8 to build records for 8.
	@DE1028 int
AS
BEGIN

	---- for testing
	--declare @term varchar(6)='20161',
	--declare @run char(1)='B',
	--declare @mode char(2)='35',
	--declare @DE1028 int=215

	-- SET NOCOUNT ON added to prevent extra result sets from
	-- interfering with SELECT statements.
	SET NOCOUNT ON;
	
	declare @term_begin_date char(8)
	declare @term_end_date char(8)
	declare @next_term_begin_date char(8)
	declare @next_term varchar(6)  --Next term following @term.
	
	declare @first_term char(5)
	declare @name varchar(100)
	declare @count int
	set @count = 0
	
	--retrieve dates for terms.
	set @term_begin_date = (select distinct sess_beg_dt 
	from st_session_a_172
	where session_key = (@term + ' 1'))
	
	--retrieve dates for terms.
	set @term_end_date = (select distinct SESS_END_DT 
	from st_session_a_172
	where session_key = (@term + ' 1'))


	--Calcuate next term ID.
	if SUBSTRING(@term,5,1) = '3'
		begin
			set @next_term = cast(@term as int) + 8
		end
	else
		begin
			set @next_term = CAST(@term as int) + 1
		end


	set @next_term_begin_date = (select distinct sess_beg_dt 
	from st_session_a_172
	where session_key = (@next_term + ' 1'))

	
	--Drop all student data tables. This will make sure that old
	--data is not used if the script fails.

	exec MIS.dbo.sp_drop_all_sdb_tables

	
	--Load SDB Core Driver
	exec [MIS].[dbo].[sp_create_sdb_driver] @term, @run

	--Load SDB Record Type 6
	exec [MIS].[dbo].[sp_create_sdb_rt6] @term, @run, @next_term, @term, @term_begin_date, @next_term_begin_date


END
GO


