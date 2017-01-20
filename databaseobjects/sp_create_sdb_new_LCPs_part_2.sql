USE [MIS]
GO

/****** Object:  StoredProcedure [dbo].[sp_create_sdb_new_LCPs_part_2]    Script Date: 1/20/2017 10:37:45 AM ******/
SET ANSI_NULLS ON
GO

SET QUOTED_IDENTIFIER ON
GO


CREATE PROCEDURE [dbo].[sp_create_sdb_new_LCPs_part_2] 
	@term varchar(6),  --Term being built.
	@run char(1), --Beginning (B) or End of term (E).
	@mode char(2),  --Mode 35 to build records for 3 and 5 or 8 to build records for 8.
	@DE1028 int
AS
BEGIN
		
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

		--Load SDB Record Type 4
		exec [MIS].[dbo].[sp_create_sdb_rt4] @term, @run

		if @run = 'E'
		begin
			-- Load SDB Record Type 9 (Industry Certification Funded)
			-- Added 21May2014, KDL
			exec [MIS].[dbo].[sp_create_sdb_rt9] @term

			--Load SDB Record Type 7
			exec [MIS].[dbo].[sp_create_sdb_rt7] @term
			
			--Load SDB Record Type 3
			exec [MIS].[dbo].[sp_create_sdb_rt3_driver] @term, @next_term, @term_begin_date, @next_term_begin_date
			
			--Load SDB Record Type 3
			exec [MIS].[dbo].[sp_create_sdb_rt3] @term, @next_term, @run, @term_begin_date, @next_term_begin_date		
			
			--Load SDB Record Type 5
			exec [MIS].[dbo].[sp_create_sdb_rt5] @term, @term_end_date		
			
			--Load SDB Record Type 5
			--Load SDB 3 and 5 Defaults.
			exec [MIS].[dbo].[sp_load_defaults] '35', @term  --load record type 1 defaults in 3 and 5 mode.			
		end


	if @mode = '8'
	begin
		exec MIS.dbo.sp_create_sdb_rt8
		--Had to use a cursor to iterate through the terms in record type 8.
		--This creates a record type 1 for each term record type 8.
		
		select
			distinct term 
			, row_number()over (order by term)CNT
		INTO #term
		from 
			State_Report_Data.dbo.sdb_rtype_8
		GROUP BY TERM	
	
		declare @i int
		declare @n int
		set @i = 1
		set @n = (SELECT max(cnt) from #term)
		
		IF  object_id('State_Report_Data.dbo.RT1','U') is not null
			DROP TABLE [state_report_data].dbo.rt1
		
				
		WHILE @i<=@n
		BEGIN
		set @term = (SELECT term from #term where cnt = @i)
		set @de1028 = (case when SUBSTRING(@term,5,1) = 1 then '2'+substring(cast(SUBSTRING(@term,1,4)-1 as char(4)),3,2) 
						 when  SUBSTRING(@term,5,1) = 2 then '3'+substring(@term,3,2) 
						 when  SUBSTRING(@term,5,1) = 3 then '1'+substring(@term,3,2) 
						end)
			
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

		--Load defaults into driver.
		exec [MIS].[dbo].[sp_load_defaults] '8',@term
		
		--Load SDB Record Type 1 driver
		exec [MIS].[dbo].[sp_create_sdb_rt1_driver] @term, 'E'
		select @term

		--Load SDB Record Type 1
		exec [MIS].[dbo].[sp_create_sdb_rt1] @term, @DE1028, @next_term, @run, @term_begin_date, @next_term_begin_date, @mode
		select @term, @DE1028, @next_term, @run, @term_begin_date, @next_term_begin_date, @mode
		IF  object_id('State_Report_Data.dbo.RT1','U') is not null
		begin
			INSERT INTO [state_report_data].dbo.RT1 SELECT * FROM [state_report_data].dbo.sdb_rtype_1 
		end
		ELSE
		BEGIN
			SELECT * INTO [state_report_data].dbo.RT1 FROM [state_report_data].dbo.sdb_rtype_1
		END	
		
		IF  object_id('State_Report_Data.dbo.sdb_driver','U') is not null
			drop table [state_report_data].[dbo].[sdb_driver];
		
		IF  object_id('State_Report_Data.dbo.sdb_flags','U') is not null
			drop table [state_report_data].[dbo].[sdb_flags];
		
		if  object_id('State_Report_Data.dbo.sdb_rtype_1','U')is not null and @i <> @n
			drop table [state_report_data].[dbo].[sdb_rtype_1];
		
		SET @i = @i+1
		END
		if  object_id('State_Report_Data.dbo.sdb_rtype_1','U')is not null and @i <> @n
			drop table [state_report_data].[dbo].[sdb_rtype_1]
		SELECT *
		INTO [state_report_data].[dbo].[sdb_rtype_1]
		FROM [state_report_data].dbo.RT1
		drop table [state_report_data].[dbo].RT1
		
	end

	if @mode = '35'
		begin
			--Load SDB Record Type 1 driver
			exec [MIS].[dbo].[sp_create_sdb_rt1_driver] @term, @run

			--Load SDB Record Type 1
			exec [MIS].[dbo].[sp_create_sdb_rt1] @term, @DE1028, @next_term, @run, @term_begin_date, @next_term_begin_date, @mode 
		end 
	else
		begin
			print 'Must use 35 mode'
		end
		
	if @mode = '35'
		begin
			--Load SDB Record Type 2
			exec [MIS].[dbo].[sp_create_sdb_rt2] @term, @run				
		end
	
	--Update student type for all record types.
	exec MIS.dbo.sp_update_student_type
	if @mode = '35'
		begin
			--Update ClassID where 4th position is charecter
			exec MIS.dbo.sp_update_ClassID
		end

/*
KDL, 03Mar2015, DE1050 and DE1051
This is to populate the developmental education elements with data according to the values and logic provided by the state
The procedure was built seperate due to the large amount of logic that was required to identify the values

*/

if @mode='35'
begin
	exec sp_update_rt1_devEd @term;
end

if @mode='35'
begin
	exec spEPI_Update @DE1028;
end

/*
KDL, 28Aug2015; 
This creates a copy of the student build as seperate schema named EOT or BOT 
*/	

if @mode='35'
begin
	exec dbo.spTransferDataSeperateSchema @run
end

/*
KDL, 04Mar2015; This procedure should always run last
It captures any error that has been added to the procedure in a table named [State_Report_Data].[Error].[SDB_Errors]
*/

exec Error.spCaptureSDBErrors @term,@run;	

END

GO


