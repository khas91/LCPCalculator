USE [Adhoc]
GO

/****** Object:  Table [dbo].[Course_Subject_Area_Xwalk]    Script Date: 1/12/2017 11:44:25 AM ******/
SET ANSI_NULLS ON
GO

SET QUOTED_IDENTIFIER ON
GO

CREATE TABLE [dbo].[Course_Subject_Area_Xwalk](
	[CRS_ID] [varchar](max) NULL,
	[SUBJECT] [varchar](max) NULL,
	[INIT_FUNC_LEVEL] [varchar](max) NULL
) ON [PRIMARY] TEXTIMAGE_ON [PRIMARY]

GO


