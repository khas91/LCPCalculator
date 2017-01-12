USE [Adhoc]
GO

/****** Object:  Table [dbo].[TABEFormRanges]    Script Date: 1/12/2017 10:11:45 AM ******/
SET ANSI_NULLS ON
GO

SET QUOTED_IDENTIFIER ON
GO

CREATE TABLE [dbo].[TABEFormRanges](
	[Form] [varchar](max) NULL,
	[SUBTEST] [varchar](max) NULL,
	[LOWER_RANGE] [int] NULL,
	[UPPER_RANGE] [int] NULL
) ON [PRIMARY] TEXTIMAGE_ON [PRIMARY]

GO

