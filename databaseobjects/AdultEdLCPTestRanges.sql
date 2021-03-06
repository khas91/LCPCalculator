USE [Adhoc]
GO
/****** Object:  Table [dbo].[AdultEdLCPTestRanges]    Script Date: 3/28/2017 3:16:42 PM ******/
SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO
CREATE TABLE [dbo].[AdultEdLCPTestRanges](
	[LCP_Type] [varchar](max) NULL,
	[SUBJECT] [varchar](max) NULL,
	[LCP] [varchar](max) NULL,
	[LowerRange] [int] NULL,
	[UpperRange] [int] NULL
) ON [PRIMARY] TEXTIMAGE_ON [PRIMARY]

GO
INSERT [dbo].[AdultEdLCPTestRanges] ([LCP_Type], [SUBJECT], [LCP], [LowerRange], [UpperRange]) VALUES (N'ESOL', N'', N'A', 0, 180)
INSERT [dbo].[AdultEdLCPTestRanges] ([LCP_Type], [SUBJECT], [LCP], [LowerRange], [UpperRange]) VALUES (N'ESOL', N'', N'B', 180, 191)
INSERT [dbo].[AdultEdLCPTestRanges] ([LCP_Type], [SUBJECT], [LCP], [LowerRange], [UpperRange]) VALUES (N'ESOL', N'', N'C', 191, 201)
INSERT [dbo].[AdultEdLCPTestRanges] ([LCP_Type], [SUBJECT], [LCP], [LowerRange], [UpperRange]) VALUES (N'ESOL', N'', N'D', 201, 211)
INSERT [dbo].[AdultEdLCPTestRanges] ([LCP_Type], [SUBJECT], [LCP], [LowerRange], [UpperRange]) VALUES (N'ESOL', N'', N'E', 211, 221)
INSERT [dbo].[AdultEdLCPTestRanges] ([LCP_Type], [SUBJECT], [LCP], [LowerRange], [UpperRange]) VALUES (N'ESOL', N'', N'F', 221, 236)
INSERT [dbo].[AdultEdLCPTestRanges] ([LCP_Type], [SUBJECT], [LCP], [LowerRange], [UpperRange]) VALUES (N'ABE', N'MA', N'A', 0, 314)
INSERT [dbo].[AdultEdLCPTestRanges] ([LCP_Type], [SUBJECT], [LCP], [LowerRange], [UpperRange]) VALUES (N'ABE', N'MA', N'B', 314, 442)
INSERT [dbo].[AdultEdLCPTestRanges] ([LCP_Type], [SUBJECT], [LCP], [LowerRange], [UpperRange]) VALUES (N'ABE', N'MA', N'C', 442, 506)
INSERT [dbo].[AdultEdLCPTestRanges] ([LCP_Type], [SUBJECT], [LCP], [LowerRange], [UpperRange]) VALUES (N'ABE', N'MA', N'D', 506, 566)
INSERT [dbo].[AdultEdLCPTestRanges] ([LCP_Type], [SUBJECT], [LCP], [LowerRange], [UpperRange]) VALUES (N'ABE', N'RE', N'E', 0, 368)
INSERT [dbo].[AdultEdLCPTestRanges] ([LCP_Type], [SUBJECT], [LCP], [LowerRange], [UpperRange]) VALUES (N'ABE', N'RE', N'F', 368, 461)
INSERT [dbo].[AdultEdLCPTestRanges] ([LCP_Type], [SUBJECT], [LCP], [LowerRange], [UpperRange]) VALUES (N'ABE', N'RE', N'G', 461, 518)
INSERT [dbo].[AdultEdLCPTestRanges] ([LCP_Type], [SUBJECT], [LCP], [LowerRange], [UpperRange]) VALUES (N'ABE', N'RE', N'H', 518, 567)
INSERT [dbo].[AdultEdLCPTestRanges] ([LCP_Type], [SUBJECT], [LCP], [LowerRange], [UpperRange]) VALUES (N'ABE', N'LA', N'J', 0, 390)
INSERT [dbo].[AdultEdLCPTestRanges] ([LCP_Type], [SUBJECT], [LCP], [LowerRange], [UpperRange]) VALUES (N'ABE', N'LA', N'K', 390, 461)
INSERT [dbo].[AdultEdLCPTestRanges] ([LCP_Type], [SUBJECT], [LCP], [LowerRange], [UpperRange]) VALUES (N'ABE', N'LA', N'M', 461, 518)
INSERT [dbo].[AdultEdLCPTestRanges] ([LCP_Type], [SUBJECT], [LCP], [LowerRange], [UpperRange]) VALUES (N'ABE', N'LA', N'N', 518, 560)
