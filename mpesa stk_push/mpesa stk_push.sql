USE [EZPOS]
GO

/****** Object:  Table [dbo].[MpesaTxn]    Script Date: 12/01/2026 22:37:57 ******/
DROP TABLE [dbo].[MpesaTxn]
GO

/****** Object:  Table [dbo].[MpesaTxn]    Script Date: 12/01/2026 22:37:57 ******/
SET ANSI_NULLS ON
GO

SET QUOTED_IDENTIFIER ON
GO

CREATE TABLE [dbo].[MpesaTxn](
	[TransID] [nvarchar](20) NOT NULL,
	[TransTime] [datetime] NULL,
	[TransAmount] [decimal](18, 2) NULL,
	[BusinessShortCode] [nvarchar](10) NULL,
	[MSISDN] [nvarchar](150) NULL,
	[KYCInfo] [nvarchar](255) NULL,
	[IsPicked] [bit] NULL,
	[DateEntered] [datetime] NULL,
	[isDeleted] [bit] NULL,
	[DocLineID] [nvarchar](100) NULL,
	[TillNo] [nvarchar](10) NULL,
	[BillNo] [nvarchar](50) NULL,
	[BillAmount] [decimal](18, 0) NULL,
	[TrnType] [nvarchar](50) NOT NULL,
	[PhoneNumber] [nvarchar](20) NULL,
	[ResultCode] [nvarchar](10) NULL,
	[ResultDesc] [nvarchar](150) NULL,
	[ResponseCode] [nvarchar](10) NULL,
	[ResponseDescription] [nvarchar](150) NULL,
	[CustomerMessage] [nvarchar](150) NULL,
	[CheckoutRequestID] [nvarchar](150) NULL,
	[AccNumber] [nvarchar](20) NULL,
	[MerchantRequestID] [nvarchar](150) NULL,
	[errorCode] [nvarchar](50) NULL,
	[errorMessage] [nvarchar](50) NULL
) ON [PRIMARY]

GO


