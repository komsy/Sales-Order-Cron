const axios = require("axios");
const moment = require("moment");
const fs = require("fs");

const paymentTableName = process.env.PAYMENT_TABLE_NAME || "[dbo].[MpesaTxn]";
const onlineURL = process.env.ONLINE_URL;
const TOKEN_URL = process.env.LIVE_TOKEN_URL ||"https://api.safaricom.co.ke/oauth/v1/generate?grant_type=client_credentials";
const passkey ="";
const consumerKey = "";
const consumerSecret ="";
const HO_SHORTCODE = "";
const partyB = ""; //Till No
const BusinessShortCode = ""; //Used in Daraja
const clientName = process.env.CLIENT_NAME || "Multitech Solutions Ltd";
let cachedToken = null;
let tokenExpiry = null;

// --------------------------------------------------
// Logging helper
// --------------------------------------------------
const logError = (context, error) => {
  fs.appendFileSync(
    "logs.txt",
    `${new Date().toISOString()} - ${context}: ${error.message}\n`
  );
};

// --------------------------------------------------
// Generate Safaricom Password
// --------------------------------------------------
const generatePassword = (shortcode, passkey, timestamp) =>
  Buffer.from(`${shortcode}${passkey}${timestamp}`).toString("base64");

// --------------------------------------------------
// Generate M-Pesa Access Token (INLINE)
// --------------------------------------------------
const generateToken = async () => {
  try {
    const auth =
      "Basic " +
      Buffer.from(`${consumerKey}:${consumerSecret}`).toString("base64");

    const response = await axios.get(TOKEN_URL, {
      headers: {
        Authorization: auth,
        "Content-Type": "application/json",
      },
      timeout: 10000,
    });
    //.log("Token Generation Response:", response.data);
    return response.data || null;
  } catch (error) {
    logError("Token Generation", error);
    return null;
  }
};

const getToken = async () => {
  // 1. Check cache
  if (cachedToken && tokenExpiry && Date.now() < tokenExpiry) {
    return cachedToken;
  }

  // 2. Fetch new token
  const responseData = await generateToken(); // Modified to return response.data

  if (!responseData || !responseData.access_token) return null;

  cachedToken = responseData.access_token;

  // 3. Dynamic Expiry: Use the API's 'expires_in' (e.g. 3599)
  // Convert to ms, and subtract 5 mins buffer for safety
  const expiresInMs = parseInt(responseData.expires_in) * 1000;
  tokenExpiry = Date.now() + (expiresInMs - 5 * 60 * 1000);

  return cachedToken;
};

// --------------------------------------------------
// Process STK Push
// --------------------------------------------------
const processStkPush = async (conn, dbData) => {
  try {
    const accessToken = await getToken();
    if (!accessToken) {
      console.log("Failed to obtain access token");
      return null;
    }

    // console.log(`Generated accessToken: ${accessToken}, process.env.SHORTCODE: ${HO_SHORTCODE}`);
    const timestamp = moment().format("YYYYMMDDHHmmss");
    const password = generatePassword(HO_SHORTCODE, passkey, timestamp);
    // console.log(`Generated Password: ${password}`);
    const response = await axios.post(
      "https://api.safaricom.co.ke/mpesa/stkpush/v1/processrequest",
      {
        BusinessShortCode: HO_SHORTCODE,
        Password: password,
        Timestamp: timestamp,
        TransactionType: "CustomerBuyGoodsOnline",
        Amount: dbData.BillAmount,
        PartyA: dbData.PhoneNumber,
        PartyB: partyB,
        PhoneNumber: dbData.PhoneNumber,
        AccountReference: clientName,
        TransactionDesc: "Lipa na M-PESA",
        CallBackURL: `${onlineURL}/stkpush`,
      },
      {
        headers: {
          Authorization: `Bearer ${accessToken}`,
          "Content-Type": "application/json",
        },
        timeout: 15000,
      }
    );

    // console.log("STK Push Response:", response.data);

    await saveResponse(conn, response.data, dbData);
    await sendStkAcknowledgment(response.data);

    return response.data;
  } catch (error) {
    logError("processStkPush", error);
    return null; // IMPORTANT: do not throw in pkg
  }
};

// --------------------------------------------------
// Save Response to DB
// --------------------------------------------------
const saveResponse = async (conn, jsonData, originalData) => {
  const query = `
    UPDATE ${paymentTableName}
    SET BusinessShortCode = ?, CheckoutRequestID = ?, MerchantRequestID = ?, ResponseDescription = ?, ResponseCode = ?, CustomerMessage = ?
    WHERE SeedID = ? AND BillNo = ? AND TillNo = ? AND DateEntered >= CAST(GETDATE() AS DATE) AND DateEntered < DATEADD(day, 1, CAST(GETDATE() AS DATE))`;

  const params = [
    BusinessShortCode,
    jsonData.CheckoutRequestID,
    jsonData.MerchantRequestID,
    jsonData.ResponseDescription,
    jsonData.ResponseCode,
    jsonData.CustomerMessage,
    originalData.SeedID,
    originalData.BillNo,
    originalData.TillNo,
  ];

  try {
    await conn.query(query, params); // Clean and simple
  } catch (err) {
    logError("saveResponse", err);
  }
  // conn.query(query, params, (err) => {
  //   if (err) logError("saveResponse", err);
  // });
};

// --------------------------------------------------
// Send STK Acknowledgment
// --------------------------------------------------
const sendStkAcknowledgment = async (data) => {
  try {
    if (!data) return;

    // console.log("Sending STK acknowledgment for data:",onlineURL, data,BusinessShortCode);
    await axios.post(
      `${onlineURL}/acknowledgeStkData`,
      [
        {
          BusinessShortCode: BusinessShortCode,
          MerchantRequestID: data.MerchantRequestID,
          CheckoutRequestID: data.CheckoutRequestID,
          ResponseCode: data.ResponseCode,
          ResponseDescription: data.ResponseDescription,
          CustomerMessage: data.CustomerMessage,
        },
      ],
      { timeout: 10000 }
    );
  } catch (error) {
    logError("sendStkAcknowledgment", error);
  }
};

module.exports = processStkPush;
