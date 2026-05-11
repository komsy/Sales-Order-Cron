// const { sendAcknowledgment,sendOnlineFromApp } = require("./apiservices");
const moment = require("moment");
const tableName = process.env.PAYMENT_TABLE_NAME || "[dbo].[MpesaTxn]";
const fs = require("fs");
const e = require("express");
const sendOnline = process.env.SEND_ONLINE;
const locationID = process.env.LOCATIONID || 0;

let sendAcknowledgment;
let sendOnlineFromApp;

try {
  ({ sendAcknowledgment, sendOnlineFromApp } = require("./apiservices"));
  // console.log("sendAcknowledgment loaded OK");
} catch (err) {
  fs.appendFileSync("fatal.log",`${new Date().toISOString()} - Failed to load sendAcknowledgment\n${err.stack}\n`);
}

// Log error utility
const logError = (context, error) => {
  const logMessage = `${new Date().toISOString()} - Error in ${context}: ${error.message }\n`;
  // console.error(logMessage);
  fs.appendFileSync("logs.txt", logMessage);
};

// Save data to the database
const saveDataToDatabase = async (conn, data) => {
  const dataToInsert = [];
  const dataForOnline = [];
  // console.log("saveDataToDatabase called with data:", data);

  for (const item of data) {
    // if (!item.TransID) continue; //Skip if no TransID

    // Track origin
    if (item && item.data) {
      dataForOnline.push(item);
      dataToInsert.push(item.TransID);
    } else {
      dataToInsert.push(item);
    }

    // -----------------------------
    // CHECK EXISTING TRANSACTION
    // -----------------------------
    const existingDataQuery = `SELECT 1 FROM ${tableName} WHERE TransID = ?`;
    const existingDataParams = [item.TransID];
    // console.log("Checking existing data for TransID:", item.TransID);
    try {
      // const existingDataResult = await new Promise((resolve, reject) => {
      //   conn.query(existingDataQuery, existingDataParams, (err, results) => {
      //     if (err) return reject(err);
      //     resolve(results);
      //   });
      // });
      const existingDataResult = await conn.query(existingDataQuery, existingDataParams);

      if (existingDataResult && existingDataResult.length > 0) {
          logError("saveDataToDatabase", new Error(`Mpesa Ref '${item.TransID}' already exists.`));
          continue;
      } 

    } catch (error) {
      logError("saveDataToDatabase - check existing data", error);
      continue;
    }
    
    // -----------------------------
    // GET NEXT SEED ID
    // -----------------------------
    let nextSeedID = 1; // default fallback
    try {
      const seedResult = await conn.query(
        `SELECT ISNULL(MAX(SeedID), 0) + 1 AS NextSeedID FROM ${tableName}`
      );
      if (seedResult && seedResult.length > 0) {
        nextSeedID = seedResult[0].NextSeedID;
      }
    } catch (error) {
      logError("saveDataToDatabase - get seed ID", error);
    }
    
    // console.log("No existing data found for TransID:", item.TransID);
    // -----------------------------
    // PREPARE QUERY
    // -----------------------------
    const currentDatetime = moment().format("YYYY-MM-DD HH:mm:ss");

    let insertQuery;
    let insertParams;

    // C2B DIRECT PAYMENT (NO TillNo / CheckoutRequestID)
    if (!item.CheckoutRequestID) {

      insertQuery = `
        INSERT INTO ${tableName} (SeedID, LocationID,QType,TransID, TransTime, TransAmount, BusinessShortCode,MSISDN, KYCInfo, TrnType, IsPicked, DateEntered,IsValid)
        VALUES (?,?,'C2B',?, ?, ?, ?, ?, ?, 'MPESA', 0, ?, 1)`;

      insertParams = [nextSeedID, locationID,
        item.TransID || "",item.created_at || currentDatetime,item.TransAmount || "",item.BusinessShortCode || "",
        item.MSISDN || "",item.FirstName || item.KYCInfo || "",currentDatetime,];

      // logError("saveDataToDatabase",new Error(`C2B Data for item ${item.TransID}. Saved.`));
    } else {
      // STK PUSH RESPONSE UPDATE
      // console.log(`STK Push response for data ${data}. Updating record.`);
      insertQuery = `
        UPDATE ${tableName}
        SET LocationID = ?,IsValid = 1, TransID = ?,TransTime = ?,TransAmount = ?,BusinessShortCode = ?,MSISDN = ?,KYCInfo = ?,TrnType = 'MPESA',
          IsPicked = 1,MerchantRequestID = ?,ResultCode = ?,ResultDesc = ? WHERE CheckoutRequestID = ? AND MerchantRequestID = ?`;
      insertParams = [locationID,
        item.TransID || "", item.created_at || currentDatetime,item.TransAmount || "0",item.BusinessShortCode || "",item.MSISDN || "",
        item.FirstName || "",item.MerchantRequestID || null,item.ResultCode ?? null,item.ResultDesc || null,item.CheckoutRequestID, item.MerchantRequestID ];
    }
    // console.log("Executing query:", insertQuery, "with params:", insertParams);
    // -----------------------------
    // EXECUTE QUERY
    // -----------------------------
    try {
      await conn.query(insertQuery, insertParams);
      // await new Promise((resolve, reject) => {
      //   conn.query(insertQuery, insertParams, (err) => {
      //     if (err) return reject(err);
      //     // console.log(`Mpesa transaction processed: ${item.TransID}`);
      //     resolve();
      //   });
      // });
    } catch (error) {
      logError("saveDataToDatabase - insert/update", error);
    }
  }

  //console.log("dataToInsert", dataToInsert);

  // -----------------------------
  // ACKNOWLEDGMENT
  // -----------------------------
  if (dataToInsert.length > 0) {
    const entry = dataToInsert[0]; // Grab the first entry for type check

    if (typeof entry != 'string') {
      // If data is an array, send acknowledgment for all
      try {
        await sendAcknowledgment(dataToInsert, true);
      } catch (error) {
        logError("sendAcknowledgment", error);
      }
    } else {
      //send to online if from app
      if (sendOnline == 1) {
        await sendOnlineFromApp(dataForOnline, true);
      }
      // If data is a single item, return it
      return dataToInsert;
    }
  }
};

module.exports = saveDataToDatabase;
