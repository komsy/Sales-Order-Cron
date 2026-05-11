const dotenv = require('dotenv');
const path = require("path"); // Ensure path is defined before using it

const isPkg = !!process.pkg;

const envPath = isPkg
  ? path.join(path.dirname(process.execPath), '.env')
  : path.join(process.cwd(), '.env');

// Load environment variables from that specific path FIRST
dotenv.config({ path: envPath });

// console.log("ENV PATH:", envPath);

// ----------------------------------------------------------------
// NOW it is safe to require other modules that use process.env
// ----------------------------------------------------------------

const express = require("express");
const http = require("http");
const fs = require("fs");
const { openConnection } = require("./dbConfig"); // This line moves down
const app = require('./app');
const server = http.createServer(app);
const axios = require("axios");

// These variables will now be correctly populated:
const delaySpeed = parseInt(process.env.DELAY_SPEED) || 3000;
const PORT = process.env.PORT || 3000;
const paymentTableName = process.env.PAYMENT_TABLE_NAME || "[dbo].[MpesaTxn]";
const paymentResultCode = process.env.PAYMENT_RESULT_CODE || -1;
const isPicked = process.env.IsPicked || 1;
const apiKey = process.env.APIKEY || "ajIhX5jJcMW0Yiz";
const fetchOnline= process.env.FETCH_ONLINE || 0;
const url = process.env.ONLINE_URL;

// const processStkPush = require("./utils/process_payment");
let processStkPush;
let saveDataToDatabase;

try {
  ( saveDataToDatabase = require("./utils/saveDataToDatabase"),
    processStkPush = require("./utils/process_payment"));
  // console.log("fetchData loaded OK");
} catch (err) {
  fs.appendFileSync("fatal.log",`${new Date().toISOString()} - Failed to load saveDataToDatabase\n${err.stack}\n` );
} 


// Logs helper
const logError = (context, error) => {
  const logMessage = `${new Date().toISOString()} - Error in ${context}: ${error.message}\n`;
  fs.appendFileSync("logs.txt", logMessage);
};

/* ---------------------------------------------------
   QUERY DATABASE 
--------------------------------------------------- */


const queryFromDatabase = async (conn) => {

  try {
    const sqlMainQuery = `SELECT TillNo,BillNo,BillAmount, PhoneNumber,SeedID  FROM ${paymentTableName}  
    WHERE ResultCode IS NULL AND IsPicked = 0 AND IsValid = 1 AND TillNo IS NOT NULL AND DateEntered >= CAST(GETDATE() AS DATE)
    AND DateEntered < DATEADD(day, 1, CAST(GETDATE() AS DATE))`; 

    // Use conn.query() with the array of parameters
    const results = await conn.query(sqlMainQuery); 
    return results;
  } catch (error) {
    logError("queryFromDatabase", error);
    return [];
  }
};


/* ---------------------------------------------------
    FETCH DATA FROM API 
--------------------------------------------------- */
const fetchData = async (conn) => {
  try {
    const response = await axios.get(`${url}/getData`, {
      headers: { "api-key": apiKey },
    });
    // console.log("Data fetched from API:", response.data);
    const data = response.data;

    if (Array.isArray(data)) {
      
      await saveDataToDatabase(conn, data);
    } else {
      console.error("Error: Data from API is not an array");
    }
  } catch (error) {
    const errorMsg =
      error.code === "ECONNABORTED"
        ? "Request timeout"
        : error.code === "ENOTFOUND"
        ? "Host not found"
        : error.message;

    logError("fetchData", new Error(errorMsg));
  }
};

/*-------------------------END OF FETCH DATA FROM API ----------------------------*/



/* ---------------------------------------------------
   CRON - RUN EVERY 1 SECONDS
--------------------------------------------------- */
const startCronJob = async () => {
  try {
    const conn = await openConnection();
    //console.log("Connected to SQL Server");

    const runTask = async () => {
      try {
        
        //Query online safaricom integration
        if (fetchOnline==1) { 
          // Initial call
          await fetchData(conn);
        }
        const results = await queryFromDatabase(conn);
        // console.log(`Found ${results.length} records to process.`);

        for (const data of results) {
          // console.log("Processing data:", data);
          await conn.query(
            `UPDATE ${paymentTableName} SET ResultCode = ?, IsPicked = ? WHERE SeedID = ?
             AND DateEntered >= CAST(GETDATE() AS DATE) AND DateEntered < DATEADD(day, 1, CAST(GETDATE() AS DATE))`,
            [paymentResultCode,isPicked,data.SeedID]
          );

        await processStkPush(conn, data);
        }
      } catch (error) {
        logError("CronTask", error);
      }
    };

    // KEEP PROCESS ALIVE
    await runTask();
    setInterval(runTask, delaySpeed);   

    process.on("SIGINT", async () => {
      await conn.close();
      console.log("Database connection closed.");
      process.exit(0);
    });

  } catch (error) {
    logError("startCronJob", error);
  }
};

startCronJob();

// start server to listen for stk push
server.listen(PORT, '0.0.0.0',() => {
    console.log(`Application running on port ${PORT} and Connected to SQL Server`)
});