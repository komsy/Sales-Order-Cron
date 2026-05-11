const fs = require("fs");
require("dotenv").config();
const express = require("express");
const router = express.Router();
// const { saveDataToDatabase } = require("../utils/saveDataToDatabase");
const { openConnection } = require("../dbConfig");
let saveDataToDatabase;

try {
  (saveDataToDatabase = require("../utils/saveDataToDatabase"));
  // console.log("saveDataToDatabase loaded OK");
} catch (err) {
  fs.appendFileSync("fatal.log",`${new Date().toISOString()} - Failed to load saveDataToDatabase\n${err.stack}\n` );
} 


// Log error utility
const logError = (context, error) => {
  const logMessage = `${new Date().toISOString()} - Error in ${context}: ${error.message}\n`;
  // console.error(logMessage);
  fs.appendFileSync("logs.txt", logMessage);
};


router.post("/saveTransactions", async (req, res) => {

  const data = req.body;
  //.log("Received data:", data);
   if (!Array.isArray(data)) {
    return res.status(400).json({ error: "Invalid data format" });
  }
  try {
    const conn = await openConnection();
    // Save data to the database
    const dataToInsert = await saveDataToDatabase(conn, data);
    //console.log("Data saved successfully:", dataToInsert);

    return res.status(200).json({
      message: "Data saved successfully",
      acknowledgments: dataToInsert, // send back inserted data
    });
  } catch (error) {
    logError("saveTransactions", error);
    // await sendAcknowledgment(data, false);
    return res.status(500).json({ error: "Internal server error" });
  }
});
  

module.exports = router;