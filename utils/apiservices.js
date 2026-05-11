const fs = require("fs");
require("dotenv").config();
const axios = require("axios");
const url = process.env.ONLINE_URL;
const apiKey = process.env.APIKEY || "ajIhX5jJcMW0Yiz";

// Log error utility
const logError = (context, error) => {
  const logMessage = `${new Date().toISOString()} - Error in h ${context}: ${error.message}\n`;
  // console.error(logMessage);
  fs.appendFileSync("logs.txt", logMessage);
};




/*-------------------------START OF SEND ACK Payment DATA BACK Online ----------------------------*/

const sendAcknowledgment = async (dataItems, success) => {
  if (!dataItems.length) return;

    //console.log('dataItems',dataItems);
  try {
    const acknowledgments = dataItems.map((item) => ({
      status: success ? "success" : "error",
      message: success ? "Data received successfully" : "Data processing failed",
      dataId: item.id
    }));
    //console.log("Acknowledgments to send:", acknowledgments);

    await axios.post(`${url}/acknowledgeData`, acknowledgments, {
      headers: { "api-key": apiKey },
    });
  } catch (ackError) {
    logError("sendAcknowledgment", ackError);
  }
};

/*-------------------------END OF SEND ACK Payment DATA ----------------------------*/



/* ---------------------------------------------------
    SEND ONLINE DATA TO APP 
--------------------------------------------------- */
const sendOnlineFromApp = async (dataItems) => {
  if (!dataItems.length) return;

  for (const item of dataItems) {
    try {
      //console.log("Sending item:", item);

      await axios.post(`${url}/confirmation`, item, {
        headers: { "api-key": apiKey },
      });
    } catch (ackError) {
      logError("sendOnlineFromAppACK", ackError);
    }
  }
};



module.exports = {sendAcknowledgment,sendOnlineFromApp};