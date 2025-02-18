const fs = require("fs");
require("dotenv").config();
const axios = require("axios");


// Log error utility
const logError = (context, error) => {
  const logMessage = `${new Date().toISOString()} - Error in ${context}: ${error.message}\n`;
  // console.error(logMessage);
  fs.appendFileSync("logs.txt", logMessage);
};

/*-------------------------START OF SEND ACK ORDER DATA ----------------------------*/

// Send acknowledgment for orders to the API
const sendAcknowledgment = async (dataItems, success) => {
  if (!dataItems.length) return;

  try {
    const url = process.env.URL;
    const acknowledgments = dataItems.map((item) => ({
      status: success ? "success" : "error",
      message: success ? "Data received successfully" : "Data processing failed",
      dataId: item.id,
    }));
    await axios.post(`${url}acknowledgeData`, acknowledgments, { headers: { "api-key": process.env.APIKEY } });
  } catch (ackError) {
    logError("sendAcknowledgment", ackError);
  }
};


/*-------------------------END OF SEND ACK ORDER DATA ----------------------------*/




/*-------------------------START OF SEND CUSTOMER, PRODUCT AND SALESMAN  DATA ----------------------------*/


const sendDataToAPI = async (data) => {
  try {
    const response = await axios.post(`${process.env.URL}productsData`, data, { headers: { "api-key": process.env.APIKEY } });
    // await processAcknowledgment(response.data, conn);
    return response.data; // Return the response data
  } catch (error) {
    logError("sendDataToAPI", error);
  }
};

const sendCustDataToAPI = async (data) => {
  try {
    const response = await axios.post(`${process.env.URL}customerData`, data, { headers: { "api-key": process.env.APIKEY } });
    // await processCustAcknowledgment(response.data, conn);
    return response.data; // Return the response data
  } catch (error) {
    logError("sendCustDataToAPI", error);
  }
};

const sendSalesmanDataToAPI = async (data) => {
  try {
    const response = await axios.post(`${process.env.URL}salesManData`, data, { headers: { "api-key": process.env.APIKEY } });
    // await processSalesmanAcknowledgment(response.data, conn);
    return response.data; // Return the response data
  } catch (error) {
    logError("sendSalesmanDataToAPI", error);
  }
};

module.exports = { axios,sendAcknowledgment,sendDataToAPI, sendCustDataToAPI, sendSalesmanDataToAPI };