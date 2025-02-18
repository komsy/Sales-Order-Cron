require("dotenv").config();
const axios = require("axios");
const sql = require("msnodesqlv8");
const fs = require("fs");
const server =process.env.DB_SERVER || "POSSERVER\SQL2012";
const database = process.env.DB_DATABASE || "EZPOS";
const username = process.env.DB_USERNAME || "sa";
const password = process.env.DB_PASSWORD || "A123456a";
const sqlport = process.env.DB_PORT || "1433";

//Local machine 
//  const config ="server=.;Database=PCC_HQ;Trusted_Connection=Yes;Driver={sql Server Native Client 11.0}";

//Remote db 
const config = `Server=${server};Database=${database};Uid=${username};Pwd=${password};Driver={SQL Server Native Client 11.0};`;

const delay = (ms) => new Promise((resolve) => setTimeout(resolve, ms));

const startCronJob = async () => {
  try {
    const conn = await openConnection();
    console.log("Connected to SQL Server");

    // Call queryFromDatabase() function initially
    await queryFromDatabase(conn);

    // Call queryFromDatabase() function every 5 seconds
    const interval = process.env.DELAY_SPEED || "5000";
    setInterval(async () => { await queryFromDatabase(conn);; }, interval);
  } catch (error) {
    console.error("Error connecting to SQL Server:", error);
  }
};

    const openConnection = () => {
    return new Promise((resolve, reject) => {
        sql.open(config, (err, conn) => {
        if (err) {
            reject(err);
        } else {
            resolve(conn);
        }
        });
    });
    };
  
  const queryFromDatabase = async (conn, data) => {
    const tableName = "[dbo].[vwProductMst]";
    const productPriceTable = "[dbo].[ProductPackingPrice]";
    const locationID = process.env.LOCATIONID;
  
    try {
      // Fetch products from the database
      const productsQuery = `
        SELECT ItmCode,LocationID,GodownName,LongName,CatCode,CatName,Unit,
        TaxCode,TaxRate,RspIncVat,WspIncVat,CurrBalance,FixUnitOfSell 
        FROM ${tableName} WHERE sendToApp = 1 AND isUpdated = 0 AND LocationID = ?`;
  
      const products = await new Promise((resolve, reject) => {
        conn.query(productsQuery, [locationID], (err, results) => {
          if (err) return reject(err);
          resolve(results);
        });
      });
    //   console.log("productsQuery", products);
      const productData = [];
      for (const product of products) {
        const productPriceQuery = `
          SELECT * FROM ${productPriceTable} WHERE ItmCode = ?`;
  
        const productPrices = await new Promise((resolve, reject) => {
          conn.query(productPriceQuery, [product.ItmCode], (err, results) => {
            if (err) return reject(err);
            resolve(results);
          });
        });
  
        // Combine product and its prices into a single object
        productData.push({ ...product, prices: productPrices });
      }
  
      if (productData && productData.length > 0) {
        console.log("Product data fetched successfully:", productData);
      
        // Send data back to the API or further process
        await sendDataToAPI(productData, conn);
        await queryCustomersFromDatabase(conn);
      } else {
        console.log("No product data available to send.");
      }
    } catch (error) {
      console.error("Error querying database:", error);
      const logMessage = `${new Date().toISOString()} - Error querying database: ${error.message}\n`;
  
      fs.appendFile("logs.txt", logMessage, (writeError) => {
        if (writeError) {
          console.error("Error writing to log file:", writeError);
        }
      });
    }
  };
  const queryCustomersFromDatabase = async (conn, data) => {
    const tableName = "[dbo].[CustomerMst]";
    const locationID = process.env.LOCATIONID;
  
    try {
      // Fetch customers from the database
      const customersQuery = `
        SELECT DISTINCT CusCode,AccType,CompanyName,LocationID,Cr_Limit,CurrBalance,ACCSTATUS
        FROM ${tableName} 
        where sendToApp =1 and isUpdated = 0 and AccType='AC' AND ACCSTATUS = 'A' AND LocationID = ?`;
  
      const customers = await new Promise((resolve, reject) => {
        conn.query(customersQuery, [locationID], (err, results) => {
          if (err) return reject(err);
          resolve(results);
        });
      });
    //   console.log("customersQuery", customers);

  
      if (customers && customers.length > 0) {
        // console.log("Customer data fetched successfully:", customers);
      
        // Send data back to the API or further process
        await sendCustDataToAPI(customers, conn);
      } else {
        console.log("No Customer data available to send.");
      }
    } catch (error) {
      console.error("Error querying database:", error);
      const logMessage = `${new Date().toISOString()} - Error querying database: ${error.message}\n`;
  
      fs.appendFile("logs.txt", logMessage, (writeError) => {
        if (writeError) {
          console.error("Error writing to log file:", writeError);
        }
      });
    }
  };

  const sendDataToAPI = async (data, conn) => {
    const apiKey = process.env.APIKEY;
    const url = process.env.URL;
    const headers = { 'api-key': apiKey };
  
    try {
      const response = await axios.post(`${url}productsData`, data, { headers });
      $results =response.data;
      await processAcknowledgment($results, conn);
    //   console.log("Data sent successfully:", response.data);
    } catch (error) {
      console.error("Error sending data to API:", error);
    }
  };

  const sendCustDataToAPI = async (data, conn) => {
    const apiKey = process.env.APIKEY;
    const url = process.env.URL;
    const headers = { 'api-key': apiKey };
  
    try {
      const response = await axios.post(`${url}customerData`, data, { headers });
      $results =response.data;
      await processCustAcknowledgment($results, conn);
    //   console.log("Data sent successfully:", response.data);
    } catch (error) {
      console.error("Error sending data to API:", error);
    }
  };


  const processAcknowledgment = async (response, conn) => {
    if (!response || !response.acknowledgments) {
      console.error("Invalid response format");
      return;
    }
  
    const ackProducts = response.acknowledgments.map((ack) => ack.productId);
  
    if (ackProducts.length > 0) {
      // Constructing query with dynamic placeholders
      const placeholders = ackProducts.map(() => '?').join(',');
      const updateQuery = `UPDATE productMst SET isUpdated = 1  WHERE ItmCode IN (${placeholders})`;
  
      try {
        await new Promise((resolve, reject) => {
          conn.query(updateQuery, ackProducts, (err, result) => {
            if (err) return reject(err);
            resolve(result);
          });
        });
  
        console.log("Products successfully marked as updated.");
      } catch (error) {
        console.error("Error updating products:", error);
      }
    } else {
      console.log("No products to update.");
    }
  };

  const processCustAcknowledgment = async (response, conn) => {
    if (!response || !response.acknowledgments) {
      console.error("Invalid response format");
      return;
    }
  
    const ackCustomers = response.acknowledgments.map((ack) => ack.customerId);
  
    if (ackCustomers.length > 0) {
      // Constructing query with dynamic placeholders
      const placeholders = ackCustomers.map(() => '?').join(',');
      const updateQuery = `UPDATE customerMst SET isUpdated = 1  WHERE CusCode IN (${placeholders}) `;
  
      try {
        await new Promise((resolve, reject) => {
          conn.query(updateQuery, ackCustomers, (err, result) => {
            if (err) return reject(err);
            resolve(result);
          });
        });
  
        console.log("Customer successfully marked as updated.");
      } catch (error) {
        console.error("Error updating Customer:", error);
      }
    } else {
      console.log("No Customer to update.");
    }
  };
startCronJob().catch((error) => {
  console.error("Error starting the cron job:", error);
});
