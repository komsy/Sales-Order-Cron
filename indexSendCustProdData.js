require("dotenv").config();
const axios = require("axios");
const sql = require("msnodesqlv8");
const fs = require("fs");

const ENV_VARS = ["DB_SERVER", "DB_DATABASE", "DB_USERNAME", "DB_PASSWORD", "LOCATIONID", "APIKEY", "URL", "DELAY_SPEED"];

// Validate environment variables
for (const varName of ENV_VARS) {
  if (!process.env[varName]) {
    console.error(`Missing required environment variable: ${varName}`);
    process.exit(1);
  }
}
//Local machine 
//  const config ="server=.;Database=PCC_HQ;Trusted_Connection=Yes;Driver={sql Server Native Client 11.0}";

const config = `Server=${process.env.DB_SERVER};Database=${process.env.DB_DATABASE};Uid=${process.env.DB_USERNAME};Pwd=${process.env.DB_PASSWORD};Driver={SQL Server Native Client 11.0};`;
const delaySpeedB = parseInt(process.env.DELAY_SPEED_B) || 8000;

const logError = (context, error) => {
  const logMessage = `${new Date().toISOString()} - Error in ${context}: ${error.message}\n`;
  console.error(logMessage);
  fs.appendFileSync("logs.txt", logMessage);
};

const openConnection = () => {
  return new Promise((resolve, reject) => {
    sql.open(config, (err, conn) => {
      if (err) reject(err);
      else resolve(conn);
    });
  });
};

const queryFromDatabase = async (conn) => {
  const tableName = "[dbo].[AppProductMst]";
  const productPriceTable = "[dbo].[AppProductPackingPrice]";
  const locationID = process.env.LOCATIONID;

  try {
    const productsQuery = `SELECT ItmCode, LocationID, GodownName, LongName, CatCode, CatName, Unit, 
    TaxCode, TaxRate, RspIncVat, WspIncVat, CurrBalance, FixUnitOfSell FROM ${tableName} 
    WHERE ItmActive = 1 AND isUpdatedToApp = 0 AND LocationID = ?`;

    const products = await new Promise((resolve, reject) => {
      conn.query(productsQuery, [locationID], (err, results) => {
        if (err) reject(err);
        else resolve(results);
      });
    });

    const productData = [];
    for (const product of products) {
      const productPriceQuery = `SELECT * FROM ${productPriceTable} WHERE ItmCode = ?`;

      const productPrices = await new Promise((resolve, reject) => {
        conn.query(productPriceQuery, [product.ItmCode], (err, results) => {
          if (err) reject(err);
          else resolve(results);
        });
      });

      productData.push({ ...product, prices: productPrices });
    }

    if (productData.length > 0) await sendDataToAPI(productData, conn);
    else console.log("No product data available to send.");

    await queryCustomersFromDatabase(conn);
  } catch (error) {
    logError("queryFromDatabase", error);
  }
};

const queryCustomersFromDatabase = async (conn) => {
  const tableName = "[dbo].[AppCustomerMst]";
  const locationID = process.env.LOCATIONID;

  try {
    const customersQuery = `SELECT DISTINCT CusCode, AccType, CompanyName, LocationID, Cr_Limit, 
    CurrBalance, ACCSTATUS FROM ${tableName} WHERE isUpdatedToApp = 0 AND AccType='AC' AND 
    ACCSTATUS='A' AND LocationID = ?`;

    const customers = await new Promise((resolve, reject) => {
      conn.query(customersQuery, [locationID], (err, results) => {
        if (err) reject(err);
        else resolve(results);
      });
    });

    if (customers.length > 0) await sendCustDataToAPI(customers, conn);
    else console.log("No Customer data available to send.");
  } catch (error) {
    logError("queryCustomersFromDatabase", error);
  }
};

const sendDataToAPI = async (data, conn) => {
  try {
    const response = await axios.post(`${process.env.URL}productsData`, data, { headers: { "api-key": process.env.APIKEY } });
    await processAcknowledgment(response.data, conn);
  } catch (error) {
    logError("sendDataToAPI", error);
  }
};

const sendCustDataToAPI = async (data, conn) => {
  try {
    const response = await axios.post(`${process.env.URL}customerData`, data, { headers: { "api-key": process.env.APIKEY } });
    await processCustAcknowledgment(response.data, conn);
  } catch (error) {
    logError("sendCustDataToAPI", error);
  }
};

const processAcknowledgment = async (response, conn) => {
  if (!response || !response.acknowledgments) {
    logError("processAcknowledgment", new Error("Invalid response format"));
    return;
  }

  const ackProducts = response.acknowledgments.map((ack) => ack.productId);
  if (ackProducts.length > 0) {
    const placeholders = ackProducts.map(() => "?").join(",");
    const updateQuery = `UPDATE [dbo].[AppProductMst] SET isUpdatedToApp = 1 WHERE ItmCode IN (${placeholders})`;

    try {
      await new Promise((resolve, reject) => {
        conn.query(updateQuery, ackProducts, (err, result) => {
          if (err) reject(err);
          else resolve(result);
        });
      });

      console.log("Products successfully marked as updated.");
    } catch (error) {
      logError("processAcknowledgment", error);
    }
  } else {
    console.log("No products to update.");
  }
};

const processCustAcknowledgment = async (response, conn) => {
  if (!response || !response.acknowledgments) {
    logError("processCustAcknowledgment", new Error("Invalid response format"));
    return;
  }

  const ackCustomers = response.acknowledgments.map((ack) => ack.customerId);
  if (ackCustomers.length > 0) {
    const placeholders = ackCustomers.map(() => "?").join(",");
    const updateQuery = `UPDATE [dbo].[AppCustomerMst] SET isUpdatedToApp = 1 WHERE CusCode IN (${placeholders})`;

    try {
      await new Promise((resolve, reject) => {
        conn.query(updateQuery, ackCustomers, (err, result) => {
          if (err) reject(err);
          else resolve(result);
        });
      });

      console.log("Customers successfully marked as updated.");
    } catch (error) {
      logError("processCustAcknowledgment", error);
    }
  } else {
    console.log("No customers to update.");
  }
};

const startCronJob = async () => {
  try {
    const conn = await openConnection();
    console.log("Connected to SQL Server");

    await queryFromDatabase(conn);

    setInterval(async () => {
      await queryFromDatabase(conn);
    }, delaySpeedB);

    process.on("SIGINT", () => {
      conn.close(() => {
        console.log("Database connection closed.");
        process.exit(0);
      });
    });
  } catch (error) {
    logError("startCronJob", error);
  }
};

startCronJob();
