require("dotenv").config();
const axios = require("axios");
const sql = require("msnodesqlv8");
const fs = require("fs");

// Required environment variables
const ENV_VARS = [
  "DB_SERVER",
  "DB_DATABASE",
  "DB_USERNAME",
  "DB_PASSWORD",
  "LOCATIONID",
  "APIKEY",
  "URL",
  "DELAY_SPEED",
];

// Validate environment variables
for (const varName of ENV_VARS) {
  if (!process.env[varName]) {
    console.error(`Missing required environment variable: ${varName}`);
    process.exit(1);
  }
}

const config = `Server=${process.env.DB_SERVER};Database=${process.env.DB_DATABASE};Uid=${process.env.DB_USERNAME};Pwd=${process.env.DB_PASSWORD};Driver={SQL Server Native Client 11.0};`;
const delaySpeed = parseInt(process.env.DELAY_SPEED) || 5000;

// Log error utility
const logError = (context, error) => {
  const logMessage = `${new Date().toISOString()} - Error in ${context}: ${error.message}\n`;
  console.error(logMessage);
  fs.appendFileSync("logs.txt", logMessage);
};

// Open a database connection
const openConnection = () => {
  return new Promise((resolve, reject) => {
    sql.open(config, (err, conn) => {
      if (err) reject(err);
      else resolve(conn);
    });
  });
};

// Fetch data from the external API
const fetchData = async (conn) => {
  const url = process.env.URL;
  
  try {
    const response = await axios.get(`${url}getData`,  { headers: { "api-key": process.env.APIKEY }});
    const data = response.data;

    if (Array.isArray(data)) {
      await saveDataToDatabase(conn, data);
    } else {
      logError("fetchData", new Error("Data from API is not an array"));
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

// Save data to the database
const saveDataToDatabase = async (conn, data) => {
  const tableNameOrders = "[dbo].[AppOrderMst]";
  const tableNameOrderItems = "[dbo].[AppOrderTrn]";

  const insertOrderQuery = `INSERT INTO ${tableNameOrders} (OrderId, LocationID, OrderDate, CusSupCode,
    CompanyName, DocStatus, DateEntered, UserId, TotalAmount, cashCustomerName, cashPhoneNumber, 
    cashPinNo, cashAddress, PaymentMethod, naration, created_at, updated_at) 
    VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)`;

  const insertOrderItemQuery = `INSERT INTO ${tableNameOrderItems} (OrderId, LocationId, ItmCode, ItmName, Quantity, Unit, UCPrice, DiscPercent, DiscAmount, ExVat, VatCode, VatRate, VatAmount, Amount, BaseUnit, DateEntered, UserId, defaultPricing, deleted_at, created_at, updated_at) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)`;

  const dataToInsert = [];

  for (const order of data) {
    dataToInsert.push(order);
    if (order.OrderId) {
      const existingDataQuery = `SELECT * FROM ${tableNameOrders} WHERE OrderId = ?`;
      const existingDataParams = [order.OrderId];

      try {
        const existingDataResult = await new Promise((resolve, reject) => {
          conn.query(existingDataQuery, existingDataParams, (err, results) => {
            if (err) reject(err);
            else resolve(results);
          });
        });

        if (existingDataResult && existingDataResult.length > 0) {
          logError(`saveDataToDatabase`, new Error(`Data with OrderId '${order.OrderId}' already exists. Skipping insertion.`));
          continue;
        }
      } catch (error) {
        logError("saveDataToDatabase - check existing data", error);
        continue;
      }

      try {
        const sqlDate = formatDateForSQL(new Date(order.OrderDate));

        await new Promise((resolve, reject) => {
          conn.query(
            insertOrderQuery,
            [ order.OrderId,order.LocationID, sqlDate,order.CusSupCode || "",order.CompanyName || "",
              order.DocStatus || "",new Date(),order.UserId || "",order.TotalAmount || 0,order.cashCustomerName || "",
              order.cashPhoneNumber || "",order.cashPinNo || "",order.cashAddress || "",order.PaymentMethod || "",
              order.naration || "",new Date(),new Date(),
            ],
            (err) => (err ? reject(err) : resolve())
          );
        });

        for (const item of order.ordertrn || []) {
          await new Promise((resolve, reject) => {
            conn.query(
              insertOrderItemQuery,
              [
                item.OrderId,item.LocationID,item.ItmCode,item.ItmName,item.Quantity,item.Unit,
                item.UCPrice,item.DiscPercent,item.DiscAmount,item.ExVat,item.VatCode,item.VatRate,
                item.VatAmount,item.Amount,item.BaseUnit,item.DateEntered,item.UserId,item.defaultPricing,
                null,new Date(),new Date(),
              ],
              (err) => (err ? reject(err) : resolve())
            );
          });
        }

        console.log("Data inserted successfully");
      } catch (error) {
        logError("saveDataToDatabase - save orders", error);
      }
    }
  }

  sendAcknowledgment(dataToInsert, true);
};

// Send acknowledgment to the API
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

// Format date for SQL
const formatDateForSQL = (date) => {
  const year = date.getFullYear();
  const month = String(date.getMonth() + 1).padStart(2, "0");
  const day = String(date.getDate()).padStart(2, "0");
  const hours = String(date.getHours()).padStart(2, "0");
  const minutes = String(date.getMinutes()).padStart(2, "0");
  const seconds = String(date.getSeconds()).padStart(2, "0");

  return `${year}-${month}-${day} ${hours}:${minutes}:${seconds}`;
};

// Start the cron job
const startCronJob = async () => {
  try {
    const conn = await openConnection();
    console.log("Connected to SQL Server");

    await fetchData(conn);

    setInterval(async () => {
      await fetchData(conn);
    }, delaySpeed);

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
