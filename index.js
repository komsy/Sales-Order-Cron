require("dotenv").config();
// const axios = require("axios");
const { openConnection } = require("./dbConfig");
const  { axios,sendAcknowledgment,sendDataToAPI, sendCustDataToAPI, sendSalesmanDataToAPI,logError } = require("./routes/apiservices");
const delaySpeedA = parseInt(process.env.DELAY_SPEED_A) || 6000;
const delaySpeedB = parseInt(process.env.DELAY_SPEED_B) || 8000;


/*-------------------------START OF FETCH ORDER DATA ----------------------------*/
// Fetch data from the external API
const fetchData = async (conn) => {
  const url = process.env.URL;
  
  try {
    const response = await axios.get(`${url}getData`,  { headers: { "api-key": process.env.APIKEY }});
    const data = response.data;

    if (Array.isArray(data) && data.length > 0) {
      await saveDataToDatabase(conn, data);
    } else {
      // logError("fetchData", new Error("Data from API is not an array"));
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
        CompanyName, DocStatus, DateEntered,  TotalAmount, cashCustomerName, cashPhoneNumber, 
        cashPinNo, cashAddress, PaymentMethod, naration, SmCode, SmName) 
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)`;

  const insertOrderItemQuery = `INSERT INTO ${tableNameOrderItems} (OrderId, LocationId, ItmCode, 
        ItmName, Quantity, Unit, UCPrice, DiscPercent, DiscAmount, ExVat, VatCode, 
        VatRate, VatAmount, Amount, BaseUnit, DateEntered, defaultPricing,SmCode, SmName) 
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)`;

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
              order.DocStatus || "",new Date(),order.TotalAmount || 0,order.cashCustomerName || "",
              order.cashPhoneNumber || "",order.cashPinNo || "",order.cashAddress || "",order.PaymentMethod || "",
              order.naration || "",order.SmCode || "",order.SmName || "",
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
                item.VatAmount,item.Amount,item.BaseUnit,item.DateEntered,item.defaultPricing,item.SmCode,item.SmName,
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

/*-------------------------END OF FETCH ORDER DATA ----------------------------*/




/*-------------------------START OF QUERY, SEND CUSTOMER, PRODUCT AND SALESMAN DATA ----------------------------*/

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

    if (productData.length > 0){
      const response = await sendDataToAPI(productData);
      await processAcknowledgment(response, conn); // Process the response here
    }
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
    CurrBalance, ACCSTATUS,SmCode, SmName FROM ${tableName} WHERE isUpdatedToApp = 0 AND AccType='AC' AND 
    ACCSTATUS='A' AND LocationID = ?`;

    const customers = await new Promise((resolve, reject) => {
      conn.query(customersQuery, [locationID], (err, results) => {
        if (err) reject(err);
        else resolve(results);
      });
    });

    if (customers.length > 0) {
      await sendCustDataToAPI(customers);
      await processCustAcknowledgment(response, conn); // Process the response here
    }
    await querySalesMenFromDatabase(conn);
  } catch (error) {
    logError("queryCustomersFromDatabase", error);
  }
};

const querySalesMenFromDatabase = async (conn) => {
  const tableName = "[dbo].[AppSalesmanMst]";
  const locationID = process.env.LOCATIONID;
  // console.log('salesmen hit')
  try {
    const salesmenQuery = `SELECT SmCode, SmName, LocationID FROM ${tableName} WHERE isUpdatedToApp = 0 AND LocationID = ?`;

    const salesmen = await new Promise((resolve, reject) => {
      conn.query(salesmenQuery, [locationID], (err, results) => {
        if (err) reject(err);
        else resolve(results);
      });
    });
    if (salesmen.length > 0) {
      await sendSalesmanDataToAPI(salesmen);
      await processSalesmanAcknowledgment(response, conn); // Process the response here
    }
  } catch (error) {
    logError("querySalesMenFromDatabase", error);
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

const processSalesmanAcknowledgment = async (response, conn) => {
  if (!response || !response.acknowledgments) {
    logError("processCustAcknowledgment", new Error("Invalid response format"));
    return;
  }

  const ackSalesman = response.acknowledgments.map((ack) => ack.SmCode);
  if (ackSalesman.length > 0) {
    const placeholders = ackSalesman.map(() => "?").join(",");
    const updateQuery = `UPDATE [dbo].[AppSalesmanMst] SET isUpdatedToApp = 1 WHERE SmCode IN (${placeholders})`;
    try {
      await new Promise((resolve, reject) => {
        conn.query(updateQuery, ackSalesman, (err, result) => {
          if (err) reject(err);
          else resolve(result);
        });
      });

      console.log("Salesman successfully marked as updated.");
    } catch (error) {
      logError("processSalesmanAcknowledgment", error);
    }
  } else {
    console.log("No Salesman to update.");
  }
};




// Start the cron job
const startCronJob = async () => {
  try {
    const conn = await openConnection();
    console.log("Connected to SQL Server");

    // Function to manage task scheduling
    const runTask = async (taskName, taskFunction, delay) => {
      while (true) {
        // console.log(`${taskName} CRON endpoint hit`);
        try {
          await taskFunction(conn);
        } catch (err) {
          logError(taskName, err);
        }
        await new Promise(resolve => setTimeout(resolve, delay));
      }
    };

    // Schedule both tasks without blocking
    runTask("Fetch Data Online", fetchData, delaySpeedA);
    runTask("Send Data Online", queryFromDatabase, delaySpeedB);

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

