require("dotenv").config();
const express = require("express");
const http = require("http");
const path = require("path");
const fs = require("fs");
const { openConnection } = require("./dbConfig");
const app = express();
const server = http.createServer(app);
const { Server } = require("socket.io");
const io = new Server(server, { cors: { origin: "*" }});
const delaySpeed = parseInt(process.env.DELAY_SPEED) || 3000;
const url = process.env.URL || "http://localhost";
const locationID = process.env.LOCATIONID || "00";
const PORT = process.env.PORT || 3000;

app.use(express.json());

// Serve images from that absolute path
const imagesPath = "C:\\Easy POS System\\TillTvImages"; // or use \\ for Windows paths
app.use('/images', express.static(imagesPath));
 
// Serve static files from the "public" directory
app.use(express.static(path.join(__dirname, "public")));

// Logs helper
const logError = (context, error) => {
  const logMessage = `${new Date().toISOString()} - Error in ${context}: ${error.message}\n`;
  fs.appendFileSync("logs.txt", logMessage);
};

/* ---------------------------------------------------
   QUERY DATABASE 
--------------------------------------------------- */

const queryFromDatabase = async (conn) => {
  const tableName = "[dbo].[vwProductMst]";
  const priceTable = "[dbo].[ProductPackingPrice]";
  const imageTable = "[dbo].[TillTvImages]";

  try {
    const sql = `SELECT VP.ItmCode, VP.LocationID, GodownName, LongName, Unit, 
    TaxCode, TaxRate, RspIncVat, WspIncVat, CurrBalance, FixUnitOfSell,ItmActive,TV.ImgPath,TV.TillNo  FROM ${tableName} VP
     join ${imageTable} TV ON TV.ItmCode=VP.itmcode WHERE VP.LocationID = ?`;
    //  AND VP.dateEntered >= DATEADD(MINUTE, -2, GETDATE())
    const products = await new Promise((resolve, reject) => {
      conn.query(sql, [locationID], (err, results) => {
        if (err) reject(err);
        else resolve(results);
      });
    });
 
    const productData = [];
    // console.log("Products fetched:", products.length);
    for (const product of products) {
      // console.log("Fetching prices for ItmCode:", product.ItmCode);
      const priceQuery = `SELECT * FROM ${priceTable} WHERE ItmCode = ?`;

      const prices = await new Promise((resolve, reject) => {
        conn.query(priceQuery, [product.ItmCode], (err, priceResults) => {
          if (err) reject(err);
          else resolve(priceResults);
        });
      });
      // console.log("Prices fetched:", prices.length);
      const winPath = product.ImgPath;
      const localImg = winPath.split("\\").pop(); // extract filename

      productData.push({
        ...product,
        prices,
        ImgFile: localImg,
      });
    }
    
    // console.log("Product data retrieved successfully:", productData);
    return productData;

  } catch (error) {
    logError("queryFromDatabase", error);
    return [];
  }
};

/* ---------------------------------------------------
   GROUP BY TILL NO
--------------------------------------------------- */
const groupByTill = (data) => {
  const result = {};
  data.forEach((p) => {
    // console.log("Processing product for TillNo:", p.TillNo);
    const till = p.TillNo || "00";
    if (!result[till]) result[till] = [];
    result[till].push(p);
  });
  return result;
};


/* ---------------------------------------------------
   CRON - RUN EVERY 2 SECONDS
--------------------------------------------------- */
let lastData = ""; // This variable holds the latest grouped product data (serialized string)
let currentGroupedData = {}; // Store the actual object data too

const startCronJob = async () => {
  try {
    const conn = await openConnection();
    console.log("Connected to SQL Server");

    const runTask = async () => {
      try {
        const products = await queryFromDatabase(conn);
        // console.log("Products fetched for cron task:", products.length);
        const grouped = groupByTill(products);
        currentGroupedData = grouped; // Store the object

        const serialized = JSON.stringify(grouped);

        if (serialized !== lastData) {
          lastData = serialized;
          // Use io.emit() here to broadcast ONLY when it changes
          io.emit("updateProducts", grouped); 
          // console.log("New products broadcasted to display screen.");
        }
        // console.log("Emitting updateProducts =>", grouped);
      } catch (error) {
        logError("CronTask", error);
      }

      // Repeat every 2 seconds
      setTimeout(runTask, delaySpeed);
    };

    // Start the cron loop
    runTask(); 


    // --- Add a connection listener ---
    io.on('connection', (socket) => {
        // console.log(`A user connected: ${socket.id}`);
        // When a new client connects, immediately send the last known data state.
        if (currentGroupedData && Object.keys(currentGroupedData).length > 0) {
            socket.emit("updateProducts", currentGroupedData);
            // console.log(`Sent initial data to new connection: ${socket.id}`);
        }
    });
    // ----------------------------------------

   // Clean shutdown
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

/* ---------------------------------------------------
   ROUTES
--------------------------------------------------- */

// Display screen page
app.get("/display", (req, res) => {
  res.sendFile(path.join(__dirname, "public", "display.html"));
});

// Start Server
server.listen(PORT, () => {
  // console.log("Server running at " + url + ":" + PORT);
  console.log("Open the display page at " + url + ":" + PORT + "/display");
});
