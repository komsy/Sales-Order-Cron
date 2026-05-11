const mssql = require("mssql");

const openConnection = async () => {
  // 1. Get the raw value from environment
  const rawServer = process.env.DB_SERVER || "";

  // 2. Safely split hostname and instance (e.g., MULTIMPESA\SQL2012)
  const parts = rawServer.split("\\");
  const host = parts[0];
  const instance = parts[1] || "";

  // 3. Construct the config object inside the function to ensure ENV variables are loaded
  const dbConfig = {
    user: process.env.DB_USERNAME,
    password: process.env.DB_PASSWORD,
    server: host, // Must be the machine name or IP only
    database: process.env.DB_DATABASE,
    options: {
      instanceName: instance, // Named instance goes here
      trustServerCertificate: true,
      encrypt: false,
    },
  };

  // 4. Proper Validation
  if (!dbConfig.server || typeof dbConfig.server !== "string") {
    throw new Error(`DB_SERVER is invalid. Value: ${dbConfig.server}`);
  }

  try {
    // connect using the config object
    const pool = await mssql.connect(dbConfig);

    return {
      // Inside openConnection return object:
        query: (sqlQuery, params = []) => {
          return new Promise((resolve, reject) => {
            const request = pool.request();
            let modifiedQuery = sqlQuery;

            // Handle 'params' being passed as the callback by mistake
            const actualParams = Array.isArray(params) ? params : [];

            actualParams.forEach((val, index) => {
              const paramName = `p${index}`;
              modifiedQuery = modifiedQuery.replace("?", `@${paramName}`);
              request.input(paramName, val);
            });

            request.query(modifiedQuery, (err, result) => {
              if (err) return reject(err);
              // Resolve with the recordset array
              resolve(result ? result.recordset : []);
            });
          });
        },

      close: async () => await pool.close(),
    };
  } catch (err) {
    console.error("Connection Error:", err.message);
    throw err;
  }
};

module.exports = { openConnection };
