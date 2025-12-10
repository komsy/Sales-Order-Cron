const sql = require("msnodesqlv8");


// Required environment variables
const ENV_VARS = ["DB_SERVER", "DB_DATABASE", "DB_USERNAME", "DB_PASSWORD",
                  "LOCATIONID", "URL", "DELAY_SPEED","PORT"];

// Validate environment variablesSQL Server Native Client 11.0
//ODBC Driver 17 for SQL Server
for (const varName of ENV_VARS) {
  if (!process.env[varName]) {
    console.error(`Missing required environment variable: ${varName}`);
    process.exit(1);
  }
}

const config = `Server=${process.env.DB_SERVER};Database=${process.env.DB_DATABASE};
                Uid=${process.env.DB_USERNAME};Pwd=${process.env.DB_PASSWORD};
                Driver={SQL Server Native Client 11.0};`;


                // Open a database connection
const openConnection = () => {
  return new Promise((resolve, reject) => {
    sql.open(config, (err, conn) => {
      if (err) reject(err);
      else resolve(conn);
    });
  });
};

module.exports = {openConnection };