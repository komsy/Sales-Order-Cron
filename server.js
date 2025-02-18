require('dotenv').config();
const http = require('http');
const port = process.env.PORT;
const server = http.createServer(app);

server.listen(port, ()=>{
    console.log(`Application running on port ${port}`)
})
