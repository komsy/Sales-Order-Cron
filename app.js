const express = require('express');
var cors = require('cors');
const mpesaRoutes = require('./routes/saveTransactions');
const app = express();

app.use(cors());
app.use(express.urlencoded({extended: true}));
app.use(express.json());
app.use('/api',mpesaRoutes);



module.exports = app;