const mysql = require('mysql');
const MySQLEvents = require('@rodrigogs/mysql-events');
const fs = require('fs');
const dotenv = require('dotenv');
dotenv.config();

const asyncQuery = (connection, query) => {
  return new Promise((resolve, reject) => {
    connection.query(query, (err, result) => {
      if (err) reject(err);
      resolve(result);
    });
  });
}


const program = async () => {
  const connection = mysql.createConnection({
    host: process.env.DB_HOST,
    port: process.env.DB_PORT,
    user: process.env.DB_USER,
    password: process.env.DB_PASS,
  });

  const instance = new MySQLEvents(connection, {
    startAtEnd: true,
    excludedSchemas: {
      mysql: true,
    },
  });

  await instance.start();

  const dbName = "cebevil"

  instance.addTrigger({
    name: 'TEST',
    expression: '*',
    statement: MySQLEvents.STATEMENTS.ALL,
    onEvent: async (event) => { // You will receive the events here
      if (event.schema !== dbName) {
        return
      }

      let logStr = ''
      // fs.appendFileSync('message.txt', 'data to append');

      const tableKeysInfo = await asyncQuery(connection, `SHOW KEYS FROM ${dbName}.${event.table} WHERE Key_name = 'PRIMARY'`)

      let primaryKeys = []
      if (tableKeysInfo && tableKeysInfo.length > 0) {
        primaryKeys = tableKeysInfo.map(key => key.Column_name)
      }

      logStr += `${event.type} on ${event.schema}.${event.table} at ${new Date(event.timestamp).toLocaleString("pt-BR")}\n`

      if(event.type === "UPDATE") {
        for (let i = 0; i < event.affectedRows.length; i++) {
          const row = event.affectedRows[i];
  
          let primaryKeyValues = {}
          for (let j = 0; j < primaryKeys.length; j++) {
            const key = primaryKeys[j];
            primaryKeyValues[key] = row.before[key]
          }
  
          logStr += `\tRow with keys: ${JSON.stringify(primaryKeyValues)}\n`
          for (let j = 0; j < event.affectedColumns.length; j++) {
            const col = event.affectedColumns[j];
            let before = row.before[col]
            let after = row.after[col]
            logStr += `\tValue of column "${col}" changed from "${before}" to "${after}"\n`
          }
        }

      } else if(event.type === "INSERT") {
        for (let i = 0; i < event.affectedRows.length; i++) {
          const row = event.affectedRows[i];
          let primaryKeyValues = {}
          for (let j = 0; j < primaryKeys.length; j++) {
            const key = primaryKeys[j];
            primaryKeyValues[key] = row.after[key]
          }

          logStr += `\tInserted new row with keys: ${JSON.stringify(primaryKeyValues)}\n`
          // logStr += `\tInserted new row with keys: ${JSON.stringify(row.after)}\n`
        }
      } else if(event.type === "DELETE") {
        for (let i = 0; i < event.affectedRows.length; i++) {
          const row = event.affectedRows[i];
          let primaryKeyValues = {}
          for (let j = 0; j < primaryKeys.length; j++) {
            const key = primaryKeys[j];
            primaryKeyValues[key] = row.before[key]
          }

          logStr += `\tDeleted row with keys: ${JSON.stringify(primaryKeyValues)}\n`
          // logStr += `\tDeleted row with keys: ${JSON.stringify(row.before)}\n`
        }
      }
      logStr += '\n'

      fs.appendFileSync('logs.txt', logStr);
      console.log(logStr);
    },
  });

  instance.on(MySQLEvents.EVENTS.CONNECTION_ERROR, console.error);
  instance.on(MySQLEvents.EVENTS.ZONGJI_ERROR, console.error);
};

program()
  .then(() => console.log('Waiting for database events...'))
  .catch(console.error);