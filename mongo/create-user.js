// Runs once, when the MongoDB data volume is empty (docker-entrypoint-initdb.d).
// The producer's login: read and write on its own database only; collection sync_state holds the send state.
db.getSiblingDB('branch_sales').createUser({
  user: 'branch_sales',
  pwd: process.env.MONGO_PASSWORD,
  roles: [{ role: 'readWrite', db: 'branch_sales' }]
});
