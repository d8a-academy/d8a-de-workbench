#!/bin/bash
# d8a-de-workbench: load the app context's source system (the shop's database)
# from the CSVs fetched by `make data CONTEXT=app`. Runs once, when the postgres
# volume is created. No files: the schema stays empty and `make check` says so.
set -e
DATA=/data/app
[ -f "$DATA/orders.csv" ] || { echo "02_load_app: no $DATA/orders.csv, app schema left empty (run make data CONTEXT=app, then make reset && make up)"; exit 0; }
psql -v ON_ERROR_STOP=1 -U "$POSTGRES_USER" -d "$POSTGRES_DB" <<'SQL'
CREATE TABLE app.users (id INTEGER PRIMARY KEY, name TEXT, country TEXT, segment TEXT, signup_date DATE, email TEXT, phone TEXT);
-- orders is loaded as the app stores it: duplicates and mixed-case statuses included. No primary key on purpose.
CREATE TABLE app.orders (id INTEGER, customer_id INTEGER, order_date DATE, status TEXT, total_amount NUMERIC(12,2));
CREATE TABLE app.products (id INTEGER PRIMARY KEY, name TEXT, category TEXT, price NUMERIC(12,2), cost NUMERIC(12,2));
CREATE TABLE app.order_items (id INTEGER PRIMARY KEY, order_id INTEGER, product_id INTEGER, quantity INTEGER, unit_price NUMERIC(12,2));
\copy app.users FROM '/data/app/customers.csv' WITH (FORMAT csv, HEADER true, NULL '')
\copy app.orders FROM '/data/app/orders.csv' WITH (FORMAT csv, HEADER true, NULL '')
\copy app.products FROM '/data/app/products.csv' WITH (FORMAT csv, HEADER true, NULL '')
\copy app.order_items FROM '/data/app/order_items.csv' WITH (FORMAT csv, HEADER true, NULL '')
ANALYZE app.users; ANALYZE app.orders; ANALYZE app.products; ANALYZE app.order_items;
SQL
echo "02_load_app: app.users, app.orders, app.products, app.order_items loaded"
