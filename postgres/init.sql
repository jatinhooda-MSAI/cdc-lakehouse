CREATE ROLE debezium WITH LOGIN REPLICATION PASSWORD 'debezium';
CREATE ROLE iceberg WITH LOGIN PASSWORD 'iceberg';
CREATE ROLE shop WITH LOGIN PASSWORD 'shop';
CREATE DATABASE iceberg_catalog OWNER iceberg;

CREATE FUNCTION touch_row() RETURNS trigger LANGUAGE plpgsql AS $$
BEGIN
  IF NEW.id IS DISTINCT FROM OLD.id THEN
    NEW.version := 1;
  ELSE
    NEW.version := OLD.version + 1;
  END IF;
  NEW.updated_at := clock_timestamp();
  RETURN NEW;
END $$;

CREATE TABLE customers (
  id bigserial PRIMARY KEY,
  email text NOT NULL UNIQUE,
  full_name text NOT NULL,
  country char(2) NOT NULL,
  created_at timestamptz NOT NULL DEFAULT clock_timestamp(),
  updated_at timestamptz NOT NULL DEFAULT clock_timestamp(),
  version bigint NOT NULL DEFAULT 1
);

CREATE TABLE products (
  id bigserial PRIMARY KEY,
  sku text NOT NULL UNIQUE,
  name text NOT NULL,
  description text,
  price numeric(12,2) NOT NULL,
  stock integer NOT NULL,
  updated_at timestamptz NOT NULL DEFAULT clock_timestamp(),
  version bigint NOT NULL DEFAULT 1
);

CREATE TABLE orders (
  id bigserial PRIMARY KEY,
  customer_id bigint NOT NULL REFERENCES customers(id),
  status text NOT NULL CHECK (status IN ('created', 'paid', 'shipped', 'delivered', 'cancelled')),
  total numeric(12,2) NOT NULL DEFAULT 0,
  created_at timestamptz NOT NULL DEFAULT clock_timestamp(),
  updated_at timestamptz NOT NULL DEFAULT clock_timestamp(),
  version bigint NOT NULL DEFAULT 1
);

CREATE TABLE order_items (
  id bigserial PRIMARY KEY,
  order_id bigint NOT NULL REFERENCES orders(id) ON DELETE CASCADE,
  product_id bigint NOT NULL REFERENCES products(id),
  quantity integer NOT NULL,
  unit_price numeric(12,2) NOT NULL,
  updated_at timestamptz NOT NULL DEFAULT clock_timestamp(),
  version bigint NOT NULL DEFAULT 1
);

CREATE TABLE payments (
  id bigserial PRIMARY KEY,
  order_id bigint NOT NULL REFERENCES orders(id) ON DELETE CASCADE,
  amount numeric(12,2) NOT NULL,
  method text NOT NULL,
  status text NOT NULL,
  created_at timestamptz NOT NULL DEFAULT clock_timestamp(),
  updated_at timestamptz NOT NULL DEFAULT clock_timestamp(),
  version bigint NOT NULL DEFAULT 1
);

CREATE TABLE type_samples (
  id bigserial PRIMARY KEY,
  c_smallint smallint,
  c_int integer,
  c_bigint bigint,
  c_numeric numeric(18,6),
  c_real real,
  c_double double precision,
  c_bool boolean,
  c_text text,
  c_varchar varchar(40),
  c_char char(3),
  c_date date,
  c_time time,
  c_ts timestamp,
  c_tstz timestamptz,
  c_uuid uuid,
  c_jsonb jsonb,
  c_bytea bytea,
  updated_at timestamptz NOT NULL DEFAULT clock_timestamp(),
  version bigint NOT NULL DEFAULT 1
);

ALTER TABLE products REPLICA IDENTITY FULL;

CREATE INDEX ON orders (status);
CREATE INDEX ON order_items (order_id);
CREATE INDEX ON payments (order_id);

CREATE TRIGGER customers_touch BEFORE UPDATE ON customers FOR EACH ROW EXECUTE FUNCTION touch_row();
CREATE TRIGGER products_touch BEFORE UPDATE ON products FOR EACH ROW EXECUTE FUNCTION touch_row();
CREATE TRIGGER orders_touch BEFORE UPDATE ON orders FOR EACH ROW EXECUTE FUNCTION touch_row();
CREATE TRIGGER order_items_touch BEFORE UPDATE ON order_items FOR EACH ROW EXECUTE FUNCTION touch_row();
CREATE TRIGGER payments_touch BEFORE UPDATE ON payments FOR EACH ROW EXECUTE FUNCTION touch_row();
CREATE TRIGGER type_samples_touch BEFORE UPDATE ON type_samples FOR EACH ROW EXECUTE FUNCTION touch_row();

CREATE TABLE latency_probe (
  id bigserial PRIMARY KEY,
  created_at timestamptz NOT NULL DEFAULT clock_timestamp()
);

CREATE TABLE debezium_heartbeat (
  id int PRIMARY KEY,
  ts timestamptz NOT NULL
);

CREATE TABLE debezium_signal (
  id varchar(64) PRIMARY KEY,
  type varchar(32) NOT NULL,
  data varchar(2048)
);

GRANT SELECT ON ALL TABLES IN SCHEMA public TO debezium;
GRANT INSERT, UPDATE ON debezium_heartbeat, debezium_signal TO debezium;
GRANT SELECT, INSERT, UPDATE, DELETE ON ALL TABLES IN SCHEMA public TO shop;
GRANT USAGE ON ALL SEQUENCES IN SCHEMA public TO shop;
REVOKE TRUNCATE ON ALL TABLES IN SCHEMA public FROM PUBLIC;

CREATE PUBLICATION dbz_pub FOR TABLE
  customers, products, orders, order_items, payments, type_samples, latency_probe, debezium_heartbeat, debezium_signal;

ALTER DEFAULT PRIVILEGES IN SCHEMA public GRANT SELECT ON TABLES TO debezium;
ALTER DEFAULT PRIVILEGES IN SCHEMA public GRANT SELECT, INSERT, UPDATE, DELETE ON TABLES TO shop;
ALTER DEFAULT PRIVILEGES IN SCHEMA public GRANT USAGE ON SEQUENCES TO shop;
