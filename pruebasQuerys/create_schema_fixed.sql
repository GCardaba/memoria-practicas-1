-- Eliminar tablas existentes
DROP TABLE IF EXISTS lineitem CASCADE;
DROP TABLE IF EXISTS orders CASCADE;
DROP TABLE IF EXISTS partsupp CASCADE;
DROP TABLE IF EXISTS customer CASCADE;
DROP TABLE IF EXISTS supplier CASCADE;
DROP TABLE IF EXISTS part CASCADE;
DROP TABLE IF EXISTS nation CASCADE;
DROP TABLE IF EXISTS region CASCADE;

-- Region (con columna extra para el trailing pipe)
CREATE TABLE region_raw (
r_regionkey  INTEGER,
r_name       CHAR(25),
r_comment    VARCHAR(152),
skip         VARCHAR(1)  -- Para el trailing |
);

CREATE TABLE region (
r_regionkey  INTEGER NOT NULL PRIMARY KEY,
r_name       CHAR(25) NOT NULL,
r_comment    VARCHAR(152)
);

-- Nation
CREATE TABLE nation_raw (
n_nationkey  INTEGER,
n_name       CHAR(25),
n_regionkey  INTEGER,
n_comment    VARCHAR(152),
skip         VARCHAR(1)
);

CREATE TABLE nation (
n_nationkey  INTEGER NOT NULL PRIMARY KEY,
n_name       CHAR(25) NOT NULL,
n_regionkey  INTEGER NOT NULL,
n_comment    VARCHAR(152),
FOREIGN KEY (n_regionkey) REFERENCES region(r_regionkey)
);

-- Supplier
CREATE TABLE supplier_raw (
s_suppkey     INTEGER,
s_name        CHAR(25),
s_address     VARCHAR(40),
s_nationkey   INTEGER,
s_phone       CHAR(15),
s_acctbal     DECIMAL(15,2),
s_comment     VARCHAR(101),
skip          VARCHAR(1)
);

CREATE TABLE supplier (
s_suppkey     INTEGER NOT NULL PRIMARY KEY,
s_name        CHAR(25) NOT NULL,
s_address     VARCHAR(40) NOT NULL,
s_nationkey   INTEGER NOT NULL,
s_phone       CHAR(15) NOT NULL,
s_acctbal     DECIMAL(15,2) NOT NULL,
s_comment     VARCHAR(101) NOT NULL,
FOREIGN KEY (s_nationkey) REFERENCES nation(n_nationkey)
);

-- Customer
CREATE TABLE customer_raw (
c_custkey     INTEGER,
c_name        VARCHAR(25),
c_address     VARCHAR(40),
c_nationkey   INTEGER,
c_phone       CHAR(15),
c_acctbal     DECIMAL(15,2),
c_mktsegment  CHAR(10),
c_comment     VARCHAR(117),
skip          VARCHAR(1)
);

CREATE TABLE customer (
c_custkey     INTEGER NOT NULL PRIMARY KEY,
c_name        VARCHAR(25) NOT NULL,
c_address     VARCHAR(40) NOT NULL,
c_nationkey   INTEGER NOT NULL,
c_phone       CHAR(15) NOT NULL,
c_acctbal     DECIMAL(15,2) NOT NULL,
c_mktsegment  CHAR(10) NOT NULL,
c_comment     VARCHAR(117) NOT NULL,
FOREIGN KEY (c_nationkey) REFERENCES nation(n_nationkey)
);

-- Part
CREATE TABLE part_raw (
p_partkey     INTEGER,
p_name        VARCHAR(55),
p_mfgr        CHAR(25),
p_brand       CHAR(10),
p_type        VARCHAR(25),
p_size        INTEGER,
p_container   CHAR(10),
p_retailprice DECIMAL(15,2),
p_comment     VARCHAR(23),
skip          VARCHAR(1)
);

CREATE TABLE part (
p_partkey     INTEGER NOT NULL PRIMARY KEY,
p_name        VARCHAR(55) NOT NULL,
p_mfgr        CHAR(25) NOT NULL,
p_brand       CHAR(10) NOT NULL,
p_type        VARCHAR(25) NOT NULL,
p_size        INTEGER NOT NULL,
p_container   CHAR(10) NOT NULL,
p_retailprice DECIMAL(15,2) NOT NULL,
p_comment     VARCHAR(23) NOT NULL
);

-- Partsupp
CREATE TABLE partsupp_raw (
ps_partkey     INTEGER,
ps_suppkey     INTEGER,
ps_availqty    INTEGER,
ps_supplycost  DECIMAL(15,2),
ps_comment     VARCHAR(199),
skip           VARCHAR(1)
);

CREATE TABLE partsupp (
ps_partkey     INTEGER NOT NULL,
ps_suppkey     INTEGER NOT NULL,
ps_availqty    INTEGER NOT NULL,
ps_supplycost  DECIMAL(15,2) NOT NULL,
ps_comment     VARCHAR(199) NOT NULL,
PRIMARY KEY (ps_partkey, ps_suppkey),
FOREIGN KEY (ps_partkey) REFERENCES part(p_partkey),
FOREIGN KEY (ps_suppkey) REFERENCES supplier(s_suppkey)
);

-- Orders
CREATE TABLE orders_raw (
o_orderkey       INTEGER,
o_custkey        INTEGER,
o_orderstatus    CHAR(1),
o_totalprice     DECIMAL(15,2),
o_orderdate      DATE,
o_orderpriority  CHAR(15),
o_clerk          CHAR(15),
o_shippriority   INTEGER,
o_comment        VARCHAR(79),
skip             VARCHAR(1)
);

CREATE TABLE orders (
o_orderkey       INTEGER NOT NULL PRIMARY KEY,
o_custkey        INTEGER NOT NULL,
o_orderstatus    CHAR(1) NOT NULL,
o_totalprice     DECIMAL(15,2) NOT NULL,
o_orderdate      DATE NOT NULL,
o_orderpriority  CHAR(15) NOT NULL,
o_clerk          CHAR(15) NOT NULL,
o_shippriority   INTEGER NOT NULL,
o_comment        VARCHAR(79) NOT NULL,
FOREIGN KEY (o_custkey) REFERENCES customer(c_custkey)
);

-- Lineitem
CREATE TABLE lineitem_raw (
l_orderkey       INTEGER,
l_partkey        INTEGER,
l_suppkey        INTEGER,
l_linenumber     INTEGER,
l_quantity       DECIMAL(15,2),
l_extendedprice  DECIMAL(15,2),
l_discount       DECIMAL(15,2),
l_tax            DECIMAL(15,2),
l_returnflag     CHAR(1),
l_linestatus     CHAR(1),
l_shipdate       DATE,
l_commitdate     DATE,
l_receiptdate    DATE,
l_shipinstruct   CHAR(25),
l_shipmode       CHAR(10),
l_comment        VARCHAR(44),
skip             VARCHAR(1)
);

CREATE TABLE lineitem (
l_orderkey       INTEGER NOT NULL,
l_partkey        INTEGER NOT NULL,
l_suppkey        INTEGER NOT NULL,
l_linenumber     INTEGER NOT NULL,
l_quantity       DECIMAL(15,2) NOT NULL,
l_extendedprice  DECIMAL(15,2) NOT NULL,
l_discount       DECIMAL(15,2) NOT NULL,
l_tax            DECIMAL(15,2) NOT NULL,
l_returnflag     CHAR(1) NOT NULL,
l_linestatus     CHAR(1) NOT NULL,
l_shipdate       DATE NOT NULL,
l_commitdate     DATE NOT NULL,
l_receiptdate    DATE NOT NULL,
l_shipinstruct   CHAR(25) NOT NULL,
l_shipmode       CHAR(10) NOT NULL,
l_comment        VARCHAR(44) NOT NULL,
PRIMARY KEY (l_orderkey, l_linenumber),
FOREIGN KEY (l_orderkey) REFERENCES orders(o_orderkey),
FOREIGN KEY (l_partkey) REFERENCES part(p_partkey),
FOREIGN KEY (l_suppkey) REFERENCES supplier(s_suppkey)
);

-- Indices
CREATE INDEX idx_customer_nationkey ON customer(c_nationkey);
CREATE INDEX idx_orders_custkey ON orders(o_custkey);
CREATE INDEX idx_orders_orderdate ON orders(o_orderdate);
CREATE INDEX idx_lineitem_orderkey ON lineitem(l_orderkey);
CREATE INDEX idx_lineitem_partkey ON lineitem(l_partkey);
CREATE INDEX idx_lineitem_suppkey ON lineitem(l_suppkey);
CREATE INDEX idx_lineitem_shipdate ON lineitem(l_shipdate);
CREATE INDEX idx_partsupp_partkey ON partsupp(ps_partkey);
CREATE INDEX idx_partsupp_suppkey ON partsupp(ps_suppkey);
CREATE INDEX idx_supplier_nationkey ON supplier(s_nationkey);
CREATE INDEX idx_nation_regionkey ON nation(n_regionkey);
