-- ============================================================
-- TABEL FACT_SALES
-- Tujuan: Menyimpan data transaksi penjualan (fakta)
-- ============================================================

IF OBJECT_ID('loading.fact_sales', 'U') IS NOT NULL
    DROP TABLE loading.fact_sales;
GO

CREATE TABLE loading.fact_sales (
    order_item_id BIGINT PRIMARY KEY,    -- ID detail pesanan (PK)
    order_id BIGINT,                     -- Relasi ke dim_orders
    product_id INT,                      -- Relasi ke dim_products
    customer_id INT,                     -- Relasi ke dim_customer
    quantity INT,                        -- Jumlah unit dibeli
    unit_price DECIMAL(18,2),            -- Harga per unit
    total_price DECIMAL(18,2)            -- Total harga (qty * unit_price)
);
GO

-- ============================================================
-- LOAD TABEL FACT_SALES dari crm_order_items + referensi ke orders dan customer
-- Tujuan:
-- Memuat data penjualan dari tabel transform.crm_order_items dengan melibatkan
-- informasi tambahan dari tabel crm_orders (khususnya customer_id) ke dalam
-- tabel fact_sales di schema loading.
-- ============================================================

INSERT INTO loading.fact_sales (
    order_item_id,   -- Primary key unik untuk setiap item order (dibuat otomatis)
    order_id,        -- ID pesanan dari tabel crm_orders
    product_id,      -- ID produk dari item pesanan
    customer_id,     -- ID pelanggan yang melakukan pesanan (join ke tabel orders)
    quantity,        -- Jumlah produk yang dipesan
    unit_price,      -- Harga satuan produk
    total_price      -- Total harga = quantity * unit_price
)
SELECT
    ROW_NUMBER() OVER (ORDER BY oi.order_id, oi.product_id) AS order_item_id,  
    -- Menghasilkan ID unik berdasarkan urutan order_id dan product_id

    oi.order_id,         -- Referensi ke ID pesanan
    oi.product_id,       -- Referensi ke ID produk
    o.customer_id,       -- Diambil dari tabel crm_orders melalui JOIN
    oi.quantity,         -- Jumlah unit yang dibeli
    oi.unit_price,       -- Harga per unit produk
    oi.quantity * oi.unit_price AS total_price  -- Total nilai penjualan
FROM transform.crm_order_items oi
JOIN transform.crm_orders o 
    ON oi.order_id = o.order_id;
    -- Join untuk mengakses customer_id yang tidak tersedia langsung di order_items


