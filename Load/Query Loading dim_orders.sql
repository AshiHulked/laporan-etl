-- ============================================================
-- TABEL DIM_ORDERS
-- Tujuan: Menyimpan informasi umum pesanan
-- ============================================================

IF OBJECT_ID('loading.dim_orders', 'U') IS NOT NULL
    DROP TABLE loading.dim_orders;
GO

CREATE TABLE loading.dim_orders (
    order_id BIGINT PRIMARY KEY,         -- ID pesanan (PK)
    order_date DATE,                     -- Tanggal pesanan
    shipping_cost DECIMAL(18,2),         -- Biaya pengiriman
    payment_method VARCHAR(50),          -- Metode pembayaran
    order_status VARCHAR(50)             -- Status pesanan
);
GO

-- ============================================================
-- LOAD TABEL DIM_ORDERS dari transform.crm_orders
-- ============================================================

-- ============================================================
-- LOAD TABEL DIM_ORDERS dari crm_orders
-- Tujuan:
-- Memasukkan data pesanan dari tabel transform.crm_orders ke dalam tabel
-- dim_orders di schema loading, dengan beberapa penyesuaian kolom.
-- ============================================================

INSERT INTO loading.dim_orders (
    order_id,         -- ID unik untuk setiap pesanan
    order_date,       -- Tanggal pesanan dilakukan
    shipping_cost,    -- Biaya pengiriman (tidak tersedia di sumber, default 0)
    payment_method,   -- Metode pembayaran yang digunakan
    order_status      -- Status pesanan, diambil dari kolom delivery_status
)
SELECT
    order_id,                          -- Diambil langsung dari crm_orders
    order_date,                        -- Tanggal pesanan
    0 AS shipping_cost,                -- Diisi 0 karena data shipping tidak tersedia
    payment_method,                    -- Metode pembayaran (contoh: kartu kredit, COD)
    delivery_status AS order_status    -- Ubah nama kolom agar sesuai konvensi dim_orders
FROM transform.crm_orders;
