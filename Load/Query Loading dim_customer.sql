-- ============================================================
-- TABEL DIM_CUSTOMER
-- Tujuan: Menyimpan data atribut pelanggan
-- ============================================================

IF OBJECT_ID('loading.dim_customer', 'U') IS NOT NULL
    DROP TABLE loading.dim_customer;
GO

CREATE TABLE loading.dim_customer (
    customer_id INT PRIMARY KEY,         -- ID pelanggan (PK)
    customer_name VARCHAR(100),          -- Nama lengkap
    email VARCHAR(100),                  -- Email
    phone VARCHAR(20),                   -- No. telepon
    address VARCHAR(200),                -- Alamat
    area VARCHAR(100),                   -- Area/wilayah
    pincode VARCHAR(10),                 -- Kode pos
    registration_date DATE,              -- Tanggal registrasi
    customer_segment VARCHAR(50),        -- Segmentasi pelanggan
    total_orders INT,                    -- Total pesanan
    avg_order_value DECIMAL(18,2)        -- Nilai rata-rata pesanan
);
GO

-- ============================================================
-- LOAD TABEL DIM_CUSTOMER dari transform.crm_customer
-- Tujuan:
-- Memindahkan data master pelanggan dari schema transform ke dalam tabel
-- dim_customer pada schema loading sebagai bagian dari proses ETL.
-- ============================================================

INSERT INTO loading.dim_customer (
    customer_id,        -- ID unik pelanggan
    customer_name,      -- Nama lengkap pelanggan
    email,              -- Alamat email pelanggan
    phone,              -- Nomor telepon pelanggan
    address,            -- Alamat lengkap pelanggan
    area,               -- Area geografis/tempat tinggal
    pincode,            -- Kode pos pelanggan
    registration_date,  -- Tanggal pelanggan melakukan registrasi
    customer_segment,   -- Segmen pelanggan (misal: Regular, Premium)
    total_orders,       -- Total jumlah pesanan yang dilakukan pelanggan
    avg_order_value     -- Nilai rata-rata dari semua pesanan pelanggan
)
SELECT
    customer_id,
    customer_name,
    email,
    phone,
    address,
    area,
    pincode,
    registration_date,
    customer_segment,
    total_orders,
    avg_order_value
FROM transform.crm_customer;
