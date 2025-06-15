-- ============================================================
-- TABEL DIM_PRODUCTS
-- Tujuan: Menyimpan data atribut produk
-- ============================================================

IF OBJECT_ID('loading.dim_products', 'U') IS NOT NULL
    DROP TABLE loading.dim_products;
GO

CREATE TABLE loading.dim_products (
    product_id INT PRIMARY KEY,          -- ID produk (PK)
    product_name VARCHAR(100),           -- Nama produk
    category VARCHAR(50),                -- Kategori produk
    brand VARCHAR(100),                  -- Merek
    stock_received INT,                  -- Jumlah stok diterima
    damaged_stock INT                    -- Jumlah stok rusak
);
GO

-- ============================================================
-- LOAD TABEL DIM_PRODUCTS dari erp_products + agregasi dari erp_inventory
-- Tujuan:
-- Memasukkan data produk ke dalam tabel dim_products pada schema loading,
-- serta menggabungkan informasi stok masuk dan stok rusak dari tabel inventori.
-- ============================================================

INSERT INTO loading.dim_products (
    product_id,       -- ID unik produk
    product_name,     -- Nama produk
    category,         -- Kategori produk (misal: Elektronik, Pakaian)
    brand,            -- Merek produk
    stock_received,   -- Total stok produk yang diterima
    damaged_stock     -- Total stok yang rusak
)
SELECT
    p.product_id,           -- Diambil dari tabel produk
    p.product_name,
    p.category,
    p.brand,
    ISNULL(SUM(i.stock_received), 0) AS stock_received,
    -- Agregasi stok diterima per produk, default 0 jika tidak ada data inventori

    ISNULL(SUM(i.damaged_stock), 0) AS damaged_stock
    -- Agregasi stok rusak per produk, default 0 jika tidak ada data inventori

FROM transform.erp_products p
LEFT JOIN transform.erp_inventory i
    ON p.product_id = i.product_id
    -- Gunakan LEFT JOIN agar semua produk tetap masuk meski belum ada data inventori

GROUP BY
    p.product_id,
    p.product_name,
    p.category,
    p.brand;
    -- GROUP BY diperlukan karena agregasi (SUM) dilakukan atas inventori
