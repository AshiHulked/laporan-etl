-- ============================================================
-- 1. CEK DATA DUPLIKAT
-- Tujuan: Menemukan kombinasi atribut produk yang muncul lebih dari 1 kali
-- ============================================================

SELECT
    product_id,
    product_name,
    category,
    brand,
    price,
    mrp,
    margin_percentage,
    shelf_life_days,
    min_stock_level,
    max_stock_level,
    COUNT(*) AS jumlah  -- Menghitung jumlah kemunculan kombinasi yang sama
FROM ekstraksi.erp_products
GROUP BY
    product_id,
    product_name,
    category,
    brand,
    price,
    mrp,
    margin_percentage,
    shelf_life_days,
    min_stock_level,
    max_stock_level
HAVING COUNT(*) > 1;  -- Hanya tampilkan kombinasi yang muncul lebih dari 1 kali

-- ============================================================
-- 2. MENGHAPUS DATA DUPLIKAT MENGGUNAKAN CTE
-- Tujuan: Menyisakan hanya satu baris unik dari data duplikat
-- ============================================================

WITH DuplicateRows AS (
    SELECT *,
           ROW_NUMBER() OVER (
               PARTITION BY 
                   product_id,
                   product_name,
                   category,
                   brand,
                   price,
                   mrp,
                   margin_percentage,
                   shelf_life_days,
                   min_stock_level,
                   max_stock_level
               ORDER BY product_id
           ) AS rn
    FROM ekstraksi.erp_products
)

-- Menghapus semua baris duplikat (nomor urut > 1)
DELETE FROM DuplicateRows
WHERE rn > 1;

-- ============================================================
-- 3. CEK DATA YANG MENGANDUNG NILAI NULL
-- Tujuan: Menampilkan baris yang memiliki nilai NULL pada kolom penting
-- ============================================================

SELECT * 
FROM ekstraksi.erp_products
WHERE 
    product_id IS NULL OR
    product_name IS NULL OR
    category IS NULL OR
    brand IS NULL OR
    price IS NULL OR
    mrp IS NULL OR
    margin_percentage IS NULL OR
    shelf_life_days IS NULL OR
    min_stock_level IS NULL OR
    max_stock_level IS NULL;

-- ============================================================
-- 4. UPDATE NILAI NULL MENJADI DEFAULT
-- Tujuan: Mengganti semua nilai NULL dengan nilai standar
-- ============================================================

UPDATE ekstraksi.erp_products SET product_id = 0 WHERE product_id IS NULL;
UPDATE ekstraksi.erp_products SET product_name = 'UNKNOWN' WHERE product_name IS NULL;
UPDATE ekstraksi.erp_products SET category = 'UNKNOWN' WHERE category IS NULL;
UPDATE ekstraksi.erp_products SET brand = 'UNKNOWN' WHERE brand IS NULL;
UPDATE ekstraksi.erp_products SET price = 0 WHERE price IS NULL;
UPDATE ekstraksi.erp_products SET mrp = 0 WHERE mrp IS NULL;
UPDATE ekstraksi.erp_products SET margin_percentage = 0 WHERE margin_percentage IS NULL;
UPDATE ekstraksi.erp_products SET shelf_life_days = 0 WHERE shelf_life_days IS NULL;
UPDATE ekstraksi.erp_products SET min_stock_level = 0 WHERE min_stock_level IS NULL;
UPDATE ekstraksi.erp_products SET max_stock_level = 0 WHERE max_stock_level IS NULL;

-- ============================================================
-- 5. PROSEDUR TRANSFORMASI DATA
-- Tujuan: Membersihkan dan memindahkan data ke tabel transform
-- ============================================================

CREATE OR ALTER PROCEDURE transform.transform_data AS
BEGIN
    -- Deklarasi variabel waktu
    DECLARE @start_time DATETIME, @end_time DATETIME,
            @batch_start_time DATETIME, @batch_end_time DATETIME;

    BEGIN TRY
        SET @batch_start_time = GETDATE();
        PRINT '=====================================';
        PRINT 'Memulai Proses Transform ERP_PRODUCTS';
        PRINT '=====================================';

        -- Awal proses
        SET @start_time = GETDATE();
        PRINT 'Truncate Table: transform.erp_products';
        TRUNCATE TABLE transform.erp_products;

        PRINT 'Insert Data: transform.erp_products';

        -- Memasukkan data yang sudah dibersihkan dan bebas duplikat
        INSERT INTO transform.erp_products (
            product_id,
            product_name,
            category,
            brand,
            price,
            mrp,
            margin_percentage,
            shelf_life_days,
            min_stock_level,
            max_stock_level
        )
        SELECT 
            COALESCE(product_id, 0),
            COALESCE(product_name, 'UNKNOWN'),
            COALESCE(category, 'UNKNOWN'),
            COALESCE(brand, 'UNKNOWN'),
            COALESCE(price, 0),
            COALESCE(mrp, 0),
            COALESCE(margin_percentage, 0),
            COALESCE(shelf_life_days, 0),
            COALESCE(min_stock_level, 0),
            COALESCE(max_stock_level, 0)
        FROM (
            SELECT *,
                   ROW_NUMBER() OVER (
                       PARTITION BY 
                           product_id,
                           product_name,
                           category,
                           brand,
                           price,
                           mrp,
                           margin_percentage,
                           shelf_life_days,
                           min_stock_level,
                           max_stock_level
                       ORDER BY product_id
                   ) AS rn
            FROM ekstraksi.erp_products
        ) AS cleaned
        WHERE rn = 1;

        -- Selesai proses
        SET @end_time = GETDATE();
        PRINT 'Durasi Upload: ' + CAST(DATEDIFF(SECOND, @start_time, @end_time) AS VARCHAR) + ' detik';
        PRINT '-------------------------------------';

    END TRY

    -- Penanganan kesalahan
    BEGIN CATCH
        PRINT '======================================';
        PRINT 'Pesan Error = ' + ERROR_MESSAGE();
        PRINT 'Kode Error  = ' + CAST(ERROR_NUMBER() AS VARCHAR);
        PRINT 'State       = ' + CAST(ERROR_STATE() AS VARCHAR);
        PRINT '======================================';
    END CATCH
END;

-- Jalankan prosedur transformasi
EXEC transform.transform_data;

-- Menampilkan isi tabel hasil transformasi
SELECT * FROM transform.erp_products;
