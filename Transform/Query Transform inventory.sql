-- ============================================================
-- 1. CEK DATA DUPLIKAT
-- Tujuan: Menemukan kombinasi data inventory yang muncul lebih dari 1 kali
-- ============================================================

SELECT
    product_id,
    transaction_date,
    stock_received,
    damaged_stock,
    COUNT(*) AS jumlah  -- Menghitung jumlah kemunculan kombinasi yang sama
FROM ekstraksi.erp_inventory
GROUP BY
    product_id,
    transaction_date,
    stock_received,
    damaged_stock
HAVING COUNT(*) > 1;  -- Hanya tampilkan kombinasi yang muncul lebih dari 1 kali

-- ============================================================
-- 2. MENGHAPUS DATA DUPLIKAT MENGGUNAKAN CTE
-- Tujuan: Menyisakan hanya satu baris unik dari data duplikat
-- ============================================================

WITH DuplicateRows AS (
    SELECT *,
           ROW_NUMBER() OVER (
               PARTITION BY product_id, transaction_date, stock_received, damaged_stock
               ORDER BY product_id  -- Bisa diganti sesuai kebutuhan
           ) AS rn
    FROM ekstraksi.erp_inventory
)
-- Menghapus semua baris duplikat (nomor urut > 1)
DELETE FROM DuplicateRows
WHERE rn > 1;

-- ============================================================
-- 3. CEK DATA YANG MENGANDUNG NILAI NULL
-- Tujuan: Menampilkan baris dengan nilai kosong pada kolom penting
-- ============================================================

SELECT * 
FROM ekstraksi.erp_inventory
WHERE 
    product_id IS NULL OR
    transaction_date IS NULL OR
    stock_received IS NULL OR
    damaged_stock IS NULL;

-- ============================================================
-- 4. UPDATE NILAI NULL MENJADI DEFAULT
-- Tujuan: Mengganti nilai NULL dengan nilai standar agar data valid
-- ============================================================

-- Ganti NULL di kolom product_id
UPDATE ekstraksi.erp_inventory
SET product_id = 0
WHERE product_id IS NULL;

-- Ganti NULL di kolom transaction_date
UPDATE ekstraksi.erp_inventory
SET transaction_date = '1900-01-01'
WHERE transaction_date IS NULL;

-- Ganti NULL di kolom stock_received
UPDATE ekstraksi.erp_inventory
SET stock_received = 0
WHERE stock_received IS NULL;

-- Ganti NULL di kolom damaged_stock
UPDATE ekstraksi.erp_inventory
SET damaged_stock = 0
WHERE damaged_stock IS NULL;

-- ============================================================
-- 5. PROSEDUR TRANSFORMASI DATA
-- Tujuan: Memindahkan data hasil bersih ke tabel transform
-- ============================================================

CREATE OR ALTER PROCEDURE transform.transform_data AS
BEGIN
    -- Deklarasi variabel waktu
    DECLARE @start_time DATETIME, @end_time DATETIME,
            @batch_start_time DATETIME, @batch_end_time DATETIME;

    BEGIN TRY
        SET @batch_start_time = GETDATE();
        PRINT '=====================================';
        PRINT 'Memulai Proses Transform ERP_INVENTORY';
        PRINT '=====================================';

        -- Awal proses transformasi
        SET @start_time = GETDATE();
        PRINT 'Truncate Table: transform.erp_inventory';
        TRUNCATE TABLE transform.erp_inventory;

        PRINT 'Insert Data: transform.erp_inventory';

        -- Memasukkan data hasil pembersihan dan deduplikasi
        INSERT INTO transform.erp_inventory (
            product_id,
            transaction_date,
            stock_received,
            damaged_stock
        )
        SELECT 
            COALESCE(product_id, -1),                         -- Ganti NULL dengan -1
            COALESCE(transaction_date, '1900-01-01'),         -- Ganti NULL dengan tanggal default
            COALESCE(stock_received, 0),                      -- Ganti NULL dengan 0
            COALESCE(damaged_stock, 0)                        -- Ganti NULL dengan 0
        FROM (
            SELECT *,
                   ROW_NUMBER() OVER (
                       PARTITION BY product_id, transaction_date, stock_received, damaged_stock
                       ORDER BY product_id
                   ) AS rn
            FROM ekstraksi.erp_inventory
        ) AS cleaned
        WHERE rn = 1;  -- Hanya ambil baris unik

        -- Catat waktu selesai
        SET @end_time = GETDATE();
        PRINT 'Durasi Upload: ' + CAST(DATEDIFF(SECOND, @start_time, @end_time) AS VARCHAR) + ' detik';
        PRINT '-------------------------------------';

    END TRY

    -- Penanganan jika terjadi kesalahan saat proses
    BEGIN CATCH
        PRINT '======================================';
        PRINT 'Pesan Error = ' + ERROR_MESSAGE();
        PRINT 'Kode Error  = ' + CAST(ERROR_NUMBER() AS VARCHAR);
        PRINT 'State       = ' + CAST(ERROR_STATE() AS VARCHAR);
        PRINT '======================================';
    END CATCH
END;

-- Menjalankan prosedur transformasi
EXEC transform.transform_data;

-- Melihat hasil akhir data setelah transformasi
SELECT * FROM transform.erp_inventory;
