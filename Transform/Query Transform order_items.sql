-- ============================================================
-- 1. CEK DATA DUPLIKAT
-- Tujuan: Menemukan kombinasi order_id, product_id, quantity, unit_price
--         yang muncul lebih dari 1 kali dalam tabel (duplikat)
-- ============================================================
SELECT 
    order_id,
    product_id,
    quantity,
    unit_price,
    COUNT(*) AS jumlah  -- Menghitung jumlah kemunculan kombinasi yang sama
FROM ekstraksi.crm_order_items
GROUP BY 
    order_id,
    product_id,
    quantity,
    unit_price
HAVING COUNT(*) > 1;     -- Hanya menampilkan yang lebih dari 1 kali (duplikat)

-- ============================================================
-- 2. MENGHAPUS DATA DUPLIKAT MENGGUNAKAN CTE (Common Table Expression)
-- Tujuan: Menyimpan hanya satu baris unik dari data yang duplikat,
--         dan menghapus baris duplikat lainnya berdasarkan nomor urut
-- ============================================================

WITH DuplicateData AS (
    SELECT *,
        ROW_NUMBER() OVER (
            PARTITION BY 
                order_id,
                product_id,
                quantity,
                unit_price
            ORDER BY order_id  -- Bisa diganti dengan kolom waktu jika tersedia
        ) AS flag  -- Memberi nomor urut dalam grup duplikat
    FROM ekstraksi.crm_order_items
)

-- Hapus semua baris yang bukan urutan pertama (berarti duplikat)
DELETE FROM DuplicateData
WHERE flag > 1;

-- ============================================================
-- 3. CEK DATA YANG MENGANDUNG NILAI NULL
-- Tujuan: Menampilkan semua baris yang memiliki nilai kosong (NULL)
--         di salah satu kolom penting
-- ============================================================
SELECT *
FROM ekstraksi.crm_order_items
WHERE 
    order_id IS NULL OR
    product_id IS NULL OR
    quantity IS NULL OR
    unit_price IS NULL;

-- ============================================================
-- 4. UPDATE NILAI NULL MENJADI DEFAULT
-- Tujuan: Mengganti nilai NULL dengan angka default (0)
-- ============================================================

-- Ganti NULL di kolom order_id
UPDATE ekstraksi.crm_order_items
SET order_id = 0
WHERE order_id IS NULL;

-- Ganti NULL di kolom product_id
UPDATE ekstraksi.crm_order_items
SET product_id = 0
WHERE product_id IS NULL;

-- Ganti NULL di kolom quantity
UPDATE ekstraksi.crm_order_items
SET quantity = 0
WHERE quantity IS NULL;

-- Ganti NULL di kolom unit_price
UPDATE ekstraksi.crm_order_items
SET unit_price = 0
WHERE unit_price IS NULL;

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
        PRINT '==========================================';
        PRINT 'Mulai Proses Transform';
        PRINT '==========================================';
        PRINT '';
        PRINT 'Transform Tabel ORDER_ITEMS';
        PRINT '------------------------------------------';

        -- Proses awal untuk tabel
        SET @start_time = GETDATE();
        PRINT 'Truncate Table: transform.crm_order_items';
        TRUNCATE TABLE transform.crm_order_items;

        PRINT 'Insert Data: transform.crm_order_items';

        -- Memasukkan data hasil pembersihan dan deduplikasi
        INSERT INTO transform.crm_order_items (
            order_id,
            product_id,
            quantity,
            unit_price
        )
        SELECT 
            COALESCE(order_id, 0) AS order_id,
            COALESCE(product_id, 0) AS product_id,
            COALESCE(quantity, 0) AS quantity,
            COALESCE(unit_price, 0.0) AS unit_price
        FROM (
            -- Memberi nomor urut agar bisa memilih baris unik
            SELECT *,
                   ROW_NUMBER() OVER (
                       PARTITION BY order_id, product_id, quantity, unit_price
                       ORDER BY order_id  -- Bisa diganti sesuai kebutuhan
                   ) AS flag
            FROM ekstraksi.crm_order_items
        ) AS src
        WHERE src.flag = 1;

        -- Catat waktu selesai
        SET @end_time = GETDATE();
        PRINT 'Durasi Upload: ' + CAST(DATEDIFF(SECOND, @start_time, @end_time) AS VARCHAR) + ' detik';

    END TRY

    -- Penanganan jika terjadi kesalahan
    BEGIN CATCH
        PRINT '==========================================';
        PRINT 'Pesan Error : ' + ERROR_MESSAGE();
        PRINT 'Kode Error  : ' + CAST(ERROR_NUMBER() AS VARCHAR);
        PRINT 'State       : ' + CAST(ERROR_STATE() AS VARCHAR);
        PRINT '==========================================';
    END CATCH
END;

-- Eksekusi prosedur untuk menjalankan proses transformasi
EXEC transform.transform_data;

SELECT * FROM transform.crm_order_items;