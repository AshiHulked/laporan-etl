-- -----------------------------------------------------------------
-- CEK & PEMBERSIHAN DATA SUMBER
-- -----------------------------------------------------------------

-- (Opsional) Menampilkan isi tabel sumber
-- SELECT * FROM ekstraksi.crm_orders;

-- Mengecek apakah terdapat data duplikat berdasarkan semua kolom utama
SELECT
    order_id,
    customer_id,
    order_date,
    promised_delivery_time,
    actual_delivery_time,
    delivery_status,
    order_total,
    payment_method,
    delivery_partner_id,
    store_id,
    COUNT(*) AS jumlah
FROM ekstraksi.crm_orders
GROUP BY 
    order_id,
    customer_id,
    order_date,
    promised_delivery_time,
    actual_delivery_time,
    delivery_status,
    order_total,
    payment_method,
    delivery_partner_id,
    store_id
HAVING COUNT(*) > 1;

-- Menghapus baris duplikat menggunakan CTE (Common Table Expression)
WITH DuplicateRows AS (
    SELECT *,
           ROW_NUMBER() OVER (
               PARTITION BY 
                   order_id,
                   customer_id,
                   order_date,
                   promised_delivery_time,
                   actual_delivery_time,
                   delivery_status,
                   order_total,
                   payment_method,
                   delivery_partner_id,
                   store_id
               ORDER BY order_id
           ) AS rn
    FROM ekstraksi.crm_orders
)
DELETE FROM DuplicateRows
WHERE rn > 1;

-- Mengecek nilai NULL pada semua kolom penting
SELECT * FROM ekstraksi.crm_orders
WHERE
    order_id IS NULL OR
    customer_id IS NULL OR 
    order_date IS NULL OR
    promised_delivery_time IS NULL OR
    actual_delivery_time IS NULL OR
    delivery_status IS NULL OR
    order_total IS NULL OR
    payment_method IS NULL OR
    delivery_partner_id IS NULL OR
    store_id IS NULL;

-- Mengganti nilai NULL angka menjadi 0
UPDATE ekstraksi.crm_orders
SET order_total = 0
WHERE order_total IS NULL;

UPDATE ekstraksi.crm_orders
SET delivery_partner_id = 0
WHERE delivery_partner_id IS NULL;

UPDATE ekstraksi.crm_orders
SET store_id = 0
WHERE store_id IS NULL;

-- Mengganti nilai NULL pada kolom tanggal dengan tanggal default
UPDATE ekstraksi.crm_orders
SET order_date = '1900-01-01'
WHERE order_date IS NULL;

UPDATE ekstraksi.crm_orders
SET promised_delivery_time = '1900-01-01'
WHERE promised_delivery_time IS NULL;

UPDATE ekstraksi.crm_orders
SET actual_delivery_time = '1900-01-01'
WHERE actual_delivery_time IS NULL;

-- Mengganti nilai NULL pada kolom teks dengan 'UNKNOWN'
UPDATE ekstraksi.crm_orders
SET delivery_status = 'UNKNOWN'
WHERE delivery_status IS NULL;

UPDATE ekstraksi.crm_orders
SET payment_method = 'UNKNOWN'
WHERE payment_method IS NULL;

-- Membuat atau memperbarui prosedur transformasi data crm_orders
CREATE OR ALTER PROCEDURE transform.transform_data AS
BEGIN
    -- Deklarasi variabel waktu proses
    DECLARE @start_time DATETIME, @end_time DATETIME,
            @batch_start_time DATETIME, @batch_end_time DATETIME;

    BEGIN TRY
        SET @batch_start_time = GETDATE();
        PRINT '=====================================';
        PRINT 'Memulai Proses Transform CRM_ORDERS';
        PRINT '=====================================';

        -- Proses transformasi dimulai
        SET @start_time = GETDATE();
        PRINT 'Truncate Table: transform.crm_orders';

        -- Kosongkan terlebih dahulu isi tabel target
        TRUNCATE TABLE transform.crm_orders;

        PRINT 'Insert Data: transform.crm_orders';

        -- Memasukkan data yang telah dibersihkan dan tanpa duplikat
        INSERT INTO transform.crm_orders (
            order_id,
            customer_id,
            order_date,
            promised_delivery_time,
            actual_delivery_time,
            delivery_status,
            order_total,
            payment_method,
            delivery_partner_id,
            store_id
        )
        SELECT 
            COALESCE(order_id, -1),                      -- Ganti NULL dengan nilai -1 (atau default lainnya)
            COALESCE(customer_id, -1),
            COALESCE(order_date, '1900-01-01'),
            COALESCE(promised_delivery_time, '1900-01-01'),
            COALESCE(actual_delivery_time, '1900-01-01'),
            COALESCE(delivery_status, 'UNKNOWN'),
            COALESCE(order_total, 0.0),
            COALESCE(payment_method, 'UNKNOWN'),
            COALESCE(delivery_partner_id, -1),
            COALESCE(store_id, -1)
        FROM (
            -- Menghindari data duplikat dengan memilih baris pertama (ROW_NUMBER = 1)
            SELECT *, 
                   ROW_NUMBER() OVER (
                       PARTITION BY 
                           order_id,
                           customer_id,
                           order_date,
                           promised_delivery_time,
                           actual_delivery_time,
                           delivery_status,
                           order_total,
                           payment_method,
                           delivery_partner_id,
                           store_id
                       ORDER BY order_id
                   ) AS rn
            FROM ekstraksi.crm_orders
        ) AS cleaned
        WHERE rn = 1;  -- Ambil hanya baris pertama dari kelompok yang sama (tanpa duplikat)

        -- Catat waktu selesai
        SET @end_time = GETDATE();
        PRINT 'Durasi Upload: ' + CAST(DATEDIFF(SECOND, @start_time, @end_time) AS VARCHAR) + ' detik';
        PRINT '-------------------------------------';
    END TRY

    -- Menangani jika ada kesalahan selama proses
    BEGIN CATCH 
        PRINT '======================================';
        PRINT 'Pesan Error = ' + ERROR_MESSAGE();               -- Menampilkan pesan kesalahan
        PRINT 'Kode Error  = ' + CAST(ERROR_NUMBER() AS VARCHAR); -- Nomor error
        PRINT 'State       = ' + CAST(ERROR_STATE() AS VARCHAR);  -- Status error
        PRINT '======================================';
    END CATCH
END

-- Menjalankan prosedur transformasi
EXEC transform.transform_data;
