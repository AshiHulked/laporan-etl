-- ============================================================
-- 1. CEK DATA DUPLIKAT
-- Tujuan: Menemukan kombinasi semua kolom yang muncul lebih dari 1 kali
--         (berarti terdapat duplikasi baris)
-- ============================================================

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
    avg_order_value,
    COUNT(*) AS jumlah  -- Menghitung jumlah kombinasi yang sama
FROM ekstraksi.crm_customer
GROUP BY 
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
HAVING COUNT(*) > 1;  -- Hanya menampilkan yang lebih dari 1 kali (duplikat)

-- ============================================================
-- 2. MENGHAPUS DATA DUPLIKAT MENGGUNAKAN CTE
-- Tujuan: Menghapus baris duplikat, hanya menyisakan satu baris terbaru
-- ============================================================

WITH DuplicateData AS (
    SELECT *,
        ROW_NUMBER() OVER (
            PARTITION BY 
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
            ORDER BY registration_date DESC  -- Data terbaru mendapat urutan 1
        ) AS flag
    FROM ekstraksi.crm_customer
)

-- Menghapus semua baris yang nomor urutnya > 1 (duplikat)
DELETE FROM DuplicateData
WHERE flag > 1;

-- ============================================================
-- 3. CEK DATA YANG MENGANDUNG NILAI NULL
-- Tujuan: Menampilkan semua baris yang memiliki data kosong (NULL)
-- ============================================================

SELECT *
FROM ekstraksi.crm_customer
WHERE 
    customer_id IS NULL OR
    customer_name IS NULL OR
    email IS NULL OR
    phone IS NULL OR
    address IS NULL OR
    area IS NULL OR
    pincode IS NULL OR
    registration_date IS NULL OR
    customer_segment IS NULL OR
    total_orders IS NULL OR
    avg_order_value IS NULL;

-- ============================================================
-- 4. UPDATE NILAI NULL MENJADI DEFAULT
-- Tujuan: Mengganti nilai NULL dengan teks 'Unknown' (untuk customer_name)
-- ============================================================

UPDATE ekstraksi.crm_customer
SET customer_name = 'Unknown'
WHERE customer_name IS NULL;

-- ============================================================
-- 5. PROSEDUR TRANSFORMASI DATA
-- Tujuan: Membersihkan dan memindahkan data ke tabel transform
-- ============================================================

CREATE OR ALTER PROCEDURE transform.transform_data AS
BEGIN
    -- Deklarasi variabel waktu transformasi
    DECLARE @start_time DATETIME, @end_time DATETIME,
            @batch_start_time DATETIME, @batch_end_time DATETIME;

    BEGIN TRY
        -- Awal proses batch
        SET @batch_start_time = GETDATE();
        PRINT '==========================================';
        PRINT 'Mulai Proses Transform';
        PRINT '==========================================';
        PRINT '';
        PRINT 'Transform Tabel CRM_CUSTOMER';
        PRINT '------------------------------------------';

        -- Mulai transformasi tabel pelanggan
        SET @start_time = GETDATE();
        PRINT 'Truncate Table: transform.crm_customer';
        TRUNCATE TABLE transform.crm_customer;

        PRINT 'Insert Data: transform.crm_customer';

        -- Memasukkan data hasil pembersihan dan deduplikasi
        INSERT INTO transform.crm_customer (
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
        )
        SELECT 
            COALESCE(customer_id, 0) AS customer_id,
            COALESCE(customer_name, 'N/A') AS customer_name,
            COALESCE(email, 'N/A') AS email,
            COALESCE(phone, 'N/A') AS phone,
            COALESCE(address, 'N/A') AS address,
            COALESCE(area, 'N/A') AS area,
            COALESCE(pincode, 'N/A') AS pincode,
            COALESCE(registration_date, '1900-01-01') AS registration_date,
            COALESCE(customer_segment, 'N/A') AS customer_segment,
            COALESCE(total_orders, 0) AS total_orders,
            COALESCE(avg_order_value, 0.0) AS avg_order_value
        FROM (
            -- Ambil hanya 1 data terakhir per customer_id
            SELECT *,
                   ROW_NUMBER() OVER (
                       PARTITION BY customer_id
                       ORDER BY registration_date DESC
                   ) AS flag
            FROM ekstraksi.crm_customer
        ) AS TAB
        WHERE customer_id IS NOT NULL
          AND TAB.flag = 1;

        -- Akhir proses untuk tabel ini
        SET @end_time = GETDATE();
        PRINT 'Durasi Upload: ' + CAST(DATEDIFF(SECOND, @start_time, @end_time) AS VARCHAR) + ' detik';

    END TRY

    -- Penanganan jika terjadi kesalahan
    BEGIN CATCH
        PRINT '==========================================';
        PRINT 'Pesan Error : ' + ERROR_MESSAGE();
        PRINT 'Pesan Error : ' + CAST(ERROR_NUMBER() AS VARCHAR);
        PRINT 'Pesan Error : ' + CAST(ERROR_STATE() AS VARCHAR);
        PRINT '==========================================';
    END CATCH
END;

-- Menjalankan prosedur transformasi
EXEC transform.transform_data;

-- (Opsional) Melihat hasil akhirnya
SELECT * FROM transform.crm_customer;
