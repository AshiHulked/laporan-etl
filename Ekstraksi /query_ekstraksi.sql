-- Mengecek apakah tabel crm_customer sudah ada. Jika ada, hapus terlebih dahulu
IF OBJECT_ID('ekstraksi.crm_customer', 'U') IS NOT NULL
    DROP TABLE ekstraksi.crm_customer;
GO

-- Membuat tabel crm_customer untuk menyimpan data pelanggan
CREATE TABLE ekstraksi.crm_customer (
    customer_id INT,                             -- ID unik untuk pelanggan
    customer_name VARCHAR(100),                  -- Nama pelanggan
    email VARCHAR(100),                          -- Alamat email pelanggan
    phone VARCHAR(20),                           -- Nomor telepon pelanggan
    address VARCHAR(200),                        -- Alamat lengkap pelanggan
    area VARCHAR(50),                            -- Wilayah tempat pelanggan berada
    pincode VARCHAR(10),                         -- Kode pos
    registration_date DATE,                      -- Tanggal pelanggan mendaftar
    customer_segment VARCHAR(50),                -- Segmentasi pelanggan (misal: retail, grosir)
    total_orders INT,                            -- Total jumlah order yang pernah dilakukan
    avg_order_value DECIMAL(10, 2)               -- Rata-rata nilai transaksi pelanggan
);
GO

-- Hapus tabel crm_orders jika sudah ada
IF OBJECT_ID('ekstraksi.crm_orders', 'U') IS NOT NULL
    DROP TABLE ekstraksi.crm_orders;
GO

-- Membuat tabel crm_orders untuk menyimpan informasi pesanan pelanggan
CREATE TABLE ekstraksi.crm_orders (
    order_id BIGINT,                             -- ID unik pesanan
    customer_id INT,                             -- ID pelanggan yang melakukan pesanan
    order_date DATETIME,                         -- Tanggal dan waktu pemesanan
    promised_delivery_time DATETIME,             -- Janji waktu pengiriman
    actual_delivery_time DATETIME,               -- Waktu pengiriman aktual
    delivery_status VARCHAR(50),                 -- Status pengiriman (misal: delivered, pending)
    order_total DECIMAL(10, 2),                  -- Total nilai dari pesanan
    payment_method VARCHAR(50),                  -- Metode pembayaran yang digunakan
    delivery_partner_id INT,                     -- ID mitra pengiriman
    store_id INT                                 -- ID toko/gerai pemroses pesanan
);
GO

-- Hapus tabel order_items jika sudah ada
IF OBJECT_ID('ekstraksi.order_items', 'U') IS NOT NULL
    DROP TABLE ekstraksi.order_items;
GO

-- Membuat tabel crm_order_items untuk menyimpan rincian produk dari setiap order
CREATE TABLE ekstraksi.crm_order_items (
    order_id BIGINT,                             -- ID pesanan
    product_id INT,                              -- ID produk yang dipesan
    quantity INT,                                -- Jumlah unit produk dalam pesanan
    unit_price DECIMAL(10, 2)                    -- Harga per unit produk saat transaksi
);
GO

-- Hapus tabel erp_products jika sudah ada
IF OBJECT_ID('ekstraksi.erp_products', 'U') IS NOT NULL
    DROP TABLE ekstraksi.erp_products;
GO

-- Membuat tabel erp_products untuk menyimpan informasi produk
CREATE TABLE ekstraksi.erp_products (
    product_id INT,                              -- ID produk
    product_name VARCHAR(100),                   -- Nama produk
    category VARCHAR(50),                        -- Kategori produk (misal: makanan, minuman)
    brand VARCHAR(100),                          -- Merek produk
    price DECIMAL(10, 2),                        -- Harga jual
    mrp DECIMAL(10, 2),                          -- Harga eceran maksimum
    margin_percentage DECIMAL(5, 2),             -- Persentase margin keuntungan
    shelf_life_days INT,                         -- Umur simpan dalam hari
    min_stock_level INT,                         -- Minimum stok yang harus tersedia
    max_stock_level INT                          -- Maksimum stok yang diperbolehkan
);
GO

-- Hapus tabel erp_inventory jika sudah ada
IF OBJECT_ID('ekstrasi.erp_inventory', 'U') IS NOT NULL
    DROP TABLE ekstrasi.erp_inventory;
GO

-- Membuat tabel erp_inventory untuk menyimpan informasi mutasi stok produk
CREATE TABLE ekstraksi.erp_inventory (
    product_id INT,                              -- ID produk
    transaction_date DATE,                       -- Tanggal transaksi inventaris
    stock_received INT,                          -- Jumlah stok yang diterima
    damaged_stock INT                            -- Jumlah stok yang rusak
);
GO

-- Procedure pakai skema ekstraksi
CREATE OR ALTER PROCEDURE ekstraksi.load_data_ekstraksi AS
BEGIN
    DECLARE @start_time DATETIME, @end_time DATETIME,
            -- start_time dan end_time untuk menyimpan waktu ekstraksi setiap tabel
            @batch_start_time DATETIME, @batch_end_time DATETIME
            -- batch_start_time dan batch_end_time untuk mencatat waktu keseluruhan proses

    BEGIN TRY
        SET @batch_start_time = GETDATE();
        PRINT '========================================';
        PRINT 'Memulai Proses Ekstraksi';
        PRINT '----------------------------------------';
        PRINT 'Ekstraksi Tabel CRM';
        PRINT ' ';

        SET @start_time = GETDATE();
        PRINT 'Truncate Table: ekstraksi.crm_customer';
        -- Menghapus semua data di tabel sebelum diisi ulang
        TRUNCATE TABLE ekstraksi.crm_customer;

        PRINT 'Insert Data: ekstraksi.crm_customer';
        -- Input data pelanggan dari file CSV
        BULK INSERT ekstraksi.crm_customer
        FROM 'C:\Users\friki\OneDrive\Dokumen\data1\customers.csv'
        -- Sesuaikan dengan lokasi file CSV masing-masing
        WITH (
            FIRSTROW = 2,
            FIELDTERMINATOR = ',',
            ROWTERMINATOR = '\n',
            TABLOCK,
            FORMAT = 'CSV',
            DATAFILETYPE = 'char'
        )

        SET @end_time = GETDATE();
        PRINT 'Durasi Upload: ' + CAST(DATEDIFF(second, @start_time, @end_time) AS VARCHAR) + ' detik';
        PRINT '----------------------------------------';
    END TRY

    BEGIN CATCH
        PRINT '========================================';
        PRINT 'Pesan Error : ' + error_message();
        PRINT 'Pesan Error : ' + CAST(ERROR_NUMBER() AS VARCHAR);
        PRINT 'Pesan Error : ' + CAST(ERROR_STATE() AS VARCHAR);
        PRINT '========================================';
    END CATCH
END

EXEC ekstraksi.load_data_ekstraksi;

-- Procedure pakai skema ekstraksi
CREATE OR ALTER PROCEDURE ekstraksi.load_data_ekstraksi AS
BEGIN
    DECLARE @start_time DATETIME, @end_time DATETIME,
            -- start_time dan end_time untuk menyimpan waktu ekstraksi setiap tabel
            @batch_start_time DATETIME, @batch_end_time DATETIME
            -- batch_start_time dan batch_end_time untuk mencatat waktu keseluruhan proses

    BEGIN TRY
        SET @batch_start_time = GETDATE();
        PRINT '========================================';
        PRINT 'Memulai Proses Ekstraksi';
        PRINT '----------------------------------------';
        PRINT 'Ekstraksi Tabel CRM';
        PRINT ' ';

        SET @start_time = GETDATE();
        PRINT 'Truncate Table: ekstraksi.crm_orders';
        -- Mengosongkan isi tabel terlebih dahulu (⚠️ Hati-hati: tabel yang dihapus crm_order_items, tapi dimasukkan ke crm_orders)
        TRUNCATE TABLE ekstraksi.crm_orders;

        PRINT 'Insert Data: ekstraksi.crm_orders';
        -- Input data pesanan dari file CSV
        BULK INSERT ekstraksi.crm_orders
        FROM 'C:\Users\friki\OneDrive\Dokumen\data1\orders.csv'
        -- Sesuaikan dengan jalur file CSV di komputer
        WITH (
            FIRSTROW = 2,
            FIELDTERMINATOR = ',',
            ROWTERMINATOR = '\n',
            TABLOCK,
            FORMAT = 'CSV',
            DATAFILETYPE = 'char'
        )

        SET @end_time = GETDATE();
        PRINT 'Durasi Upload: ' + CAST(DATEDIFF(second, @start_time, @end_time) AS VARCHAR) + ' detik';
        PRINT '----------------------------------------';
    END TRY

    BEGIN CATCH
        PRINT '========================================';
        PRINT 'Pesan Error : ' + error_message();
        PRINT 'Pesan Error : ' + CAST(ERROR_NUMBER() AS VARCHAR);
        PRINT 'Pesan Error : ' + CAST(ERROR_STATE() AS VARCHAR);
        PRINT '========================================';
    END CATCH
END

EXEC ekstraksi.load_data_ekstraksi;


-- Membuat atau mengubah procedure load_data_ekstraksi di skema ekstraksi
CREATE OR ALTER PROCEDURE ekstraksi.load_data_ekstraksi AS
BEGIN
    -- Variabel untuk mencatat waktu mulai dan selesai ekstraksi
    DECLARE @start_time DATETIME, @end_time DATETIME,
            @batch_start_time DATETIME, @batch_end_time DATETIME;

    BEGIN TRY
        -- Mencatat waktu awal proses keseluruhan
        SET @batch_start_time = GETDATE();
        PRINT '========================================';
        PRINT 'Memulai Proses Ekstraksi';
        PRINT '----------------------------------------';
        PRINT 'Ekstraksi Tabel CRM';
        PRINT ' ';

        -- =============================
        -- EKSTRAKSI DATA: crm_order_items
        -- =============================
        SET @start_time = GETDATE();
        PRINT 'Truncate Table: ekstraksi.crm_order_items';

        -- Hapus seluruh data pada tabel crm_order_items (INI PERLU DIPERHATIKAN)
        -- *catatan: Anda melakukan TRUNCATE pada tabel yang berbeda dari target BULK INSERT
        TRUNCATE TABLE ekstraksi.crm_order_items;

        PRINT 'Insert Data: ekstraksi.crm_order_items';

        -- Mengisi ulang data ke tabel crm_orders dari file CSV
        BULK INSERT ekstraksi.crm_order_items
        FROM 'C:\Users\friki\OneDrive\Dokumen\data1\order_items.csv'
        WITH (
            FIRSTROW = 2,              -- Lewati baris pertama (header)
            FIELDTERMINATOR = ',',     -- Pemisah antar kolom adalah koma
            ROWTERMINATOR = '\n',      -- Baris dipisahkan dengan newline
            TABLOCK,                   -- Kunci tabel untuk mencegah akses selama proses
            FORMAT = 'CSV',            -- Format file CSV
            DATAFILETYPE = 'char'      -- Tipe data karakter
        );

        SET @end_time = GETDATE();

        -- Tampilkan durasi waktu upload untuk 1 tabel
        PRINT 'Durasi Upload: ' + CAST(DATEDIFF(second, @start_time, @end_time) AS VARCHAR) + ' detik';
        PRINT '----------------------------------------';
    END TRY

    -- Blok penanganan error jika terjadi kegagalan saat proses ekstraksi
    BEGIN CATCH
        PRINT '========================================';
        PRINT 'Pesan Error : ' + error_message();         -- Tampilkan pesan error
        PRINT 'Pesan Error : ' + CAST(ERROR_NUMBER() AS VARCHAR);  -- Nomor error
        PRINT 'Pesan Error : ' + CAST(ERROR_STATE() AS VARCHAR);   -- Status error
        PRINT '========================================';
    END CATCH
END

-- Eksekusi procedure untuk melakukan ekstraksi
EXEC ekstraksi.load_data_ekstraksi;

-- Procedure pakai skema ekstraksi
CREATE OR ALTER PROCEDURE ekstraksi.load_data_ekstraksi AS
BEGIN
    DECLARE @start_time DATETIME, @end_time DATETIME,
            -- start_time dan end_time untuk menyimpan waktu ekstraksi setiap tabel
            @batch_start_time DATETIME, @batch_end_time DATETIME
            -- batch_start_time dan batch_end_time untuk mencatat waktu keseluruhan proses

    BEGIN TRY
        SET @batch_start_time = GETDATE();
        PRINT '========================================';
        PRINT 'Memulai Proses Ekstraksi';
        PRINT '----------------------------------------';
        PRINT 'Ekstraksi Tabel ERP';
        PRINT ' ';

        SET @start_time = GETDATE();
        PRINT 'Truncate Table: ekstraksi.erp_products';
        -- Menghapus isi tabel sebelum dimasukkan data baru
        TRUNCATE TABLE ekstraksi.erp_products;

        PRINT 'Insert Data: ekstraksi.erp_products';
        -- Proses input data dari file CSV ke tabel
        BULK INSERT ekstraksi.erp_products
        FROM 'C:\Users\friki\OneDrive\Dokumen\data1\products.csv'
        -- Sesuaikan dengan lokasi file di laptop masing-masing
        WITH (
            FIRSTROW = 2,
            FIELDTERMINATOR = ',',
            ROWTERMINATOR = '\n',
            TABLOCK,
            FORMAT = 'CSV',
            DATAFILETYPE = 'char'
        )

        SET @end_time = GETDATE();
        PRINT 'Durasi Upload: ' + CAST(DATEDIFF(second, @start_time, @end_time) AS VARCHAR) + ' detik';
        PRINT '----------------------------------------';
    END TRY

    -- Menangani error jika proses gagal
    BEGIN CATCH
        PRINT '========================================';
        PRINT 'Pesan Error : ' + error_message();
        PRINT 'Pesan Error : ' + CAST(ERROR_NUMBER() AS VARCHAR);
        PRINT 'Pesan Error : ' + CAST(ERROR_STATE() AS VARCHAR);
        PRINT '========================================';
    END CATCH
END

EXEC ekstraksi.load_data_ekstraksi;


-- Prosedur untuk mengisi ulang data dari file CSV ke tabel erp_inventory
CREATE OR ALTER PROCEDURE ekstraksi.load_data_ekstraksi AS
BEGIN
    DECLARE @start_time DATETIME, @end_time DATETIME,
            @batch_start_time DATETIME, @batch_end_time DATETIME;

    BEGIN TRY
        SET @batch_start_time = GETDATE();
        PRINT '========================================';
        PRINT 'Memulai Proses Ekstraksi';
        PRINT '----------------------------------------';
        PRINT 'Ekstraksi Tabel ERP';

        SET @start_time = GETDATE();
        PRINT 'Truncate Table: ekstraksi.erp_inventory';

        -- Hapus semua isi tabel agar tidak terjadi duplikasi data
        TRUNCATE TABLE ekstraksi.erp_inventory;

        PRINT 'Insert Data: ekstraksi.erp_inventory';

        -- Load data dari file CSV
        BULK INSERT ekstraksi.erp_inventory
        FROM 'C:\Users\friki\OneDrive\Dokumen\data1\inventory.csv'
        WITH (
            FIRSTROW = 2,                         -- Baris pertama berisi header
            FIELDTERMINATOR = ',',                -- Pemisah antar kolom
            ROWTERMINATOR = '\n',                 -- Baris baru diakhiri newline
            TABLOCK,                              -- Lock tabel selama proses
            FORMAT = 'CSV',
            DATAFILETYPE = 'char'
        );

        SET @end_time = GETDATE();
        PRINT 'Durasi Upload: ' + CAST(DATEDIFF(second, @start_time, @end_time) AS VARCHAR) + ' detik';
        PRINT '----------------------------------------';
    END TRY

    -- Tangkap pesan error jika terjadi kegagalan proses
    BEGIN CATCH
        PRINT '========================================';
        PRINT 'Pesan Error : ' + error_message();
        PRINT 'Pesan Error : ' + CAST(ERROR_NUMBER() AS VARCHAR);
        PRINT 'Pesan Error : ' + CAST(ERROR_STATE() AS VARCHAR);
        PRINT '========================================';
    END CATCH
END

-- Eksekusi prosedur
EXEC ekstraksi.load_data_ekstraksi;

