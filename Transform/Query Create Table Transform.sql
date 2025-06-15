-- Mengecek apakah tabel 'crm_customer' di skema transform sudah ada
-- Jika ada, maka tabel tersebut akan dihapus terlebih dahulu
IF OBJECT_ID('transform.crm_customer', 'U') IS NOT NULL
    DROP TABLE transform.crm_customer;
GO

-- Membuat ulang tabel 'crm_customer' pada skema transform
CREATE TABLE transform.crm_customer (
    customer_id INT,                             -- ID unik pelanggan
    customer_name VARCHAR(100),                  -- Nama pelanggan
    email VARCHAR(100),                          -- Alamat email
    phone VARCHAR(50),                           -- Nomor telepon
    address VARCHAR(200),                        -- Alamat lengkap
    area VARCHAR(100),                           -- Wilayah/tempat tinggal
    pincode VARCHAR(20),                         -- Kode pos
    registration_date DATE,                      -- Tanggal registrasi pelanggan
    customer_segment VARCHAR(50),                -- Segmentasi pelanggan (misal: retail, grosir)
    total_orders INT,                            -- Jumlah total pesanan
    avg_order_value DECIMAL(18,2),               -- Rata-rata nilai pesanan
    dwh_create_date DATETIME DEFAULT GETDATE()   -- Tanggal pembuatan data di data warehouse
);
GO

-- Menghapus tabel 'crm_orders' jika sudah ada
IF OBJECT_ID('transform.crm_orders', 'U') IS NOT NULL
    DROP TABLE transform.crm_orders;
GO

-- Membuat ulang tabel 'crm_orders' pada skema transform
CREATE TABLE transform.crm_orders (
    order_id BIGINT,                             -- ID unik pesanan
    customer_id INT,                             -- ID pelanggan yang melakukan pesanan
    order_date DATETIME,                         -- Tanggal pemesanan
    promised_delivery_time DATETIME,             -- Waktu pengiriman yang dijanjikan
    actual_delivery_time DATETIME,               -- Waktu pengiriman aktual
    delivery_status VARCHAR(50),                 -- Status pengiriman (misal: sukses, gagal)
    order_total DECIMAL(18, 2),                  -- Total nilai pesanan
    payment_method VARCHAR(50),                  -- Metode pembayaran
    delivery_partner_id INT,                     -- ID mitra pengiriman
    store_id INT,                                -- ID toko yang memproses pesanan
    dwh_create_date DATETIME DEFAULT GETDATE()   -- Tanggal masuk ke data warehouse
);
GO

-- Menghapus tabel 'crm_order_items' jika sudah ada
IF OBJECT_ID('transform.crm_order_items', 'U') IS NOT NULL
    DROP TABLE transform.crm_order_items;
GO

-- Membuat ulang tabel 'crm_order_items' pada skema transform
CREATE TABLE transform.crm_order_items (
    order_id BIGINT,                             -- ID pesanan
    product_id INT,                              -- ID produk
    quantity INT,                                -- Jumlah unit produk
    unit_price DECIMAL(18, 2),                   -- Harga per unit produk
    dwh_create_date DATETIME DEFAULT GETDATE()   -- Tanggal masuk ke data warehouse
);
GO

-- Menghapus tabel 'erp_products' jika sudah ada
IF OBJECT_ID('transform.erp_products', 'U') IS NOT NULL
    DROP TABLE transform.erp_products;
GO

-- Membuat ulang tabel 'erp_products' pada skema transform
CREATE TABLE transform.erp_products (
    product_id INT,                              -- ID produk
    product_name VARCHAR(100),                   -- Nama produk
    category VARCHAR(50),                        -- Kategori produk
    brand VARCHAR(100),                          -- Merek produk
    price DECIMAL(18, 2),                        -- Harga jual
    mrp DECIMAL(18, 2),                          -- Harga eceran maksimum
    margin_percentage DECIMAL(5, 2),             -- Persentase margin keuntungan
    shelf_life_days INT,                         -- Umur simpan produk (dalam hari)
    min_stock_level INT,                         -- Stok minimum
    max_stock_level INT,                         -- Stok maksimum
    dwh_create_date DATETIME DEFAULT GETDATE()   -- Tanggal masuk ke data warehouse
);
GO

-- Menghapus tabel 'erp_inventory' jika sudah ada
IF OBJECT_ID('transform.erp_inventory', 'U') IS NOT NULL
    DROP TABLE transform.erp_inventory;
GO

-- Membuat ulang tabel 'erp_inventory' pada skema transform
CREATE TABLE transform.erp_inventory (
    product_id INT,                              -- ID produk
    transaction_date DATE,                       -- Tanggal transaksi stok
    stock_received INT,                          -- Jumlah stok yang diterima
    damaged_stock INT,                           -- Jumlah stok yang rusak
    dwh_create_date DATETIME DEFAULT GETDATE()   -- Tanggal masuk ke data warehouse
);
GO


