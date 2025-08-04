-- Create the database
CREATE DATABASE IF NOT EXISTS product_catalog;

-- Create a dedicated database user for your application (recommended over using root)
-- Replace 'your_db_user' and 'your_db_password' with strong credentials.
-- '%' allows connection from any host (adjust to a specific Tailscale IP if you want more security).
CREATE USER 'lenovo_mysql'@'%' IDENTIFIED BY '123789456';

-- Grant all privileges on the new database to this user
GRANT ALL PRIVILEGES ON product_catalog.* TO 'lenovo_mysql'@'%';

-- Apply changes
FLUSH PRIVILEGES;