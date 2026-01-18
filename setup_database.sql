-- Таблица аккаунтов
CREATE TABLE IF NOT EXISTS accounts (
    id INT AUTO_INCREMENT PRIMARY KEY,
    phone VARCHAR(20) NOT NULL,
    session_string TEXT,
    status ENUM('active', 'banned', 'flood_wait') DEFAULT 'active',
    last_used TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- Таблица источников
CREATE TABLE IF NOT EXISTS sources (
    id INT AUTO_INCREMENT PRIMARY KEY,
    channel_link VARCHAR(100) NOT NULL UNIQUE
);

-- Таблица получателей
CREATE TABLE IF NOT EXISTS destinations (
    id INT AUTO_INCREMENT PRIMARY KEY,
    chat_link VARCHAR(100) NOT NULL UNIQUE
);

-- История
CREATE TABLE IF NOT EXISTS history (
    id INT AUTO_INCREMENT PRIMARY KEY,
    source_message_id INT NOT NULL,
    account_id INT,
    status VARCHAR(20),
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    FOREIGN KEY (account_id) REFERENCES accounts(id)
);

INSERT INTO sources (channel_link) VALUES ('rentvseyabali');
INSERT INTO destinations (chat_link) VALUES ('balibike_sic'), ('balimotorbikes'), ('balibikefamaly');