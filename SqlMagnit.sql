-- Создаю таблицу пользователей
CREATE TABLE magnit.loyalty_users (
    id UUID PRIMARY KEY,                   
    card_number VARCHAR(30) UNIQUE NOT NULL, 
    phone VARCHAR(20) NOT NULL,            
    bonus_balance NUMERIC(10, 2) DEFAULT 0.00 
);

-- Создаю таблицу чеков
CREATE TABLE magnit.receipts (
    id UUID PRIMARY KEY,                   
    user_id UUID NOT NULL,                 
    total_amount NUMERIC(10, 2) NOT NULL,  
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP, 
    FOREIGN KEY (user_id) REFERENCES magnit.loyalty_users(id)
);

-- Создаю таблицу для истории бонусов
CREATE TABLE magnit.bonus_transactions (
    id UUID PRIMARY KEY,                   
    user_id UUID NOT NULL,                 
    receipt_id UUID NOT NULL,              
    amount_earned NUMERIC(10, 2) NOT NULL, 
    processed_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP, 
    FOREIGN KEY (user_id) REFERENCES magnit.loyalty_users(id),
    FOREIGN KEY (receipt_id) REFERENCES magnit.receipts(id)
);

-- Наполняю тестовыми данными (для примера - чек на 5000 рублей)
INSERT INTO magnit.loyalty_users (id, card_number, phone, bonus_balance)
VALUES ('c1111111-1111-1111-1111-111111111111', '2300001234567', '+79998887766', 0.00);

INSERT INTO magnit.receipts (id, user_id, total_amount)
VALUES ('ade11111-1111-1111-1111-111111111111', 'c1111111-1111-1111-1111-111111111111', 5000.00);

INSERT INTO magnit.bonus_transactions (id, user_id, receipt_id, amount_earned)
VALUES ('b2222222-2222-2222-2222-222222222222', 'c1111111-1111-1111-1111-111111111111', 'ade11111-1111-1111-1111-111111111111', 50.00);

UPDATE magnit.loyalty_users 
SET bonus_balance = bonus_balance + 50.00
WHERE id = 'c1111111-1111-1111-1111-111111111111';


SELECT 
    u.card_number,
    r.total_amount AS receipt_rub,
    t.amount_earned AS bonuses_earned,
    u.bonus_balance AS total_balance
FROM magnit.loyalty_users u
JOIN magnit.receipts r ON r.user_id = u.id
JOIN magnit.bonus_transactions t ON t.receipt_id = r.id;