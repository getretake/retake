DROP EXTENSION IF EXISTS pg_analytics;
CREATE EXTENSION IF NOT EXISTS pg_analytics;

CREATE TABLE analytics_test (
    id SERIAL PRIMARY KEY,
    event_date DATE,
    user_id INT,
    event_name VARCHAR(50),
    session_duration INT,
    page_views INT,
    revenue DECIMAL(10, 2)
);

INSERT INTO analytics_test (event_date, user_id, event_name, session_duration, page_views, revenue)
VALUES
('2024-01-01', 1, 'Login', 300, 5, 20.00),
('2024-01-02', 2, 'Purchase', 450, 8, 150.50),
('2024-01-03', 3, 'Logout', 100, 2, 0.00),
('2024-01-04', 4, 'Signup', 200, 3, 0.00),
('2024-01-05', 5, 'ViewProduct', 350, 6, 30.75),
('2024-01-06', 1, 'AddToCart', 500, 10, 75.00),
('2024-01-07', 2, 'RemoveFromCart', 250, 4, 0.00),
('2024-01-08', 3, 'Checkout', 400, 7, 200.25),
('2024-01-09', 4, 'Payment', 550, 11, 300.00),
('2024-01-10', 5, 'Review', 600, 9, 50.00),
('2024-01-11', 6, 'Login', 320, 3, 0.00),
('2024-01-12', 7, 'Purchase', 480, 7, 125.30),
('2024-01-13', 8, 'Logout', 150, 2, 0.00),
('2024-01-14', 9, 'Signup', 240, 4, 0.00),
('2024-01-15', 10, 'ViewProduct', 360, 5, 45.00),
('2024-01-16', 6, 'AddToCart', 510, 9, 80.00),
('2024-01-17', 7, 'RemoveFromCart', 270, 3, 0.00),
('2024-01-18', 8, 'Checkout', 430, 6, 175.50),
('2024-01-19', 9, 'Payment', 560, 12, 250.00),
('2024-01-20', 10, 'Review', 610, 10, 60.00);
