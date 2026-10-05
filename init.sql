CREATE TABLE public.orders (
    order_id    INT PRIMARY KEY,
    customer_id INT           NOT NULL,
    status      TEXT          NOT NULL,
    amount      NUMERIC(10,2) NOT NULL
);

-- Full before-images on UPDATE/DELETE so deletes carry the whole row.
ALTER TABLE public.orders REPLICA IDENTITY FULL;
