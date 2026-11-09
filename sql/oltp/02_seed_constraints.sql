CREATE INDEX IF NOT EXISTS idx_customers_updated_at ON customers(updated_at);
CREATE INDEX IF NOT EXISTS idx_couriers_updated_at ON couriers(updated_at);
CREATE INDEX IF NOT EXISTS idx_shipments_updated_at ON shipments(updated_at);
CREATE INDEX IF NOT EXISTS idx_events_updated_at ON shipment_tracking_events(updated_at);
CREATE INDEX IF NOT EXISTS idx_events_shipment_id ON shipment_tracking_events(shipment_id);
CREATE INDEX IF NOT EXISTS idx_cod_updated_at ON cod_settlements(updated_at);
CREATE INDEX IF NOT EXISTS idx_tickets_updated_at ON customer_tickets(updated_at);