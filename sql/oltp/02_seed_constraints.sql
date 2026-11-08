-- Tạo Indexes trên cột updated_at của OLTP để tăng tốc Incremental Extract
CREATE INDEX IF NOT EXISTS idx_oltp_customers_updated ON customers(updated_at);
CREATE INDEX IF NOT EXISTS idx_oltp_hubs_updated ON hubs(updated_at);
CREATE INDEX IF NOT EXISTS idx_oltp_couriers_updated ON couriers(updated_at);
CREATE INDEX IF NOT EXISTS idx_oltp_shipments_updated ON shipments(updated_at);
CREATE INDEX IF NOT EXISTS idx_oltp_events_updated ON shipment_tracking_events(updated_at);
CREATE INDEX IF NOT EXISTS idx_oltp_events_shipment ON shipment_tracking_events(shipment_id);

-- Seed dữ liệu danh mục tĩnh chuẩn (Service Types, Payment Methods, Status Reasons)
INSERT INTO service_types (service_code, service_name, sla_hours, is_express) VALUES
('STD_ECON', 'Chuyển phát Tiết kiệm', 72, FALSE),
('STD_FAST', 'Chuyển phát Nhanh Liên tỉnh', 48, FALSE),
('EXP_24H', 'Chuyển phát Hỏa tốc 24H', 24, TRUE),
('EXP_SAME', 'Giao Ngay Nội Thành (Same-Day)', 12, TRUE),
('HEAVY_CARGO', 'Vận tải Hàng nặng B2B', 96, FALSE)
ON CONFLICT (service_code) DO NOTHING;

INSERT INTO payment_methods (payment_method_code, payment_method_name, payer_type, has_cod) VALUES
('CASH_SENDER', 'Tiền mặt Người gửi trả', 'Sender', FALSE),
('COD_RECEIVER', 'Thu hộ COD & Phí người nhận trả', 'Receiver', TRUE),
('COD_FREE_SHIP', 'Thu hộ COD - Shop trả cước', 'Sender', TRUE),
('WALLET_PREPAID', 'Ví điện tử trả trước', 'Sender', FALSE),
('B2B_MONTHLY', 'Công nợ cuối tháng B2B', 'Sender', FALSE)
ON CONFLICT (payment_method_code) DO NOTHING;

INSERT INTO status_reasons (status_code, status_name, reason_code, reason_name, event_category) VALUES
('CREATED', 'Mới tạo vận đơn', 'RS_INIT', 'Khách hàng tạo đơn trên hệ thống', 'Normal'),
('PICKED_UP', 'Đã lấy hàng', 'RS_PICK_OK', 'Bưu tá lấy hàng thành công tại điểm gửi', 'Normal'),
('IN_TRANSIT', 'Đang trung chuyển', 'RS_HUB_IN', 'Hàng nhập kho trung tâm phân loại (Inbound)', 'Normal'),
('IN_TRANSIT', 'Đang xuất kho', 'RS_HUB_OUT', 'Hàng xuất kho đi tuyến tiếp theo (Outbound)', 'Normal'),
('DELIVERING', 'Đang giao hàng', 'RS_OUT_DELIV', 'Shipper đang đi giao chặng cuối', 'Normal'),
('DELIVERED', 'Giao thành công', 'RS_DELIV_OK', 'Người nhận đã ký nhận hàng', 'Terminal'),
('DELIVERY_FAILED', 'Giao thất bại', 'RS_CUST_BUSY', 'Khách hẹn giao lại ca sau / Không nghe máy', 'Exception'),
('RETURNED', 'Hoàn hàng', 'RS_REFUSED', 'Người nhận từ chối nhận hàng', 'Terminal'),
('RETURNED', 'Hoàn hàng', 'RS_WRONG_ADDR', 'Sai địa chỉ / Số điện thoại không liên lạc được', 'Terminal'),
('CANCELLED', 'Đã hủy', 'RS_SHOP_CANCEL', 'Người gửi yêu cầu hủy đơn trước khi lấy', 'Terminal')
ON CONFLICT (reason_code) DO NOTHING;