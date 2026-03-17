# ML Specification & Data Schema

## 1. ML Specification
Tài liệu này định nghĩa các thông số kỹ thuật cho mô hình Machine Learning dự báo giá iPhone và phát hiện bất thường.

* **Input features:** `price_vnd` (lag 1,3,7), `day_of_week`, `is_weekend`, `days_since_release`, `discount_pct`, `source_id`
* **Target variable:** `price_vnd` tại t+1 đến t+7 (dự báo 7 ngày tới)
* **Model chính:** Facebook Prophet (xử lý seasonality tốt) + ARIMA làm baseline
* **Anomaly method:** Z-score (|z| > 3) + IQR — đơn giản, dễ giải thích
* **Retrain schedule:** Mỗi 7 ngày — chạy tự động qua GitHub Actions
* **Evaluation metric:** MAE và MAPE (Mean Absolute Percentage Error) — dễ giải thích với non-technical
* **Output:** Lưu vào bảng `ml_predictions`, expose qua API endpoint cho dashboard

## 2. Schema Output ML
Định nghĩa cấu trúc bảng đầu ra từ Data Scientist để Data Analyst (Thạch Phạm) sử dụng cho Dashboard.

### Bảng: ml_predictions
* `prediction_id`: BIGSERIAL (Khoá chính)
* `model_id`: INT (FK - Liên kết dim_model)
* `source_id`: INT (FK - Nguồn dữ liệu dùng để train)
* `forecast_date`: DATE (Ngày được dự báo - 7 ngày tới)
* `predicted_price`: BIGINT (Giá dự báo VND)
* `lower_bound`: BIGINT (Khoảng tin cậy dưới 80%)
* `upper_bound`: BIGINT (Khoảng tin cậy trên 80%)
* `model_version`: VARCHAR(50) (Phiên bản model — vd: "prophet_v1.2")
* `trained_at`: TIMESTAMPTZ (Thời điểm model được train/update)

### Bảng: ml_anomalies
* (Sẽ được thiết kế dựa trên flag `is_anomaly` tích hợp vào `fact_prices`)