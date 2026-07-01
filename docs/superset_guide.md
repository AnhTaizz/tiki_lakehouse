# Apache Superset Guide (Tiki Data Lakehouse)

This document provides instructions on how to connect to and visualize data from the Data Lakehouse using **Apache Superset**.

Data flow: `Silver Iceberg` → `Spark Gold Job` → `Reporting Postgres (port 5432)` → `Superset (port 8088)`.
For real-time streaming: `Kafka` → `Spark Streaming` → `Reporting Postgres (Speed Layer)` → `Superset`.

---

## 1. Login to Superset
After running `docker compose up -d`, open your browser and navigate to:
- **URL**: [http://localhost:8088](http://localhost:8088)
- **Default Username**: `admin`
- **Default Password**: `password123` 
*(If you changed `SUPERSET_ADMIN_PASSWORD` in the `.env` file, use that password instead).*

---

## 2. Connect to the Database (Data Source)
First, you need to tell Superset where the Gold data is stored.
1. In the top right corner, click **Settings** → **Database Connections**.
2. Click the **+ Database** button.
3. Select **PostgreSQL** and fill in the following:
   - **Host:** `reporting-postgres` *(This is the container name)*
   - **Port:** `5432`
   - **Database Name:** `reporting`
   - **Username:** `reporting`
   - **Password:** `reporting123`
   - **Display Name:** Optional (e.g., *Tiki Gold Data*)
4. Click **Test Connection**. If a green success message appears → Click **Connect** and **Finish**.

---

## 3. Register Tables (Create Dataset)
Superset can only visualize tables that you explicitly register as datasets.
1. In the top menu, select **Datasets**.
2. Click the **+ Dataset** button.
3. Fill in the following:
   - Database: `Tiki Gold Data` (Created in step 2).
   - Schema: `public`
   - Table: Select the table you want to visualize (e.g., `brand_performance`, `top_products`, `realtime_events`).
4. Click **Create Dataset and Create Chart**.

---

## 4. Building Charts
In the Explore interface, follow these steps to create beautiful charts:

### Select Axes & Metrics
- **X-axis:** Drag and drop or select a descriptive column (e.g., `brand_name`). *This is mandatory for Echarts*.
- **Metrics:** Select a calculation. Example: `SUM(total_quantity_sold)` or `AVG(avg_rating)`.

### Troubleshooting & Tips

| Common Issue | Solution |
| :--- | :--- |
| **"Create chart" button is grayed out**, error *"Add required control values..."* | You haven't filled in the **X-axis**. Drag a display column (e.g., `brand_name`) into the X-axis box. Do not put it in the Dimensions box. |
| Chart is messy, bars are not sorted | Superset defaults to alphabetical sorting. Scroll down to **X-Axis Sort By**, select your metric (e.g., `SUM(...)`) and enable **Sort Descending**. |
| Too many columns, text overlaps | Change the **Row limit** (or Series limit) to `10` or `20` to only get the Top 10/20. To make it more readable, switch the chart type to **Bar Chart (Horizontal)**. |
| Metric names are ugly (e.g., `SUM(total_quantity_sold)`) | Click directly on the Metric box → In the popup, click the **pencil icon ✏️** next to the name at the top → Type a better name (e.g., *Total Units Sold*) → Click Save. |

---

## 5. Add Charts to a Dashboard
1. Once you are satisfied with the chart, click **Update Chart** (to preview).
2. Click **Save** in the top right corner.
3. Name your Chart (e.g., *Top 20 Best Selling Brands*).
4. Under "Add to dashboard", select **Add to new dashboard** and name it (e.g., *Tiki Overview*).
5. Click **Save & Go to Dashboard**.

Repeat the process from **Step 3** for other Gold tables, selecting "Add to existing dashboard" to group all your charts into a single centralized dashboard for your project demonstration.
