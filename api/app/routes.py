from fastapi import APIRouter, Depends, Query
from sqlalchemy import text
from sqlalchemy.orm import Session

from app.database import get_db

router = APIRouter()


@router.get("/health")
def health(db: Session = Depends(get_db)):
    db.execute(text("SELECT 1"))
    return {"status": "ok"}


@router.get("/events/recent")
def recent_events(
    limit: int = Query(50, ge=1, le=500),
    db: Session = Depends(get_db),
):
    rows = db.execute(
        text(
            """
            SELECT event_id, event_type, customer_id, product_id, category,
                   price, quantity, country, device, payment_method,
                   kafka_topic, event_time
            FROM analytics.events
            ORDER BY event_time DESC
            LIMIT :limit
            """
        ),
        {"limit": limit},
    ).mappings().all()
    return {"count": len(rows), "events": [dict(r) for r in rows]}


@router.get("/analytics/sales-by-category")
def sales_by_category(
    minutes: int = Query(60, ge=1, le=1440),
    db: Session = Depends(get_db),
):
    rows = db.execute(
        text(
            """
            SELECT window_start, window_end, category, total_revenue,
                   order_count, avg_order_value
            FROM analytics.sales_by_category
            WHERE window_start >= now() - (:minutes || ' minutes')::interval
            ORDER BY window_start DESC, total_revenue DESC
            """
        ),
        {"minutes": minutes},
    ).mappings().all()
    return {"count": len(rows), "windows": [dict(r) for r in rows]}


@router.get("/analytics/sales-by-country")
def sales_by_country(
    minutes: int = Query(60, ge=1, le=1440),
    db: Session = Depends(get_db),
):
    rows = db.execute(
        text(
            """
            SELECT window_start, window_end, country, total_revenue,
                   order_count, avg_order_value
            FROM analytics.sales_by_country
            WHERE window_start >= now() - (:minutes || ' minutes')::interval
            ORDER BY window_start DESC, total_revenue DESC
            """
        ),
        {"minutes": minutes},
    ).mappings().all()
    return {"count": len(rows), "windows": [dict(r) for r in rows]}


@router.get("/analytics/device-stats")
def device_stats(
    minutes: int = Query(60, ge=1, le=1440),
    db: Session = Depends(get_db),
):
    rows = db.execute(
        text(
            """
            SELECT window_start, window_end, device, event_count, unique_sessions
            FROM analytics.device_stats
            WHERE window_start >= now() - (:minutes || ' minutes')::interval
            ORDER BY window_start DESC
            """
        ),
        {"minutes": minutes},
    ).mappings().all()
    return {"count": len(rows), "windows": [dict(r) for r in rows]}


@router.get("/analytics/event-funnel")
def event_funnel(
    minutes: int = Query(60, ge=1, le=1440),
    db: Session = Depends(get_db),
):
    rows = db.execute(
        text(
            """
            SELECT event_type, SUM(event_count) AS event_count
            FROM analytics.event_type_counts
            WHERE window_start >= now() - (:minutes || ' minutes')::interval
            GROUP BY event_type
            ORDER BY event_count DESC
            """
        ),
        {"minutes": minutes},
    ).mappings().all()
    return {"count": len(rows), "funnel": [dict(r) for r in rows]}


@router.get("/analytics/summary")
def summary(
    minutes: int = Query(60, ge=1, le=1440),
    db: Session = Depends(get_db),
):
    totals = db.execute(
        text(
            """
            SELECT
                COALESCE(SUM(total_revenue), 0) AS total_revenue,
                COALESCE(SUM(order_count), 0) AS total_orders
            FROM analytics.sales_by_category
            WHERE window_start >= now() - (:minutes || ' minutes')::interval
            """
        ),
        {"minutes": minutes},
    ).mappings().one()

    top_category = db.execute(
        text(
            """
            SELECT category, SUM(total_revenue) AS total_revenue
            FROM analytics.sales_by_category
            WHERE window_start >= now() - (:minutes || ' minutes')::interval
            GROUP BY category
            ORDER BY total_revenue DESC
            LIMIT 1
            """
        ),
        {"minutes": minutes},
    ).mappings().first()

    top_country = db.execute(
        text(
            """
            SELECT country, SUM(total_revenue) AS total_revenue
            FROM analytics.sales_by_country
            WHERE window_start >= now() - (:minutes || ' minutes')::interval
            GROUP BY country
            ORDER BY total_revenue DESC
            LIMIT 1
            """
        ),
        {"minutes": minutes},
    ).mappings().first()

    return {
        "window_minutes": minutes,
        "total_revenue": float(totals["total_revenue"]),
        "total_orders": int(totals["total_orders"]),
        "top_category": dict(top_category) if top_category else None,
        "top_country": dict(top_country) if top_country else None,
    }
