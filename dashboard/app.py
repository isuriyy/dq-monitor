"""
Phase 6 — Data Quality Monitoring Dashboard
Run: python -m streamlit run dashboard/app.py
"""
import sys, os
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

import streamlit as st
import plotly.express as px
import plotly.graph_objects as go
import pandas as pd
from datetime import datetime

from dashboard.data_loader import (
    load_snapshot_history, compute_dq_scores,
    build_null_heatmap, load_anomaly_history,
    load_alert_log, load_json_report,
)

st.set_page_config(
    page_title="DQ Monitor Dashboard",
    page_icon="🔍",
    layout="wide",
    initial_sidebar_state="expanded",
)

st.markdown("""
<style>
.metric-card{background:#1e2130;border-radius:12px;padding:20px;border:1px solid #2d3250;text-align:center}
.score-number{font-size:3rem;font-weight:700}
.status-badge{display:inline-block;padding:4px 14px;border-radius:20px;font-size:.8rem;font-weight:600;margin-top:6px}
.section-header{font-size:1.1rem;font-weight:600;color:#e0e0e0;margin:24px 0 12px;padding-bottom:6px;border-bottom:1px solid #2d3250}
</style>
""", unsafe_allow_html=True)

with st.sidebar:
    st.markdown("## 🔍 DQ Monitor")
    st.markdown("---")
    page = st.radio("Navigation", [
        "📊 Overview",
        "📈 Row Count Trends",
        "🌡️ Null % Heatmap",
        "⚠️ Anomaly Timeline",
        "🔔 Alert Log",
        "⚙️ Connections",
    ], label_visibility="collapsed")
    st.markdown("---")
    if st.button("🔄 Refresh Data"):
        st.cache_data.clear()
        st.rerun()
    st.markdown("---")
    st.markdown("**Last updated**")
    st.markdown(f"`{datetime.now().strftime('%Y-%m-%d %H:%M:%S')}`")

@st.cache_data(ttl=60)
def load_all():
    history        = load_snapshot_history()
    anomaly_report = load_json_report("anomaly_report.json")
    gx_report      = load_json_report("gx_report.json")
    dq_scores      = compute_dq_scores(history, anomaly_report, gx_report)
    heatmap_df     = build_null_heatmap(history)
    anomalies      = load_anomaly_history()
    alert_log      = load_alert_log()
    return history, anomaly_report, gx_report, dq_scores, heatmap_df, anomalies, alert_log

history, anomaly_report, gx_report, dq_scores, heatmap_df, anomalies, alert_log = load_all()

def safe_df(df):
    """Convert all columns to safe types for Arrow serialization."""
    df = df.copy()
    for col in df.columns:
        if df[col].dtype == object:
            df[col] = df[col].astype(str)
    return df

# ════════════════════════════════════════════════════════════
#  PAGE 1 — OVERVIEW
# ════════════════════════════════════════════════════════════
if page == "📊 Overview":
    st.markdown("# 📊 Data Quality Overview")
    st.markdown(f"*{datetime.now().strftime('%A, %d %B %Y — %H:%M')}*")
    st.markdown("---")

    total_anomalies = anomaly_report.get("total_anomalies", 0)
    critical        = anomaly_report.get("critical", 0)
    gx_failed       = gx_report.get("total_failed", 0)
    avg_score       = round(sum(s["score"] for s in dq_scores) / len(dq_scores), 1) if dq_scores else 0
    overall_status  = anomaly_report.get("overall_status", "CLEAN")
    total_snapshots = sum(len(df) for df in history.values())

    c1,c2,c3,c4,c5 = st.columns(5)
    c1.metric("Avg DQ Score", f"{avg_score}/100")
    c2.metric("Overall Status", overall_status)
    c3.metric("Total Anomalies", total_anomalies,
              delta=f"{critical} critical" if critical else None, delta_color="inverse")
    c4.metric("GX Checks Failed", gx_failed, delta_color="inverse")
    c5.metric("Snapshots Stored", total_snapshots)

    st.markdown("---")
    st.markdown('<div class="section-header">DQ Score per Table</div>', unsafe_allow_html=True)

    cols = st.columns(len(dq_scores)) if dq_scores else st.columns(1)
    for i, s in enumerate(dq_scores):
        with cols[i]:
            st.markdown(f"""
            <div class="metric-card">
                <div style="font-size:.9rem;color:#aaa;margin-bottom:8px">{s['table'].upper()}</div>
                <div class="score-number" style="color:{s['color']}">{s['score']}</div>
                <div style="color:#aaa;font-size:.8rem">/ 100</div>
                <div class="status-badge" style="background:{s['color']}22;color:{s['color']}">{s['status']}</div>
            </div>""", unsafe_allow_html=True)
            if s["issues"]:
                with st.expander("Issues"):
                    for issue in s["issues"]:
                        st.markdown(f"- {issue}")

    st.markdown('<div class="section-header">Score Comparison</div>', unsafe_allow_html=True)
    if dq_scores:
        df_sc = pd.DataFrame(dq_scores)
        fig = go.Figure(go.Bar(x=df_sc["table"], y=df_sc["score"],
                               marker_color=df_sc["color"],
                               text=df_sc["score"], textposition="outside"))
        fig.update_layout(yaxis=dict(range=[0,110], title="DQ Score"),
                          xaxis_title="Table",
                          plot_bgcolor="#1e2130", paper_bgcolor="#1e2130",
                          font_color="#e0e0e0", height=320, margin=dict(t=20,b=20))
        fig.add_hline(y=90, line_dash="dash", line_color="#27ae60",
                      annotation_text="Healthy (90)")
        fig.add_hline(y=70, line_dash="dash", line_color="#f39c12",
                      annotation_text="Warning (70)")
        st.plotly_chart(fig, use_container_width=True)

    if anomalies:
        st.markdown('<div class="section-header">Latest Anomalies</div>', unsafe_allow_html=True)
        df_a = pd.DataFrame(anomalies[:10])
        df_a = df_a.astype(str)
        st.dataframe(df_a[["detected_at","table","severity","metric","today","expected","score"]],
                     use_container_width=True, hide_index=True)

# ════════════════════════════════════════════════════════════
#  PAGE 2 — ROW COUNT TRENDS
# ════════════════════════════════════════════════════════════
elif page == "📈 Row Count Trends":
    st.markdown("# 📈 Row Count Trends")
    st.markdown("A sudden drop = pipeline failure. A sudden spike = duplicate data loaded.")
    st.markdown("---")

    selected = st.multiselect("Select tables", list(history.keys()), default=list(history.keys()))

    if selected:
        fig = go.Figure()
        colors = ["#3498db","#2ecc71","#e74c3c","#f39c12","#9b59b6"]
        for i, table in enumerate(selected):
            df = history[table]
            fig.add_trace(go.Scatter(x=df["profiled_at"], y=df["row_count"],
                                     name=table, mode="lines+markers",
                                     line=dict(color=colors[i%len(colors)], width=2),
                                     marker=dict(size=5),
                                     hovertemplate=f"<b>{table}</b><br>Date: %{{x}}<br>Rows: %{{y:,}}<extra></extra>"))
        fig.update_layout(title="Row Count Over Time", xaxis_title="Date", yaxis_title="Row Count",
                          plot_bgcolor="#1e2130", paper_bgcolor="#1e2130",
                          font_color="#e0e0e0", height=420, hovermode="x unified",
                          legend=dict(bgcolor="#1e2130"))
        st.plotly_chart(fig, use_container_width=True)

        st.markdown("---")
        cols = st.columns(len(selected))
        for i, table in enumerate(selected):
            df = history[table]
            latest = int(df.iloc[-1]["row_count"])
            prev   = int(df.iloc[-2]["row_count"]) if len(df)>1 else latest
            with cols[i]:
                st.metric(f"{table} — latest", f"{latest:,}", delta=latest-prev)

        st.markdown("---")
        for table in selected:
            df = history[table]
            st.markdown(f"**{table}** — full history")
            fig2 = px.area(df, x="profiled_at", y="row_count",
                           color_discrete_sequence=["#3498db"], height=180)
            fig2.update_layout(plot_bgcolor="#1e2130", paper_bgcolor="#1e2130",
                               font_color="#e0e0e0", margin=dict(t=10,b=10), showlegend=False)
            st.plotly_chart(fig2, use_container_width=True)

# ════════════════════════════════════════════════════════════
#  PAGE 3 — NULL % HEATMAP
# ════════════════════════════════════════════════════════════
elif page == "🌡️ Null % Heatmap":
    st.markdown("# 🌡️ Null % Heatmap")
    st.markdown("Dark red = high nulls. White/green = clean. Shows which columns are degrading over time.")
    st.markdown("---")

    if heatmap_df.empty:
        st.warning("No null % data yet. Run `python main.py` a few times.")
    else:
        fig = px.imshow(heatmap_df.values,
                        x=[str(c) for c in heatmap_df.columns],
                        y=heatmap_df.index.tolist(),
                        color_continuous_scale="RdYlGn_r",
                        aspect="auto", title="Null % per Column over Time",
                        labels=dict(x="Date", y="Column", color="Null %"),
                        zmin=0, zmax=100)
        fig.update_layout(plot_bgcolor="#1e2130", paper_bgcolor="#1e2130",
                          font_color="#e0e0e0",
                          height=max(300, len(heatmap_df)*30+100),
                          xaxis=dict(tickangle=-45))
        fig.update_traces(hovertemplate="<b>%{y}</b><br>Date: %{x}<br>Null %%: %{z:.1f}%%<extra></extra>")
        st.plotly_chart(fig, use_container_width=True)

        st.markdown("---")
        st.markdown("### Worst columns by average null %")
        avg_nulls = heatmap_df.mean(axis=1).sort_values(ascending=False).reset_index()
        avg_nulls.columns = ["Column","Average Null %"]
        avg_nulls["Average Null %"] = avg_nulls["Average Null %"].round(2)
        fig2 = px.bar(avg_nulls.head(15), x="Average Null %", y="Column",
                      orientation="h", color="Average Null %",
                      color_continuous_scale="RdYlGn_r", height=400)
        fig2.update_layout(plot_bgcolor="#1e2130", paper_bgcolor="#1e2130",
                           font_color="#e0e0e0", showlegend=False, margin=dict(t=10))
        st.plotly_chart(fig2, use_container_width=True)

        with st.expander("View raw null % data"):
            st.dataframe(heatmap_df.round(2), use_container_width=True)

# ════════════════════════════════════════════════════════════
#  PAGE 4 — ANOMALY TIMELINE
# ════════════════════════════════════════════════════════════
elif page == "⚠️ Anomaly Timeline":
    st.markdown("# ⚠️ Anomaly Timeline")
    st.markdown("Every anomaly detected by Z-Score, IQR, and Isolation Forest.")
    st.markdown("---")

    if not anomalies:
        st.info("No anomalies yet. Run `python run_anomaly_detection.py`.")
    else:
        df_a = pd.DataFrame(anomalies)

        c1,c2 = st.columns(2)
        with c1:
            sev_filter = st.multiselect("Filter by severity",
                                        ["CRITICAL","HIGH","MEDIUM"],
                                        default=["CRITICAL","HIGH","MEDIUM"])
        with c2:
            tbl_filter = st.multiselect("Filter by table",
                                        df_a["table"].unique().tolist(),
                                        default=df_a["table"].unique().tolist())

        filtered = df_a[df_a["severity"].isin(sev_filter) & df_a["table"].isin(tbl_filter)]

        c1,c2,c3 = st.columns(3)
        c1.metric("Critical", len(filtered[filtered["severity"]=="CRITICAL"]))
        c2.metric("High",     len(filtered[filtered["severity"]=="HIGH"]))
        c3.metric("Medium",   len(filtered[filtered["severity"]=="MEDIUM"]))

        st.markdown("---")
        c1,c2 = st.columns([1,2])
        with c1:
            sev_cnt = filtered["severity"].value_counts().reset_index()
            sev_cnt.columns = ["Severity","Count"]
            fig_pie = px.pie(sev_cnt, names="Severity", values="Count",
                             color="Severity",
                             color_discrete_map={"CRITICAL":"#c0392b","HIGH":"#e67e22","MEDIUM":"#f1c40f"},
                             hole=0.5, height=280)
            fig_pie.update_layout(paper_bgcolor="#1e2130", font_color="#e0e0e0",
                                  margin=dict(t=20,b=20))
            st.plotly_chart(fig_pie, use_container_width=True)
        with c2:
            tbl_cnt = filtered["table"].value_counts().reset_index()
            tbl_cnt.columns = ["Table","Anomaly Count"]
            fig_bar = px.bar(tbl_cnt, x="Table", y="Anomaly Count",
                             color="Anomaly Count", color_continuous_scale="Reds", height=280)
            fig_bar.update_layout(plot_bgcolor="#1e2130", paper_bgcolor="#1e2130",
                                  font_color="#e0e0e0", showlegend=False, margin=dict(t=20,b=20))
            st.plotly_chart(fig_bar, use_container_width=True)

        st.markdown("---")
        st.markdown("### All anomaly records")
        display = filtered.copy().astype(str)

        def color_sev(val):
            return {"CRITICAL":"background-color:#c0392b;color:white",
                    "HIGH":"background-color:#e67e22;color:white",
                    "MEDIUM":"background-color:#f1c40f;color:black"}.get(val,"")

        styled = display.style.map(color_sev, subset=["severity"])
        st.dataframe(styled, use_container_width=True, hide_index=True)

# ════════════════════════════════════════════════════════════
#  PAGE 5 — ALERT LOG
# ════════════════════════════════════════════════════════════
elif page == "🔔 Alert Log":
    st.markdown("# 🔔 Alert Log")
    st.markdown("Every alert sent via Slack and email.")
    st.markdown("---")

    if alert_log.empty:
        st.info("No alerts yet. Run `python run_alerting.py`.")
    else:
        c1,c2,c3 = st.columns(3)
        c1.metric("Total alerts sent", len(alert_log))
        critical_alerts = len(alert_log[alert_log["Severity"]=="CRITICAL"]) if "Severity" in alert_log else 0
        c2.metric("Critical alerts", critical_alerts)
        c3.metric("Channels used", alert_log["Channel"].nunique() if "Channel" in alert_log else 0)

        st.markdown("---")
        if "Sent at" in alert_log.columns:
            al2 = alert_log.copy()
            al2["Sent at parsed"] = pd.to_datetime(al2["Sent at"], format="ISO8601")
            al2["Date"] = al2["Sent at parsed"].dt.date
            daily = al2.groupby("Date").size().reset_index(name="Alerts")
            fig = px.bar(daily, x="Date", y="Alerts",
                         color_discrete_sequence=["#e74c3c"],
                         title="Alerts sent per day", height=250)
            fig.update_layout(plot_bgcolor="#1e2130", paper_bgcolor="#1e2130",
                              font_color="#e0e0e0", margin=dict(t=40,b=20))
            st.plotly_chart(fig, use_container_width=True)

        st.markdown("---")
        st.markdown("### Full alert history")
        def color_sev(val):
            return {"CRITICAL":"background-color:#c0392b;color:white",
                    "HIGH":"background-color:#e67e22;color:white",
                    "MEDIUM":"background-color:#f1c40f;color:black"}.get(val,"")

        cols_to_show = [c for c in ["Sent at","Channel","Severity","Summary"] if c in alert_log.columns]
        display_log  = alert_log[cols_to_show].astype(str)
        styled_log   = display_log.style.map(color_sev, subset=["Severity"] if "Severity" in cols_to_show else [])
        st.dataframe(styled_log, use_container_width=True, hide_index=True)

        if "Channel" in alert_log.columns:
            st.markdown("---")
            st.markdown("### Alerts by channel")
            ch_cnt = alert_log["Channel"].value_counts().reset_index()
            ch_cnt.columns = ["Channel","Count"]
            fig2 = px.pie(ch_cnt, names="Channel", values="Count",
                          color_discrete_sequence=["#3498db","#2ecc71"], hole=0.4, height=250)
            fig2.update_layout(paper_bgcolor="#1e2130", font_color="#e0e0e0", margin=dict(t=20,b=20))
            st.plotly_chart(fig2, use_container_width=True)

# ════════════════════════════════════════════════════════════
#  PAGE 6 — CONNECTION MANAGER
# ════════════════════════════════════════════════════════════
elif page == "⚙️ Connections":
    from dashboard.connections import render_connections_page
    render_connections_page()
