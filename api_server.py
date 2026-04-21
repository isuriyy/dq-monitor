"""
Connection API server — api_server.py
Runs alongside the web dashboard server.
Handles: test connection, save connection, list connections, remove connection.

Run in a second CMD window:
    python api_server.py

Runs on http://localhost:5050
The web dashboard calls this API when you use the Connections form.
"""

import json
import os
import yaml
from flask import Flask, request, jsonify
from flask_cors import CORS
from sqlalchemy import create_engine, inspect, text

app = Flask(__name__)
CORS(app)  # Allow web dashboard to call this API

SOURCES_PATH = "./config/sources.yaml"


def load_sources():
    if not os.path.exists(SOURCES_PATH):
        return []
    with open(SOURCES_PATH) as f:
        data = yaml.safe_load(f) or {}
    return data.get("sources", [])


def save_sources(sources):
    os.makedirs(os.path.dirname(SOURCES_PATH), exist_ok=True)
    with open(SOURCES_PATH, "w") as f:
        yaml.dump({"sources": sources}, f, default_flow_style=False)


def build_url(src):
    d = src.get("dialect", "")
    if d == "sqlite":
        return f"sqlite:///{src['path']}"
    elif d == "postgresql":
        return (f"postgresql+psycopg2://{src['user']}:{src['password']}"
                f"@{src['host']}:{src.get('port', 5432)}/{src['database']}")
    elif d == "mysql":
        return (f"mysql+pymysql://{src['user']}:{src['password']}"
                f"@{src['host']}:{src.get('port', 3306)}/{src['database']}")
    elif d == "cloud":
        return src.get("connection_string", "")
    return ""


@app.route("/api/connections", methods=["GET"])
def get_connections():
    return jsonify(load_sources())


@app.route("/api/connections/test", methods=["POST"])
def test_connection():
    src = request.json
    try:
        url = build_url(src)
        if not url:
            return jsonify({"ok": False, "message": "Could not build URL — check all fields."})
        kwargs = {}
        if src.get("dialect") != "sqlite":
            kwargs = {"connect_args": {"connect_timeout": 5}}
        engine = create_engine(url, **kwargs)
        with engine.connect() as conn:
            conn.execute(text("SELECT 1"))
        tables = inspect(engine).get_table_names()
        return jsonify({
            "ok": True,
            "message": f"Connected successfully — found {len(tables)} table(s)",
            "tables": tables
        })
    except Exception as e:
        return jsonify({"ok": False, "message": str(e)})


@app.route("/api/connections/save", methods=["POST"])
def save_connection():
    src = request.json
    if not src.get("name"):
        return jsonify({"ok": False, "message": "Connection name is required."})
    sources = load_sources()
    if src["name"] in [s["name"] for s in sources]:
        return jsonify({"ok": False, "message": f"'{src['name']}' already exists. Use a different name."})
    sources.append(src)
    save_sources(sources)
    return jsonify({"ok": True, "message": f"'{src['name']}' saved to sources.yaml"})


@app.route("/api/connections/remove", methods=["POST"])
def remove_connection():
    name = request.json.get("name")
    sources = load_sources()
    sources = [s for s in sources if s["name"] != name]
    save_sources(sources)
    return jsonify({"ok": True, "message": f"'{name}' removed."})


@app.route("/api/connections/tables", methods=["POST"])
def list_tables():
    src = request.json
    try:
        engine = create_engine(build_url(src))
        tables = inspect(engine).get_table_names()
        return jsonify({"ok": True, "tables": tables})
    except Exception as e:
        return jsonify({"ok": False, "message": str(e)})


@app.route("/api/export", methods=["POST"])
def export_data():
    try:
        import subprocess, sys
        result = subprocess.run(
            [sys.executable, "export_dashboard_data.py"],
            capture_output=True, text=True, cwd="."
        )
        if result.returncode == 0:
            return jsonify({"ok": True, "message": "Data exported successfully."})
        else:
            return jsonify({"ok": False, "message": result.stderr[-300:]})
    except Exception as e:
        return jsonify({"ok": False, "message": str(e)})




# ── Report generation endpoints ───────────────────────────────
import sys
import io
from flask import send_file, Response

def _generate_pdf_bytes():
    """Generate PDF in memory and return as bytes."""
    import tempfile, os
    # Always run from the script directory
    base_dir = os.path.dirname(os.path.abspath(__file__))
    sys.path.insert(0, base_dir)
    old_cwd = os.getcwd()
    os.chdir(base_dir)
    try:
        from generate_report import load_data, generate_pdf
        summary, dq_scores, anomalies, alert_log, charts, sources = load_data()
        with tempfile.NamedTemporaryFile(suffix='.pdf', delete=False) as tmp:
            tmp_path = tmp.name
        generate_pdf(tmp_path, summary, dq_scores, anomalies, alert_log, charts, sources)
        with open(tmp_path, 'rb') as f:
            data = f.read()
        os.unlink(tmp_path)
        return data
    finally:
        os.chdir(old_cwd)

def _generate_csv_bytes():
    """Generate CSV in memory and return as bytes."""
    import os, csv, io as _io
    base_dir = os.path.dirname(os.path.abspath(__file__))
    sys.path.insert(0, base_dir)
    old_cwd = os.getcwd()
    os.chdir(base_dir)
    try:
        from generate_report import load_data
        summary, dq_scores, anomalies, alert_log, charts, sources = load_data()
        buf = _io.StringIO()
        w = csv.writer(buf)
        w.writerow(['DQ Monitor — Anomaly Report'])
        w.writerow(['Generated', __import__('datetime').datetime.now().strftime('%Y-%m-%d %H:%M')])
        w.writerow([])
        w.writerow(['Table Health Scores'])
        w.writerow(['Table', 'Score', 'Status', 'Issues'])
        for s in dq_scores:
            w.writerow([s['table'], s['score'], s['status'], '; '.join(s.get('issues', []))])
        w.writerow([])
        w.writerow(['Anomalies'])
        w.writerow(['Detected At','Table','Severity','Metric','Detector','Today','Expected','Score','Explanation'])
        for a in anomalies:
            w.writerow([a.get('detected_at',''), a.get('table',''), a.get('severity',''),
                        a.get('metric',''), a.get('detector',''), a.get('today',''),
                        a.get('expected',''), a.get('score',''), a.get('explanation','')])
        return buf.getvalue().encode('utf-8')
    finally:
        os.chdir(old_cwd)


def _generate_excel_bytes():
    """Generate Excel in memory and return as bytes."""
    import tempfile, os
    base_dir = os.path.dirname(os.path.abspath(__file__))
    sys.path.insert(0, base_dir)
    old_cwd = os.getcwd()
    os.chdir(base_dir)
    try:
        from generate_report import load_data, generate_excel
        summary, dq_scores, anomalies, alert_log, charts, sources = load_data()
        with tempfile.NamedTemporaryFile(suffix='.xlsx', delete=False) as tmp:
            tmp_path = tmp.name
        generate_excel(tmp_path, summary, dq_scores, anomalies, alert_log, charts)
        with open(tmp_path, 'rb') as f:
            data = f.read()
        os.unlink(tmp_path)
        return data
    finally:
        os.chdir(old_cwd)


@app.route('/api/report/pdf')
def download_pdf():
    try:
        from datetime import datetime
        pdf_bytes = _generate_pdf_bytes()
        date_str  = datetime.now().strftime('%Y-%m-%d')
        return Response(
            pdf_bytes,
            mimetype='application/pdf',
            headers={'Content-Disposition': f'attachment; filename=dq_report_{date_str}.pdf'}
        )
    except Exception as e:
        return jsonify({'ok': False, 'message': str(e)}), 500


@app.route('/api/report/csv')
def download_csv():
    try:
        from datetime import datetime
        csv_bytes = _generate_csv_bytes()
        date_str  = datetime.now().strftime('%Y-%m-%d')
        return Response(
            csv_bytes,
            mimetype='text/csv',
            headers={'Content-Disposition': f'attachment; filename=dq_report_{date_str}.csv'}
        )
    except Exception as e:
        return jsonify({'ok': False, 'message': str(e)}), 500


@app.route('/api/report/excel')
def download_excel():
    try:
        from datetime import datetime
        xl_bytes = _generate_excel_bytes()
        date_str = datetime.now().strftime('%Y-%m-%d')
        return Response(
            xl_bytes,
            mimetype='application/vnd.openxmlformats-officedocument.spreadsheetml.sheet',
            headers={'Content-Disposition': f'attachment; filename=dq_report_{date_str}.xlsx'}
        )
    except Exception as e:
        return jsonify({'ok': False, 'message': str(e)}), 500



# ── AI Assistant endpoint ─────────────────────────────────────
@app.route('/api/ask', methods=['POST'])
def ask_ai():
    try:
        import sys, os
        base_dir = os.path.dirname(os.path.abspath(__file__))
        sys.path.insert(0, base_dir)
        old_cwd = os.getcwd()
        os.chdir(base_dir)
        try:
            from anomaly.ai_assistant import ask
            data     = request.json
            question = data.get('question', '').strip()
            if not question:
                return jsonify({'ok': False, 'message': 'No question provided'})
            result = ask(question)
            return jsonify({'ok': True, **result})
        finally:
            os.chdir(old_cwd)
    except Exception as e:
        return jsonify({'ok': False, 'message': str(e)}), 500



# ── AI Chat Assistant endpoint ─────────────────────────────────
@app.route('/api/chat', methods=['POST'])
def chat_endpoint():
    try:
        import os, sys
        base_dir = os.path.dirname(os.path.abspath(__file__))
        sys.path.insert(0, base_dir)
        old_cwd = os.getcwd()
        os.chdir(base_dir)
        try:
            from anomaly.llm_assistant import chat
            data    = request.json or {}
            message = data.get('message', '')
            history = data.get('history', [])
            if not message:
                return jsonify({'ok': False, 'message': 'No message provided'})
            response = chat(message, history)
            return jsonify({'ok': True, 'response': response})
        finally:
            os.chdir(old_cwd)
    except Exception as e:
        return jsonify({'ok': False, 'response': f'Error: {str(e)[:200]}'}), 500

if __name__ == "__main__":
    print("\n DQ Monitor — Connection API Server")
    print(" Running on http://localhost:5050")
    print(" Keep this window open alongside the web dashboard server.\n")
    app.run(host="0.0.0.0", port=5050, debug=False, use_reloader=False)
