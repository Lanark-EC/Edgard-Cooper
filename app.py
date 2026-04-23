from flask import Flask, render_template, request, send_file, jsonify
import io
import traceback
from datetime import datetime

app = Flask(__name__)
app.config['MAX_CONTENT_LENGTH'] = 50 * 1024 * 1024  # 50MB max upload

# ─── Import script logic ───────────────────────────────────────
from scripts.launch_check import run_launch_check
from scripts.garvis_export import run_garvis_export

# ─── Routes ───────────────────────────────────────────────────
@app.route("/")
def index():
    return render_template("index.html")

@app.route("/run/launch-check", methods=["POST"])
def api_launch_check():
    try:
        orders_file = request.files.get("orders_file")
        launch_file = request.files.get("launch_file")

        if not orders_file or not launch_file:
            return jsonify({"error": "Upload beide bestanden."}), 400

        output_buf, stats = run_launch_check(orders_file, launch_file)

        filename = f"launch_check_{datetime.now().strftime('%Y%m%d_%H%M')}.xlsx"
        return send_file(
            output_buf,
            mimetype="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
            as_attachment=True,
            download_name=filename
        )
    except Exception as e:
        traceback.print_exc()
        return jsonify({"error": str(e)}), 500

@app.route("/run/garvis-export", methods=["POST"])
def api_garvis_export():
    try:
        garvis_file = request.files.get("garvis_file")

        if not garvis_file:
            return jsonify({"error": "Upload een Garvis export bestand."}), 400

        output_buf, stats = run_garvis_export(garvis_file)

        filename = f"GARVIS_OVERVIEW_{datetime.now().strftime('%Y%m%dT%H%M')}.xlsx"
        return send_file(
            output_buf,
            mimetype="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
            as_attachment=True,
            download_name=filename
        )
    except Exception as e:
        traceback.print_exc()
        return jsonify({"error": str(e)}), 500

if __name__ == "__main__":
    app.run(debug=True, port=5000)
