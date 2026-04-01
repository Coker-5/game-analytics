"""
应用入口
"""

from game_analytics import create_app
from game_analytics.config import Config

app = create_app()
app.config["SERVER_NAME"] = None

if __name__ == "__main__":
    app.run(host="0.0.0.0", port=Config.FLASK_PORT, debug=False)
