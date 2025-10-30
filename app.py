from flask import Flask, request
import subprocess
import threading

app = Flask(__name__)

HTML = """
<!DOCTYPE html>
<html>
<head>
    <meta charset="UTF-8">
    <title>Красивый сайт</title>
    <style>
        body {
            margin: 0;
            font-family: Arial;
            background: linear-gradient(135deg, #8f00ff, #00d4ff);
            color: white;
            text-align: center;
        }
        .block {
            margin-top: 120px;
        }
        button {
            background: #ffffff22;
            border: 2px solid white;
            padding: 15px 35px;
            font-size: 22px;
            border-radius: 15px;
            color: white;
            cursor: pointer;
            transition: .3s;
        }
        button:hover {
            background: white;
            color: black;
        }
    </style>
</head>
<body>
    <div class="block">
        <h1>Привет, еблан 🤙</h1>
        <p>Нажми кнопку, и я ебану запуск твоего bot1.py</p>

        <form method="post" action="/run">
            <button type="submit">Запустить бота</button>
        </form>
    </div>
</body>
</html>
"""

def run_bot():
    subprocess.call(["python3", "bot1.py"])

@app.route("/", methods=["GET"])
def index():
    return HTML

@app.route("/run", methods=["POST"])
def run():
    thread = threading.Thread(target=run_bot)
    thread.start()
    return "<h2>✅ Бот запущен, братан!</h2>"

if __name__ == "__main__":
    app.run(host="0.0.0.0", port=5000)
