from flask import Flask

app = Flask(__name__)

@app.route("/")
def home():
    return "<h1>Flask App is Running in Docker!</h1>"

if __name__ == "__main__":
    # The host '0.0.0.0' makes the server accessible from outside the container
    app.run(host="0.0.0.0", port=5000)
