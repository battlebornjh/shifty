from flask import Flask, render_template, jsonify
import json

app = Flask(__name__)

@app.route('/')
def index():
    # Sample data for the two lines
    labels = ["9/2-9/10", "9/3-9/11", "9/4-9/12", "9/5-9/13", "9/6-9/14", "9/7-9/15", "9/8-9/16", "9/9-9/17" , "9/10-9/18", "9/11-9/19"]
    data1 = [225.3, 223.45, 223.0, 232.8, 238.48, 241.51, 328.33, 307.86, 292.18, 302.14]
    data2 = [24.77, 24.61, 24.08, 24.77, 25.27, 24.9, 30.57, 29.58, 28.76, 29.34]
    
    return render_template('chart.html', labels=labels, data1=data1, data2=data2)

if __name__ == '__main__':
    app.run(debug=True)